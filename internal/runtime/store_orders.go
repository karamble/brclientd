// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/companyzero/bisonrelay/client/resources/simplestore"
	"github.com/companyzero/bisonrelay/zkidentity"
)

var storeUIDRE = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// storeOrderStatuses are the simplestore order states a merchant may set.
var storeOrderStatuses = map[string]bool{
	"placed":    true,
	"paid":      true,
	"shipped":   true,
	"completed": true,
	"canceled":  true,
}

func (s *storeController) ordersDir() string {
	return filepath.Join(s.storeDir, "orders")
}

// listOrders walks orders/<uid>/order-*.json across all customers and returns
// them newest first. Orders are plain JSON (simplestore's jsonfile) so we read
// them directly into the exported Order type.
func (s *storeController) listOrders() ([]*simplestore.Order, error) {
	dir := s.ordersDir()
	userDirs, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*simplestore.Order{}, nil
		}
		return nil, err
	}
	var out []*simplestore.Order
	for _, ud := range userDirs {
		if !ud.IsDir() {
			continue
		}
		udir := filepath.Join(dir, ud.Name())
		files, err := os.ReadDir(udir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasPrefix(f.Name(), "order-") || !strings.HasSuffix(f.Name(), ".json") {
				continue
			}
			o, err := readStoreOrder(filepath.Join(udir, f.Name()))
			if err != nil {
				continue
			}
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PlacedTS.After(out[j].PlacedTS) })
	return out, nil
}

func readStoreOrder(path string) (*simplestore.Order, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var o simplestore.Order
	if err := json.Unmarshal(data, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

// orderPath builds orders/<uid>/order-NNNNNNNN.json (8-digit zero-padded, the
// simplestore filename pattern).
func (s *storeController) orderPath(uidHex string, id uint64) (string, error) {
	if !storeUIDRE.MatchString(uidHex) {
		return "", fmt.Errorf("invalid uid")
	}
	return filepath.Join(s.ordersDir(), uidHex, fmt.Sprintf("order-%08d.json", id)), nil
}

func writeStoreOrder(path string, o *simplestore.Order) error {
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// dmBuyer best-effort DMs the order's buyer. Errors are ignored (the order
// file is the source of truth; a failed DM should not fail the write).
func (s *storeController) dmBuyer(uidHex, msg string) {
	var uid zkidentity.ShortID
	if err := uid.FromString(uidHex); err != nil {
		return
	}
	_ = s.client.PM(uid, msg)
}

// setOrderStatus updates one order's status in place (atomic temp+rename) and
// DMs the buyer. The store's own admin flow notifies via StatusChanged; we
// write the file directly, so we send the DM here.
func (s *storeController) setOrderStatus(uidHex string, id uint64, status string) error {
	if !storeOrderStatuses[status] {
		return fmt.Errorf("invalid status %q", status)
	}
	path, err := s.orderPath(uidHex, id)
	if err != nil {
		return err
	}
	o, err := readStoreOrder(path)
	if err != nil {
		return fmt.Errorf("read order: %w", err)
	}
	oldStatus := o.Status
	o.Status = simplestore.OrderStatus(status)
	if err := writeStoreOrder(path, o); err != nil {
		return err
	}
	s.dmBuyer(uidHex, fmt.Sprintf("Your order #%d is now %q.", id, status))
	// Auto-settlement delivers files when it marks an order paid; a manual
	// placed->paid bypasses it, so deliver here. The guard prevents resends on
	// re-saving paid or shipped/completed->paid (already delivered).
	if oldStatus == simplestore.StatusPlaced && o.Status == simplestore.StatusPaid {
		s.sendOrderFiles(o)
	}
	return nil
}

// expiredOrderMsg is the substring BR v0.2.4's buggy invoiceExpired()
// (simplestore.go:482-526) puts in the StatusChanged msg (simplestore.go:520).
// invoiceExpired is a copy of invoiceSettled and wrongly sets StatusPaid
// (simplestore.go:499), so order.Status is StatusPaid in both callbacks and the
// msg is the only signal. Reported upstream (project_br_upstream_gc_bugs_todo).
const expiredOrderMsg = "identified as expired"

// cancelIfExpired intercepts BR v0.2.4's invoiceExpired() bug. On the expiry
// anomaly it rewrites the order to canceled (simplestore has no "expired" state),
// publishes the corrected status, records an info note, and returns true so the
// caller skips the false "payment received" handling. Returns false otherwise.
func (s *storeController) cancelIfExpired(order *simplestore.Order, msg string) bool {
	if order.Status != simplestore.StatusPaid || !strings.Contains(msg, expiredOrderMsg) {
		return false
	}
	path, err := s.orderPath(order.User.String(), uint64(order.ID))
	if err == nil {
		if o, rerr := readStoreOrder(path); rerr == nil {
			o.Status = simplestore.StatusCanceled
			if werr := writeStoreOrder(path, o); werr != nil {
				s.logFn("SSTR").Warnf("expired order %d: write: %v", uint64(order.ID), werr)
			}
			order = o
		}
	}
	s.publishOrder("store-order-status", order, msg)
	s.addOrderNote("Store order expired", order)
	return true
}

// sendOrderFiles delivers an order's digital files to the buyer, mirroring
// simplestore.invoiceSettled. SendFile is synchronous and can block on large
// files, so each send runs in its own goroutine (as the lib does). The progress
// channel must be nil (a non-nil channel hangs the send in BR v0.2.4).
func (s *storeController) sendOrderFiles(o *simplestore.Order) {
	uid := o.User
	id := uint64(o.ID)
	for _, item := range o.Cart.Items {
		if item == nil || item.Product == nil || item.Product.SendFilename == "" {
			continue
		}
		fname := item.Product.SendFilename
		if !filepath.IsAbs(fname) {
			fname = filepath.Join(s.storeDir, fname)
		}
		go func(fname string) {
			if err := s.client.SendFile(uid, 0, fname, nil); err != nil {
				s.logFn("SSTR").Errorf("order %d: send file %s: %v", id, fname, err)
				if s.notes != nil {
					s.notes.add("warn", "Store file delivery failed",
						fmt.Sprintf("Order #%d: could not send %s: %v",
							id, filepath.Base(fname), err), uid.String())
				}
				return
			}
			s.logFn("SSTR").Infof("order %d: sent file %s", id, filepath.Base(fname))
		}(fname)
	}
}

// addOrderComment appends a merchant comment to an order and DMs the buyer
// (simplestore appends the comment but leaves notifying the buyer a TODO).
func (s *storeController) addOrderComment(uidHex string, id uint64, comment string) error {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return fmt.Errorf("comment is empty")
	}
	path, err := s.orderPath(uidHex, id)
	if err != nil {
		return err
	}
	o, err := readStoreOrder(path)
	if err != nil {
		return fmt.Errorf("read order: %w", err)
	}
	o.Comments = append(o.Comments, simplestore.OrderComment{
		Timestamp: time.Now(),
		FromAdmin: true,
		Comment:   comment,
	})
	if err := writeStoreOrder(path, o); err != nil {
		return err
	}
	s.dmBuyer(uidHex, fmt.Sprintf("New message about your order #%d: %s", id, comment))
	return nil
}
