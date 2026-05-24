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

	"github.com/companyzero/bisonrelay/client/resources/simplestore"
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

// setOrderStatus updates one order's status in place (atomic temp+rename). Note
// this does not push a Bison Relay DM to the customer the way the store's own
// admin flow does; the customer sees the new status when they next view the
// order.
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
	o.Status = simplestore.OrderStatus(status)
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
