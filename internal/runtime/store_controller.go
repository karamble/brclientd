// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/client/resources"
	"github.com/companyzero/bisonrelay/client/resources/simplestore"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/decred/slog"
)

// switchableProvider is a resources.Provider whose delegate can be swapped at
// runtime. BR binds one provider at the resource root; this lets a node flip
// between hosting static pages and a simplestore without restarting the BR
// client (the store controller swaps the delegate).
type switchableProvider struct {
	mu     sync.RWMutex
	active resources.Provider
}

func (s *switchableProvider) Fulfill(ctx context.Context, uid clientintf.UserID,
	req *rpc.RMFetchResource) (*rpc.RMFetchResourceReply, error) {

	s.mu.RLock()
	p := s.active
	s.mu.RUnlock()
	if p == nil {
		// No delegate means hosting is deactivated. Return ErrProviderNotFound
		// (not a NotFound reply) so client.handleFetchResource sends nothing
		// back to the peer, matching upstream's "no resources.upstream" node. A
		// NotFound reply would still be delivered and render as an empty page.
		return nil, resources.ErrProviderNotFound
	}
	return p.Fulfill(ctx, uid, req)
}

func (s *switchableProvider) set(p resources.Provider) {
	s.mu.Lock()
	s.active = p
	s.mu.Unlock()
}

// Resource-hosting modes. A node serves one at a time (BR binds a single
// resource provider at the root). hostModeOff binds no delegate so the node
// serves nothing over the relay, mirroring upstream BR's empty resources.upstream.
const (
	hostModeOff   = "off"
	hostModePages = "pages"
	hostModeStore = "store"
)

// storeMode is the persisted hosting choice. PayType/Account/ShipCharge are
// retained across off/pages so switching back to the store remembers them.
type storeMode struct {
	Mode       string  `json:"mode"`
	PayType    string  `json:"pay_type"`
	Account    string  `json:"account"`
	ShipCharge float64 `json:"ship_charge"`
}

// storeController owns the resource-hosting mode for this node. It binds the
// switchableProvider to either a filesystem pages resource or a running
// simplestore, persists the choice, and can flip between them at runtime.
type storeController struct {
	prov     *switchableProvider
	client   *client.Client
	lnPay    *client.DcrlnPaymentClient
	notifs   *notifBus
	notes    *notificationStore
	logFn    func(subsys string) slog.Logger
	pagesDir string
	storeDir string
	modeFile string
	rootCtx  context.Context

	mu          sync.Mutex
	mode        storeMode
	store       *simplestore.Store
	storeCancel context.CancelFunc
}

// newStoreController builds the controller, preferring a previously-persisted
// mode over the passed config default. applyInitial must be called once to bind
// the provider.
func newStoreController(rootCtx context.Context, prov *switchableProvider, c *client.Client,
	lnPay *client.DcrlnPaymentClient, notifs *notifBus, notes *notificationStore,
	logFn func(string) slog.Logger,
	pagesDir, storeDir string, def storeMode) *storeController {

	ctrl := &storeController{
		prov:     prov,
		client:   c,
		lnPay:    lnPay,
		notifs:   notifs,
		notes:    notes,
		logFn:    logFn,
		pagesDir: pagesDir,
		storeDir: storeDir,
		modeFile: filepath.Join(filepath.Dir(storeDir), "store-mode.json"),
		rootCtx:  rootCtx,
		mode:     def,
	}
	if m, ok := ctrl.loadMode(); ok {
		ctrl.mode = m
	}
	return ctrl
}

// applyInitial binds the provider for the current mode (and starts the store if
// in store mode). Called once after the BR client exists.
func (s *storeController) applyInitial() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyModeLocked()
}

// applyModeLocked binds the resource provider for s.mode. An unknown/empty mode
// is treated as off so a node never serves something it was not configured to.
func (s *storeController) applyModeLocked() error {
	switch s.mode.Mode {
	case hostModeStore:
		return s.enableStoreLocked()
	case hostModePages:
		return s.enablePagesLocked()
	default:
		return s.enableOffLocked()
	}
}

// Mode returns the current hosting mode.
func (s *storeController) Mode() storeMode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

// SetMode switches hosting at runtime and persists the choice. Disabling the
// store cancels its invoice watcher, so orders awaiting payment will not
// auto-settle until the store is re-enabled.
func (s *storeController) SetMode(m storeMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopStoreLocked()
	s.mode = m
	if err := s.applyModeLocked(); err != nil {
		return err
	}
	return s.saveMode()
}

// enableOffLocked deactivates hosting: the switchableProvider's delegate is
// cleared, so any remote FetchResource hits the nil-delegate path and gets
// ResourceStatusNotFound regardless of files on disk.
func (s *storeController) enableOffLocked() error {
	s.prov.set(nil)
	return nil
}

func (s *storeController) enablePagesLocked() error {
	if s.pagesDir == "" {
		s.prov.set(nil)
		return nil
	}
	if err := ensurePagesDir(s.pagesDir); err != nil {
		return fmt.Errorf("prepare pages dir: %w", err)
	}
	s.prov.set(resources.NewFilesystemResource(s.pagesDir, s.logFn("PAGE")))
	return nil
}

func (s *storeController) enableStoreLocked() error {
	// WriteTemplate returns nil when it seeds a fresh (empty) store dir and
	// os.ErrExist when a store already exists. On a fresh seed, overlay the
	// dcrpulse themed demo (replacing the bare simplestore sample); never touch
	// an existing store.
	werr := simplestore.WriteTemplate(s.storeDir)
	if werr != nil && !errors.Is(werr, os.ErrExist) {
		return fmt.Errorf("write simplestore template: %w", werr)
	}
	if werr == nil {
		if err := seedDemoStore(s.storeDir); err != nil {
			return fmt.Errorf("seed demo store: %w", err)
		}
	}
	payType := s.mode.PayType
	if payType == "" {
		payType = "ln"
	}
	st, err := simplestore.New(simplestore.Config{
		Root:        s.storeDir,
		Log:         s.logFn("SSTR"),
		LiveReload:  true,
		Client:      s.client,
		PayType:     simplestore.PayType(payType),
		Account:     s.mode.Account,
		ShipCharge:  s.mode.ShipCharge,
		LNPayClient: s.lnPay,
		ExchangeRateProvider: func() float64 {
			dcrPrice, _ := s.client.Rates().Get()
			return dcrPrice
		},
		OrderPlaced: func(order *simplestore.Order, msg string) {
			s.publishOrder("store-order-placed", order, msg)
			s.addOrderNote("New store order", order)
		},
		StatusChanged: func(order *simplestore.Order, msg string) {
			s.publishOrder("store-order-status", order, msg)
			if order.Status == simplestore.StatusPaid {
				s.addOrderNote("Store payment received", order)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("init simplestore: %w", err)
	}
	storeCtx, cancel := context.WithCancel(s.rootCtx)
	s.store = st
	s.storeCancel = cancel
	go func() {
		if err := st.Run(storeCtx); err != nil && !errors.Is(err, context.Canceled) {
			s.logFn("SSTR").Errorf("simplestore.Run exited: %v", err)
		}
	}()
	s.prov.set(st)
	return nil
}

func (s *storeController) stopStoreLocked() {
	if s.storeCancel != nil {
		s.storeCancel()
		s.storeCancel = nil
	}
	s.store = nil
}

func (s *storeController) publishOrder(typ string, order *simplestore.Order, msg string) {
	s.notifs.Publish(NotifEvent{
		Type: typ,
		Payload: map[string]any{
			"order_id": uint64(order.ID),
			"user":     order.User.String(),
			"status":   string(order.Status),
			"msg":      msg,
		},
	})
}

// addOrderNote records a persisted bell note for a store order event (a new
// order or a settled payment) so the dashboard's notification bell surfaces it.
func (s *storeController) addOrderNote(subject string, order *simplestore.Order) {
	if s.notes == nil {
		return
	}
	buyer := order.User.String()
	if nick, err := s.client.UserNick(order.User); err == nil && nick != "" {
		buyer = nick
	}
	detail := fmt.Sprintf("Order #%d for $%.2f from %s", uint64(order.ID), order.Total(), buyer)
	s.notes.add("info", subject, detail, order.User.String())
}

func (s *storeController) loadMode() (storeMode, bool) {
	data, err := os.ReadFile(s.modeFile)
	if err != nil {
		return storeMode{}, false
	}
	var m storeMode
	if err := json.Unmarshal(data, &m); err != nil {
		return storeMode{}, false
	}
	if m.Mode == "" {
		m.Mode = hostModeOff
	}
	return m, true
}

func (s *storeController) saveMode() error {
	data, err := json.Marshal(s.mode)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.modeFile), 0o700); err != nil {
		return err
	}
	return os.WriteFile(s.modeFile, data, 0o600)
}
