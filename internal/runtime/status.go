// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"sync"
	"time"

	"github.com/decred/slog"
)

// Stage describes brclientd's current runtime stage from a consumer's point
// of view. It is derived from BR's notifications and wallet-usability checks.
type Stage string

const (
	StageStarting       Stage = "starting"
	StageWalletChecking Stage = "wallet-checking"
	StageConnecting     Stage = "connecting"
	StageReady          Stage = "ready"
	StageDisconnected   Stage = "disconnected"
)

// Status is the on-the-wire JSON shape returned by /status.
type Status struct {
	Stage          Stage     `json:"stage"`
	Nick           string    `json:"nick,omitempty"`
	ServerNode     string    `json:"serverNode,omitempty"`
	WalletCheckErr string    `json:"walletCheckErr,omitempty"`
	LastUpdated    time.Time `json:"lastUpdated"`
}

// Tracker holds the current Status with mutex protection and serves both
// the BR notification callbacks (writers) and the /status HTTP handler
// (reader).
type Tracker struct {
	mu     sync.RWMutex
	status Status
	log    slog.Logger
}

// NewTracker returns a Tracker initialised with stage=starting.
func NewTracker(log slog.Logger) *Tracker {
	return &Tracker{
		status: Status{Stage: StageStarting, LastUpdated: time.Now()},
		log:    log,
	}
}

// Get returns the current snapshot.
func (t *Tracker) Get() Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}

// SetNick updates the identity nick once and never changes again.
func (t *Tracker) SetNick(nick string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status.Nick == "" {
		t.status.Nick = nick
	}
}

// SetServerNode records the BR server's LN node pubkey for display.
func (t *Tracker) SetServerNode(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.ServerNode = node
	t.status.LastUpdated = time.Now()
}

// SetWalletErr stores the latest CheckLNWalletUsable error and pushes the
// stage to wallet-checking.
func (t *Tracker) SetWalletErr(msg string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Stage = StageWalletChecking
	t.status.WalletCheckErr = msg
	t.status.LastUpdated = time.Now()
	t.log.Debugf("status: wallet-checking (%s)", msg)
}

// ClearWalletErr drops a previous wallet error and bumps the stage to
// connecting. Used right before the BR client establishes the server session.
func (t *Tracker) ClearWalletErr() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.WalletCheckErr = ""
	if t.status.Stage == StageWalletChecking {
		t.status.Stage = StageConnecting
	}
	t.status.LastUpdated = time.Now()
}

// SetConnected pushes the stage to ready or disconnected. Triggered from
// OnServerSessionChangedNtfn.
func (t *Tracker) SetConnected(connected bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if connected {
		t.status.Stage = StageReady
		t.status.WalletCheckErr = ""
	} else {
		t.status.Stage = StageDisconnected
	}
	t.status.LastUpdated = time.Now()
	t.log.Debugf("status: connected=%v stage=%s", connected, t.status.Stage)
}
