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
	StageWaitingForDcrlnd  Stage = "waiting-for-dcrlnd"
	StageWaitingForChannel Stage = "waiting-for-channel"
	StageNeedsIdentity     Stage = "needs-identity"
	StageStarting          Stage = "starting"
	StageWalletChecking    Stage = "wallet-checking"
	StageConnecting        Stage = "connecting"
	StageReady             Stage = "ready"
	StageDisconnected      Stage = "disconnected"
)

// Status is the on-the-wire JSON shape returned by /status.
type Status struct {
	Stage           Stage     `json:"stage"`
	Nick            string    `json:"nick,omitempty"`
	ServerNode      string    `json:"serverNode,omitempty"`
	RecommendedPeer string    `json:"recommendedPeer,omitempty"`
	WalletCheckErr  string    `json:"walletCheckErr,omitempty"`
	LastUpdated     time.Time `json:"lastUpdated"`
	// ConnectedAt is the timestamp of the last false->true transition into
	// StageReady. Zero while disconnected. The Stats / Network view uses it
	// to render a "connected since X" counter.
	ConnectedAt time.Time `json:"connectedAt,omitempty"`
}

// Tracker holds the current Status with mutex protection and serves both
// the BR notification callbacks (writers) and the /status HTTP handler
// (reader).
type Tracker struct {
	mu     sync.RWMutex
	status Status
	log    slog.Logger
}

// NewTracker returns a Tracker initialised with stage=waiting-for-dcrlnd.
// The dcrlnd unlock gate runs before BR client.Run.
func NewTracker(log slog.Logger) *Tracker {
	return &Tracker{
		status: Status{Stage: StageWaitingForDcrlnd, LastUpdated: time.Now()},
		log:    log,
	}
}

// SetDcrlndWaiting records that the daemon is still waiting for dcrlnd to be
// unlocked. The supplied reason (typically the latest GetInfo error message)
// goes into WalletCheckErr so the dashboard can render the underlying cause.
func (t *Tracker) SetDcrlndWaiting(reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Stage = StageWaitingForDcrlnd
	t.status.WalletCheckErr = reason
	t.status.LastUpdated = time.Now()
}

// MarkDcrlndReady advances past the dcrlnd gate. Called once GetInfo succeeds.
func (t *Tracker) MarkDcrlndReady() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Stage = StageStarting
	t.status.WalletCheckErr = ""
	t.status.LastUpdated = time.Now()
}

// SetRecommendedPeer publishes the BR-recommended hub URI so the dashboard
// can render an "open channel to X" wizard step.
func (t *Tracker) SetRecommendedPeer(uri string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.RecommendedPeer = uri
	t.status.LastUpdated = time.Now()
}

// SetChannelWaiting moves the daemon into waiting-for-channel and records the
// most recent reason (e.g. "no active channel to hub").
func (t *Tracker) SetChannelWaiting(reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Stage = StageWaitingForChannel
	t.status.WalletCheckErr = reason
	t.status.LastUpdated = time.Now()
}

// MarkChannelReady clears the channel gate.
func (t *Tracker) MarkChannelReady() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Stage = StageStarting
	t.status.WalletCheckErr = ""
	t.status.LastUpdated = time.Now()
}

// SetNeedsIdentity marks the runtime as blocked on the pre-setup endpoint
// receiving CreateIdentity. The dashboard wizard pivots to its nick/name
// form when it sees this stage.
func (t *Tracker) SetNeedsIdentity() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Stage = StageNeedsIdentity
	t.status.WalletCheckErr = ""
	t.status.LastUpdated = time.Now()
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
		if t.status.ConnectedAt.IsZero() {
			t.status.ConnectedAt = time.Now()
		}
	} else {
		t.status.Stage = StageDisconnected
		t.status.ConnectedAt = time.Time{}
	}
	t.status.LastUpdated = time.Now()
	t.log.Debugf("status: connected=%v stage=%s", connected, t.status.Stage)
}
