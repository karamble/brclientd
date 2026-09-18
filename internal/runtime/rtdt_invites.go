// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"sort"
	"sync"
)

// RTDTInvite is one call invitation that has not been answered yet.
type RTDTInvite struct {
	SessRV      string `json:"sess_rv"`
	Inviter     string `json:"inviter"`
	InviterNick string `json:"inviter_nick"`
	Size        uint32 `json:"size"`
	Description string `json:"description"`
	AsPublisher bool   `json:"as_publisher"`
	PeerID      uint32 `json:"peer_id"`
	IsInstant   bool   `json:"is_instant"`
	ReceivedMs  int64  `json:"received_ms"`
}

// RTDTInviteStore keeps the call invitations that have arrived and not yet been
// answered. Bison Relay writes each invite to disk but exposes no way to list
// them, so a browser that was closed when a call came in could never find out
// about it. This is the enumerable copy the dashboard reads when it loads.
type RTDTInviteStore struct {
	mu      sync.RWMutex
	invites map[string]RTDTInvite
}

// NewRTDTInviteStore returns an empty store. Construct one at startup and share
// it between the BR client, which fills it, and the status server, which serves
// and clears it.
func NewRTDTInviteStore() *RTDTInviteStore {
	return &RTDTInviteStore{invites: make(map[string]RTDTInvite)}
}

// Add records an invitation, replacing any earlier one for the same session so
// a re-sent invite does not show up twice.
func (s *RTDTInviteStore) Add(inv RTDTInvite) {
	if s == nil || inv.SessRV == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invites[inv.SessRV] = inv
}

// Remove drops an invitation once it has been accepted, canceled, or the
// session it points at is gone. Removing one that was never there is fine.
func (s *RTDTInviteStore) Remove(sessRV string) {
	if s == nil || sessRV == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.invites, sessRV)
}

// List returns the outstanding invitations, newest first.
func (s *RTDTInviteStore) List() []RTDTInvite {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]RTDTInvite, 0, len(s.invites))
	for _, inv := range s.invites {
		out = append(out, inv)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].ReceivedMs != out[j].ReceivedMs {
			return out[i].ReceivedMs > out[j].ReceivedMs
		}
		return out[i].SessRV < out[j].SessRV
	})
	return out
}
