// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"sync"
	"time"
)

// gcBlockedReinvite records an incoming GC invite the BR client rejected
// because a local copy of the GC already exists (client_groupchat.go
// handleGCInvite drops such invites without storing them). Typical after
// restoring a backup whose GC membership the network has since moved past:
// the admin re-invites, the stale local copy blocks it, and without this
// record the only trace is a log line.
type gcBlockedReinvite struct {
	GCID        string    `json:"gcid"`
	Name        string    `json:"name"`
	From        string    `json:"from"`
	FromNick    string    `json:"fromNick"`
	Count       int       `json:"count"`
	LastAttempt time.Time `json:"lastAttempt"`
}

// gcReinviteTracker keeps blocked re-invites in memory so the status server
// can report them and the dashboard can offer recovery (leave the stale
// copy, then request a fresh invite). Not persisted: the sender's client
// retries delivery, so entries regenerate after a restart.
type gcReinviteTracker struct {
	mu sync.Mutex
	m  map[string]gcBlockedReinvite
}

func newGCReinviteTracker() *gcReinviteTracker {
	return &gcReinviteTracker{m: make(map[string]gcBlockedReinvite)}
}

// Record notes a blocked re-invite attempt and returns the updated entry.
func (t *gcReinviteTracker) Record(gcid, name, from, fromNick string) gcBlockedReinvite {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.m[gcid]
	e.GCID = gcid
	e.Name = name
	e.From = from
	e.FromNick = fromNick
	e.Count++
	e.LastAttempt = time.Now()
	t.m[gcid] = e
	return e
}

func (t *gcReinviteTracker) List() []gcBlockedReinvite {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]gcBlockedReinvite, 0, len(t.m))
	for _, e := range t.m {
		out = append(out, e)
	}
	return out
}

func (t *gcReinviteTracker) Clear(gcid string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, gcid)
}
