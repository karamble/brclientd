// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import "testing"

func inv(rv string, receivedMs int64) RTDTInvite {
	return RTDTInvite{SessRV: rv, Inviter: "u-" + rv, ReceivedMs: receivedMs}
}

// The whole point of the store is that a call which arrived while no browser
// was open is still there to answer when one opens.
func TestInviteStoreKeepsAnInviteUntilItIsAnswered(t *testing.T) {
	s := NewRTDTInviteStore()
	s.Add(inv("aa", 100))

	if got := len(s.List()); got != 1 {
		t.Fatalf("listed %d invites, want the one that arrived", got)
	}

	s.Remove("aa")
	if got := s.List(); len(got) != 0 {
		t.Errorf("an answered invite is still offered: %+v", got)
	}
}

// A re-sent invite must not stack a second call for the same session.
func TestInviteStoreCollapsesARepeatedInvite(t *testing.T) {
	s := NewRTDTInviteStore()
	s.Add(inv("aa", 100))
	s.Add(RTDTInvite{SessRV: "aa", Inviter: "u-aa", InviterNick: "later", ReceivedMs: 200})

	got := s.List()
	if len(got) != 1 {
		t.Fatalf("listed %d invites, want 1 for a single session", len(got))
	}
	if got[0].InviterNick != "later" {
		t.Errorf("kept the stale copy (nick %q), want the most recent invite", got[0].InviterNick)
	}
}

// The newest call belongs at the top; that is the one the user is being rung
// for right now.
func TestInviteStoreListsNewestFirst(t *testing.T) {
	s := NewRTDTInviteStore()
	s.Add(inv("old", 100))
	s.Add(inv("new", 300))
	s.Add(inv("mid", 200))

	got := s.List()
	if len(got) != 3 {
		t.Fatalf("listed %d invites, want 3", len(got))
	}
	for i, want := range []string{"new", "mid", "old"} {
		if got[i].SessRV != want {
			t.Errorf("position %d is %q, want %q (newest first)", i, got[i].SessRV, want)
		}
	}
}

// The BR client calls into this before the store exists in some start-up
// orderings, and a dropped call is better than a crashed daemon.
func TestInviteStoreToleratesBeingAbsent(t *testing.T) {
	var s *RTDTInviteStore
	s.Add(inv("aa", 100))
	s.Remove("aa")
	if got := s.List(); got != nil {
		t.Errorf("a nil store listed %+v, want nothing", got)
	}
}
