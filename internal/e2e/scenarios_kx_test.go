// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build brclientde2e

package e2e

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// TestSuggestKXThreeWay covers the mediated key exchange: bob knows both
// alice and charlie, suggests they KX, alice accepts the suggestion, and
// the two strangers end up connected through the mediator. This is the
// same path a directory's introduce flow rides.
func TestSuggestKXThreeWay(t *testing.T) {
	alice, bob, charlie := trio(t)
	bobUID := bob.ownUID()
	charlieUID := charlie.ownUID()

	bob.mustPost("/contacts/suggest-kx", map[string]any{"invitee": alice.ownUID(), "target": charlieUID})

	sugg := alice.waitEvent("kx-suggested", nil, 30*time.Second)
	// The payload field named "invitee" carries the MEDIATOR's uid (the
	// upstream callback parameter is misnamed); pin that contract here.
	mediator, _ := sugg["invitee"].(string)
	target, _ := sugg["target"].(string)
	if mediator != bobUID {
		t.Fatalf("kx-suggested mediator = %q, want bob %q", mediator, bobUID)
	}
	if target != charlieUID {
		t.Fatalf("kx-suggested target = %q, want charlie %q", target, charlieUID)
	}

	alice.mustPost("/contacts/accept-suggestion", map[string]any{"mediator": mediator, "target": target})
	waitContact(t, alice, charlie)
	waitContact(t, charlie, alice)
}

// TestKXReset resets the ratchet with a live peer and asserts the pair can
// exchange PMs on the re-established ratchet. Completion has no dedicated
// event; the reset entry clearing from /kx/list plus flowing PMs is the
// observable contract.
func TestKXReset(t *testing.T) {
	alice, bob := pair(t)
	bobUID := bob.ownUID()

	alice.mustPost("/contacts/kx-reset", map[string]any{"uid": bobUID})

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		body, code := alice.get("/kx/list")
		if code == 200 && !strings.Contains(string(body), `"is_for_reset":true`) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	alice.mustPost("/messages/send", map[string]any{"user": "bob", "message": "post-reset ping"})
	bob.waitEvent("pm", func(p map[string]any) bool { return payloadContains(p, "post-reset ping") }, 45*time.Second)
	bob.mustPost("/messages/send", map[string]any{"user": "alice", "message": "post-reset pong"})
	alice.waitEvent("pm", func(p map[string]any) bool { return payloadContains(p, "post-reset pong") }, 45*time.Second)
}

// TestOfflineDeliveryAndRestart is the daemon-persistence scenario the
// upstream suite cannot express: stop a daemon, let a peer message it
// while it is down, boot it again on the same data dir, and assert the
// queued message arrives, history persisted across the restart, and the
// ratchet still works.
func TestOfflineDeliveryAndRestart(t *testing.T) {
	alice, bob := pair(t)
	bobUID := bob.ownUID()

	bob.mustPost("/messages/send", map[string]any{"user": "alice", "message": "before the restart"})
	alice.waitEvent("pm", func(p map[string]any) bool { return payloadContains(p, "before the restart") }, 30*time.Second)

	alice.stop()
	bob.mustPost("/messages/send", map[string]any{"user": "alice", "message": "sent while offline"})
	alice.restart()

	alice.waitEvent("pm", func(p map[string]any) bool { return payloadContains(p, "sent while offline") }, 60*time.Second)

	body, code := alice.get("/history/pm?uid=" + bobUID + "&page=0&page_size=20")
	if code != 200 {
		t.Fatalf("history after restart: HTTP %d", code)
	}
	if !bytes.Contains(body, []byte("before the restart")) {
		t.Fatalf("pre-restart history lost: %s", body)
	}
	if !bytes.Contains(body, []byte("sent while offline")) {
		t.Fatalf("offline message missing from history: %s", body)
	}

	alice.mustPost("/messages/send", map[string]any{"user": "bob", "message": "back online"})
	bob.waitEvent("pm", func(p map[string]any) bool { return payloadContains(p, "back online") }, 30*time.Second)
}
