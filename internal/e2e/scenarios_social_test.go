// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build brclientde2e

package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// TestGroupChat covers the GC lifecycle: create, invite, accept, messages
// in both directions, served history, and a kick observed by the kicked
// member.
func TestGroupChat(t *testing.T) {
	alice, bob := pair(t)
	bobUID := bob.ownUID()

	out := alice.mustPost("/gc/create", map[string]any{"name": "e2egc"})
	var gc struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(out, &gc); err != nil || gc.ID == "" {
		t.Fatalf("create gc: %v (%s)", err, out)
	}

	alice.mustPost("/gc/"+gc.ID+"/invite", map[string]any{"uid": bobUID})
	inv := bob.waitEvent("gc-invited", func(p map[string]any) bool {
		return payloadContains(p, gc.ID)
	}, 30*time.Second)
	iid, ok := inv["iid"].(float64)
	if !ok || iid == 0 {
		t.Fatalf("gc-invited carries no usable iid: %v", inv)
	}
	bob.mustPost("/gc/invites/accept", map[string]any{"iid": uint64(iid)})
	bob.waitEvent("gc-joined", func(p map[string]any) bool { return payloadContains(p, gc.ID) }, 30*time.Second)
	alice.waitEvent("gc-invite-accepted", func(p map[string]any) bool { return payloadContains(p, gc.ID) }, 30*time.Second)

	alice.mustPost("/gc/"+gc.ID+"/message", map[string]any{"message": "hello gc from alice", "mode": 0})
	bob.waitEvent("gc-message", func(p map[string]any) bool { return payloadContains(p, "hello gc from alice") }, 30*time.Second)
	bob.mustPost("/gc/"+gc.ID+"/message", map[string]any{"message": "hello gc from bob", "mode": 0})
	alice.waitEvent("gc-message", func(p map[string]any) bool { return payloadContains(p, "hello gc from bob") }, 30*time.Second)

	body, code := alice.get("/gc/" + gc.ID + "/history?page=0&page_size=20")
	if code != 200 {
		t.Fatalf("gc history: HTTP %d", code)
	}
	if !bytes.Contains(body, []byte("hello gc from alice")) || !bytes.Contains(body, []byte("hello gc from bob")) {
		t.Fatalf("gc history misses messages: %s", body)
	}

	alice.mustPost("/gc/"+gc.ID+"/kick", map[string]any{"uid": bobUID, "reason": "e2e kick"})
	bob.waitEvent("gc-parted", func(p map[string]any) bool {
		self, _ := p["self"].(bool)
		kicked, _ := p["kicked"].(bool)
		return self && kicked
	}, 30*time.Second)
}

// TestPostsAndComments covers the posts flow on the default auto-subscribe
// behavior: a new post reaches the KXd peer, a comment travels back, and
// both are served over the status API.
func TestPostsAndComments(t *testing.T) {
	alice, bob := pair(t)
	aliceUID := alice.ownUID()

	// KX auto-subscribes both sides; sync on bob's subscription to alice
	// having resolved before posting.
	bob.waitEvent("posts-subscribed", nil, 30*time.Second)

	out := alice.mustPost("/posts/new", map[string]any{"post": "# E2E post\n\nhello subscribers", "descr": "e2e"})
	var post struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(out, &post); err != nil || post.ID == "" {
		t.Fatalf("posts/new: %v (%s)", err, out)
	}

	bob.waitEvent("post-received", func(p map[string]any) bool { return payloadContains(p, post.ID) }, 30*time.Second)

	bob.mustPost("/posts/comment", map[string]any{"uid": aliceUID, "pid": post.ID, "comment": "nice post"})
	alice.waitEvent("post-status-received", func(p map[string]any) bool { return payloadContains(p, "nice post") }, 30*time.Second)

	body, code := alice.get("/posts/comments?uid=" + aliceUID + "&pid=" + post.ID)
	if code != 200 {
		t.Fatalf("posts/comments: HTTP %d", code)
	}
	if !bytes.Contains(body, []byte("nice post")) {
		t.Fatalf("comments listing misses the comment: %s", body)
	}
}

// TestAvatarPropagation sets an avatar and asserts the profile update
// reaches the KXd peer and reads back locally.
func TestAvatarPropagation(t *testing.T) {
	alice, bob := pair(t)
	aliceUID := alice.ownUID()

	avatar := base64.StdEncoding.EncodeToString(jpegBytes(64))
	alice.mustPost("/avatar", map[string]any{"avatar": avatar})

	bob.waitEvent("profile-updated", func(p map[string]any) bool {
		uid, _ := p["uid"].(string)
		return uid == aliceUID
	}, 30*time.Second)

	body, code := alice.get("/public-identity")
	if code != 200 {
		t.Fatalf("public-identity: HTTP %d", code)
	}
	var resp struct {
		Avatar string `json:"avatar"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Avatar != avatar {
		t.Fatalf("avatar read-back differs: got %d chars, want %d", len(resp.Avatar), len(avatar))
	}
}
