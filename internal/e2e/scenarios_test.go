// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build brclientde2e

package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"testing"
	"time"
)

// payloadContains reports whether any string anywhere in the payload
// contains substr, so assertions do not depend on exact payload shapes.
func payloadContains(node any, substr string) bool {
	switch v := node.(type) {
	case string:
		return bytes.Contains([]byte(v), []byte(substr))
	case map[string]any:
		for _, val := range v {
			if payloadContains(val, substr) {
				return true
			}
		}
	case []any:
		for _, item := range v {
			if payloadContains(item, substr) {
				return true
			}
		}
	}
	return false
}

func jpegBytes(size int) []byte {
	b := make([]byte, size)
	copy(b, []byte{0xff, 0xd8, 0xff, 0xe0})
	return b
}

// TestKXAndPM covers the basic pair: programmatic invite KX, PMs in both
// directions with delivery acks, and the served history round-trip.
func TestKXAndPM(t *testing.T) {
	alice, bob := pair(t)

	alice.mustPost("/messages/send", map[string]any{"user": "bob", "message": "hello from alice"})
	bob.waitEvent("pm", func(p map[string]any) bool { return payloadContains(p, "hello from alice") }, 30*time.Second)
	alice.waitEvent("pm-delivered", nil, 30*time.Second)

	bob.mustPost("/messages/send", map[string]any{"user": "alice", "message": "hello back"})
	alice.waitEvent("pm", func(p map[string]any) bool { return payloadContains(p, "hello back") }, 30*time.Second)

	aliceUID := alice.ownUID()
	body, code := bob.get("/history/pm?uid=" + aliceUID + "&page=0&page_size=20")
	if code != 200 {
		t.Fatalf("history: HTTP %d", code)
	}
	if !bytes.Contains(body, []byte("hello from alice")) {
		t.Fatalf("bob's history misses alice's message: %s", body)
	}
}

var embedRefRE = regexp.MustCompile(`embeds/[0-9a-f]{16}/[A-Za-z0-9._-]+`)
var embedDataRE = regexp.MustCompile(`data=([A-Za-z0-9+/=]+)`)

// TestPagesEmbedImportRemoteFetch is the full loop this harness exists for:
// a received chat embed is extracted to disk, imported into the pages tree,
// referenced by a hosted page via localfilename, and arrives inlined when
// the peer fetches the page over the relay.
func TestPagesEmbedImportRemoteFetch(t *testing.T) {
	alice, bob := pair(t)
	img := jpegBytes(512)

	msg := "picture --embed[alt=pic,type=image/jpeg,data=" + base64.StdEncoding.EncodeToString(img) + "]--"
	bob.mustPost("/messages/send", map[string]any{"user": "alice", "message": msg})
	alice.waitEvent("pm", func(p map[string]any) bool { return payloadContains(p, "picture") }, 30*time.Second)

	// The logged history carries the rewritten localfilename form.
	bobUID := bob.ownUID()
	var ref string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		body, _ := alice.get("/history/pm?uid=" + bobUID + "&page=0&page_size=10")
		if m := embedRefRE.Find(body); m != nil {
			ref = string(m)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if ref == "" {
		t.Fatal("received embed never appeared in history with a localfilename")
	}

	alice.mustPost("/pages/local/import-embed", map[string]any{
		"source": ref, "dest": "articles/img/hero.jpg",
	})
	page := "Fluxkompensator test page\n\n--embed[alt=hero,type=image/jpeg,localfilename=articles/img/hero.jpg]--\n\nbody text\n"
	alice.mustPost("/pages/local/save", map[string]any{"name": "index.md", "content": page})
	alice.mustPost("/store/mode", map[string]any{"mode": "pages"})

	aliceUID := alice.ownUID()
	out := bob.mustPost("/pages/fetch", map[string]any{"uid": aliceUID, "path": []string{"index.md"}})
	var fetched struct {
		Markdown string `json:"markdown"`
	}
	if err := json.Unmarshal(out, &fetched); err != nil {
		t.Fatalf("decode fetch reply: %v", err)
	}
	if !bytes.Contains([]byte(fetched.Markdown), []byte("body text")) {
		t.Fatalf("fetched page misses body: %q", fetched.Markdown)
	}
	m := embedDataRE.FindStringSubmatch(fetched.Markdown)
	if m == nil {
		t.Fatalf("fetched page carries no inlined embed data: %q", fetched.Markdown)
	}
	got, err := base64.StdEncoding.DecodeString(m[1])
	if err != nil {
		t.Fatalf("decode inlined embed: %v", err)
	}
	if !bytes.Equal(got, img) {
		t.Fatalf("inlined embed bytes differ: got %d bytes, want %d", len(got), len(img))
	}
}

// TestContentFilter verifies a PM content filter drops matching messages
// from both the live event feed and the served history.
func TestContentFilter(t *testing.T) {
	alice, bob := pair(t)

	alice.mustPost("/filters", map[string]any{"regexp": "forbidden"})

	bob.mustPost("/messages/send", map[string]any{"user": "alice", "message": "this is forbidden stuff"})
	bob.mustPost("/messages/send", map[string]any{"user": "alice", "message": "clean message"})

	alice.waitEvent("pm", func(p map[string]any) bool {
		if payloadContains(p, "forbidden") {
			t.Fatal("filtered message leaked into the event feed")
		}
		return payloadContains(p, "clean message")
	}, 30*time.Second)
	alice.assertNoEvent("pm", func(p map[string]any) bool { return payloadContains(p, "forbidden") }, 2*time.Second)

	bobUID := bob.ownUID()
	body, _ := alice.get("/history/pm?uid=" + bobUID + "&page=0&page_size=20")
	if bytes.Contains(body, []byte("forbidden")) {
		t.Fatalf("filtered message served in history: %s", body)
	}
	if !bytes.Contains(body, []byte("clean message")) {
		t.Fatalf("clean message missing from history: %s", body)
	}
}

// TestFileSend pushes a file to a peer and asserts the download completes
// and lands in the receiver's downloads list.
func TestFileSend(t *testing.T) {
	alice, bob := pair(t)

	content := []byte("brclientd e2e file payload\n")
	bob.sendFile("alice", "hello.txt", content)

	alice.waitEvent("file-download-completed", nil, 90*time.Second)
	body, code := alice.get("/downloads")
	if code != 200 {
		t.Fatalf("downloads: HTTP %d", code)
	}
	if !bytes.Contains(body, []byte("hello.txt")) {
		t.Fatalf("downloads list misses the file: %s", body)
	}
}

// TestTipFreeScheme pins the free-scheme tip semantics: the payee's free
// invoice always carries zero milliatoms, so a real-amount tip terminates
// with the library's amount-mismatch failure on the payer while the payee
// records a zero-DCR receipt. Real tips need the dcrlnd scheme; what this
// asserts is that the flow terminates cleanly and both event surfaces fire.
func TestTipFreeScheme(t *testing.T) {
	alice, bob := pair(t)

	alice.mustPost("/tip", map[string]any{"user": "bob", "dcrAmount": 0.001, "maxAttempts": 1})
	payload := alice.waitEvent("tip-failed", func(p map[string]any) bool {
		return payloadContains(p, "milliatoms requested in invoice")
	}, 60*time.Second)
	if !payloadContains(payload, "bob") {
		t.Fatalf("tip-failed payload misses the recipient: %v", payload)
	}
	bob.waitEvent("tip-received", nil, 30*time.Second)
}
