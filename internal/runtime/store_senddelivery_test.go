// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

// TestResolveSendFile verifies the digital-download delivery guard: a product's
// SendFilename must name a regular file inside the store dir. It is the check
// that stops a poisoned product from shipping arbitrary host files (dcrlnd's
// admin.macaroon, the wallet db) to whoever placed the order.
func TestResolveSendFile(t *testing.T) {
	storeDir := t.TempDir()
	outside := t.TempDir()

	// A legitimate digital good, and one in a subdirectory.
	if err := os.WriteFile(filepath.Join(storeDir, "manual.pdf"), []byte("pdf"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(storeDir, "goods", "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storeDir, "goods", "sub", "book.epub"), []byte("epub"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The secret the attack aims at, and a symlink inside the store dir that
	// points at it: the lexical gate alone cannot see this one.
	secret := filepath.Join(outside, "admin.macaroon")
	if err := os.WriteFile(secret, []byte("macaroon"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(storeDir, "leak.pdf")); err != nil {
		t.Skipf("symlinks unsupported here: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(storeDir, "escape")); err != nil {
		t.Fatal(err)
	}
	// Real files that ONLY the lexical media gate refuses: os.Root would happily
	// resolve both, so these are what make validateStoreMediaRel load-bearing.
	if err := os.MkdirAll(filepath.Join(storeDir, "orders"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storeDir, "orders", "order-1.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storeDir, "index.tmpl"), []byte("tmpl"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := &storeController{storeDir: storeDir}

	t.Run("files inside the store dir resolve", func(t *testing.T) {
		for _, rel := range []string{"manual.pdf", "goods/sub/book.epub"} {
			got, err := s.resolveSendFile(rel)
			if err != nil {
				t.Fatalf("resolveSendFile(%q) unexpected error: %v", rel, err)
			}
			if want := filepath.Join(storeDir, rel); got != want {
				t.Fatalf("resolveSendFile(%q) = %q, want %q", rel, got, want)
			}
		}
	})

	t.Run("escapes are refused", func(t *testing.T) {
		bad := []string{
			"",            // empty
			secret,        // absolute path to the real target
			"/etc/passwd", // absolute
			"../" + filepath.Base(outside) + "/admin.macaroon", // parent escape
			"goods/../../etc/passwd",                           // traversal via segments
			"leak.pdf",                                         // symlink inside the store dir aimed outside
			"escape/admin.macaroon",                            // via a symlinked directory
			"goods",                                            // a directory, not a file
			"orders/order-1.json",                              // operational dir, refused by the media gate
			"index.tmpl",                                       // template, refused by the media gate
			"missing.pdf",                                      // does not exist
		}
		for _, rel := range bad {
			if got, err := s.resolveSendFile(rel); err == nil {
				t.Errorf("resolveSendFile(%q) = %q, want error", rel, got)
			}
		}
	})
}
