// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/companyzero/bisonrelay/client/resources"
)

func TestTemplateHasUnsafeEmbed(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{name: "absolute path", content: `--embed[localfilename=/etc/passwd]--`, want: true},
		{name: "parent traversal", content: `--embed[localfilename=../../etc/passwd]--`, want: true},
		{name: "bracket in a later arg hides the embed from a naive scan",
			content: `--embed[localfilename=/etc/passwd,x=]y]--`, want: true},
		{name: "traversal behind a bracket", content: `--embed[localfilename=../x,x=]y]--`, want: true},
		{name: "traversal that a bare-bracket cutoff would hide",
			content: `--embed[localfilename=a]/../../../etc/passwd]--`, want: true},
		{name: "unsafe embed after a safe one on the same line",
			content: `--embed[localfilename=ok.png]-- --embed[localfilename=/etc/passwd]--`, want: true},
		{name: "backslash traversal", content: `--embed[localfilename=..\..\secret]--`, want: true},
		{name: "unterminated embed is still filtered",
			content: `--embed[localfilename=/etc/passwd`, want: true},
		{name: "localfilename outside any embed is still filtered",
			content: `see localfilename=/etc/passwd here`, want: true},
		{name: "a longer key still contains the argument", content: `--embed[xlocalfilename=/etc/passwd]--`, want: true},
		{name: "empty value", content: `--embed[localfilename=]--`, want: true},
		{name: "space in the value", content: `--embed[localfilename=a b.png]--`, want: true},
		{name: "dot component", content: `--embed[localfilename=./img.png]--`, want: true},
		{name: "equals is not in the allowed set", content: `--embed[localfilename=a=b.png]--`, want: true},
		{name: "relative path is allowed", content: `--embed[localfilename=img.png]--`},
		{name: "relative subdirectory is allowed", content: `--embed[localfilename=sub/img.png]--`},
		{name: "dots inside a name are allowed", content: `--embed[localfilename=a.b.png]--`},
		{name: "an imported asset name is allowed", content: `--embed[localfilename=assets/pic-1_v2.jpeg]--`},
		{name: "other args are ignored", content: `--embed[type=image/png,alt=hi,localfilename=img.png]--`},
		{name: "embed with no localfilename", content: `--embed[type=image/png,data=AAAA]--`},
		{name: "no embeds at all", content: "# a page\n\nsome text\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := templateHasUnsafeEmbed(tc.content); got != tc.want {
				t.Fatalf("templateHasUnsafeEmbed(%q) = %v, want %v", tc.content, got, tc.want)
			}
		})
	}
}

// TestEmbedGateMatchesRenderer asserts the property the gate exists for: content
// the gate accepts must not make the pinned BR library read outside the root. It
// fails if a BR bump changes embed parsing out from under storeEmbedRE.
func TestEmbedGateMatchesRenderer(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "pages")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(dir, "secret.txt")
	const secretData = "SUPERSECRET-MACAROON-BYTES"
	if err := os.WriteFile(secret, []byte(secretData), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "img.png"), []byte("legit"), 0o600); err != nil {
		t.Fatal(err)
	}
	leaked := base64.StdEncoding.EncodeToString([]byte(secretData))

	payloads := []string{
		`--embed[localfilename=` + secret + `]--`,
		`--embed[localfilename=` + secret + `,x=]y]--`,
		`--embed[localfilename=` + secret + `,a=1,b=]]--`,
		`--embed[x=]y],localfilename=` + secret + `]--`,
		`--embed[localfilename=../secret.txt]--`,
		`--embed[localfilename=../secret.txt,x=]y]--`,
		`--embed[localfilename=img.png]--`,
		`--embed[localfilename=` + secret + `]-- --embed[localfilename=img.png]--`,
	}
	for _, p := range payloads {
		if templateHasUnsafeEmbed(p) {
			continue // rejected on the way in, never reaches disk
		}
		out := resources.ProcessEmbeds(p, root, nil)
		if strings.Contains(out, leaked) {
			t.Fatalf("gate accepted %q but ProcessEmbeds inlined the out-of-root file", p)
		}
	}
}
