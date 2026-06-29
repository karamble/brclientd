// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// all: is required so files beginning with "_" (e.g. _cover.tmpl, the managed
// cover partial) are embedded; the default go:embed pattern excludes them.
//go:embed all:demostore
var demoStoreFS embed.FS

// seedDemoStore overlays the dcrpulse themed demo (banner header, a small cover
// grid and a few display-only products) onto a freshly written store dir,
// replacing the bare simplestore sample. It is only called right after
// WriteTemplate created a new store, so it never touches an existing merchant
// store. The library's operational templates (cart/order/orders/...) are kept;
// only the presentation templates are overridden and the sample products
// replaced. All assets are tiny so they add little to the binary.
func seedDemoStore(storeDir string) error {
	// Drop the library's sample products and demo image so only our demo remains.
	for _, f := range []string{
		filepath.Join(storeDir, "products", "first-type.toml"),
		filepath.Join(storeDir, "products", "second-type.toml"),
		filepath.Join(storeDir, "test.png"),
	} {
		_ = os.Remove(f)
	}
	// Copy every embedded asset into the store dir, preserving subdirectories.
	return fs.WalkDir(demoStoreFS, "demostore", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel("demostore", p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		dest := filepath.Join(storeDir, rel)
		if d.IsDir() {
			return os.MkdirAll(dest, 0o700)
		}
		data, err := demoStoreFS.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read embedded demo file %s: %w", p, err)
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return err
		}
		return os.WriteFile(dest, data, 0o644)
	})
}
