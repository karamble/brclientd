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

// all: is required so files beginning with "_" or "." are embedded too.
//go:embed all:demopages
var demoPagesFS embed.FS

// seedDemoPages writes the dcrpulse demo blog into a fresh pages dir, replacing
// the bare welcome page: a landing that lists two fictional articles in both a
// --grid-- (3-column) and a --grid2-- (one-per-row, feed-style) layout, plus the
// articles themselves (each with a header image and an --endofpost-- intro/main
// cutoff). Callers invoke this only on an empty pages dir.
func seedDemoPages(pagesDir string) error {
	return fs.WalkDir(demoPagesFS, "demopages", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel("demopages", p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		dest := filepath.Join(pagesDir, rel)
		if d.IsDir() {
			return os.MkdirAll(dest, 0o700)
		}
		data, err := demoPagesFS.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read embedded demo page %s: %w", p, err)
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return err
		}
		return os.WriteFile(dest, data, 0o644)
	})
}
