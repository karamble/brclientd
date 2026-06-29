// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"fmt"
	"io/fs"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// storeFileInfo describes one user-managed media file under the store dir that
// the dashboard's asset manager can list, preview or delete: cover images, the
// header banner, digital-download goods, etc.
type storeFileInfo struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified"`
	Mime     string `json:"mime,omitempty"`
}

// storeFileSkipDirs are the store subdirectories that hold operational state or
// source (orders/carts/pending invoices, the catalog TOML) rather than
// user-managed media, so the asset listing and deletes leave them alone.
var storeFileSkipDirs = map[string]bool{
	"orders":          true,
	"carts":           true,
	"pendinginvoices": true,
	"products":        true,
}

// sanitizeStoreRel cleans a caller-supplied relative path and keeps it inside
// the store dir. Mirrors the guard in saveStoreFile.
func (s *storeController) sanitizeStoreRel(relPath string) (string, error) {
	rel := strings.TrimPrefix(filepath.Clean("/"+strings.TrimSpace(relPath)), "/")
	if rel == "" || strings.Contains(rel, "..") {
		return "", fmt.Errorf("invalid file path")
	}
	return rel, nil
}

func relTopDir(rel string) string {
	return strings.SplitN(filepath.ToSlash(rel), "/", 2)[0]
}

// validateStoreMediaRel is the single gate for user-managed media access
// (upload/read/delete). On top of sanitizeStoreRel it normalizes each path
// segment (trailing dots/spaces, which could otherwise slip a suffix check),
// refuses templates (managed via their own editor; an uploaded *.tmpl would be
// parsed and executed by the store) and refuses the operational subdirs (the
// product catalog, orders, carts and pending invoices are not media).
func (s *storeController) validateStoreMediaRel(relPath string) (string, error) {
	rel, err := s.sanitizeStoreRel(relPath)
	if err != nil {
		return "", err
	}
	parts := strings.Split(rel, "/")
	for i, p := range parts {
		p = strings.TrimRight(p, ". ")
		if p == "" {
			return "", fmt.Errorf("invalid file path")
		}
		parts[i] = p
	}
	rel = strings.Join(parts, "/")
	if storeFileSkipDirs[relTopDir(rel)] {
		return "", fmt.Errorf("cannot access store-managed files")
	}
	lower := strings.ToLower(rel)
	if strings.HasSuffix(lower, ".tmpl") || strings.HasSuffix(lower, ".tmp") {
		return "", fmt.Errorf("templates are managed separately")
	}
	return rel, nil
}

// listStoreFiles walks the store dir and returns the user-managed media files,
// skipping *.tmpl templates (managed via their own editor) and the operational
// subdirs.
func (s *storeController) listStoreFiles() ([]storeFileInfo, error) {
	out := make([]storeFileInfo, 0)
	err := filepath.WalkDir(s.storeDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			if p == s.storeDir {
				return nil
			}
			rel, _ := filepath.Rel(s.storeDir, p)
			if storeFileSkipDirs[relTopDir(rel)] {
				return fs.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".tmpl") || strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		rel, err := filepath.Rel(s.storeDir, p)
		if err != nil {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		out = append(out, storeFileInfo{
			Path:     filepath.ToSlash(rel),
			Size:     info.Size(),
			Modified: info.ModTime().Unix(),
			Mime:     mime.TypeByExtension(filepath.Ext(p)),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// isStoreImageSlot reports whether the destination is a display image shown to
// customers (the header banner or a product cover), which must be a real image.
func isStoreImageSlot(relPath string) bool {
	rel := strings.ToLower(strings.TrimSpace(relPath))
	rel = strings.TrimPrefix(rel, "./")
	return rel == "banner.jpg" || strings.HasPrefix(rel, "covers/")
}

func isImageFilename(relPath string) bool {
	switch strings.ToLower(filepath.Ext(strings.TrimRight(relPath, ". "))) {
	case ".jpg", ".jpeg", ".png", ".webp", ".gif":
		return true
	}
	return false
}

// readStoreFile returns one file's bytes and mime for preview or download.
func (s *storeController) readStoreFile(relPath string) ([]byte, string, error) {
	rel, err := s.validateStoreMediaRel(relPath)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(filepath.Join(s.storeDir, rel))
	if err != nil {
		return nil, "", err
	}
	return data, mime.TypeByExtension(filepath.Ext(rel)), nil
}

// deleteStoreFile removes one media file under the store dir. Templates and the
// operational subdirs are off-limits.
func (s *storeController) deleteStoreFile(relPath string) error {
	rel, err := s.validateStoreMediaRel(relPath)
	if err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(s.storeDir, rel)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
