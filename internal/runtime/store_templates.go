// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// storeTemplateNameRE matches a flat template filename ending in .tmpl. Store
// templates live directly in the store root (no subdirectories).
var storeTemplateNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+\.tmpl$`)

// storeEmbedFileRE matches a plain relative path built from the same character
// set br_page_import_embed can produce, and nothing else.
var storeEmbedFileRE = regexp.MustCompile(`^([A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+$`)

// safeEmbedLocalFile reports whether v is a relative path that filepath.Join
// cannot take outside the directory it is joined to. It is an allow list: only
// a plain relative filename passes, so a construct nobody anticipated is denied
// rather than permitted.
func safeEmbedLocalFile(v string) bool {
	if v == "" || len(v) > 255 || !storeEmbedFileRE.MatchString(v) {
		return false
	}
	for _, part := range strings.Split(v, "/") {
		if part == "." || part == ".." {
			return false
		}
	}
	return true
}

// templateHasUnsafeEmbed reports whether template content carries a localfilename
// that is not a plain relative file inside the store dir. The store's
// ProcessEmbeds inlines that file's bytes into served pages, so a stored
// template could otherwise read arbitrary server files (wallet keys, /etc, ...).
// We block it at write time since ProcessEmbeds itself lives in the pinned BR
// library. The check is a hard filter on the raw text rather than a copy of that
// library's embed parsing, so a future BR version cannot parse its way past it.
func templateHasUnsafeEmbed(content string) bool {
	const key = "localfilename="
	for i := 0; ; {
		j := strings.Index(content[i:], key)
		if j < 0 {
			return false
		}
		i += j + len(key)
		// The value runs to the next argument or the end of the embed. It may
		// not stop at a bare "]": ProcessEmbeds cleans "a]/../../x" out of the
		// root, so truncating there would hide the traversal.
		v := content[i:]
		if k := strings.Index(v, ","); k >= 0 {
			v = v[:k]
		}
		if k := strings.Index(v, "]--"); k >= 0 {
			v = v[:k]
		}
		if !safeEmbedLocalFile(strings.TrimSpace(v)) {
			return true
		}
	}
}

func validateTemplateName(name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" || strings.Contains(name, "..") ||
		strings.ContainsAny(name, `/\`) || !storeTemplateNameRE.MatchString(name) {
		return "", false
	}
	return name, true
}

type storeTemplateInfo struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified"`
}

// listTemplates lists the *.tmpl files in the store root (the Go templates
// simplestore renders the storefront from).
func (s *storeController) listTemplates() ([]storeTemplateInfo, error) {
	entries, err := os.ReadDir(s.storeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []storeTemplateInfo{}, nil
		}
		return nil, err
	}
	out := make([]storeTemplateInfo, 0)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".tmpl") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, storeTemplateInfo{Name: e.Name(), Size: info.Size(), Modified: info.ModTime().Unix()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *storeController) readTemplate(name string) (string, error) {
	n, ok := validateTemplateName(name)
	if !ok {
		return "", fmt.Errorf("invalid template name")
	}
	data, err := os.ReadFile(filepath.Join(s.storeDir, n))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// saveTemplate writes a template via temp+rename so the store's watcher never
// reloads a partial file. A bad template makes reloadStore skip the change and
// keep the previous one, so a broken save is recoverable.
func (s *storeController) saveTemplate(name, content string) error {
	n, ok := validateTemplateName(name)
	if !ok {
		return fmt.Errorf("template name must be letters, digits, dash, underscore or dot ending in .tmpl")
	}
	if templateHasUnsafeEmbed(content) {
		return fmt.Errorf("template embeds may only reference files inside the store directory")
	}
	if err := os.MkdirAll(s.storeDir, 0o700); err != nil {
		return err
	}
	tmp := filepath.Join(s.storeDir, n+".tmp")
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.storeDir, n))
}

func (s *storeController) deleteTemplate(name string) error {
	n, ok := validateTemplateName(name)
	if !ok {
		return fmt.Errorf("invalid template name")
	}
	if err := os.Remove(filepath.Join(s.storeDir, n)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
