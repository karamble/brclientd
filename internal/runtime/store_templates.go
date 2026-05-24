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
