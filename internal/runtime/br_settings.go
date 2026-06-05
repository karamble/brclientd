// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// brSettings is the persisted shape of settings.json in the data dir.
// Pointer fields distinguish "unset" (default applies) from an explicit
// false, which JSON's zero value cannot.
type brSettings struct {
	SendReceiveReceipts *bool `json:"send_receive_receipts,omitempty"`
}

// defaultSendReceiveReceipts matches upstream brclient and bruig, which both
// ship sendrecvreceipts enabled.
const defaultSendReceiveReceipts = true

// brSettingsStore persists daemon settings the dashboard can change at
// runtime. Values consumed by client.Config are fixed at client construction,
// so changing them requires a daemon restart; the store is what the relaunch
// reads its new values from.
type brSettingsStore struct {
	path string
}

func newBRSettingsStore(dataDir string) *brSettingsStore {
	return &brSettingsStore{path: filepath.Join(dataDir, "settings.json")}
}

func (s *brSettingsStore) load() brSettings {
	var out brSettings
	data, err := os.ReadFile(s.path)
	if err != nil {
		return out
	}
	_ = json.Unmarshal(data, &out)
	return out
}

func (s *brSettingsStore) sendReceiveReceipts() bool {
	cur := s.load()
	if cur.SendReceiveReceipts == nil {
		return defaultSendReceiveReceipts
	}
	return *cur.SendReceiveReceipts
}

// setSendReceiveReceipts does a read-modify-write so other keys added to
// brSettings later survive the update.
func (s *brSettingsStore) setSendReceiveReceipts(v bool) error {
	cur := s.load()
	cur.SendReceiveReceipts = &v
	data, err := json.MarshalIndent(cur, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}
