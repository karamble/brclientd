// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// brNote is one persisted daemon notification. The live notif bus only
// reaches subscribers connected at publish time; notes worth showing in the
// dashboard bell are also stored here so they survive the browser being
// closed (and daemon restarts).
type brNote struct {
	ID       int64     `json:"id"`
	TS       time.Time `json:"ts"`
	Severity string    `json:"severity"`
	Subject  string    `json:"subject"`
	Detail   string    `json:"detail"`
	UID      string    `json:"uid,omitempty"`
}

const maxStoredNotes = 100

type notificationStore struct {
	mu    sync.Mutex
	path  string
	notes []brNote
	next  int64
}

func newNotificationStore(dataDir string) *notificationStore {
	s := &notificationStore{path: filepath.Join(dataDir, "notifications.json"), next: 1}
	data, err := os.ReadFile(s.path)
	if err == nil {
		_ = json.Unmarshal(data, &s.notes)
		for _, n := range s.notes {
			if n.ID >= s.next {
				s.next = n.ID + 1
			}
		}
	}
	return s
}

func (s *notificationStore) add(severity, subject, detail, uid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notes = append(s.notes, brNote{
		ID:       s.next,
		TS:       time.Now(),
		Severity: severity,
		Subject:  subject,
		Detail:   detail,
		UID:      uid,
	})
	s.next++
	if len(s.notes) > maxStoredNotes {
		s.notes = s.notes[len(s.notes)-maxStoredNotes:]
	}
	if data, err := json.MarshalIndent(s.notes, "", " "); err == nil {
		_ = os.WriteFile(s.path, data, 0o600)
	}
}

// recent returns up to n notes, newest first.
func (s *notificationStore) recent(n int) []brNote {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 || n > len(s.notes) {
		n = len(s.notes)
	}
	out := make([]brNote, 0, n)
	for i := len(s.notes) - 1; i >= len(s.notes)-n; i-- {
		out = append(out, s.notes[i])
	}
	return out
}
