// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// archivedGroupID is the reserved builtin group; it is never stored in the
// groups list and cannot be renamed or deleted.
const archivedGroupID = "archived"

const defaultAutoArchiveDays = 30

type contactGroup struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// contactGroupAssignment places one contact (keyed by its zkidentity hex
// string, never by nick) into exactly one group. Pinned keeps an archived
// contact archived even when new messages arrive; Auto marks assignments
// made by the inactivity sweeper, which are the only ones auto-unarchived.
type contactGroupAssignment struct {
	Group  string    `json:"group"`
	Pinned bool      `json:"pinned,omitempty"`
	Auto   bool      `json:"auto,omitempty"`
	Since  time.Time `json:"since"`
}

type contactGroupsFile struct {
	AutoArchiveDays *int                              `json:"auto_archive_days,omitempty"`
	Groups          []contactGroup                    `json:"groups"`
	Contacts        map[string]contactGroupAssignment `json:"contacts"`
}

// contactGroupsStore persists the contact group layout to groups.json in the
// BR data dir, so it rides along in /backup tarballs and is per-wallet.
type contactGroupsStore struct {
	mu   sync.Mutex
	path string
	data contactGroupsFile
}

func newContactGroupsStore(dataDir string) *contactGroupsStore {
	s := &contactGroupsStore{path: filepath.Join(dataDir, "groups.json")}
	if data, err := os.ReadFile(s.path); err == nil {
		_ = json.Unmarshal(data, &s.data)
	}
	if s.data.Contacts == nil {
		s.data.Contacts = make(map[string]contactGroupAssignment)
	}
	return s
}

// persist writes the file; callers hold s.mu.
func (s *contactGroupsStore) persist() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}

func (s *contactGroupsStore) autoArchiveDays() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.AutoArchiveDays == nil {
		return defaultAutoArchiveDays
	}
	return *s.data.AutoArchiveDays
}

func (s *contactGroupsStore) setAutoArchiveDays(n int) error {
	if n < 0 || n > 3650 {
		return errors.New("auto_archive_days out of range")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.AutoArchiveDays = &n
	return s.persist()
}

// snapshot returns a copy of the stored state plus the effective threshold.
func (s *contactGroupsStore) snapshot() contactGroupsFile {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := contactGroupsFile{
		Groups:   append([]contactGroup(nil), s.data.Groups...),
		Contacts: make(map[string]contactGroupAssignment, len(s.data.Contacts)),
	}
	days := defaultAutoArchiveDays
	if s.data.AutoArchiveDays != nil {
		days = *s.data.AutoArchiveDays
	}
	out.AutoArchiveDays = &days
	for k, v := range s.data.Contacts {
		out.Contacts[k] = v
	}
	sort.Slice(out.Groups, func(i, j int) bool {
		return strings.ToLower(out.Groups[i].Name) < strings.ToLower(out.Groups[j].Name)
	})
	return out
}

func (s *contactGroupsStore) groupExists(id string) bool {
	for _, g := range s.data.Groups {
		if g.ID == id {
			return true
		}
	}
	return false
}

// assign moves a contact into a group ("" = back to the regular list,
// archivedGroupID allowed). Manual assignments always clear the Auto flag.
func (s *contactGroupsStore) assign(uidHex, group string, pinned bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if group == "" {
		delete(s.data.Contacts, uidHex)
		return s.persist()
	}
	if group != archivedGroupID && !s.groupExists(group) {
		return errors.New("unknown group")
	}
	if group != archivedGroupID {
		pinned = false
	}
	s.data.Contacts[uidHex] = contactGroupAssignment{
		Group:  group,
		Pinned: pinned,
		Since:  time.Now(),
	}
	return s.persist()
}

func (s *contactGroupsStore) createGroup(name string) (contactGroup, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 64 {
		return contactGroup{}, errors.New("invalid group name")
	}
	if strings.EqualFold(name, "archived") {
		return contactGroup{}, errors.New("name is reserved")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.data.Groups {
		if strings.EqualFold(g.Name, name) {
			return contactGroup{}, errors.New("group name already exists")
		}
	}
	var rnd [4]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return contactGroup{}, err
	}
	g := contactGroup{ID: "g-" + hex.EncodeToString(rnd[:]), Name: name}
	s.data.Groups = append(s.data.Groups, g)
	return g, s.persist()
}

func (s *contactGroupsStore) renameGroup(id, name string) error {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 64 || strings.EqualFold(name, "archived") {
		return errors.New("invalid group name")
	}
	if id == archivedGroupID {
		return errors.New("the archived group cannot be renamed")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.data.Groups {
		if g.ID != id && strings.EqualFold(g.Name, name) {
			return errors.New("group name already exists")
		}
	}
	for i := range s.data.Groups {
		if s.data.Groups[i].ID == id {
			s.data.Groups[i].Name = name
			return s.persist()
		}
	}
	return errors.New("unknown group")
}

// deleteGroup removes a group and returns its members to the regular list.
func (s *contactGroupsStore) deleteGroup(id string) error {
	if id == archivedGroupID {
		return errors.New("the archived group cannot be deleted")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i := range s.data.Groups {
		if s.data.Groups[i].ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return errors.New("unknown group")
	}
	s.data.Groups = append(s.data.Groups[:idx], s.data.Groups[idx+1:]...)
	for uid, a := range s.data.Contacts {
		if a.Group == id {
			delete(s.data.Contacts, uid)
		}
	}
	return s.persist()
}

// archived reports whether the contact is currently in the archived group.
func (s *contactGroupsStore) archived(uidHex string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Contacts[uidHex].Group == archivedGroupID
}

// maybeAutoUnarchive returns the contact to the regular list when it was
// auto-archived and not pinned. Returns true when an unarchive happened.
func (s *contactGroupsStore) maybeAutoUnarchive(uidHex string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.data.Contacts[uidHex]
	if !ok || a.Group != archivedGroupID || !a.Auto || a.Pinned {
		return false
	}
	delete(s.data.Contacts, uidHex)
	_ = s.persist()
	return true
}

// autoArchive archives a contact with the Auto flag, but only when the
// contact has no assignment yet (manual choices are never overridden).
// Returns true when an assignment was made.
func (s *contactGroupsStore) autoArchive(uidHex string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Contacts[uidHex]; ok {
		return false
	}
	s.data.Contacts[uidHex] = contactGroupAssignment{
		Group: archivedGroupID,
		Auto:  true,
		Since: time.Now(),
	}
	_ = s.persist()
	return true
}

// prune drops assignments for contacts no longer in the addressbook.
func (s *contactGroupsStore) prune(valid map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for uid := range s.data.Contacts {
		if !valid[uid] {
			delete(s.data.Contacts, uid)
			changed = true
		}
	}
	if changed {
		_ = s.persist()
	}
}
