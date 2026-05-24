// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/zkidentity"
)

// pageFetchTimeout bounds how long /pages/fetch waits for a resource reply
// before giving up. Remote replies travel over the relay so allow a generous
// window; local fetches resolve near-instantly.
const pageFetchTimeout = 30 * time.Second

// handlePagesFetch fetches a single page (resource) and blocks until the
// reply lands, turning BR's async fetch-then-notify flow into one request.
// Body: {uid, path, session_id?, parent_page?, data?, async_target_id?}.
// When uid is our own identity the page is served from the local
// FilesystemResource; otherwise it is requested from the remote peer. The
// reply arrives via the resource-fetched notification (see br_client.go), so
// we subscribe to the notif bus before issuing the fetch.
func (s *StatusServer) handlePagesFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if s.Notifs == nil {
		http.Error(w, "notifications unavailable", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		UID           string          `json:"uid"`
		Path          []string        `json:"path"`
		SessionID     uint64          `json:"session_id"`
		ParentPage    uint64          `json:"parent_page"`
		Data          json.RawMessage `json:"data,omitempty"`
		AsyncTargetID string          `json:"async_target_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	uid, err := parsePageUID(req.UID)
	if err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Path) == 0 {
		req.Path = []string{"index.md"}
	}
	var data json.RawMessage
	if len(req.Data) > 0 && string(req.Data) != "null" {
		data = req.Data
	}

	// Subscribe before issuing the fetch: FetchLocalResource fires the
	// resource-fetched notification synchronously during the call, so the
	// subscription must already be live to catch it.
	ch, unsub := s.Notifs.Subscribe()
	defer unsub()

	uidHex := uid.String()
	isLocal := uid == c.PublicID()
	sessionID := req.SessionID
	var wantTag uint64

	if isLocal {
		if err := c.FetchLocalResource(req.Path, nil, data, req.AsyncTargetID); err != nil {
			http.Error(w, "fetch local page: "+err.Error(), http.StatusBadGateway)
			return
		}
	} else {
		sess := clientintf.PagesSessionID(req.SessionID)
		if sess == 0 {
			ns, err := c.NewPagesSession()
			if err != nil {
				http.Error(w, "new pages session: "+err.Error(), http.StatusBadGateway)
				return
			}
			sess = ns
			sessionID = uint64(ns)
		}
		tag, err := c.FetchResource(uid, req.Path, nil, sess,
			clientintf.PagesSessionID(req.ParentPage), data, req.AsyncTargetID)
		// ErrAlreadyHaveBundledResource means the page was already cached from
		// a prior bundle and the notification fired synchronously; treat it as
		// success and let the wait loop pick the event up by path.
		if err != nil && !errors.Is(err, client.ErrAlreadyHaveBundledResource) {
			http.Error(w, "fetch page: "+err.Error(), http.StatusBadGateway)
			return
		}
		wantTag = uint64(tag)
	}

	ctx, cancel := context.WithTimeout(r.Context(), pageFetchTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			http.Error(w, "timed out waiting for page reply", http.StatusGatewayTimeout)
			return
		case evt, ok := <-ch:
			if !ok {
				http.Error(w, "notification stream closed", http.StatusBadGateway)
				return
			}
			if evt.Type != "resource-fetched" || !pagesEventMatches(evt, uidHex, wantTag, req.Path, req.AsyncTargetID) {
				continue
			}
			out := map[string]any{
				"session_id":      sessionID,
				"page_id":         evt.Payload["page_id"],
				"parent_page":     evt.Payload["parent_page"],
				"status":          evt.Payload["status"],
				"meta":            evt.Payload["meta"],
				"markdown":        evt.Payload["data"],
				"async_target_id": evt.Payload["async_target_id"],
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
			return
		}
	}
}

// parsePageUID accepts a user identity as either hex (the form used by
// contacts/posts uids) or base64 (the form ChatService.UserPublicIdentity and
// the dashboard's /br/identity return for the local identity), so callers can
// pass whichever they happen to hold.
func parsePageUID(s string) (zkidentity.ShortID, error) {
	var uid zkidentity.ShortID
	if err := uid.FromString(s); err == nil {
		return uid, nil
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(raw) != len(uid) {
		return uid, fmt.Errorf("not a hex or base64 identity")
	}
	copy(uid[:], raw)
	return uid, nil
}

// pagesEventMatches reports whether a resource-fetched event is the reply to
// the fetch we just issued. Remote replies correlate by the request tag
// returned from FetchResource; local fetches (and bundled replies) carry tag
// 0, so they correlate by uid + path + async target instead.
func pagesEventMatches(evt NotifEvent, uidHex string, wantTag uint64, path []string, asyncTargetID string) bool {
	if u, _ := evt.Payload["uid"].(string); u != uidHex {
		return false
	}
	if wantTag != 0 {
		if t, _ := evt.Payload["tag"].(uint64); t == wantTag {
			return true
		}
		// Fall through to path matching: a bundled reply returns tag 0, so the
		// tag we hold won't appear on the event.
	}
	if a, _ := evt.Payload["async_target_id"].(string); a != asyncTargetID {
		return false
	}
	p, _ := evt.Payload["path"].([]string)
	return pathsEqual(p, path)
}

func pathsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// pageNameRE matches a single flat markdown filename: no slashes, must end in
// ".md". Subdirectory hosting is a follow-up.
var pageNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+\.md$`)

func validatePageName(name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" || strings.Contains(name, "..") || !pageNameRE.MatchString(name) {
		return "", false
	}
	return name, true
}

// handlePagesLocalList lists the markdown files this node hosts from PagesDir.
func (s *StatusServer) handlePagesLocalList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.PagesDir == "" {
		http.Error(w, "pages dir not configured", http.StatusServiceUnavailable)
		return
	}
	entries, err := os.ReadDir(s.PagesDir)
	if err != nil {
		if os.IsNotExist(err) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"pages": []any{}})
			return
		}
		http.Error(w, "read pages dir: "+err.Error(), http.StatusBadGateway)
		return
	}
	type pageInfo struct {
		Name     string `json:"name"`
		Size     int64  `json:"size"`
		Modified int64  `json:"modified"`
	}
	pages := make([]pageInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		pages = append(pages, pageInfo{Name: e.Name(), Size: info.Size(), Modified: info.ModTime().Unix()})
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].Name < pages[j].Name })
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"pages": pages})
}

// handlePagesLocalFile returns the raw markdown of one hosted page. Query: name.
func (s *StatusServer) handlePagesLocalFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.PagesDir == "" {
		http.Error(w, "pages dir not configured", http.StatusServiceUnavailable)
		return
	}
	name, ok := validatePageName(r.URL.Query().Get("name"))
	if !ok {
		http.Error(w, "invalid page name", http.StatusBadRequest)
		return
	}
	data, err := os.ReadFile(filepath.Join(s.PagesDir, name))
	if os.IsNotExist(err) {
		http.Error(w, "page not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "read page: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"name": name, "content": string(data)})
}

// handlePagesLocalSave writes (creates or overwrites) one hosted page. Body:
// {name, content}.
func (s *StatusServer) handlePagesLocalSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.PagesDir == "" {
		http.Error(w, "pages dir not configured", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	name, ok := validatePageName(req.Name)
	if !ok {
		http.Error(w, "invalid page name", http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(s.PagesDir, 0o700); err != nil {
		http.Error(w, "create pages dir: "+err.Error(), http.StatusBadGateway)
		return
	}
	if err := os.WriteFile(filepath.Join(s.PagesDir, name), []byte(req.Content), 0o600); err != nil {
		http.Error(w, "write page: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handlePagesLocalDelete removes one hosted page. Body: {name}.
func (s *StatusServer) handlePagesLocalDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.PagesDir == "" {
		http.Error(w, "pages dir not configured", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	name, ok := validatePageName(req.Name)
	if !ok {
		http.Error(w, "invalid page name", http.StatusBadRequest)
		return
	}
	if err := os.Remove(filepath.Join(s.PagesDir, name)); err != nil && !os.IsNotExist(err) {
		http.Error(w, "delete page: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
