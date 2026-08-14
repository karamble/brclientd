// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/zkidentity"

	"github.com/karamble/brclientd/internal/identity"
)

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
	//
	// Filtered, because this waits for one event on a bus that carries every
	// other kind. An unfiltered subscription here can have the reply it is
	// waiting for evicted by a burst of unrelated traffic - a file transfer
	// publishes per chunk - and then waits out the request context for a
	// message that already came and went.
	ch, unsub := s.Notifs.SubscribeFiltered(pagesEventType)
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

	// No fixed fetch deadline: a page reply travels over the relay and its
	// transfer size and time are unbounded, so bound the wait by the request
	// context (the caller's connection) instead of a timeout. A caller that
	// navigates away cancels the request, which unblocks this wait; the
	// deferred unsub then drops the subscription.
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			http.Error(w, "page fetch canceled", http.StatusGatewayTimeout)
			return
		case evt, ok := <-ch:
			if !ok {
				http.Error(w, "notification stream closed", http.StatusBadGateway)
				return
			}
			if evt.Type != pagesEventType || !pagesEventMatches(evt, uidHex, wantTag, req.Path, req.AsyncTargetID) {
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

// pagesEventType is the one event this handler waits for, named once so the
// subscription filter and the match cannot drift apart.
const pagesEventType = "resource-fetched"

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

// pageNameRE matches a markdown filename, optionally under subdirectories:
// slash-separated segments of [A-Za-z0-9_.-], ending in ".md". With no leading
// slash and the ".." guard in validatePageName, a matching name can only
// resolve within PagesDir.
var pageNameRE = regexp.MustCompile(`^([A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.md$`)

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
	if _, err := os.Stat(s.PagesDir); os.IsNotExist(err) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"pages": []any{}})
		return
	}
	type pageInfo struct {
		Name     string `json:"name"`
		Size     int64  `json:"size"`
		Modified int64  `json:"modified"`
	}
	// Walk subdirectories so hosted pages organized in folders (docs/intro.md)
	// are listed with their path relative to PagesDir.
	pages := make([]pageInfo, 0)
	err := filepath.WalkDir(s.PagesDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".md") {
			return nil
		}
		rel, err := filepath.Rel(s.PagesDir, path)
		if err != nil {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		pages = append(pages, pageInfo{Name: filepath.ToSlash(rel), Size: info.Size(), Modified: info.ModTime().Unix()})
		return nil
	})
	if err != nil {
		http.Error(w, "read pages dir: "+err.Error(), http.StatusBadGateway)
		return
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
	// Cap the page body so a direct (localhost) caller can't write an unbounded
	// page; the dashboard already limits proxied requests to 1 MiB.
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
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
	// Pages are run through ProcessEmbeds when served, so an absolute or
	// traversing embed localfilename would read files outside the pages dir.
	if templateHasUnsafeEmbed(req.Content) {
		http.Error(w, "page embeds may only reference files inside the pages directory", http.StatusBadRequest)
		return
	}
	full := filepath.Join(s.PagesDir, name)
	// Refuse to write through a pre-existing symlink (which could redirect the
	// write outside the pages dir).
	if fi, err := os.Lstat(full); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		http.Error(w, "refusing to write through a symlink", http.StatusBadRequest)
		return
	}
	// Create any parent directories so subdirectory pages (docs/intro.md) save.
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		http.Error(w, "create pages dir: "+err.Error(), http.StatusBadGateway)
		return
	}
	if err := os.WriteFile(full, []byte(req.Content), 0o600); err != nil {
		http.Error(w, "write page: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// chatEmbedNameRE matches the localfilename form PM history uses for a
// received chat embed: embeds/<16-hex ShortLogID>/<file>. The file segment has
// no separators, so a matching name names exactly one file in one peer's
// embed folder.
var chatEmbedNameRE = regexp.MustCompile(`^embeds/([0-9a-f]{16})/([A-Za-z0-9._-]+)$`)

// pageAssetNameRE mirrors pageNameRE for the raster image types a page may
// reference via an embed localfilename. Import destinations are restricted to
// this set; there is deliberately no endpoint that deletes these assets.
var pageAssetNameRE = regexp.MustCompile(`^([A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.(?:jpg|jpeg|jfif|png|gif|webp)$`)

func validatePageAssetName(name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" || strings.Contains(name, "..") || !pageAssetNameRE.MatchString(name) {
		return "", false
	}
	return name, true
}

// maxImportEmbedBytes caps imported images well under the ~1 MiB resource
// reply payload: page replies are not chunked, so a page plus its base64
// inlined embeds must fit one message or the whole fetch fails.
const maxImportEmbedBytes = 512 << 10

// handlePagesLocalImportEmbed copies one received chat embed into the pages
// directory so a hosted page can reference it via an embed localfilename and
// ProcessEmbeds inlines it when the page is served. Body: {source, dest}.
// The source must be a raster image in the chat-embeds store (the clientdb
// layout under <DataDir>/db/embeds, not EmbedsRoot) and the destination an
// image path inside PagesDir. Both sides resolve through os.Root, so neither
// traversal nor symlinks can escape their directory. Imports never overwrite.
func (s *StatusServer) handlePagesLocalImportEmbed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.PagesDir == "" {
		http.Error(w, "pages dir not configured", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var req struct {
		Source string `json:"source"`
		Dest   string `json:"dest"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	src := strings.TrimSpace(req.Source)
	m := chatEmbedNameRE.FindStringSubmatch(src)
	if m == nil || strings.Contains(src, "..") {
		http.Error(w, "invalid source: must be embeds/<uid16>/<file>", http.StatusBadRequest)
		return
	}
	dest, ok := validatePageAssetName(req.Dest)
	if !ok {
		http.Error(w, "invalid dest: must be an image path inside the pages directory", http.StatusBadRequest)
		return
	}

	embedsRoot, err := os.OpenRoot(filepath.Join(identity.PathsIn(s.DataDir).Root, "embeds"))
	if err != nil {
		http.Error(w, "open embeds dir: "+err.Error(), http.StatusNotFound)
		return
	}
	defer embedsRoot.Close()
	rel := m[1] + "/" + m[2]
	fi, err := embedsRoot.Lstat(rel)
	if err != nil {
		http.Error(w, "source embed not found", http.StatusNotFound)
		return
	}
	if !fi.Mode().IsRegular() {
		http.Error(w, "source is not a regular file", http.StatusBadRequest)
		return
	}
	if fi.Size() > maxImportEmbedBytes {
		http.Error(w, fmt.Sprintf("source exceeds %d bytes; a page referencing it could not be served in one resource reply", maxImportEmbedBytes), http.StatusBadRequest)
		return
	}
	f, err := embedsRoot.Open(rel)
	if err != nil {
		http.Error(w, "open source embed: "+err.Error(), http.StatusBadGateway)
		return
	}
	data, err := io.ReadAll(io.LimitReader(f, maxImportEmbedBytes+1))
	_ = f.Close()
	if err != nil {
		http.Error(w, "read source embed: "+err.Error(), http.StatusBadGateway)
		return
	}
	if len(data) > maxImportEmbedBytes {
		http.Error(w, "source grew past the import size cap", http.StatusBadRequest)
		return
	}
	contentType := http.DetectContentType(data)
	switch contentType {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
	default:
		http.Error(w, "source is not a raster image", http.StatusBadRequest)
		return
	}

	// PagesDir exists once pages were ever enabled; create it here so an
	// import on a fresh node does not depend on hosting having run first.
	if err := os.MkdirAll(s.PagesDir, 0o700); err != nil {
		http.Error(w, "create pages dir: "+err.Error(), http.StatusBadGateway)
		return
	}
	pagesRoot, err := os.OpenRoot(s.PagesDir)
	if err != nil {
		http.Error(w, "open pages dir: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer pagesRoot.Close()
	if _, err := pagesRoot.Lstat(dest); err == nil {
		http.Error(w, "dest already exists", http.StatusConflict)
		return
	}
	if dir := path.Dir(dest); dir != "." {
		if err := pagesRoot.MkdirAll(dir, 0o700); err != nil {
			http.Error(w, "create dest dir: "+err.Error(), http.StatusBadGateway)
			return
		}
	}
	// O_EXCL both enforces no-overwrite atomically and refuses a path that
	// exists as a symlink.
	wf, err := pagesRoot.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		http.Error(w, "create dest: "+err.Error(), http.StatusBadGateway)
		return
	}
	if _, err := wf.Write(data); err != nil {
		_ = wf.Close()
		http.Error(w, "write dest: "+err.Error(), http.StatusBadGateway)
		return
	}
	if err := wf.Close(); err != nil {
		http.Error(w, "close dest: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"source":      src,
		"dest":        dest,
		"sizeBytes":   len(data),
		"contentType": contentType,
	})
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
