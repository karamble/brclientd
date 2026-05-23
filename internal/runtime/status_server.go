// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/slog"

	"github.com/karamble/brclientd/internal/certgen"
)

// StatusServer serves the mTLS HTTP surface dcrpulse-style dashboards use
// alongside clientrpc: /status for the runtime tracker snapshot, and
// /history/pm for paginated PM history reads (a wire-exposed wrapper around
// clientdb.ReadLogPM since BR's clientrpc.proto has no history RPC).
type StatusServer struct {
	Log       slog.Logger
	Certs     certgen.Triplet
	Listen    string
	Tracker   *Tracker
	DB        *clientdb.DB
	UploadDir string

	clientMu sync.RWMutex
	client   *client.Client
}

// SetClient attaches a live *client.Client to the StatusServer once the BR
// runtime has booted past the gates. /contacts returns 503 until this is
// called.
func (s *StatusServer) SetClient(c *client.Client) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	s.client = c
}

func (s *StatusServer) currentClient() *client.Client {
	s.clientMu.RLock()
	defer s.clientMu.RUnlock()
	return s.client
}

// Run blocks until ctx is cancelled or the server fails.
func (s *StatusServer) Run(ctx context.Context) error {
	tlsCfg, err := s.Certs.LoadServerTLSConfig()
	if err != nil {
		return fmt.Errorf("status: load tls config: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/history/pm", s.handleHistoryPM)
	mux.HandleFunc("/contacts", s.handleContacts)
	mux.HandleFunc("/contacts/rename", s.handleRenameContact)
	mux.HandleFunc("/invites/redeem-key", s.handleRedeemPaidInvite)
	mux.HandleFunc("/files/send", s.handleSendFile)

	srv := &http.Server{
		Addr:              s.Listen,
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		s.Log.Infof("Status endpoint listening on %s (mTLS)", s.Listen)
		err := srv.ListenAndServeTLS("", "")
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
		return ctx.Err()
	case err := <-serveErr:
		return err
	}
}

func (s *StatusServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Tracker.Get())
}

// handleHistoryPM returns paginated PM history for a given peer UID. Query
// params:
//
//	uid       hex-encoded zkidentity.ShortID of the peer (32 bytes / 64 hex chars)
//	page_size entries per page (default 50, max 500)
//	page      zero-based page index (default 0)
//
// Wraps clientdb.ReadLogPM directly; brclientd remains the source of truth
// for chat history so dashboard consumers can stay stateless.
func (s *StatusServer) handleHistoryPM(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.DB == nil {
		http.Error(w, "history unavailable: clientdb not attached", http.StatusServiceUnavailable)
		return
	}
	uidStr := r.URL.Query().Get("uid")
	if uidStr == "" {
		http.Error(w, "uid query param is required", http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(uidStr); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	pageSize := parsePositiveInt(r.URL.Query().Get("page_size"), 50, 500)
	pageNum := parseNonNegativeInt(r.URL.Query().Get("page"), 0)

	var entries []clientdb.PMLogEntry
	err := s.DB.View(r.Context(), func(tx clientdb.ReadTx) error {
		got, err := s.DB.ReadLogPM(tx, uid, pageSize, pageNum)
		if err != nil {
			return err
		}
		entries = got
		return nil
	})
	if err != nil {
		http.Error(w, "read pm log: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		UID      string                `json:"uid"`
		Page     int                   `json:"page"`
		PageSize int                   `json:"page_size"`
		Entries  []clientdb.PMLogEntry `json:"entries"`
	}{
		UID:      uidStr,
		Page:     pageNum,
		PageSize: pageSize,
		Entries:  entries,
	})
}

// handleContacts returns the BR client's in-memory address book entries.
// 503 until the BR client has been instantiated (i.e. until past the
// gate / pre-setup phase).
func (s *StatusServer) handleContacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	entries := c.AddressBook()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Entries []*clientdb.AddressBookEntry `json:"entries"`
	}{Entries: entries})
}

// handleRenameContact sets the local NickAlias for a contact. Pure
// clientdb-only mutation; nothing is broadcast to the BR network. Mirrors
// bruig's "Rename User" action (user_context_menu.dart) which calls
// client.RenameUser at client_kx.go:606.
func (s *StatusServer) handleRenameContact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		UID      string `json:"uid"`
		NewNick  string `json:"new_nick"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.NewNick = strings.TrimSpace(req.NewNick)
	if req.NewNick == "" {
		http.Error(w, "new_nick is required", http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(req.UID); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.RenameUser(uid, req.NewNick); err != nil {
		http.Error(w, "rename user: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleRedeemPaidInvite accepts a bech32-encoded PaidInviteKey ("brpik1..."),
// fetches the encrypted invite blob from the BR server, decrypts it, and
// runs AcceptInvite to start a key exchange. The bridge between brpik1 keys
// and BR's clientrpc ChatService.AcceptInvite RPC, which only accepts the
// binary OOB invite blob.
func (s *StatusServer) handleRedeemPaidInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Key = strings.TrimSpace(req.Key)
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	var pik clientintf.PaidInviteKey
	if err := pik.Decode(req.Key); err != nil {
		http.Error(w, "decode paid invite key: "+err.Error(), http.StatusBadRequest)
		return
	}

	fetchCtx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	invite, err := c.FetchPrepaidInvite(fetchCtx, pik, io.Discard)
	if err != nil {
		http.Error(w, "fetch prepaid invite: "+err.Error(), http.StatusBadGateway)
		return
	}
	if err := c.AcceptInvite(invite); err != nil {
		http.Error(w, "accept invite: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleSendFile accepts a multipart upload (fields: user, file) and hands
// the file off to BR's file-transfer subsystem via c.SendFile. The uploaded
// bytes are persisted under UploadDir; BR's send queue references that path
// for the lifetime of the transfer (chunks are read on demand), so we keep
// the file in place rather than auto-deleting after the call returns.
func (s *StatusServer) handleSendFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if s.UploadDir == "" {
		http.Error(w, "upload directory not configured", http.StatusInternalServerError)
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "parse multipart: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()

	userField := strings.TrimSpace(r.FormValue("user"))
	if userField == "" {
		http.Error(w, "user field is required", http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file part missing: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	ru, err := c.UserByNick(userField)
	if err != nil {
		http.Error(w, "user lookup: "+err.Error(), http.StatusNotFound)
		return
	}

	safeName := sanitizeUploadName(header.Filename)
	if safeName == "" {
		http.Error(w, "invalid filename", http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(s.UploadDir, 0o700); err != nil {
		http.Error(w, "mkdir uploads: "+err.Error(), http.StatusInternalServerError)
		return
	}
	storedPath, err := storeUpload(s.UploadDir, safeName, file)
	if err != nil {
		http.Error(w, "store upload: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := c.SendFile(ru.ID(), 0, storedPath, nil); err != nil {
		_ = os.Remove(storedPath)
		http.Error(w, "send file: "+err.Error(), http.StatusBadGateway)
		return
	}
	// BR's sendPreparedSendqItemListSync waits for the relay to ack each
	// chunk before SendFile returns, so once we're here every byte has
	// been read from the source file and the file is no longer referenced
	// by the send queue. BR does not clean these up itself.
	_ = os.Remove(storedPath)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"filename": safeName,
		"size":     header.Size,
	})
}

func sanitizeUploadName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "" || name == "." || name == ".." {
		return ""
	}
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		case r == '.' || r == '_' || r == '-' || r == ' ':
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return ""
	}
	if len(out) > 200 {
		out = out[:200]
	}
	return string(out)
}

func storeUpload(dir, name string, src io.Reader) (string, error) {
	candidate := filepath.Join(dir, name)
	if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
		return writeUpload(candidate, src)
	}
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	for i := 1; i < 1000; i++ {
		c := filepath.Join(dir, fmt.Sprintf("%s-%d%s", stem, i, ext))
		if _, err := os.Stat(c); errors.Is(err, os.ErrNotExist) {
			return writeUpload(c, src)
		}
	}
	return "", errors.New("could not find a free upload filename")
}

func writeUpload(path string, src io.Reader) (string, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, src); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func parsePositiveInt(s string, fallback, max int) int {
	if s == "" {
		return fallback
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return fallback
	}
	if v > max {
		return max
	}
	return v
}

func parseNonNegativeInt(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return fallback
	}
	return v
}
