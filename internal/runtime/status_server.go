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
	Log     slog.Logger
	Certs   certgen.Triplet
	Listen  string
	Tracker *Tracker
	DB      *clientdb.DB

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
	mux.HandleFunc("/invites/redeem-key", s.handleRedeemPaidInvite)

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
