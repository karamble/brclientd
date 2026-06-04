// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package setup serves the one-shot HTTPS endpoint that bootstraps a new
// brclientd identity. While no local identity exists in the BR clientdb,
// the server binds to the configured clientrpc address with mTLS using the
// generated cert triplet. As soon as POST /create-identity succeeds the
// server shuts down so the clientrpc surface (later phases) can claim the
// port.
package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/slog"

	"github.com/karamble/brclientd/internal/certgen"
)

// Request is the JSON body accepted by POST /create-identity.
type Request struct {
	Nick string `json:"nick"`
	Name string `json:"name"`
}

// StagingFileName is the name a restore tarball is staged under in the app
// data dir, outside the data dir so it is never part of a backup walk. The
// next boot extracts it before anything opens the clientdb.
const StagingFileName = "restore-pending.tar.gz"

// StagingPath returns the staging location for a restore tarball.
func StagingPath(appDataDir string) string {
	return filepath.Join(appDataDir, StagingFileName)
}

// KXResetMarkerName marks that a restore was extracted and the post-restore
// KX reset pass has not completed yet. A restored snapshot is older than the
// network's last-seen ratchet state, so rendezvous points are stale in both
// directions until every ratchet is reset; the runtime performs the reset
// once connected and removes the marker. Lives in the app data dir so the
// data-dir wipe during extraction cannot delete it.
const KXResetMarkerName = "restore-kxreset-pending"

// KXResetMarkerPath returns the marker location for a pending post-restore
// KX reset.
func KXResetMarkerPath(appDataDir string) string {
	return filepath.Join(appDataDir, KXResetMarkerName)
}

// ErrRestorePending signals that a restore tarball was staged and the daemon
// must exit so its supervisor relaunch can extract it at boot.
var ErrRestorePending = errors.New("restore backup staged; restart required")

// maxRestoreBytes caps a restore upload. BR state dirs with embeds and
// shared-file downloads can be large, but anything beyond this is a runaway.
const maxRestoreBytes = 5 << 30

// Server hosts the pre-setup endpoint. When a CreateIdentity request lands,
// the server builds a zkidentity and pushes it into IdentityChan. BR's
// client.Run is blocked on LocalIDIniter reading from the same channel and
// persists the identity to the clientdb itself. Alternatively a full-state
// backup tarball POSTed to /restore-backup is staged on disk and Run returns
// ErrRestorePending so the daemon restarts to extract it.
type Server struct {
	Log          slog.Logger
	Certs        certgen.Triplet
	Listen       string
	IdentityChan chan<- *zkidentity.FullIdentity
	AppDataDir   string
}

// Run blocks until either CreateIdentity succeeds (identity persisted, server
// shut down cleanly) or ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	tlsCfg, err := s.Certs.LoadServerTLSConfig()
	if err != nil {
		return fmt.Errorf("load tls config: %w", err)
	}

	done := make(chan struct{})
	restored := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/create-identity", s.handleCreate(done))
	mux.HandleFunc("/restore-backup", s.handleRestore(restored))

	srv := &http.Server{
		Addr:              s.Listen,
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		s.Log.Infof("Pre-setup endpoint listening on %s (mTLS)", s.Listen)
		err := srv.ListenAndServeTLS("", "")
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
		return ctx.Err()
	case <-done:
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
		return nil
	case <-restored:
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
		return ErrRestorePending
	}
}

func (s *Server) handleCreate(done chan<- struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 16*1024))
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		var req Request
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		req.Nick = strings.TrimSpace(req.Nick)
		req.Name = strings.TrimSpace(req.Name)
		if req.Nick == "" {
			http.Error(w, "nick is required", http.StatusBadRequest)
			return
		}
		id, err := zkidentity.New(req.Name, req.Nick)
		if err != nil {
			s.Log.Errorf("zkidentity.New: %v", err)
			http.Error(w, "create identity: "+err.Error(), http.StatusInternalServerError)
			return
		}
		select {
		case s.IdentityChan <- id:
		case <-r.Context().Done():
			http.Error(w, "request cancelled", http.StatusServiceUnavailable)
			return
		}
		s.Log.Infof("Local identity submitted: nick=%q name=%q", req.Nick, req.Name)
		w.WriteHeader(http.StatusNoContent)
		select {
		case done <- struct{}{}:
		default:
		}
	}
}

// handleRestore stages a full-state backup tarball (produced by GET /backup
// on a running node) for extraction at the next boot. The clientdb is
// already open by the time this server runs, so extracting in-process is not
// safe; the daemon restarts instead.
func (s *Server) handleRestore(restored chan<- struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if s.AppDataDir == "" {
			http.Error(w, "app data dir not configured", http.StatusInternalServerError)
			return
		}
		if err := os.MkdirAll(s.AppDataDir, 0o700); err != nil {
			http.Error(w, "create app data dir: "+err.Error(), http.StatusInternalServerError)
			return
		}
		staging := StagingPath(s.AppDataDir)
		partial := staging + ".partial"
		f, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			http.Error(w, "create staging file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		// The .partial name plus the atomic rename below guarantee a
		// truncated upload never triggers a destructive restore on the
		// next boot.
		n, err := io.Copy(f, http.MaxBytesReader(w, r.Body, maxRestoreBytes))
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			os.Remove(partial)
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if n == 0 {
			os.Remove(partial)
			http.Error(w, "empty body", http.StatusBadRequest)
			return
		}
		if err := os.Rename(partial, staging); err != nil {
			os.Remove(partial)
			http.Error(w, "stage backup: "+err.Error(), http.StatusInternalServerError)
			return
		}
		s.Log.Infof("Restore backup staged at %s (%d bytes); restarting to extract", staging, n)
		w.WriteHeader(http.StatusNoContent)
		select {
		case restored <- struct{}{}:
		default:
		}
	}
}
