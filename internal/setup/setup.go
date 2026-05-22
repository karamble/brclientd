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
	"strings"
	"time"

	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/decred/slog"

	"github.com/karamble/brclientd/internal/certgen"
	"github.com/karamble/brclientd/internal/identity"
)

// Request is the JSON body accepted by POST /create-identity.
type Request struct {
	Nick string `json:"nick"`
	Name string `json:"name"`
}

// Server hosts the pre-setup endpoint.
type Server struct {
	Log    slog.Logger
	DB     *clientdb.DB
	Certs  certgen.Triplet
	Listen string
}

// Run blocks until either CreateIdentity succeeds (identity persisted, server
// shut down cleanly) or ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	tlsCfg, err := s.Certs.LoadServerTLSConfig()
	if err != nil {
		return fmt.Errorf("load tls config: %w", err)
	}

	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/create-identity", s.handleCreate(done))

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
		if err := identity.Create(r.Context(), s.DB, req.Nick, req.Name); err != nil {
			s.Log.Errorf("Create identity failed: %v", err)
			http.Error(w, "create identity: "+err.Error(), http.StatusInternalServerError)
			return
		}
		s.Log.Infof("Local identity created: nick=%q name=%q", req.Nick, req.Name)
		w.WriteHeader(http.StatusNoContent)
		select {
		case done <- struct{}{}:
		default:
		}
	}
}
