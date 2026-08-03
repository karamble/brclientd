// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"net/http"
	"time"
)

// handleResourceRequests lets a third-party service dock on the resources
// interface and serve BR page fetches dynamically. It is a long-lived NDJSON
// stream (mirroring handleNotifications): connecting installs the docked
// service as the resource-provider override; disconnecting removes it, so the
// node reverts to its configured off/pages/store mode. Single occupant - a
// second dock while one is held gets 409. mTLS (RequireAndVerifyClientCert) is
// the authorization gate.
func (s *StatusServer) handleResourceRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not configured", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	ch := make(chan resourceRequestEvent, 64)
	if !ctrl.DockResources(ch) {
		http.Error(w, "resources interface already docked", http.StatusConflict)
		return
	}
	defer ctrl.UndockResources(ch)

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	s.Log.Infof("resources interface docked by %s", peerCommonName(r))
	defer s.Log.Infof("resources interface undocked (%s)", peerCommonName(r))

	// The keepalive is a bare newline, not a typed event: docked services
	// decode resourceRequestEvent and answer by correlation id, so a typed
	// heartbeat would arrive as a bogus id-0 request. json.Decoder skips
	// inter-value whitespace, making the newline invisible to every consumer
	// while still defeating NAT and middlebox idle timeouts.
	keepalive := time.NewTicker(streamKeepaliveInterval)
	defer keepalive.Stop()
	enc := json.NewEncoder(w)
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-keepalive.C:
			if _, err := w.Write([]byte("\n")); err != nil {
				return
			}
			flusher.Flush()
		case evt := <-ch:
			if err := enc.Encode(evt); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// handleResourceFulfill receives a docked service's reply to a forwarded fetch
// and routes it to the waiting request.
func (s *StatusServer) handleResourceFulfill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not configured", http.StatusServiceUnavailable)
		return
	}
	var rep resourceReply
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&rep); err != nil {
		http.Error(w, "invalid reply: "+err.Error(), http.StatusBadRequest)
		return
	}
	ctrl.FulfillResource(rep)
	w.WriteHeader(http.StatusNoContent)
}

// peerCommonName returns the mTLS client cert CN for logging (empty if none;
// RequireAndVerifyClientCert guarantees a verified cert reached the handler).
func peerCommonName(r *http.Request) string {
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		return r.TLS.PeerCertificates[0].Subject.CommonName
	}
	return "unknown"
}
