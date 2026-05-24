// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	goruntime "runtime"
	"strings"
)

// client_endpoints.go exposes a handful of BR client methods over the status
// server that were previously reachable only through BR's generic clientrpc
// (VersionService.Version, ChatService.{UserPublicIdentity,PM,WriteNewInvite,
// AcceptInvite}, PaymentsService.TipUser). Mirroring them here keeps the status
// surface a complete, self-contained REST API for any consumer.

// handleVersion reports the daemon name/version triple, replicating clientrpc
// VersionService.Version.
func (s *StatusServer) handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := struct {
		AppName    string `json:"appName"`
		AppVersion string `json:"appVersion"`
		GoRuntime  string `json:"goRuntime"`
	}{
		AppName:    s.AppName,
		AppVersion: s.AppVersion,
		GoRuntime:  goruntime.Version(),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handlePublicIdentity returns the local user's public identity, replicating
// clientrpc ChatService.UserPublicIdentity. Identity and sig key are emitted
// base64-encoded to match the clientrpc wire shape consumers already parse.
func (s *StatusServer) handlePublicIdentity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	pub := c.Public()
	avatar := ""
	if len(pub.Avatar) > 0 {
		avatar = base64.StdEncoding.EncodeToString(pub.Avatar)
	}
	resp := struct {
		Name     string `json:"name"`
		Nick     string `json:"nick"`
		Identity string `json:"identity"`
		SigKey   string `json:"sigKey"`
		Avatar   string `json:"avatar"`
	}{
		Name:     pub.Name,
		Nick:     pub.Nick,
		Identity: base64.StdEncoding.EncodeToString(pub.Identity[:]),
		SigKey:   base64.StdEncoding.EncodeToString(pub.SigKey[:]),
		Avatar:   avatar,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleSetAvatar sets or clears the local user's avatar via
// client.UpdateLocalAvatar. Body: {avatar} base64-encoded image bytes; an
// empty string clears it. BR caps avatars at 200KiB and broadcasts the change
// to all contacts (RMProfileUpdate).
func (s *StatusServer) handleSetAvatar(w http.ResponseWriter, r *http.Request) {
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
		Avatar string `json:"avatar"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var avatar []byte
	if strings.TrimSpace(req.Avatar) != "" {
		raw, err := base64.StdEncoding.DecodeString(req.Avatar)
		if err != nil {
			http.Error(w, "decode avatar: "+err.Error(), http.StatusBadRequest)
			return
		}
		avatar = raw
	}
	if err := c.UpdateLocalAvatar(avatar); err != nil {
		http.Error(w, "update avatar: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleSendMessage sends a private message to a user, replicating clientrpc
// ChatService.PM. user may be a nick, alias, or hex peer UID (UserByNick
// resolves all three).
func (s *StatusServer) handleSendMessage(w http.ResponseWriter, r *http.Request) {
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
		User    string `json:"user"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.User = strings.TrimSpace(req.User)
	if req.User == "" {
		http.Error(w, "user is required", http.StatusBadRequest)
		return
	}
	user, err := c.UserByNick(req.User)
	if err != nil {
		http.Error(w, "resolve user: "+err.Error(), http.StatusBadGateway)
		return
	}
	if err := c.PM(user.ID(), req.Message); err != nil {
		http.Error(w, "send pm: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleCreateInvite creates an OOB prepaid invite, replicating clientrpc
// ChatService.WriteNewInvite. Returns both the raw invite blob (base64) and
// the bech32 brpik1 key; sharing either KXs a peer to this node.
func (s *StatusServer) handleCreateInvite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	var b bytes.Buffer
	_, key, err := c.CreatePrepaidInvite(&b, nil)
	if err != nil {
		http.Error(w, "create invite: "+err.Error(), http.StatusInternalServerError)
		return
	}
	encKey, err := key.Encode()
	if err != nil {
		http.Error(w, "encode invite key: "+err.Error(), http.StatusInternalServerError)
		return
	}
	resp := struct {
		InviteBytes string `json:"inviteBytes"`
		InviteKey   string `json:"inviteKey"`
	}{
		InviteBytes: base64.StdEncoding.EncodeToString(b.Bytes()),
		InviteKey:   encKey,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleAcceptInvite reads and accepts a previously-shared OOB invite blob,
// replicating clientrpc ChatService.AcceptInvite. inviteBytes is base64.
func (s *StatusServer) handleAcceptInvite(w http.ResponseWriter, r *http.Request) {
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
		InviteBytes string `json:"inviteBytes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.InviteBytes))
	if err != nil {
		http.Error(w, "decode invite bytes: "+err.Error(), http.StatusBadRequest)
		return
	}
	invite, err := c.ReadInvite(bytes.NewReader(raw))
	if err != nil {
		http.Error(w, "read invite: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.AcceptInvite(invite); err != nil {
		http.Error(w, "accept invite: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleTip sends a DCR tip to a user over LN, replicating clientrpc
// PaymentsService.TipUser. The terminal outcome is surfaced via the
// notifications stream; this returns once the attempt is queued.
func (s *StatusServer) handleTip(w http.ResponseWriter, r *http.Request) {
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
		User        string  `json:"user"`
		DCRAmount   float64 `json:"dcrAmount"`
		MaxAttempts int32   `json:"maxAttempts"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.User = strings.TrimSpace(req.User)
	if req.User == "" {
		http.Error(w, "user is required", http.StatusBadRequest)
		return
	}
	user, err := c.UserByNick(req.User)
	if err != nil {
		http.Error(w, "resolve user: "+err.Error(), http.StatusBadGateway)
		return
	}
	if err := c.TipUser(user.ID(), req.DCRAmount, req.MaxAttempts); err != nil {
		http.Error(w, "tip user: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
