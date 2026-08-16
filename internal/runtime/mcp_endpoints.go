// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"net/http"

	"github.com/karamble/brmcp/bridge"
)

// The BR-MCP client bridge's dashboard surface: settings round-trip,
// pending payment approvals, and the spend log.

func (s *StatusServer) mcpBridgeOr503(w http.ResponseWriter) *bridge.Bridge {
	s.mcpEngMu.Lock()
	b := s.mcpEng
	s.mcpEngMu.Unlock()
	if b == nil {
		http.Error(w, "MCP engine not yet running", http.StatusServiceUnavailable)
		return nil
	}
	return b
}

// SetMCPEngine wires the BR-MCP client bridge once the runtime built it.
func (s *StatusServer) SetMCPEngine(b *bridge.Bridge) {
	s.mcpEngMu.Lock()
	defer s.mcpEngMu.Unlock()
	s.mcpEng = b
}

func mcpWriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// mcpSettingsReply is the settings reply plus the listener's most recent
// allowed-IP denial (in-memory, cleared by the agent's next successful
// request) so the dashboard can offer the observed address for allowing.
type mcpSettingsReply struct {
	bridge.Settings
	LastDenied *bridge.DeniedAttempt `json:"last_denied,omitempty"`
}

// handleMCPSettings serves GET/POST /settings/mcpclient.
func (s *StatusServer) handleMCPSettings(w http.ResponseWriter, r *http.Request) {
	b := s.mcpBridgeOr503(w)
	if b == nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		mcpWriteJSON(w, mcpSettingsReply{Settings: b.Settings(), LastDenied: b.LastDenied()})
	case http.MethodPost:
		var req bridge.Settings
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := b.ApplySettings(req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mcpWriteJSON(w, mcpSettingsReply{Settings: b.Settings(), LastDenied: b.LastDenied()})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleMCPPending serves GET /mcp/pending: payments parked for approval.
func (s *StatusServer) handleMCPPending(w http.ResponseWriter, r *http.Request) {
	b := s.mcpBridgeOr503(w)
	if b == nil {
		return
	}
	mcpWriteJSON(w, struct {
		Pending []bridge.PendingPayment `json:"pending"`
	}{Pending: b.PendingPayments()})
}

// handleMCPPendingResolve serves POST /mcp/pending/resolve {id, approve}.
func (s *StatusServer) handleMCPPendingResolve(w http.ResponseWriter, r *http.Request) {
	b := s.mcpBridgeOr503(w)
	if b == nil {
		return
	}
	var req struct {
		ID      string `json:"id"`
		Approve bool   `json:"approve"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	if !b.ResolvePayment(req.ID, req.Approve) {
		http.Error(w, "no such pending payment", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleMCPSpend serves GET /mcp/spend: the recorded payments plus the
// rolling 24h total the daily cap is enforced against.
func (s *StatusServer) handleMCPSpend(w http.ResponseWriter, r *http.Request) {
	b := s.mcpBridgeOr503(w)
	if b == nil {
		return
	}
	entries, today := b.SpendLog()
	if entries == nil {
		entries = []bridge.SpendEntry{}
	}
	mcpWriteJSON(w, struct {
		Entries    []bridge.SpendEntry `json:"entries"`
		TodayAtoms int64               `json:"today_atoms"`
	}{Entries: entries, TodayAtoms: today})
}
