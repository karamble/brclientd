// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"net/http"
	"sort"
)

// The BR-MCP client engine's dashboard surface: settings round-trip,
// pending payment approvals, and the spend log.

func (s *StatusServer) mcpEngineOr503(w http.ResponseWriter) *mcpEngine {
	s.mcpEngMu.Lock()
	e := s.mcpEng
	s.mcpEngMu.Unlock()
	if e == nil {
		http.Error(w, "MCP engine not yet running", http.StatusServiceUnavailable)
		return nil
	}
	return e
}

// SetMCPEngine wires the BR-MCP client engine once the runtime built it.
func (s *StatusServer) SetMCPEngine(e *mcpEngine) {
	s.mcpEngMu.Lock()
	defer s.mcpEngMu.Unlock()
	s.mcpEng = e
}

func mcpWriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// handleMCPSettings serves GET/POST /settings/mcpclient.
func (s *StatusServer) handleMCPSettings(w http.ResponseWriter, r *http.Request) {
	e := s.mcpEngineOr503(w)
	if e == nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		mcpWriteJSON(w, e.currentSettings())
	case http.MethodPost:
		var req mcpClientSettings
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := e.applySettings(req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mcpWriteJSON(w, e.currentSettings())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleMCPPending serves GET /mcp/pending: payments parked for approval.
func (s *StatusServer) handleMCPPending(w http.ResponseWriter, r *http.Request) {
	e := s.mcpEngineOr503(w)
	if e == nil {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	list := e.pendingList()
	sort.Slice(list, func(i, j int) bool { return list[i].Created < list[j].Created })
	mcpWriteJSON(w, struct {
		Pending []*mcpPending `json:"pending"`
	}{Pending: list})
}

// handleMCPPendingResolve serves POST /mcp/pending/resolve {id, approve}.
func (s *StatusServer) handleMCPPendingResolve(w http.ResponseWriter, r *http.Request) {
	e := s.mcpEngineOr503(w)
	if e == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
	if !e.resolvePending(req.ID, req.Approve) {
		http.Error(w, "no such pending payment", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleMCPSpend serves GET /mcp/spend: the recorded payments plus the
// rolling 24h total the daily cap is enforced against.
func (s *StatusServer) handleMCPSpend(w http.ResponseWriter, r *http.Request) {
	e := s.mcpEngineOr503(w)
	if e == nil {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	entries, today := e.spendSummary()
	if entries == nil {
		entries = []mcpSpendEntry{}
	}
	mcpWriteJSON(w, struct {
		Entries    []mcpSpendEntry `json:"entries"`
		TodayAtoms int64           `json:"today_atoms"`
	}{Entries: entries, TodayAtoms: today})
}
