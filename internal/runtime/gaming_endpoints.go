// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"net/http"

	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/zkidentity"
	gamingwire "github.com/karamble/dcrgaming-sdk/pkg/gaming/wire"
)

// Keep recognition tied to the public, game-independent wire package. A host
// must hide frames for unknown games too, without interpreting their payloads.
const gamingSampleEnvelope = gamingwire.SampleEnvelope

func isGamingEnvelope(text string) bool { return gamingwire.IsEnvelope(text) }

func withoutGamingFrames(entries []clientdb.PMLogEntry) []clientdb.PMLogEntry {
	out := entries[:0]
	for _, entry := range entries {
		if !isGamingEnvelope(entry.Message) {
			out = append(out, entry)
		}
	}
	return out
}

// handleGamingHistory serves one group chat's protocol frames from the gaming
// journal, with each sender's authenticated UID. GET pages it newest first;
// DELETE prunes the group once the dashboard has settled its funds. The
// ordinary /gc/{gcid}/history endpoint never shows these frames.
func (s *StatusServer) handleGamingHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.GamingJournal == nil {
		http.Error(w, "gaming journal unavailable", http.StatusServiceUnavailable)
		return
	}
	var gcid zkidentity.ShortID
	if err := gcid.FromString(r.URL.Query().Get("gcid")); err != nil {
		http.Error(w, "invalid gcid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if r.Method == http.MethodDelete {
		removed, err := s.GamingJournal.prune(gcid.String())
		if err != nil {
			http.Error(w, "prune gaming journal: "+err.Error(), http.StatusInternalServerError)
			return
		}
		s.Log.Infof("Pruned %d gaming frames of settled group %s", removed, gcid)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	pageSize := parsePositiveInt(r.URL.Query().Get("page_size"), 50, 500)
	pageNum := parseNonNegativeInt(r.URL.Query().Get("page"), 0)
	entries, err := s.GamingJournal.history(gcid.String())
	if err != nil {
		http.Error(w, "read gaming journal: "+err.Error(), http.StatusInternalServerError)
		return
	}
	entries = historyPage(entries, pageSize, pageNum)
	if entries == nil {
		entries = []gamingJournalEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		GCID     string               `json:"gcid"`
		Page     int                  `json:"page"`
		PageSize int                  `json:"page_size"`
		Entries  []gamingJournalEntry `json:"entries"`
	}{gcid.String(), pageNum, pageSize, entries})
}
