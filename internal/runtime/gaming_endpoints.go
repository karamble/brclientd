// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"math"
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

func onlyGamingFrames(entries []clientdb.PMLogEntry) []clientdb.PMLogEntry {
	out := entries[:0]
	for _, entry := range entries {
		if isGamingEnvelope(entry.Message) {
			out = append(out, entry)
		}
	}
	return out
}

// handleGamingHistory serves the raw protocol frames for one group chat. The
// ordinary /gc/{gcid}/history endpoint removes these before pagination; this
// endpoint is exclusively for dcrpulse's durable inbox recovery and outbox
// reconciliation.
func (s *StatusServer) handleGamingHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if s.DB == nil {
		http.Error(w, "history unavailable: clientdb not attached", http.StatusServiceUnavailable)
		return
	}
	var gcid zkidentity.ShortID
	if err := gcid.FromString(r.URL.Query().Get("gcid")); err != nil {
		http.Error(w, "invalid gcid: "+err.Error(), http.StatusBadRequest)
		return
	}
	pageSize := parsePositiveInt(r.URL.Query().Get("page_size"), 50, 500)
	pageNum := parseNonNegativeInt(r.URL.Query().Get("page"), 0)

	dbGC, err := c.GetGCDB(gcid)
	if err != nil {
		http.Error(w, "get gc: "+err.Error(), http.StatusNotFound)
		return
	}
	var entries []clientdb.PMLogEntry
	err = s.DB.View(r.Context(), func(tx clientdb.ReadTx) error {
		got, err := s.DB.ReadLogGCMsg(tx, dbGC.Name(), gcid, math.MaxInt32, 0)
		if err != nil {
			return err
		}
		entries = got
		return nil
	})
	if err != nil {
		http.Error(w, "read gc log: "+err.Error(), http.StatusInternalServerError)
		return
	}
	entries = historyPage(onlyGamingFrames(entries), pageSize, pageNum)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		GCID     string                `json:"gcid"`
		Page     int                   `json:"page"`
		PageSize int                   `json:"page_size"`
		Entries  []clientdb.PMLogEntry `json:"entries"`
	}{gcid.String(), pageNum, pageSize, entries})
}
