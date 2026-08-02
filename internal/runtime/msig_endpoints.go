// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"

	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/zkidentity"

	"github.com/karamble/brclientd/internal/msig"
)

// handleMsigHistory serves the shared-wallet coordination frames exchanged
// with one contact, both directions, oldest first. It is the dashboard's
// replay source after downtime: live delivery rides the notification
// stream, which drops events for slow consumers, while the PM log already
// records every frame (LogPM runs before notifications fire, and locally
// sent frames are logged by the send path).
//
//	GET /msig/history?uid=<hex>&limit=N&since=<unixsecs>
//
// limit keeps the newest N frames after filtering (default 200, max 1000);
// since drops frames older than the given unix timestamp first. local_nick
// lets the caller tell its own frames apart from the peer's.
func (s *StatusServer) handleMsigHistory(w http.ResponseWriter, r *http.Request) {
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
	limit := parsePositiveInt(r.URL.Query().Get("limit"), 200, 1000)
	since := parseNonNegativeInt(r.URL.Query().Get("since"), 0)

	var entries []clientdb.PMLogEntry
	err := s.DB.View(r.Context(), func(tx clientdb.ReadTx) error {
		got, err := s.DB.ReadLogPM(tx, uid, math.MaxInt32, 0)
		if err != nil {
			return err
		}
		entries = got
		return nil
	})
	if errors.Is(err, clientdb.ErrNotFound) {
		entries = nil
		err = nil
	}
	if err != nil {
		http.Error(w, "read pm log: "+err.Error(), http.StatusInternalServerError)
		return
	}

	frames := entries[:0]
	for _, e := range entries {
		if !msig.IsEnvelope(e.Message) {
			continue
		}
		if since > 0 && e.Timestamp < int64(since) {
			continue
		}
		frames = append(frames, e)
	}
	if len(frames) > limit {
		frames = frames[len(frames)-limit:]
	}

	localNick := ""
	if c := s.currentClient(); c != nil {
		localNick = c.LocalNick()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		UID       string                `json:"uid"`
		LocalNick string                `json:"local_nick"`
		Entries   []clientdb.PMLogEntry `json:"entries"`
	}{uidStr, localNick, frames})
}
