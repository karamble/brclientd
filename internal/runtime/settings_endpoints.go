// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/zkidentity"
)

// connectionOut is the wire shape of GET /connection. "online" is the last
// REQUESTED intent (BR has no getter for it; the daemon boots online),
// while "connected"/"stage" report the effective session state - the two
// can briefly disagree while a connection is being (re)established.
type connectionOut struct {
	Online      bool      `json:"online"`
	Connected   bool      `json:"connected"`
	Stage       Stage     `json:"stage"`
	ServerNode  string    `json:"server_node,omitempty"`
	ConnectedAt time.Time `json:"connected_at,omitempty"`
	Policy      policyOut `json:"policy"`
}

// handleConnection reports the connection intent + effective state and the
// server policy (GET), or flips the intent via client.GoOnline /
// client.RemainOffline (POST {online: bool}). The offline intent is
// runtime-only upstream: it does not survive a daemon restart.
func (s *StatusServer) handleConnection(w http.ResponseWriter, r *http.Request) {
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		status := s.Tracker.Get()
		s.onlineMu.RLock()
		online := !s.remainOffline
		s.onlineMu.RUnlock()
		out := connectionOut{
			Online:      online,
			Connected:   c.ServerSession() != nil,
			Stage:       status.Stage,
			ServerNode:  status.ServerNode,
			ConnectedAt: status.ConnectedAt,
		}
		if c.ServerSession() != nil {
			out.Policy = policyFromSession(c)
		} else if p, ok := s.Tracker.ServerPolicySnapshot(); ok {
			out.Policy = policyOutFrom(p)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	case http.MethodPost:
		var req struct {
			Online bool `json:"online"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Online {
			c.GoOnline()
		} else {
			c.RemainOffline()
		}
		s.onlineMu.Lock()
		s.remainOffline = !req.Online
		s.onlineMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleBehavior reports (GET) or updates (POST, partial) the runtime-changeable
// Bison Relay behavior settings. These map to client.Config fields fixed at BR
// client construction, so a POST persists to settings.json and does NOT restart;
// the new values take effect on the next daemon restart. GET returns {saved,
// effective} so the dashboard can flag settings whose saved value differs from
// the value the running daemon booted with (a pending change).
func (s *StatusServer) handleBehavior(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Saved     brBehavior `json:"saved"`
			Effective brBehavior `json:"effective"`
		}{Saved: s.Settings.behavior(), Effective: s.EffectiveBehavior})
	case http.MethodPost:
		var u brBehaviorUpdate
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.Settings.applyBehavior(u); err != nil {
			http.Error(w, "persist settings: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleKXSearches lists the outstanding KX searches (looking for a post
// author across the network via its commenters).
func (s *StatusServer) handleKXSearches(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	targets, err := c.ListKXSearches()
	if err != nil {
		http.Error(w, "list kx searches: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type searchRow struct {
		Target string `json:"target"`
		Nick   string `json:"nick"`
	}
	out := make([]searchRow, 0, len(targets))
	for _, t := range targets {
		out = append(out, searchRow{Target: t.String(), Nick: c.UserLogNick(t)})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Searches []searchRow `json:"searches"`
	}{Searches: out})
}

// handleMediateIDs lists (GET) or cancels (POST /cancel via body
// {mediator, target}) the in-flight mediated introduction requests.
func (s *StatusServer) handleMediateIDs(w http.ResponseWriter, r *http.Request) {
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		reqs, err := c.ListMediateIDs()
		if err != nil {
			http.Error(w, "list mediate ids: "+err.Error(), http.StatusInternalServerError)
			return
		}
		type midRow struct {
			Mediator     string    `json:"mediator"`
			MediatorNick string    `json:"mediator_nick"`
			Target       string    `json:"target"`
			TargetNick   string    `json:"target_nick"`
			Date         time.Time `json:"date"`
			Manual       bool      `json:"manual"`
		}
		out := make([]midRow, 0, len(reqs))
		for _, m := range reqs {
			out = append(out, midRow{
				Mediator:     m.Mediator.String(),
				MediatorNick: c.UserLogNick(m.Mediator),
				Target:       m.Target.String(),
				TargetNick:   c.UserLogNick(m.Target),
				Date:         m.Date,
				Manual:       m.Manual,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			MediateIDs []midRow `json:"mediate_ids"`
		}{MediateIDs: out})
	case http.MethodPost:
		var req struct {
			Mediator string `json:"mediator"`
			Target   string `json:"target"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		mediator, ok := parseUIDHex(w, "mediator", req.Mediator)
		if !ok {
			return
		}
		target, ok := parseUIDHex(w, "target", req.Target)
		if !ok {
			return
		}
		if err := c.CancelMediateID(mediator, target); err != nil {
			http.Error(w, "cancel mediate id: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// filterOut is the wire shape of a content filter. clientdb.ContentFilter
// carries no json tags and scopes via pointer ShortIDs, so an explicit DTO
// keeps the API snake_case with hex scope ids ("" = unscoped).
type filterOut struct {
	ID               uint64 `json:"id"`
	UID              string `json:"uid,omitempty"`
	GC               string `json:"gc,omitempty"`
	Regexp           string `json:"regexp"`
	SkipPMs          bool   `json:"skip_pms"`
	SkipGCMs         bool   `json:"skip_gcms"`
	SkipPosts        bool   `json:"skip_posts"`
	SkipPostComments bool   `json:"skip_post_comments"`
}

func filterOutFrom(cf clientdb.ContentFilter) filterOut {
	out := filterOut{
		ID:               cf.ID,
		Regexp:           cf.Regexp,
		SkipPMs:          cf.SkipPMs,
		SkipGCMs:         cf.SkipGCMs,
		SkipPosts:        cf.SkipPosts,
		SkipPostComments: cf.SkipPostComments,
	}
	if cf.UID != nil {
		out.UID = cf.UID.String()
	}
	if cf.GC != nil {
		out.GC = cf.GC.String()
	}
	return out
}

// handleFilters lists the active content filters (GET) or upserts one
// (POST). Filters suppress matching content (PMs, GC messages, posts,
// comments) before it reaches the UI; the regexp is matched against the
// message text.
func (s *StatusServer) handleFilters(w http.ResponseWriter, r *http.Request) {
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		cfs := c.ListContentFilters()
		out := make([]filterOut, len(cfs))
		for i, cf := range cfs {
			out[i] = filterOutFrom(cf)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Filters []filterOut `json:"filters"`
		}{Filters: out})
	case http.MethodPost:
		var req filterOut
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Regexp == "" {
			http.Error(w, "regexp is required", http.StatusBadRequest)
			return
		}
		// Validate here so user typos come back as 400 instead of the
		// generic store error.
		if _, err := regexp.Compile(req.Regexp); err != nil {
			http.Error(w, "invalid regexp: "+err.Error(), http.StatusBadRequest)
			return
		}
		cf := clientdb.ContentFilter{
			ID:               req.ID,
			Regexp:           req.Regexp,
			SkipPMs:          req.SkipPMs,
			SkipGCMs:         req.SkipGCMs,
			SkipPosts:        req.SkipPosts,
			SkipPostComments: req.SkipPostComments,
		}
		if req.UID != "" {
			uid, ok := parseUIDHex(w, "uid", req.UID)
			if !ok {
				return
			}
			cf.UID = &uid
		}
		if req.GC != "" {
			gcid, ok := parseUIDHex(w, "gc", req.GC)
			if !ok {
				return
			}
			cf.GC = &gcid
		}
		// StoreContentFilter upserts by ID but never assigns one, so a
		// create (id 0) must pick the next free ID itself.
		if cf.ID == 0 {
			var max uint64
			for _, existing := range c.ListContentFilters() {
				if existing.ID > max {
					max = existing.ID
				}
			}
			cf.ID = max + 1
		}
		if err := c.StoreContentFilter(&cf); err != nil {
			http.Error(w, "store filter: "+err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(filterOutFrom(cf))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleDeleteFilter removes a content filter by id.
func (s *StatusServer) handleDeleteFilter(w http.ResponseWriter, r *http.Request) {
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
		ID uint64 `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.RemoveContentFilter(req.ID); err != nil {
		http.Error(w, "remove filter: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleSubscribeAllPosts subscribes to the posts of every KX'd contact.
// The call pushes one subscription RM per contact through the send queue,
// so it can take a while on large address books; it returns once all are
// queued.
func (s *StatusServer) handleSubscribeAllPosts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.SubscribeToAllRemotePosts(nil); err != nil {
		http.Error(w, "subscribe all posts: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// kxOut is one in-flight key exchange in GET /kx/list. The peer fields are
// only known on the accepting side (the initiator has no public identity
// for the peer yet).
type kxOut struct {
	InitialRV  string `json:"initial_rv"`
	Stage      string `json:"stage"`
	IsForReset bool   `json:"is_for_reset"`
	MediatorID string `json:"mediator_id,omitempty"`
	PeerNick   string `json:"peer_nick,omitempty"`
	PeerID     string `json:"peer_id,omitempty"`
	Timestamp  int64  `json:"timestamp"`
}

// handleKXList reports the in-flight key exchanges (including reset KXs) as
// a diagnostic for the Settings tab.
func (s *StatusServer) handleKXList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	kxs, err := c.ListKXs()
	if err != nil {
		http.Error(w, "list kxs: "+err.Error(), http.StatusBadGateway)
		return
	}
	var zero zkidentity.ShortID
	out := make([]kxOut, len(kxs))
	for i, kx := range kxs {
		o := kxOut{
			InitialRV:  kx.InitialRV.String(),
			Stage:      kx.Stage.String(),
			IsForReset: kx.IsForReset,
			Timestamp:  kx.Timestamp.Unix(),
		}
		if kx.MediatorID != nil {
			o.MediatorID = kx.MediatorID.String()
		}
		if kx.Public.Identity != zero {
			o.PeerID = kx.Public.Identity.String()
			o.PeerNick = kx.Public.Nick
		}
		out[i] = o
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		KXs []kxOut `json:"kxs"`
	}{KXs: out})
}
