// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
)

// Routes (registered from status_server.go Run()):
//   GET    /rtdt/sessions
//   POST   /rtdt/sessions/create
//   POST   /rtdt/sessions/create-instant
//   POST   /rtdt/sessions/{rv}/invite
//   POST   /rtdt/sessions/{rv}/accept
//   POST   /rtdt/sessions/{rv}/join
//   POST   /rtdt/sessions/{rv}/leave
//   POST   /rtdt/sessions/{rv}/dissolve
//   POST   /rtdt/sessions/{rv}/kick
//   POST   /rtdt/sessions/{rv}/remove
//   POST   /rtdt/sessions/{rv}/rotate-cookies
//
// All POST endpoints take JSON bodies. The {rv} path parameter is the
// 64-char hex of the session RV. The /sessions endpoint returns the BR
// db.RTDTSession objects plus a live flag per session.

// rtdtRouteHandler dispatches /rtdt/sessions* requests. We register a single
// HandleFunc on "/rtdt/sessions" and one on "/rtdt/sessions/" to cover both
// the bare list endpoint and the per-session routes, since net/http's
// ServeMux doesn't pattern-match path params.
func (s *StatusServer) handleRTDT(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/rtdt/sessions")
	switch {
	case path == "" || path == "/":
		s.handleRTDTList(w, r)
	case path == "/create":
		s.handleRTDTCreate(w, r)
	case path == "/create-instant":
		s.handleRTDTCreateInstant(w, r)
	default:
		// /<rv>/<action>
		rest := strings.TrimPrefix(path, "/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			http.NotFound(w, r)
			return
		}
		var rv zkidentity.ShortID
		if err := rv.FromString(parts[0]); err != nil {
			http.Error(w, "invalid session RV: "+err.Error(), http.StatusBadRequest)
			return
		}
		switch parts[1] {
		case "invite":
			s.handleRTDTInvite(w, r, rv)
		case "accept":
			s.handleRTDTAccept(w, r, rv)
		case "join":
			s.handleRTDTJoin(w, r, rv)
		case "leave":
			s.handleRTDTLeave(w, r, rv)
		case "dissolve":
			s.handleRTDTDissolve(w, r, rv)
		case "kick":
			s.handleRTDTKick(w, r, rv)
		case "remove":
			s.handleRTDTRemove(w, r, rv)
		case "rotate-cookies":
			s.handleRTDTRotateCookies(w, r, rv)
		case "audio":
			s.handleRTDTAudioWS(w, r, rv)
		case "messages":
			s.handleRTDTMessages(w, r, rv)
		case "chat":
			s.handleRTDTChat(w, r, rv)
		default:
			http.NotFound(w, r)
		}
	}
}

// handleRTDTMessages returns the chat messages tracked for a live session
// (TrackRTDTChatMessages is enabled in the client config; the buffer lives
// only for the session's lifetime).
func (s *StatusServer) handleRTDTMessages(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodGet)
	if c == nil {
		return
	}
	msgs := c.GetRTDTMessages(rv)
	type chatMsg struct {
		PeerID    uint32 `json:"peer_id"`
		Message   string `json:"message"`
		Timestamp int64  `json:"timestamp"`
	}
	out := make([]chatMsg, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, chatMsg{
			PeerID:    uint32(m.SourceID),
			Message:   m.Message,
			Timestamp: m.Timestamp,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Messages []chatMsg `json:"messages"`
	}{Messages: out})
}

// handleRTDTChat sends a text message into a live session. Body: {message}.
func (s *StatusServer) handleRTDTChat(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Message = strings.TrimSpace(req.Message)
	if req.Message == "" {
		http.Error(w, "message is required", http.StatusBadRequest)
		return
	}
	if err := c.SendRTDTChatMsg(rv, req.Message); err != nil {
		http.Error(w, "send chat: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// requireRTDTClient is the standard "BR client up?" guard plus method check.
func (s *StatusServer) requireRTDTClient(w http.ResponseWriter, r *http.Request, method string) *client.Client {
	if r.Method != method {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return nil
	}
	return c
}

// sessionSummary is the wire shape returned by /rtdt/sessions. It carries
// just the fields the dashboard needs to render the room list; the full
// AppointCookie / SessionCookie / OwnerSecret are kept server-side.
type rtdtSessionSummary struct {
	RV          string                  `json:"rv"`
	Description string                  `json:"description"`
	Size        uint32                  `json:"size"`
	Owner       string                  `json:"owner"`
	IsInstant   bool                    `json:"is_instant"`
	LocalPeerID uint32                  `json:"local_peer_id"`
	IsAdmin     bool                    `json:"is_admin"`
	Live        bool                    `json:"live"`
	HotAudio    bool                    `json:"hot_audio"`
	Members     []rtdtMemberSummary     `json:"members"`
	Publishers  []rtdtPublisherSummary  `json:"publishers"`
	LivePeers   []rtdtLivePeerSummary   `json:"live_peers,omitempty"`
}

type rtdtMemberSummary struct {
	UID       string `json:"uid"`
	PeerID    uint32 `json:"peer_id"`
	Publisher bool   `json:"publisher"`
	Accepted  bool   `json:"accepted"`
}

type rtdtPublisherSummary struct {
	UID    string `json:"uid"`
	PeerID uint32 `json:"peer_id"`
	Alias  string `json:"alias"`
}

type rtdtLivePeerSummary struct {
	PeerID         uint32 `json:"peer_id"`
	HasSoundStream bool   `json:"has_sound_stream"`
	HasSound       bool   `json:"has_sound"`
}

func summarizeSession(c *client.Client, sess *clientdb.RTDTSession) rtdtSessionSummary {
	live := c.GetLiveRTSession(&sess.Metadata.RV)
	out := rtdtSessionSummary{
		RV:          sess.Metadata.RV.String(),
		Description: sess.Metadata.Description,
		Size:        sess.Metadata.Size,
		Owner:       sess.Metadata.Owner.String(),
		IsInstant:   sess.Metadata.IsInstant,
		LocalPeerID: uint32(sess.LocalPeerID),
		IsAdmin:     sess.LocalIsAdmin(),
		Live:        live != nil,
		Members:     make([]rtdtMemberSummary, 0, len(sess.Members)),
		Publishers:  make([]rtdtPublisherSummary, 0, len(sess.Metadata.Publishers)),
	}
	for _, m := range sess.Members {
		out.Members = append(out.Members, rtdtMemberSummary{
			UID:       m.UID.String(),
			PeerID:    uint32(m.PeerID),
			Publisher: m.Publisher,
			Accepted:  m.AcceptedTimestamp != nil,
		})
	}
	for _, p := range sess.Metadata.Publishers {
		out.Publishers = append(out.Publishers, rtdtPublisherSummary{
			UID:    p.PublisherID.String(),
			PeerID: uint32(p.PeerID),
			Alias:  p.Alias,
		})
	}
	if live != nil {
		out.HotAudio = live.HotAudio
		out.LivePeers = make([]rtdtLivePeerSummary, 0, len(live.Peers))
		for pid, p := range live.Peers {
			out.LivePeers = append(out.LivePeers, rtdtLivePeerSummary{
				PeerID:         uint32(pid),
				HasSoundStream: p.HasSoundStream,
				HasSound:       p.HasSound,
			})
		}
	}
	return out
}

func (s *StatusServer) handleRTDTList(w http.ResponseWriter, r *http.Request) {
	c := s.requireRTDTClient(w, r, http.MethodGet)
	if c == nil {
		return
	}
	rvs := c.ListRTDTSessions()
	out := make([]rtdtSessionSummary, 0, len(rvs))
	for i := range rvs {
		rv := rvs[i]
		sess, err := c.GetRTDTSession(&rv)
		if err != nil || sess == nil {
			continue
		}
		out = append(out, summarizeSession(c, sess))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Sessions []rtdtSessionSummary `json:"sessions"`
	}{Sessions: out})
}

func (s *StatusServer) handleRTDTCreate(w http.ResponseWriter, r *http.Request) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		Size        uint16 `json:"size"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Size < 2 {
		http.Error(w, "size must be at least 2", http.StatusBadRequest)
		return
	}
	sess, err := c.CreateRTDTSession(req.Size, req.Description)
	if err != nil {
		http.Error(w, "create session: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(summarizeSession(c, sess))
}

func (s *StatusServer) handleRTDTCreateInstant(w http.ResponseWriter, r *http.Request) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		UIDs []string `json:"uids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.UIDs) == 0 {
		http.Error(w, "uids is required", http.StatusBadRequest)
		return
	}
	users := make([]clientintf.UserID, 0, len(req.UIDs))
	for _, u := range req.UIDs {
		var id clientintf.UserID
		if err := id.FromString(u); err != nil {
			http.Error(w, fmt.Sprintf("invalid uid %q: %v", u, err), http.StatusBadRequest)
			return
		}
		users = append(users, id)
	}
	sess, err := c.CreateInstantRTDTSession(users)
	if err != nil {
		http.Error(w, "create instant session: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(summarizeSession(c, sess))
}

func (s *StatusServer) handleRTDTInvite(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		UIDs        []string `json:"uids"`
		AsPublisher bool     `json:"as_publisher"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.UIDs) == 0 {
		http.Error(w, "uids is required", http.StatusBadRequest)
		return
	}
	users := make([]clientintf.UserID, 0, len(req.UIDs))
	for _, u := range req.UIDs {
		var id clientintf.UserID
		if err := id.FromString(u); err != nil {
			http.Error(w, fmt.Sprintf("invalid uid %q: %v", u, err), http.StatusBadRequest)
			return
		}
		users = append(users, id)
	}
	if err := c.InviteToRTDTSession(rv, req.AsPublisher, users...); err != nil {
		http.Error(w, "invite: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleRTDTAccept(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		Inviter     string `json:"inviter"`
		AsPublisher bool   `json:"as_publisher"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var inviter clientintf.UserID
	if err := inviter.FromString(req.Inviter); err != nil {
		http.Error(w, "invalid inviter: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.AcceptRTDTSessionInviteByRV(inviter, rv, req.AsPublisher); err != nil {
		http.Error(w, "accept: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleRTDTJoin(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	if s.Log != nil {
		s.Log.Infof("RTDT join: calling JoinLiveRTDTSession rv=%s", rv.ShortLogID())
	}
	t0 := time.Now()
	err := c.JoinLiveRTDTSession(rv)
	dt := time.Since(t0)
	if err != nil {
		if s.Log != nil {
			s.Log.Warnf("RTDT join: failed rv=%s dur=%s err=%v", rv.ShortLogID(), dt, err)
		}
		http.Error(w, "join: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if s.Log != nil {
		s.Log.Infof("RTDT join: success rv=%s dur=%s", rv.ShortLogID(), dt)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleRTDTLeave(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	if err := c.ExitRTDTSession(&rv); err != nil {
		http.Error(w, "leave: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleRTDTDissolve(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	if err := c.DissolveRTDTSession(&rv); err != nil {
		http.Error(w, "dissolve: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleRTDTKick(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		PeerID     uint32 `json:"peer_id"`
		BanSeconds int64  `json:"ban_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	ban := time.Duration(req.BanSeconds) * time.Second
	if err := c.KickFromLiveRTDTSession(&rv, rpc.RTDTPeerID(req.PeerID), ban); err != nil {
		http.Error(w, "kick: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleRTDTRemove(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		UID    string `json:"uid"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var uid clientintf.UserID
	if err := uid.FromString(req.UID); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.RemoveRTDTMember(&rv, &uid, req.Reason); err != nil {
		http.Error(w, "remove: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleRTDTRotateCookies(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	c := s.requireRTDTClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	if err := c.RotateRTDTAppointmentCookies(&rv); err != nil {
		http.Error(w, "rotate cookies: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

