// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
)

// Routes (registered from status_server.go Run()):
//   GET    /gc
//   POST   /gc/create
//   GET    /gc/invites
//   POST   /gc/invites/accept
//   GET    /gc/{gcid}
//   POST   /gc/{gcid}/invite
//   POST   /gc/{gcid}/message
//   GET    /gc/{gcid}/history
//   POST   /gc/{gcid}/part
//   POST   /gc/{gcid}/kill
//   POST   /gc/{gcid}/kick
//   POST   /gc/{gcid}/block
//   POST   /gc/{gcid}/unblock
//   POST   /gc/{gcid}/admins
//   POST   /gc/{gcid}/owner
//   POST   /gc/{gcid}/upgrade
//   POST   /gc/{gcid}/alias
//   POST   /gc/{gcid}/resend-list

// handleGC dispatches the /gc surface. Mirrors rtdt.go's pattern: net/http
// ServeMux has no path-param syntax so we hang a single dispatcher off both
// /gc and /gc/, then parse the rest manually.
func (s *StatusServer) handleGC(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/gc")
	switch {
	case path == "" || path == "/":
		s.handleGCList(w, r)
	case path == "/create":
		s.handleGCCreate(w, r)
	case path == "/invites":
		s.handleGCInvitesList(w, r)
	case path == "/invites/accept":
		s.handleGCInvitesAccept(w, r)
	default:
		rest := strings.TrimPrefix(path, "/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) < 1 || parts[0] == "" {
			http.NotFound(w, r)
			return
		}
		var gcid zkidentity.ShortID
		if err := gcid.FromString(parts[0]); err != nil {
			http.Error(w, "invalid gcid: "+err.Error(), http.StatusBadRequest)
			return
		}
		action := ""
		if len(parts) == 2 {
			action = parts[1]
		}
		switch action {
		case "":
			s.handleGCDetail(w, r, gcid)
		case "invite":
			s.handleGCInvite(w, r, gcid)
		case "message":
			s.handleGCMessage(w, r, gcid)
		case "history":
			s.handleGCHistory(w, r, gcid)
		case "part":
			s.handleGCPart(w, r, gcid)
		case "kill":
			s.handleGCKill(w, r, gcid)
		case "kick":
			s.handleGCKick(w, r, gcid)
		case "block":
			s.handleGCBlock(w, r, gcid)
		case "unblock":
			s.handleGCUnblock(w, r, gcid)
		case "admins":
			s.handleGCAdmins(w, r, gcid)
		case "owner":
			s.handleGCOwner(w, r, gcid)
		case "upgrade":
			s.handleGCUpgrade(w, r, gcid)
		case "alias":
			s.handleGCAlias(w, r, gcid)
		case "resend-list":
			s.handleGCResendList(w, r, gcid)
		default:
			http.NotFound(w, r)
		}
	}
}

func (s *StatusServer) requireGCClient(w http.ResponseWriter, r *http.Request, method string) *client.Client {
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

// gcSummary is the wire shape returned by /gc and /gc/{gcid}. Keep it
// shallow so the dashboard doesn't depend on internal BR struct shapes.
type gcSummary struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Alias       string   `json:"alias,omitempty"`
	Generation  uint64   `json:"generation"`
	Version     uint8    `json:"version"`
	Owner       string   `json:"owner"`
	Members     []string `json:"members"`
	ExtraAdmins []string `json:"extra_admins,omitempty"`
	Blocked     []string `json:"blocked,omitempty"`
	LocalIsOwner bool    `json:"local_is_owner"`
	LocalIsAdmin bool    `json:"local_is_admin"`
}

func summarizeGC(c *client.Client, dbGC *clientdb.GroupChat, includeBlocklist bool) gcSummary {
	out := gcSummary{
		ID:         dbGC.Metadata.ID.String(),
		Name:       dbGC.Metadata.Name,
		Alias:      dbGC.Alias,
		Generation: dbGC.Metadata.Generation,
		Version:    dbGC.Metadata.Version,
	}
	if len(dbGC.Metadata.Members) > 0 {
		out.Owner = dbGC.Metadata.Members[0].String()
	}
	out.Members = make([]string, 0, len(dbGC.Metadata.Members))
	for _, m := range dbGC.Metadata.Members {
		out.Members = append(out.Members, m.String())
	}
	out.ExtraAdmins = make([]string, 0, len(dbGC.Metadata.ExtraAdmins))
	for _, a := range dbGC.Metadata.ExtraAdmins {
		out.ExtraAdmins = append(out.ExtraAdmins, a.String())
	}
	self := c.PublicID()
	if len(dbGC.Metadata.Members) > 0 && dbGC.Metadata.Members[0] == self {
		out.LocalIsOwner = true
		out.LocalIsAdmin = true
	}
	for _, a := range dbGC.Metadata.ExtraAdmins {
		if a == self {
			out.LocalIsAdmin = true
			break
		}
	}
	if includeBlocklist {
		if bl, err := c.GetGCBlockList(dbGC.Metadata.ID); err == nil {
			out.Blocked = make([]string, 0, len(bl))
			for k := range bl {
				out.Blocked = append(out.Blocked, k)
			}
		}
	}
	return out
}

func (s *StatusServer) handleGCList(w http.ResponseWriter, r *http.Request) {
	c := s.requireGCClient(w, r, http.MethodGet)
	if c == nil {
		return
	}
	gcs, err := c.ListGCs()
	if err != nil {
		http.Error(w, "list gcs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]gcSummary, 0, len(gcs))
	for i := range gcs {
		out = append(out, summarizeGC(c, &gcs[i], false))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		GCs []gcSummary `json:"gcs"`
	}{GCs: out})
}

func (s *StatusServer) handleGCCreate(w http.ResponseWriter, r *http.Request) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	gcid, err := c.NewGroupChat(req.Name)
	if err != nil {
		http.Error(w, "create gc: "+err.Error(), http.StatusInternalServerError)
		return
	}
	dbGC, err := c.GetGCDB(gcid)
	if err != nil {
		http.Error(w, "load created gc: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(summarizeGC(c, &dbGC, false))
}

// gcInviteSummary is the wire shape for a pending invite.
type gcInviteSummary struct {
	ID          uint64 `json:"id"`
	GCID        string `json:"gcid"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	From        string `json:"from"`
	Expires     int64  `json:"expires"`
	Version     uint8  `json:"version"`
	Accepted    bool   `json:"accepted"`
}

func (s *StatusServer) handleGCInvitesList(w http.ResponseWriter, r *http.Request) {
	c := s.requireGCClient(w, r, http.MethodGet)
	if c == nil {
		return
	}
	invites, err := c.ListGCInvitesFor(nil)
	if err != nil {
		http.Error(w, "list invites: "+err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]gcInviteSummary, 0, len(invites))
	for _, inv := range invites {
		out = append(out, gcInviteSummary{
			ID:          inv.ID,
			GCID:        inv.Invite.ID.String(),
			Name:        inv.Invite.Name,
			Description: inv.Invite.Description,
			From:        inv.User.String(),
			Expires:     inv.Invite.Expires,
			Version:     inv.Invite.Version,
			Accepted:    inv.Accepted,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Invites []gcInviteSummary `json:"invites"`
	}{Invites: out})
}

func (s *StatusServer) handleGCInvitesAccept(w http.ResponseWriter, r *http.Request) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		IID uint64 `json:"iid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.IID == 0 {
		http.Error(w, "iid is required", http.StatusBadRequest)
		return
	}
	if err := c.AcceptGroupChatInvite(req.IID); err != nil {
		http.Error(w, "accept: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCDetail(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodGet)
	if c == nil {
		return
	}
	dbGC, err := c.GetGCDB(gcid)
	if err != nil {
		http.Error(w, "get gc: "+err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(summarizeGC(c, &dbGC, true))
}

func (s *StatusServer) handleGCInvite(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		UID string `json:"uid"`
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
	if err := c.InviteToGroupChat(gcid, uid); err != nil {
		http.Error(w, "invite: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCMessage(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		Message string           `json:"message"`
		Mode    rpc.MessageMode  `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Message == "" {
		http.Error(w, "message is required", http.StatusBadRequest)
		return
	}
	// Block until the broadcast loop completes. v1 ignores per-member
	// progress; the dashboard treats GC send as fire-and-forget.
	if err := c.GCMessage(gcid, req.Message, req.Mode, nil); err != nil {
		http.Error(w, "send: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCHistory(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodGet)
	if c == nil {
		return
	}
	if s.DB == nil {
		http.Error(w, "history unavailable: clientdb not attached", http.StatusServiceUnavailable)
		return
	}
	pageSize := parsePositiveInt(r.URL.Query().Get("page_size"), 50, 500)
	pageNum := parseNonNegativeInt(r.URL.Query().Get("page"), 0)

	// ReadLogGCMsg keys the log file by GC name + ID, so fetch the
	// metadata first.
	dbGC, err := c.GetGCDB(gcid)
	if err != nil {
		http.Error(w, "get gc: "+err.Error(), http.StatusNotFound)
		return
	}
	gcName := dbGC.Metadata.Name

	var entries []clientdb.PMLogEntry
	err = s.DB.View(r.Context(), func(tx clientdb.ReadTx) error {
		got, err := s.DB.ReadLogGCMsg(tx, gcName, gcid, pageSize, pageNum)
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		GCID     string                `json:"gcid"`
		Page     int                   `json:"page"`
		PageSize int                   `json:"page_size"`
		Entries  []clientdb.PMLogEntry `json:"entries"`
	}{
		GCID:     gcid.String(),
		Page:     pageNum,
		PageSize: pageSize,
		Entries:  entries,
	})
}

func (s *StatusServer) handleGCPart(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if err := c.PartFromGC(gcid, req.Reason); err != nil {
		http.Error(w, "part: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCKill(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if err := c.KillGroupChat(gcid, req.Reason); err != nil {
		http.Error(w, "kill: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCKick(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
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
	if err := c.GCKick(gcid, uid, req.Reason); err != nil {
		http.Error(w, "kick: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCBlock(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		UID string `json:"uid"`
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
	if err := c.AddToGCBlockList(gcid, uid); err != nil {
		http.Error(w, "block: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCUnblock(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		UID string `json:"uid"`
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
	if err := c.RemoveFromGCBlockList(gcid, uid); err != nil {
		http.Error(w, "unblock: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCAdmins(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		ExtraAdmins []string `json:"extra_admins"`
		Reason      string   `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	admins := make([]zkidentity.ShortID, 0, len(req.ExtraAdmins))
	for _, s := range req.ExtraAdmins {
		var id zkidentity.ShortID
		if err := id.FromString(s); err != nil {
			http.Error(w, fmt.Sprintf("invalid admin uid %q: %v", s, err), http.StatusBadRequest)
			return
		}
		admins = append(admins, id)
	}
	if err := c.ModifyGCAdmins(gcid, admins, req.Reason); err != nil {
		http.Error(w, "modify admins: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCOwner(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		NewOwner string `json:"new_owner"`
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var newOwner clientintf.UserID
	if err := newOwner.FromString(req.NewOwner); err != nil {
		http.Error(w, "invalid new_owner: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.ModifyGCOwner(gcid, newOwner, req.Reason); err != nil {
		http.Error(w, "modify owner: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCUpgrade(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		NewVersion uint8 `json:"new_version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.UpgradeGC(gcid, req.NewVersion); err != nil {
		http.Error(w, "upgrade: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCAlias(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		Alias string `json:"alias"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.AliasGC(gcid, req.Alias); err != nil {
		http.Error(w, "alias: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCResendList(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireGCClient(w, r, http.MethodPost)
	if c == nil {
		return
	}
	var req struct {
		UID string `json:"uid,omitempty"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	var uidPtr *clientintf.UserID
	if req.UID != "" {
		var uid clientintf.UserID
		if err := uid.FromString(req.UID); err != nil {
			http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
			return
		}
		uidPtr = &uid
	}
	if err := c.ResendGCList(gcid, uidPtr); err != nil {
		http.Error(w, "resend list: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
