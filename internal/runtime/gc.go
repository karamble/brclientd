// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/karamble/brclientd/internal/gaming"
)

// gcidHandler binds {gcid}. PathValue is already decoded, so FromString's
// 64-hex check is what rejects a value carrying a slash.
func (s *StatusServer) gcidHandler(h func(http.ResponseWriter, *http.Request, zkidentity.ShortID)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var gcid zkidentity.ShortID
		if err := gcid.FromString(r.PathValue("gcid")); err != nil {
			http.Error(w, "invalid gcid: "+err.Error(), http.StatusBadRequest)
			return
		}
		h(w, r, gcid)
	}
}

// gcSummary is the wire shape returned by /gc and /gc/{gcid}. Keep it
// shallow so the dashboard doesn't depend on internal BR struct shapes.
type gcSummary struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Alias        string   `json:"alias,omitempty"`
	Generation   uint64   `json:"generation"`
	Version      uint8    `json:"version"`
	Owner        string   `json:"owner"`
	Members      []string `json:"members"`
	ExtraAdmins  []string `json:"extra_admins,omitempty"`
	Blocked      []string `json:"blocked,omitempty"`
	LocalIsOwner bool     `json:"local_is_owner"`
	LocalIsAdmin bool     `json:"local_is_admin"`
	// LocalIsMember is false once the local client has been removed from the GC
	// (the BR client keeps the GC entry but drops us from the member list); the
	// dashboard uses this to mark a GC we were kicked from.
	LocalIsMember bool `json:"local_is_member"`
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
	for _, m := range dbGC.Metadata.Members {
		if m == self {
			out.LocalIsMember = true
			break
		}
	}
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	// Blocked re-invites recorded by the OnRMReceived hook. Prune lazily:
	// once the local GC is gone (user left or killed it outside this
	// process) the entry is moot and the next invite will store normally.
	var blocked []gcBlockedReinvite
	if s.Reinvites != nil {
		for _, e := range s.Reinvites.List() {
			var gcid zkidentity.ShortID
			if err := gcid.FromString(e.GCID); err != nil {
				s.Reinvites.Clear(e.GCID)
				continue
			}
			if _, err := c.GetGCDB(gcid); errors.Is(err, clientdb.ErrNotFound) {
				s.Reinvites.Clear(e.GCID)
				continue
			}
			blocked = append(blocked, e)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Invites          []gcInviteSummary   `json:"invites"`
		BlockedReinvites []gcBlockedReinvite `json:"blocked_reinvites"`
	}{Invites: out, BlockedReinvites: blocked})
}

func (s *StatusServer) handleGCInvitesAccept(w http.ResponseWriter, r *http.Request) {
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
	if c == nil {
		return
	}
	var req struct {
		Message string          `json:"message"`
		Mode    rpc.MessageMode `json:"mode"`
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

// handleGCClearHistory permanently deletes the local scrollback (and inline
// media) for one group chat. dcrpulse-original, mirroring handleClearPMHistory:
// BR exposes no clear-history API, so this operates directly on the on-disk
// message store. Membership, the group itself and the ability to keep chatting
// are untouched; only this client's copy goes, and the log is recreated on the
// next message. Irreversible. Pure filesystem, so it works without a live BR
// client.
func (s *StatusServer) handleGCClearHistory(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	if s.MsgsRoot == "" {
		http.Error(w, "history paths not configured", http.StatusServiceUnavailable)
		return
	}
	// Glob by gcid, not name: the log filename embeds the GC's display name,
	// which changes on rename/alias, so a group can have several historical
	// log files (the same reason gcNameFromMsgLog has to scan for them).
	logs, err := filepath.Glob(filepath.Join(s.MsgsRoot, "groupchat.*."+gcid.String()+".log"))
	if err != nil {
		http.Error(w, "glob gc logs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for _, f := range logs {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			http.Error(w, "remove gc log: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if dir := s.chatEmbedsDir(gcid); dir != "" {
		if err := os.RemoveAll(dir); err != nil {
			http.Error(w, "remove embeds: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCHistory(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireClient(w)
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
	// metadata first. The writers key by GroupChat.Name() (local alias
	// when set, e.g. "trading_1" on a name collision; see bisonrelay
	// client_groupchat.go:1075 logging by GCAlias), so the reader must
	// resolve the same way or aliased GCs read an empty history.
	dbGC, err := c.GetGCDB(gcid)
	if err != nil {
		http.Error(w, "get gc: "+err.Error(), http.StatusNotFound)
		return
	}
	gcName := dbGC.Name()

	// Fetch the whole log, not one raw page of it: filtered messages are
	// dropped below, and paging raw entries first would serve empty pages
	// whenever the newest stretch of the log is dominated by dropped
	// entries. clientdb parses the entire file per call regardless, so
	// reading it all costs nothing extra.
	var entries []clientdb.PMLogEntry
	err = s.DB.View(r.Context(), func(tx clientdb.ReadTx) error {
		got, err := s.DB.ReadLogGCMsg(tx, gcName, gcid, math.MaxInt32, 0)
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
	// Apply active content filters to served history. The BR library filters GC
	// messages only on the live receive path, so messages stored before a rule
	// was added remain in the log; drop matches here. PMLogEntry carries no
	// sender uid, so use the zero uid: global/gc-scoped rules still match on
	// content (uid-scoped rules are skipped, a safe no-op).
	// Gaming envelope frames are protocol traffic, not chat; they are hidden
	// unconditionally so no user filter is ever needed for them.
	var zeroUID clientintf.UserID
	filtered := entries[:0]
	for _, e := range entries {
		if gaming.IsEnvelope(e.Message) {
			continue
		}
		if ok, _ := c.FilterGCM(zeroUID, gcid, e.Message); ok {
			continue
		}
		filtered = append(filtered, e)
	}
	entries = historyPage(filtered, pageSize, pageNum)
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
	var req struct {
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	c := s.requireClient(w)
	if c == nil {
		return
	}
	if err := c.PartFromGC(gcid, req.Reason); err != nil {
		// PartFromGC deletes the GC before notifying members and the
		// notify step errors spuriously when the GC has unKXd members
		// (client_groupchat.go sendToGCMembers re-reads the deleted
		// GC). If the GC entry is gone the part did its local job, so
		// report success; this also makes parting an already-gone GC
		// idempotent.
		if _, gerr := c.GetGCDB(gcid); errors.Is(gerr, clientdb.ErrNotFound) {
			s.Log.Warnf("Part from GC %s succeeded locally despite error: %v", gcid, err)
			if s.Reinvites != nil {
				s.Reinvites.Clear(gcid.String())
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "part: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if s.Reinvites != nil {
		s.Reinvites.Clear(gcid.String())
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCKill(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	var req struct {
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	c := s.requireClient(w)
	if c == nil {
		return
	}
	if err := c.KillGroupChat(gcid, req.Reason); err != nil {
		http.Error(w, "kill: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if s.Reinvites != nil {
		s.Reinvites.Clear(gcid.String())
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleGCKick(w http.ResponseWriter, r *http.Request, gcid zkidentity.ShortID) {
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	c := s.requireClient(w)
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
	var req struct {
		UID string `json:"uid,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	c := s.requireClient(w)
	if c == nil {
		return
	}
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
