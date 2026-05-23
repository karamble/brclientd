// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/slog"

	"github.com/karamble/brclientd/internal/certgen"
)

// StatusServer serves the mTLS HTTP surface dcrpulse-style dashboards use
// alongside clientrpc: /status for the runtime tracker snapshot, and
// /history/pm for paginated PM history reads (a wire-exposed wrapper around
// clientdb.ReadLogPM since BR's clientrpc.proto has no history RPC).
type StatusServer struct {
	Log       slog.Logger
	Certs     certgen.Triplet
	Listen    string
	Tracker   *Tracker
	DB        *clientdb.DB
	UploadDir string
	Notifs    *notifBus

	clientMu sync.RWMutex
	client   *client.Client
}

// SetClient attaches a live *client.Client to the StatusServer once the BR
// runtime has booted past the gates. /contacts returns 503 until this is
// called.
func (s *StatusServer) SetClient(c *client.Client) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	s.client = c
}

func (s *StatusServer) currentClient() *client.Client {
	s.clientMu.RLock()
	defer s.clientMu.RUnlock()
	return s.client
}

// Run blocks until ctx is cancelled or the server fails.
func (s *StatusServer) Run(ctx context.Context) error {
	tlsCfg, err := s.Certs.LoadServerTLSConfig()
	if err != nil {
		return fmt.Errorf("status: load tls config: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/history/pm", s.handleHistoryPM)
	mux.HandleFunc("/contacts", s.handleContacts)
	mux.HandleFunc("/contacts/rename", s.handleRenameContact)
	mux.HandleFunc("/contacts/kx-reset", s.handleKXReset)
	mux.HandleFunc("/contacts/handshake", s.handleHandshake)
	mux.HandleFunc("/contacts/suggest-kx", s.handleSuggestKX)
	mux.HandleFunc("/contacts/trans-reset", s.handleTransReset)
	mux.HandleFunc("/contacts/accept-suggestion", s.handleAcceptSuggestion)
	mux.HandleFunc("/contacts/subscribe-posts", s.handleSubscribePosts)
	mux.HandleFunc("/contacts/unsubscribe-posts", s.handleUnsubscribePosts)
	mux.HandleFunc("/contacts/list-posts", s.handleListPosts)
	mux.HandleFunc("/contacts/list-content", s.handleListContent)
	mux.HandleFunc("/contacts/fetch-post", s.handleFetchPost)
	mux.HandleFunc("/posts/feed", s.handlePostsFeed)
	mux.HandleFunc("/posts/body", s.handlePostBody)
	mux.HandleFunc("/posts/comments", s.handlePostComments)
	mux.HandleFunc("/posts/comment", s.handlePostComment)
	mux.HandleFunc("/posts/new", s.handlePostsNew)
	mux.HandleFunc("/shared-files", s.handleSharedFiles)
	mux.HandleFunc("/notifications", s.handleNotifications)
	mux.HandleFunc("/invites/redeem-key", s.handleRedeemPaidInvite)
	mux.HandleFunc("/files/send", s.handleSendFile)

	srv := &http.Server{
		Addr:              s.Listen,
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		s.Log.Infof("Status endpoint listening on %s (mTLS)", s.Listen)
		err := srv.ListenAndServeTLS("", "")
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
		return ctx.Err()
	case err := <-serveErr:
		return err
	}
}

func (s *StatusServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Tracker.Get())
}

// handleHistoryPM returns paginated PM history for a given peer UID. Query
// params:
//
//	uid       hex-encoded zkidentity.ShortID of the peer (32 bytes / 64 hex chars)
//	page_size entries per page (default 50, max 500)
//	page      zero-based page index (default 0)
//
// Wraps clientdb.ReadLogPM directly; brclientd remains the source of truth
// for chat history so dashboard consumers can stay stateless.
func (s *StatusServer) handleHistoryPM(w http.ResponseWriter, r *http.Request) {
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
	pageSize := parsePositiveInt(r.URL.Query().Get("page_size"), 50, 500)
	pageNum := parseNonNegativeInt(r.URL.Query().Get("page"), 0)

	var entries []clientdb.PMLogEntry
	err := s.DB.View(r.Context(), func(tx clientdb.ReadTx) error {
		got, err := s.DB.ReadLogPM(tx, uid, pageSize, pageNum)
		if err != nil {
			return err
		}
		entries = got
		return nil
	})
	if err != nil {
		http.Error(w, "read pm log: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		UID      string                `json:"uid"`
		Page     int                   `json:"page"`
		PageSize int                   `json:"page_size"`
		Entries  []clientdb.PMLogEntry `json:"entries"`
	}{
		UID:      uidStr,
		Page:     pageNum,
		PageSize: pageSize,
		Entries:  entries,
	})
}

// handleContacts returns the BR client's in-memory address book entries.
// Each entry is augmented with posts_subscribed (whether we currently
// subscribe to that user's posts) so the dashboard sub-nav can render the
// right Subscribe / Unsubscribe state without an extra round-trip.
// 503 until the BR client has been instantiated (i.e. until past the
// gate / pre-setup phase).
func (s *StatusServer) handleContacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	entries := c.AddressBook()
	subs, _ := c.ListPostSubscriptions()
	subscribed := make(map[zkidentity.ShortID]bool, len(subs))
	for _, s := range subs {
		subscribed[s.To] = true
	}
	type contactEntry struct {
		*clientdb.AddressBookEntry
		PostsSubscribed bool `json:"posts_subscribed"`
	}
	out := make([]contactEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, contactEntry{
			AddressBookEntry: e,
			PostsSubscribed:  subscribed[e.ID.Identity],
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Entries []contactEntry `json:"entries"`
	}{Entries: out})
}

// handleRenameContact sets the local NickAlias for a contact. Pure
// clientdb-only mutation; nothing is broadcast to the BR network. Mirrors
// bruig's "Rename User" action (user_context_menu.dart) which calls
// client.RenameUser at client_kx.go:606.
func (s *StatusServer) handleRenameContact(w http.ResponseWriter, r *http.Request) {
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
		UID      string `json:"uid"`
		NewNick  string `json:"new_nick"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.NewNick = strings.TrimSpace(req.NewNick)
	if req.NewNick == "" {
		http.Error(w, "new_nick is required", http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(req.UID); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.RenameUser(uid, req.NewNick); err != nil {
		http.Error(w, "rename user: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleKXReset triggers a ratchet reset with the specified user. Mirrors
// bruig's "Request Ratchet Reset" action; calls client.ResetRatchet at
// client_kx.go:370. Used when the local key state has drifted and messages
// stop arriving in either direction.
func (s *StatusServer) handleKXReset(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.ResetRatchet(uid); err != nil {
		http.Error(w, "reset ratchet: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleHandshake starts a 3-way handshake with the specified user. Mirrors
// bruig's "Perform Handshake" action; calls client.Handshake at
// client.go:1163. Used to verify the ratchet is still operational.
func (s *StatusServer) handleHandshake(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.Handshake(uid); err != nil {
		http.Error(w, "handshake: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleSuggestKX asks `invitee` to KX with `target`. The sub-nav user is
// the invitee; the user picks the target from their existing contacts.
// Wraps client.SuggestKX (client_kx.go:636).
func (s *StatusServer) handleSuggestKX(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Invitee string `json:"invitee"`
		Target  string `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	invitee, ok := parseUIDHex(w, "invitee", req.Invitee)
	if !ok {
		return
	}
	target, ok := parseUIDHex(w, "target", req.Target)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.SuggestKX(invitee, target); err != nil {
		http.Error(w, "suggest kx: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleTransReset asks `mediator` to forward a reset request to `target`.
// The sub-nav user is the target (we want to repair the ratchet with them);
// the user picks the mediator from their existing contacts. Wraps
// client.RequestTransitiveReset (client_transreset.go:30).
func (s *StatusServer) handleTransReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
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
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.RequestTransitiveReset(mediator, target); err != nil {
		http.Error(w, "trans reset: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleAcceptSuggestion responds to an incoming KX suggestion by asking
// the mediator (the contact who suggested) to introduce us to the target.
// Wraps client.RequestMediateIdentity (client_autokx.go:43).
func (s *StatusServer) handleAcceptSuggestion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
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
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.RequestMediateIdentity(mediator, target); err != nil {
		http.Error(w, "accept suggestion: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleSubscribePosts asks the remote user to start sending us their posts.
// BR transmits the request and notifies via OnRemoteSubscriptionChanged
// when the reply lands; until then the subscription state is in flight.
func (s *StatusServer) handleSubscribePosts(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.SubscribeToPosts(uid); err != nil {
		http.Error(w, "subscribe to posts: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListPosts asks the remote user to send the list of posts they
// have authored. Async: the response arrives via OnPostsListReceived and
// is published as a posts-list-received event for subscribers.
func (s *StatusServer) handleListPosts(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.ListUserPosts(uid); err != nil {
		http.Error(w, "list user posts: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListContent asks the remote user to send the list of files they
// have shared. Lists both global and local-shared directories. Async: the
// response arrives via OnContentListReceived and is published as a
// content-list-received event for subscribers.
func (s *StatusServer) handleListContent(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.ListUserContent(uid, []string{"*"}, ""); err != nil {
		http.Error(w, "list user content: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleUnsubscribePosts asks the remote user to stop sending posts. As
// with subscribe, this is asynchronous and the new state surfaces via
// OnRemoteSubscriptionChanged.
func (s *StatusServer) handleUnsubscribePosts(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.UnsubscribeToPosts(uid); err != nil {
		http.Error(w, "unsubscribe from posts: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleFetchPost asks the remote user for a specific post. BR has two
// paths and we have to pick the right one based on our subscription
// state with the author:
//
//   - When NOT already subscribed: SubscribeToPostsAndFetch sends an
//     RMPostsSubscribe with GetPost set. The remote, on first-subscribe,
//     returns the post inline with the subscribe reply.
//   - When ALREADY subscribed: the same SubscribeToPostsAndFetch call
//     is a no-op at the remote side because their handlePostsSubscribe
//     short-circuits on ErrAlreadySubscribed and skips sending the post
//     (client_posts.go:57). The post never arrives. We have to call
//     GetUserPost instead, which sends a standalone RMGetPost.
//
// Either way, the post body arrives via OnPostRcvdNtfn which feeds the
// local feed cache and fires the post-received live event.
func (s *StatusServer) handleFetchPost(w http.ResponseWriter, r *http.Request) {
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
		UID string `json:"uid"`
		PID string `json:"pid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	uid, ok := parseUIDHex(w, "uid", req.UID)
	if !ok {
		return
	}
	var pid zkidentity.ShortID
	if err := pid.FromString(req.PID); err != nil {
		http.Error(w, "invalid pid: "+err.Error(), http.StatusBadRequest)
		return
	}
	subs, err := c.ListPostSubscriptions()
	if err != nil {
		http.Error(w, "list post subscriptions: "+err.Error(), http.StatusInternalServerError)
		return
	}
	subscribed := false
	for _, sub := range subs {
		if sub.To == uid {
			subscribed = true
			break
		}
	}
	if subscribed {
		if err := c.GetUserPost(uid, pid, false); err != nil {
			http.Error(w, "get user post: "+err.Error(), http.StatusBadGateway)
			return
		}
	} else {
		if err := c.SubscribeToPostsAndFetch(uid, pid); err != nil {
			http.Error(w, "subscribe + fetch post: "+err.Error(), http.StatusBadGateway)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// handlePostsFeed returns the local list of all posts (received from
// subscribed users + ours). Lightweight summaries only — bodies are
// fetched on demand via /posts/body.
func (s *StatusServer) handlePostsFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	posts, err := c.ListPosts()
	if err != nil {
		http.Error(w, "list posts: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type postSummary struct {
		ID           string `json:"id"`
		From         string `json:"from"`
		AuthorID     string `json:"author_id"`
		AuthorNick   string `json:"author_nick"`
		Date         int64  `json:"date"`
		LastStatusTS int64  `json:"last_status_ts"`
		Title        string `json:"title"`
	}
	out := make([]postSummary, 0, len(posts))
	for _, p := range posts {
		out = append(out, postSummary{
			ID:           p.ID.String(),
			From:         p.From.String(),
			AuthorID:     p.AuthorID.String(),
			AuthorNick:   p.AuthorNick,
			Date:         p.Date.Unix(),
			LastStatusTS: p.LastStatusTS.Unix(),
			Title:        p.Title,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Posts []postSummary `json:"posts"`
	}{Posts: out})
}

// handlePostBody returns the full PostMetadata for the requested
// (author, post) pair. ?uid=<hex>&pid=<hex>.
func (s *StatusServer) handlePostBody(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	uidStr := strings.TrimSpace(r.URL.Query().Get("uid"))
	pidStr := strings.TrimSpace(r.URL.Query().Get("pid"))
	if uidStr == "" || pidStr == "" {
		http.Error(w, "uid and pid query params are required", http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(uidStr); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	var pid zkidentity.ShortID
	if err := pid.FromString(pidStr); err != nil {
		http.Error(w, "invalid pid: "+err.Error(), http.StatusBadRequest)
		return
	}
	pm, err := c.ReadPost(uid, pid)
	if err != nil {
		http.Error(w, "read post: "+err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// PostMetadata.Attributes carries the markdown body under the
	// "main" key (per BR's convention). We pass the whole map through
	// so the dashboard can pick out what it needs.
	_ = json.NewEncoder(w).Encode(pm)
}

// handleSharedFiles returns the list of files the local user has shared
// (whether globally or with specific peers). Used by the BR editor's
// "Link to shared content" picker so authors can reference paid or free
// downloads inside a post body via --embed[download=,cost=,...]--.
func (s *StatusServer) handleSharedFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	files, err := c.ListLocalSharedFiles()
	if err != nil {
		http.Error(w, "list shared files: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type sharedFile struct {
		FID      string `json:"fid"`
		Filename string `json:"filename"`
		Cost     uint64 `json:"cost"`
		Size     uint64 `json:"size"`
		Global   bool   `json:"global"`
	}
	out := make([]sharedFile, 0, len(files))
	for _, f := range files {
		out = append(out, sharedFile{
			FID:      f.SF.FID.String(),
			Filename: f.SF.Filename,
			Cost:     f.Cost,
			Size:     f.Size,
			Global:   f.Global,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Files []sharedFile `json:"files"`
	}{Files: out})
}

// handlePostsNew authors a new post and shares it with our subscribers.
// Body: {post (markdown body), descr?}. Returns the created summary so
// the frontend can navigate to the detail view immediately.
func (s *StatusServer) handlePostsNew(w http.ResponseWriter, r *http.Request) {
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
		Post  string `json:"post"`
		Descr string `json:"descr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Post = strings.TrimSpace(req.Post)
	if req.Post == "" {
		http.Error(w, "post body is required", http.StatusBadRequest)
		return
	}
	summ, err := c.CreatePost(req.Post, req.Descr)
	if err != nil {
		http.Error(w, "create post: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":         summ.ID.String(),
		"from":       summ.From.String(),
		"author_id":  summ.AuthorID.String(),
		"author_nick": summ.AuthorNick,
		"date":       summ.Date.Unix(),
		"title":      summ.Title,
	})
}

// handlePostComments returns the list of comment status updates on a
// post. Filters out hearts and other non-comment status types so the
// frontend can render a flat comment list directly.
func (s *StatusServer) handlePostComments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	uidStr := strings.TrimSpace(r.URL.Query().Get("uid"))
	pidStr := strings.TrimSpace(r.URL.Query().Get("pid"))
	if uidStr == "" || pidStr == "" {
		http.Error(w, "uid and pid query params are required", http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(uidStr); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	var pid zkidentity.ShortID
	if err := pid.FromString(pidStr); err != nil {
		http.Error(w, "invalid pid: "+err.Error(), http.StatusBadRequest)
		return
	}
	updates, err := c.ListPostStatusUpdates(uid, pid)
	if err != nil {
		http.Error(w, "list status updates: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type comment struct {
		StatusFrom string `json:"status_from"`
		FromNick   string `json:"from_nick"`
		Comment    string `json:"comment"`
		Parent     string `json:"parent,omitempty"`
		Timestamp  int64  `json:"timestamp"`
		Identifier string `json:"identifier,omitempty"`
	}
	out := make([]comment, 0, len(updates))
	for _, u := range updates {
		body, ok := u.Attributes[rpc.RMPSComment]
		if !ok || body == "" {
			continue
		}
		var ts int64
		if tsStr := u.Attributes[rpc.RMPTimestamp]; tsStr != "" {
			if n, err := strconv.ParseInt(tsStr, 10, 64); err == nil {
				ts = n
			}
		}
		out = append(out, comment{
			StatusFrom: u.Attributes[rpc.RMPStatusFrom],
			FromNick:   u.Attributes[rpc.RMPFromNick],
			Comment:    body,
			Parent:     u.Attributes[rpc.RMPParent],
			Timestamp:  ts,
			Identifier: u.Attributes[rpc.RMPIdentifier],
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Comments []comment `json:"comments"`
	}{Comments: out})
}

// handlePostComment posts a new comment on a remote user's post. Body
// fields: uid (author of the post), pid (post id), comment (text), and
// optional parent (parent comment id, for threading).
func (s *StatusServer) handlePostComment(w http.ResponseWriter, r *http.Request) {
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
		UID     string `json:"uid"`
		PID     string `json:"pid"`
		Comment string `json:"comment"`
		Parent  string `json:"parent"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Comment = strings.TrimSpace(req.Comment)
	if req.UID == "" || req.PID == "" || req.Comment == "" {
		http.Error(w, "uid, pid, and comment are required", http.StatusBadRequest)
		return
	}
	uid, ok := parseUIDHex(w, "uid", req.UID)
	if !ok {
		return
	}
	var pid zkidentity.ShortID
	if err := pid.FromString(req.PID); err != nil {
		http.Error(w, "invalid pid: "+err.Error(), http.StatusBadRequest)
		return
	}
	var parent *zkidentity.ShortID
	if req.Parent != "" {
		var p zkidentity.ShortID
		if err := p.FromString(req.Parent); err != nil {
			http.Error(w, "invalid parent: "+err.Error(), http.StatusBadRequest)
			return
		}
		parent = &p
	}
	cid, err := c.CommentPost(uid, pid, req.Comment, parent)
	if err != nil {
		http.Error(w, "comment post: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Identifier string `json:"identifier"`
	}{Identifier: cid.String()})
}

// handleNotifications streams JSONL events from the in-process notif bus to
// a long-lived subscriber (the dashboard's bisonrelay_events.go). Each line
// is one JSON object. The endpoint flushes after every event so consumers
// see them in real time. Events brclientd publishes here are ones BR has no
// upstream clientrpc stream for (e.g. OnKXSuggested) plus any other
// daemon-side notifications we surface in the future.
func (s *StatusServer) handleNotifications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Notifs == nil {
		http.Error(w, "notification bus not configured", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch, unsub := s.Notifs.Subscribe()
	defer unsub()
	enc := json.NewEncoder(w)
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			if err := enc.Encode(evt); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// parseUIDHex decodes a hex-encoded ShortID, writing a 400 with the named
// field on failure. Returns ok=false so callers can return immediately.
func parseUIDHex(w http.ResponseWriter, field, hex string) (zkidentity.ShortID, bool) {
	var uid zkidentity.ShortID
	if err := uid.FromString(hex); err != nil {
		http.Error(w, "invalid "+field+": "+err.Error(), http.StatusBadRequest)
		return uid, false
	}
	return uid, true
}

// decodeUIDOnlyBody enforces POST, decodes a {uid: "<hex>"} JSON body, and
// parses the uid into a zkidentity.ShortID. Shared by per-user action
// endpoints that take only a uid argument (KX reset, handshake, future
// suggest-KX and transitive reset). On any failure it writes the response
// status and returns ok=false; callers should return immediately.
func (s *StatusServer) decodeUIDOnlyBody(w http.ResponseWriter, r *http.Request) (zkidentity.ShortID, bool) {
	var zero zkidentity.ShortID
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return zero, false
	}
	var req struct {
		UID string `json:"uid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return zero, false
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(req.UID); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return zero, false
	}
	return uid, true
}

// handleRedeemPaidInvite accepts a bech32-encoded PaidInviteKey ("brpik1..."),
// fetches the encrypted invite blob from the BR server, decrypts it, and
// runs AcceptInvite to start a key exchange. The bridge between brpik1 keys
// and BR's clientrpc ChatService.AcceptInvite RPC, which only accepts the
// binary OOB invite blob.
func (s *StatusServer) handleRedeemPaidInvite(w http.ResponseWriter, r *http.Request) {
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
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Key = strings.TrimSpace(req.Key)
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	var pik clientintf.PaidInviteKey
	if err := pik.Decode(req.Key); err != nil {
		http.Error(w, "decode paid invite key: "+err.Error(), http.StatusBadRequest)
		return
	}

	fetchCtx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	invite, err := c.FetchPrepaidInvite(fetchCtx, pik, io.Discard)
	if err != nil {
		http.Error(w, "fetch prepaid invite: "+err.Error(), http.StatusBadGateway)
		return
	}
	if err := c.AcceptInvite(invite); err != nil {
		http.Error(w, "accept invite: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleSendFile accepts a multipart upload (fields: user, file) and hands
// the file off to BR's file-transfer subsystem via c.SendFile. The uploaded
// bytes are persisted under UploadDir; BR's send queue references that path
// for the lifetime of the transfer (chunks are read on demand), so we keep
// the file in place rather than auto-deleting after the call returns.
func (s *StatusServer) handleSendFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if s.UploadDir == "" {
		http.Error(w, "upload directory not configured", http.StatusInternalServerError)
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "parse multipart: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()

	userField := strings.TrimSpace(r.FormValue("user"))
	if userField == "" {
		http.Error(w, "user field is required", http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file part missing: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	ru, err := c.UserByNick(userField)
	if err != nil {
		http.Error(w, "user lookup: "+err.Error(), http.StatusNotFound)
		return
	}

	safeName := sanitizeUploadName(header.Filename)
	if safeName == "" {
		http.Error(w, "invalid filename", http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(s.UploadDir, 0o700); err != nil {
		http.Error(w, "mkdir uploads: "+err.Error(), http.StatusInternalServerError)
		return
	}
	storedPath, err := storeUpload(s.UploadDir, safeName, file)
	if err != nil {
		http.Error(w, "store upload: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := c.SendFile(ru.ID(), 0, storedPath, nil); err != nil {
		_ = os.Remove(storedPath)
		http.Error(w, "send file: "+err.Error(), http.StatusBadGateway)
		return
	}
	// BR's sendPreparedSendqItemListSync waits for the relay to ack each
	// chunk before SendFile returns, so once we're here every byte has
	// been read from the source file and the file is no longer referenced
	// by the send queue. BR does not clean these up itself.
	_ = os.Remove(storedPath)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"filename": safeName,
		"size":     header.Size,
	})
}

func sanitizeUploadName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "" || name == "." || name == ".." {
		return ""
	}
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		case r == '.' || r == '_' || r == '-' || r == ' ':
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return ""
	}
	if len(out) > 200 {
		out = out[:200]
	}
	return string(out)
}

func storeUpload(dir, name string, src io.Reader) (string, error) {
	candidate := filepath.Join(dir, name)
	if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
		return writeUpload(candidate, src)
	}
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	for i := 1; i < 1000; i++ {
		c := filepath.Join(dir, fmt.Sprintf("%s-%d%s", stem, i, ext))
		if _, err := os.Stat(c); errors.Is(err, os.ErrNotExist) {
			return writeUpload(c, src)
		}
	}
	return "", errors.New("could not find a free upload filename")
}

func writeUpload(path string, src io.Reader) (string, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, src); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func parsePositiveInt(s string, fallback, max int) int {
	if s == "" {
		return fallback
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return fallback
	}
	if v > max {
		return max
	}
	return v
}

func parseNonNegativeInt(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return fallback
	}
	return v
}
