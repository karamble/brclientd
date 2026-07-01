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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/client/timestats"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/slog"

	"github.com/karamble/brclientd/internal/certgen"
	"github.com/karamble/brclientd/internal/identity"
)

// StatusServer serves the mTLS HTTP surface dcrpulse-style dashboards use
// alongside clientrpc: /status for the runtime tracker snapshot, and
// /history/pm for paginated PM history reads (a wire-exposed wrapper around
// clientdb.ReadLogPM since BR's clientrpc.proto has no history RPC).
type StatusServer struct {
	Log          slog.Logger
	Certs        certgen.Triplet
	Listen       string
	Tracker      *Tracker
	DB           *clientdb.DB
	UploadDir    string
	MsgsRoot     string
	EmbedsRoot   string
	PagesDir     string
	DataDir      string
	Notifs       *notifBus
	AudioRouter  *RTDTAudioRouter
	Reinvites    *gcReinviteTracker
	Unrepl       *unreplTracker
	DownloadCaps *downloadCapTracker
	Notes        *notificationStore
	Groups       *contactGroupsStore
	AppName      string
	AppVersion   string

	// Settings persists dashboard-changeable daemon settings; EffectiveBehavior
	// is the resolved set of BR behavior values this process booted with (fixed
	// at BR client construction), so /settings/behavior can flag saved values
	// that differ and need a restart to take effect. RestartCh is closed by
	// requestRestart to make Run return ErrRestartRequested so the supervisor
	// relaunches the daemon with the persisted settings.
	Settings          *brSettingsStore
	EffectiveBehavior brBehavior
	RestartCh         chan struct{}
	restartOnce       sync.Once

	clientMu sync.RWMutex
	client   *client.Client

	// backupMu serializes /backup requests; the backup holds a clientdb read
	// transaction while it tars the whole data dir.
	backupMu sync.Mutex

	// remainOffline mirrors the last requested connection intent. BR has
	// no getter for it (GoOnline/RemainOffline only feed the ConnKeeper),
	// and the daemon always boots online, so false is the right default.
	onlineMu      sync.RWMutex
	remainOffline bool

	storeCtrlMu sync.RWMutex
	storeCtrl   *storeController

	lnPayMu sync.RWMutex
	lnPay   *client.DcrlnPaymentClient
}

// SetClient attaches a live *client.Client to the StatusServer once the BR
// runtime has booted past the gates. /contacts returns 503 until this is
// called.
func (s *StatusServer) SetClient(c *client.Client) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	s.client = c
}

// SetStoreController wires the resource-hosting mode controller so /store/mode
// can read and flip between pages and simplestore hosting.
func (s *StatusServer) SetStoreController(ctrl *storeController) {
	s.storeCtrlMu.Lock()
	defer s.storeCtrlMu.Unlock()
	s.storeCtrl = ctrl
}

// SetLNPay wires the dcrlnd payment client so /downloads can decode the
// per-chunk invoices a paid download was bought with into a paid-amount total.
func (s *StatusServer) SetLNPay(pc *client.DcrlnPaymentClient) {
	s.lnPayMu.Lock()
	defer s.lnPayMu.Unlock()
	s.lnPay = pc
}

func (s *StatusServer) currentLNPay() *client.DcrlnPaymentClient {
	s.lnPayMu.RLock()
	defer s.lnPayMu.RUnlock()
	return s.lnPay
}

// invoiceAtomsCache memoizes decoded bolt11 invoice amounts (invoices are
// immutable) so /downloads does not re-hit dcrlnd for the same chunk invoices.
var (
	invoiceAtomsMu    sync.Mutex
	invoiceAtomsCache = map[string]int64{}
)

// invoiceAtoms decodes a bolt11 invoice to its atom amount (cached). Returns 0
// on any decode error so the list never fails on a single undecodable invoice.
func (s *StatusServer) invoiceAtoms(ctx context.Context, pc *client.DcrlnPaymentClient, invoice string) int64 {
	if invoice == "" {
		return 0
	}
	invoiceAtomsMu.Lock()
	a, ok := invoiceAtomsCache[invoice]
	invoiceAtomsMu.Unlock()
	if ok {
		return a
	}
	dec, err := pc.DecodeInvoice(ctx, invoice)
	if err != nil {
		return 0
	}
	atoms := dec.MAtoms / 1000
	invoiceAtomsMu.Lock()
	invoiceAtomsCache[invoice] = atoms
	invoiceAtomsMu.Unlock()
	return atoms
}

func (s *StatusServer) currentStoreController() *storeController {
	s.storeCtrlMu.RLock()
	defer s.storeCtrlMu.RUnlock()
	return s.storeCtrl
}

func (s *StatusServer) currentClient() *client.Client {
	s.clientMu.RLock()
	defer s.clientMu.RUnlock()
	return s.client
}

func (s *StatusServer) requestRestart() {
	s.restartOnce.Do(func() { close(s.RestartCh) })
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
	mux.HandleFunc("/history/pm/clear", s.handleClearPMHistory)
	mux.HandleFunc("/contacts", s.handleContacts)
	mux.HandleFunc("/contacts/rename", s.handleRenameContact)
	mux.HandleFunc("/contacts/groups", s.handleContactGroups)
	mux.HandleFunc("/contacts/groups/assign", s.handleContactGroupsAssign)
	mux.HandleFunc("/contacts/groups/settings", s.handleContactGroupsSettings)
	mux.HandleFunc("/contacts/kx-reset", s.handleKXReset)
	mux.HandleFunc("/contacts/reset-all", s.handleKXResetAll)
	mux.HandleFunc("/contacts/block", s.handleBlockContact)
	mux.HandleFunc("/contacts/blocked", s.handleBlockedContacts)
	mux.HandleFunc("/contacts/unblock", s.handleUnblockContact)
	mux.HandleFunc("/contacts/ignore", s.handleIgnoreContact)
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
	mux.HandleFunc("/posts/embed-data", s.handlePostsEmbedData)
	mux.HandleFunc("/posts/comments", s.handlePostComments)
	mux.HandleFunc("/posts/comment", s.handlePostComment)
	mux.HandleFunc("/posts/hearts", s.handlePostHearts)
	mux.HandleFunc("/posts/heart", s.handlePostHeart)
	mux.HandleFunc("/posts/receivereceipts", s.handlePostReceiveReceipts)
	mux.HandleFunc("/posts/comment-receivereceipts", s.handlePostCommentReceiveReceipts)
	mux.HandleFunc("/posts/relay", s.handlePostRelay)
	mux.HandleFunc("/posts/new", s.handlePostsNew)
	mux.HandleFunc("/shared-files", s.handleSharedFiles)
	mux.HandleFunc("/shared-files/add", s.handleSharedFileAdd)
	mux.HandleFunc("/shared-files/remove", s.handleSharedFileRemove)
	mux.HandleFunc("/downloads", s.handleDownloads)
	mux.HandleFunc("/downloads/cancel", s.handleDownloadCancel)
	mux.HandleFunc("/downloads/delete", s.handleDownloadDelete)
	mux.HandleFunc("/content/get", s.handleContentGet)
	mux.HandleFunc("/content/file", s.handleContentFile)
	mux.HandleFunc("/rates", s.handleRates)
	mux.HandleFunc("/store/mode", s.handleStoreMode)
	mux.HandleFunc("/store/products", s.handleStoreProducts)
	mux.HandleFunc("/store/products/delete", s.handleStoreProductDelete)
	mux.HandleFunc("/store/orders", s.handleStoreOrders)
	mux.HandleFunc("/store/orders/status", s.handleStoreOrderStatus)
	mux.HandleFunc("/store/orders/comment", s.handleStoreOrderComment)
	mux.HandleFunc("/store/files/upload", s.handleStoreFileUpload)
	mux.HandleFunc("/store/files/list", s.handleStoreFilesList)
	mux.HandleFunc("/store/files/get", s.handleStoreFileGet)
	mux.HandleFunc("/store/files/delete", s.handleStoreFileDelete)
	mux.HandleFunc("/store/templates", s.handleStoreTemplates)
	mux.HandleFunc("/store/templates/file", s.handleStoreTemplateFile)
	mux.HandleFunc("/store/templates/save", s.handleStoreTemplateSave)
	mux.HandleFunc("/store/templates/delete", s.handleStoreTemplateDelete)
	mux.HandleFunc("/notifications", s.handleNotifications)
	mux.HandleFunc("/notifications/recent", s.handleRecentNotifications)
	mux.HandleFunc("/notifications/delete", s.handleDeleteNotification)
	mux.HandleFunc("/notifications/clear", s.handleClearNotifications)
	mux.HandleFunc("/version", s.handleVersion)
	mux.HandleFunc("/public-identity", s.handlePublicIdentity)
	mux.HandleFunc("/avatar", s.handleSetAvatar)
	mux.HandleFunc("/messages/send", s.handleSendMessage)
	mux.HandleFunc("/invites/create", s.handleCreateInvite)
	mux.HandleFunc("/invites/accept", s.handleAcceptInvite)
	mux.HandleFunc("/tip", s.handleTip)
	mux.HandleFunc("/payments/tips", s.handleTipAttempts)
	mux.HandleFunc("/payments/tips/running", s.handleRunningTipAttempts)
	mux.HandleFunc("/invites/redeem-key", s.handleRedeemPaidInvite)
	mux.HandleFunc("/files/send", s.handleSendFile)
	mux.HandleFunc("/stats/overview", s.handleStatsOverview)
	mux.HandleFunc("/stats/payments", s.handleStatsPayments)
	mux.HandleFunc("/stats/payments/clear", s.handleClearPayStats)
	mux.HandleFunc("/stats/network", s.handleStatsNetwork)
	mux.HandleFunc("/stats/contacts", s.handleStatsContacts)
	mux.HandleFunc("/stats/posts", s.handleStatsPosts)
	mux.HandleFunc("/rtdt/sessions", s.handleRTDT)
	mux.HandleFunc("/rtdt/sessions/", s.handleRTDT)
	mux.HandleFunc("/gc", s.handleGC)
	mux.HandleFunc("/gc/", s.handleGC)
	mux.HandleFunc("/pages/fetch", s.handlePagesFetch)
	mux.HandleFunc("/pages/local", s.handlePagesLocalList)
	mux.HandleFunc("/pages/local/file", s.handlePagesLocalFile)
	mux.HandleFunc("/pages/local/save", s.handlePagesLocalSave)
	mux.HandleFunc("/pages/local/delete", s.handlePagesLocalDelete)
	mux.HandleFunc("/backup", s.handleBackup)
	mux.HandleFunc("/connection", s.handleConnection)
	mux.HandleFunc("/settings/behavior", s.handleBehavior)
	mux.HandleFunc("/filters", s.handleFilters)
	mux.HandleFunc("/filters/delete", s.handleDeleteFilter)
	mux.HandleFunc("/posts/subscribe-all", s.handleSubscribeAllPosts)
	mux.HandleFunc("/kx/list", s.handleKXList)
	mux.HandleFunc("/kx/searches", s.handleKXSearches)
	mux.HandleFunc("/kx/mediateids", s.handleMediateIDs)

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
	case <-s.RestartCh:
		// Graceful shutdown drains the in-flight POST that already wrote
		// its response before signaling the restart.
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
		return ErrRestartRequested
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
	if errors.Is(err, clientdb.ErrNotFound) {
		// A contact with a missing addressbook identity file (seen in
		// data restored from an incomplete KX) has no readable history;
		// serve an empty thread instead of erroring the chat window.
		s.Log.Warnf("history/pm: addressbook identity missing for %s; serving empty history", uidStr)
		entries = nil
		err = nil
	}
	if err != nil {
		http.Error(w, "read pm log: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Apply active content filters to served history (the dashboard renders it
	// verbatim). FilterPM gets the conversation uid, so uid-scoped PM rules work.
	if c := s.currentClient(); c != nil {
		filtered := entries[:0]
		for _, e := range entries {
			if ok, _ := c.FilterPM(uid, e.Message); ok {
				continue
			}
			filtered = append(filtered, e)
		}
		entries = filtered
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
		PostsSubscribed bool      `json:"posts_subscribed"`
		LastDecTime     time.Time `json:"last_dec_time"`
	}
	out := make([]contactEntry, 0, len(entries))
	for _, e := range entries {
		ce := contactEntry{
			AddressBookEntry: e,
			PostsSubscribed:  subscribed[e.ID.Identity],
		}
		if ru, err := c.UserByID(e.ID.Identity); err == nil && ru != nil {
			ce.LastDecTime = ru.RatchetDebugInfo().LastDecTime
		}
		out = append(out, ce)
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
		UID     string `json:"uid"`
		NewNick string `json:"new_nick"`
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

// publishGroupsChanged tells live UIs to re-fetch the contact group layout.
func (s *StatusServer) publishGroupsChanged(uid, group string) {
	if s.Notifs == nil {
		return
	}
	s.Notifs.Publish(NotifEvent{
		Type:      "contact-groups-changed",
		Timestamp: time.Now(),
		Payload:   map[string]any{"uid": uid, "group": group},
	})
}

// handleContactGroups serves the contact group layout (GET) and group
// create/rename/delete actions (POST). Contacts are keyed by their
// zkidentity hex string; "archived" is the reserved builtin group.
func (s *StatusServer) handleContactGroups(w http.ResponseWriter, r *http.Request) {
	if s.Groups == nil {
		http.Error(w, "groups store not available", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Groups.snapshot())
	case http.MethodPost:
		var req struct {
			Action string `json:"action"`
			ID     string `json:"id"`
			Name   string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		switch req.Action {
		case "create":
			g, err := s.Groups.createGroup(req.Name)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			s.publishGroupsChanged("", g.ID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(g)
		case "rename":
			if err := s.Groups.renameGroup(req.ID, req.Name); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			s.publishGroupsChanged("", req.ID)
			w.WriteHeader(http.StatusNoContent)
		case "delete":
			if err := s.Groups.deleteGroup(req.ID); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			s.publishGroupsChanged("", "")
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unknown action", http.StatusBadRequest)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleContactGroupsAssign moves one contact into a group ("" returns it to
// the regular list). Pinned keeps an archived contact archived when new
// messages arrive.
func (s *StatusServer) handleContactGroupsAssign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Groups == nil {
		http.Error(w, "groups store not available", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		UID    string `json:"uid"`
		Group  string `json:"group"`
		Pinned bool   `json:"pinned"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(req.UID); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.Groups.assign(uid.String(), req.Group, req.Pinned); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.publishGroupsChanged(uid.String(), req.Group)
	w.WriteHeader(http.StatusNoContent)
}

// handleContactGroupsSettings updates the auto-archive threshold in days
// (0 disables the sweeper).
func (s *StatusServer) handleContactGroupsSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Groups == nil {
		http.Error(w, "groups store not available", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		AutoArchiveDays int `json:"auto_archive_days"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.Groups.setAutoArchiveDays(req.AutoArchiveDays); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.publishGroupsChanged("", "")
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

// handleKXResetAll initiates a ratchet reset with every contact whose last
// received message is older than age_days (0 = upstream's 30-day default).
// Mirrors brclient's /rresetold. Initiation only: the resets complete via
// mailbox ping-pong whenever each peer comes online; no state is tracked.
func (s *StatusServer) handleKXResetAll(w http.ResponseWriter, r *http.Request) {
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
		AgeDays int `json:"age_days"`
	}
	// An empty body selects the default age.
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.AgeDays < 0 {
		http.Error(w, "age_days must not be negative", http.StatusBadRequest)
		return
	}
	interval := time.Duration(req.AgeDays) * 24 * time.Hour
	res, err := c.ResetAllOldRatchets(interval, nil)
	if err != nil {
		http.Error(w, "reset all ratchets: "+err.Error(), http.StatusBadGateway)
		return
	}
	started := make([]string, len(res))
	for i, uid := range res {
		started[i] = uid.String()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Started []string `json:"started"`
		Count   int      `json:"count"`
	}{Started: started, Count: len(started)})
}

// handleBlockContact blocks a user. Mirrors bruig's "Block User" action;
// calls client.Block at client_kx.go:537. This is destructive: BR sends an
// RMBlock to the peer AND removes the user from the local address book
// (RemoveUser deletes inbound/<uid>: ratchet + unacked RMs). Irreversible
// short of a fresh KX. NOTE: RemoveUser does NOT delete the PM message log
// under MsgsRoot, so the on-disk history is orphaned after a block; use
// handleClearPMHistory to remove that.
func (s *StatusServer) handleBlockContact(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.Block(uid); err != nil {
		http.Error(w, "block user: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleClearPMHistory permanently deletes the local PM history (and inline
// media) for one contact. dcrpulse-original: BR exposes no clear-history API,
// so this operates directly on the on-disk message store. It removes every
// MsgsRoot/*.<uid>.log (the filename embeds the contact's nick, which can
// change over time, so we glob by uid) plus the contact's embeds dir. The
// address book entry + ratchet are left intact so messaging can continue;
// only the local copy is wiped (the peer keeps theirs). Irreversible. Pure
// filesystem, so it works without a live BR client.
func (s *StatusServer) handleClearPMHistory(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	if s.MsgsRoot == "" {
		http.Error(w, "history paths not configured", http.StatusServiceUnavailable)
		return
	}
	logs, err := filepath.Glob(filepath.Join(s.MsgsRoot, "*."+uid.String()+".log"))
	if err != nil {
		http.Error(w, "glob pm logs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for _, f := range logs {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			http.Error(w, "remove pm log: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if s.EmbedsRoot != "" {
		if err := os.RemoveAll(filepath.Join(s.EmbedsRoot, uid.String())); err != nil {
			http.Error(w, "remove embeds: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleIgnoreContact sets or clears the local ignore flag for a user.
// Mirrors bruig's "Ignore/Un-ignore User" action; calls client.Ignore at
// client_kx.go:505. Pure local clientdb mutation (nothing broadcast); the
// flag surfaces as the `ignored` field of /contacts entries. Idempotent: a
// no-op if the user is already in the requested state (client.Ignore itself
// errors in that case).
func (s *StatusServer) handleIgnoreContact(w http.ResponseWriter, r *http.Request) {
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
		UID    string `json:"uid"`
		Ignore bool   `json:"ignore"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(req.UID); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if cur, err := c.IsIgnored(uid); err == nil && cur == req.Ignore {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := c.Ignore(uid, req.Ignore); err != nil {
		http.Error(w, "ignore user: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// blockedUsersFile mirrors the BR library's clientdb filename for the persisted
// block list (a uid-hex -> block-time map). The library loads it into an
// in-memory set once at DB open and only rewrites it on a new block, exposing
// no API to list or remove entries, so brclientd owns this file directly for
// the dashboard's blocked-contacts management.
const blockedUsersFile = "blockedusers.json"

// blockedUsersPath returns the block-list file at the clientdb root, derived
// the same way the DB itself is opened (identity.PathsIn).
func (s *StatusServer) blockedUsersPath() string {
	return filepath.Join(identity.PathsIn(s.DataDir).Root, blockedUsersFile)
}

// readBlockedUsers loads the block map. A missing file means nothing was ever
// blocked.
func (s *StatusServer) readBlockedUsers() (map[string]time.Time, error) {
	b, err := os.ReadFile(s.blockedUsersPath())
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]time.Time{}, nil
		}
		return nil, err
	}
	blocked := make(map[string]time.Time)
	if err := json.Unmarshal(b, &blocked); err != nil {
		return nil, err
	}
	return blocked, nil
}

// writeBlockedUsers persists the block map with an atomic temp-file + rename so
// a concurrent read never sees a partial file. The only racing writer is
// client.Block; the immediate restart that follows an unblock keeps the window
// small.
func (s *StatusServer) writeBlockedUsers(blocked map[string]time.Time) error {
	b, err := json.Marshal(blocked)
	if err != nil {
		return err
	}
	path := s.blockedUsersPath()
	tmp, err := os.CreateTemp(filepath.Dir(path), blockedUsersFile+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

// handleBlockedContacts lists the locally blocked users. dcrpulse-original: BR
// exposes no API to enumerate the block list, so this reads blockedusers.json
// directly, newest block first. Pure filesystem read; works without a live BR
// client. Blocked users have no address book entry (deleted on block), so only
// the uid and block time are available.
func (s *StatusServer) handleBlockedContacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	blocked, err := s.readBlockedUsers()
	if err != nil {
		http.Error(w, "read blocked users: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type entry struct {
		UID       string    `json:"uid"`
		BlockedAt time.Time `json:"blockedAt"`
	}
	out := struct {
		Blocked []entry `json:"blocked"`
	}{Blocked: make([]entry, 0, len(blocked))}
	for uid, ts := range blocked {
		out.Blocked = append(out.Blocked, entry{UID: uid, BlockedAt: ts})
	}
	sort.Slice(out.Blocked, func(i, j int) bool {
		return out.Blocked[i].BlockedAt.After(out.Blocked[j].BlockedAt)
	})
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		s.Log.Warnf("encode blocked users: %v", err)
	}
}

// handleUnblockContact removes one uid from the BR block list. dcrpulse-original:
// BR has no unblock API and never re-reads blockedusers.json after DB open, so
// this rewrites the file with the uid dropped and then requests a daemon restart
// so the reopened clientdb loads the change. Until that restart the live
// client's in-memory block set is unchanged. BR's block is symmetric (the peer
// also blocked this client), so reconnecting still needs the peer to unblock
// plus a fresh KX.
func (s *StatusServer) handleUnblockContact(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	blocked, err := s.readBlockedUsers()
	if err != nil {
		http.Error(w, "read blocked users: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, found := blocked[uid.String()]; !found {
		http.Error(w, "user is not blocked", http.StatusNotFound)
		return
	}
	delete(blocked, uid.String())
	if err := s.writeBlockedUsers(blocked); err != nil {
		http.Error(w, "write blocked users: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s.requestRestart()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct {
		Restarting bool `json:"restarting"`
	}{Restarting: true}); err != nil {
		s.Log.Warnf("encode unblock response: %v", err)
	}
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
		if err := c.GetUserPost(uid, pid, true); err != nil {
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
// handlePostsFeed returns every post in the local feed enriched with all
// locally derivable data: clean title/snippet (embed shortcodes stripped),
// embed metadata (payload bytes served by /posts/embed-data), heart and
// comment aggregates, relayer attribution and receive-receipt counts for
// own posts. A post whose content file fails to read degrades to its bare
// summary instead of failing the whole feed.
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
	myID := c.PublicID()
	myIDStr := myID.String()
	myNick := c.LocalNick()
	out := make([]feedPost, 0, len(posts))
	for _, p := range posts {
		row := feedPost{
			ID:           p.ID.String(),
			From:         p.From.String(),
			AuthorID:     p.AuthorID.String(),
			AuthorNick:   p.AuthorNick,
			Date:         p.Date.Unix(),
			LastStatusTS: p.LastStatusTS.Unix(),
			Title:        p.Title,
		}
		if p.From != p.AuthorID {
			row.Relayed = true
			row.RelayerNick = c.UserLogNick(p.From)
		}
		// Posts are keyed by author in clientdb, so reads use AuthorID
		// even when the post arrived through a relayer (From).
		if pm, err := c.ReadPost(p.AuthorID, p.ID); err == nil {
			plain, embeds := stripEmbeds(pm.Attributes[rpc.RMPMain])
			row.Title = deriveTitle(pm.Attributes, plain)
			if row.Title == "" {
				row.Title = fallbackEmbedTitle(embeds)
			}
			row.Description = strings.TrimSpace(pm.Attributes[rpc.RMPDescription])
			row.Snippet, row.HasMore = deriveSnippet(plain, feedSnippetCap)
			row.Embeds = embeds
			row.FirstImage = firstImage(embeds)
			// The author's true publish time rides in the post metadata as a
			// hex timestamp; Date stays the local ModTime so new activity keeps
			// bumping posts up the feed.
			if tsStr := pm.Attributes[rpc.RMPTimestamp]; tsStr != "" {
				if n, err := strconv.ParseInt(tsStr, 16, 64); err == nil {
					row.Published = n
				}
			}
		}
		if updates, err := c.ListPostStatusUpdates(p.AuthorID, p.ID); err == nil {
			row.HeartsCount, row.HeartedByMe, row.HeartedBy = aggregateHearts(updates, myIDStr)
			var unrepl []unreplComment
			if s.Unrepl != nil {
				unrepl = s.Unrepl.list(row.AuthorID, row.ID)
			}
			row.CommentsCount, row.CommenterCount, row.LastCommentTS, row.LastCommentNick =
				aggregateComments(updates, myIDStr, myNick, unrepl)
		}
		if p.AuthorID == myID {
			if rrs, err := c.ListPostReceiveReceipts(p.ID); err == nil {
				row.ReceiptCount = len(rrs)
			}
		}
		out = append(out, row)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Posts []feedPost `json:"posts"`
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
		FID      string   `json:"fid"`
		Filename string   `json:"filename"`
		Cost     uint64   `json:"cost"`
		Size     uint64   `json:"size"`
		Global   bool     `json:"global"`
		Shares   []string `json:"shares"`
	}
	out := make([]sharedFile, 0, len(files))
	for _, f := range files {
		shares := make([]string, 0, len(f.Shares))
		for _, id := range f.Shares {
			shares = append(shares, id.String())
		}
		out = append(out, sharedFile{
			FID:      f.SF.FID.String(),
			Filename: f.SF.Filename,
			Cost:     f.Cost,
			Size:     f.Size,
			Global:   f.Global,
			Shares:   shares,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Files []sharedFile `json:"files"`
	}{Files: out})
}

// handleSharedFileAdd receives a multipart upload + sharing parameters
// and registers the file as a local SharedFile. cost_atoms is the
// per-fetch price in atoms (1 DCR = 1e8; 0 = free) - shared-file costs use
// atoms, not the milli-atoms of payment records - target_uid optional (empty
// string = global share). The upload file is read by c.ShareFile into
// BR's internal content store and removed from UploadDir after.
func (s *StatusServer) handleSharedFileAdd(w http.ResponseWriter, r *http.Request) {
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

	costStr := strings.TrimSpace(r.FormValue("cost_atoms"))
	var cost uint64
	if costStr != "" {
		v, err := strconv.ParseUint(costStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid cost_atoms: "+err.Error(), http.StatusBadRequest)
			return
		}
		cost = v
	}
	descr := strings.TrimSpace(r.FormValue("descr"))
	targetUIDStr := strings.TrimSpace(r.FormValue("target_uid"))
	var targetUID *zkidentity.ShortID
	if targetUIDStr != "" {
		var uid zkidentity.ShortID
		if err := uid.FromString(targetUIDStr); err != nil {
			http.Error(w, "invalid target_uid: "+err.Error(), http.StatusBadRequest)
			return
		}
		targetUID = &uid
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file part missing: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

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
	sf, _, err := c.ShareFile(storedPath, targetUID, cost, descr)
	// ShareFile reads the file into its own content store; we no
	// longer need the upload regardless of success.
	_ = os.Remove(storedPath)
	if err != nil {
		http.Error(w, "share file: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"fid":      sf.FID.String(),
		"filename": sf.Filename,
		"cost":     cost,
		"global":   targetUID == nil,
	})
}

// handleSharedFileRemove unshares a previously-shared file. Body:
// {fid, target_uid?}. target_uid empty = remove the global share entry;
// otherwise revokes the share with just that user.
func (s *StatusServer) handleSharedFileRemove(w http.ResponseWriter, r *http.Request) {
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
		FID       string `json:"fid"`
		TargetUID string `json:"target_uid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var fid zkidentity.ShortID
	if err := fid.FromString(req.FID); err != nil {
		http.Error(w, "invalid fid: "+err.Error(), http.StatusBadRequest)
		return
	}
	var targetUID *zkidentity.ShortID
	if strings.TrimSpace(req.TargetUID) != "" {
		var uid zkidentity.ShortID
		if err := uid.FromString(req.TargetUID); err != nil {
			http.Error(w, "invalid target_uid: "+err.Error(), http.StatusBadRequest)
			return
		}
		targetUID = &uid
	}
	if err := c.UnshareFile(fid, targetUID); err != nil {
		http.Error(w, "unshare file: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDownloads returns the flat list of in-flight + completed file
// downloads tracked by BR. Sent files (uploads we're serving) are
// included so the sender side can see progress too.
func (s *StatusServer) handleDownloads(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	downloads, err := c.ListDownloads()
	if err != nil {
		http.Error(w, "list downloads: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type entry struct {
		UID           string `json:"uid"`
		Nick          string `json:"nick"`
		FID           string `json:"fid"`
		Filename      string `json:"filename"`
		Size          uint64 `json:"size"`
		TotalChunks   int    `json:"total_chunks"`
		MissingChunks int    `json:"missing_chunks"`
		DiskPath      string `json:"disk_path"`
		Pushed        bool   `json:"pushed"`
		Description   string `json:"description,omitempty"`
		Cost          uint64 `json:"cost,omitempty"`
		PaidAtoms     int64  `json:"paid_atoms,omitempty"`
	}
	out := make([]entry, 0, len(downloads))
	for _, d := range downloads {
		var filename, description string
		var size, cost uint64
		var total int
		if d.Metadata != nil {
			filename = d.Metadata.Filename
			size = d.Metadata.Size
			total = len(d.Metadata.Manifest)
			description = d.Metadata.Description
			cost = d.Metadata.Cost
		}
		// A chunk we've stored locally lands in ChunkStateDownloaded
		// (incoming) or ChunkStateUploaded (outgoing). Anything else
		// (has_invoice, paying_invoice, requested_chunk, paid, ...)
		// counts as still-in-flight for progress purposes.
		missing := 0
		for _, st := range d.ChunkStates {
			if st != clientdb.ChunkStateDownloaded && st != clientdb.ChunkStateUploaded {
				missing++
			}
		}
		// Drop completed downloads whose file is gone from disk (e.g. deleted
		// via /downloads/delete) so a stale row doesn't linger pointing at a
		// missing file.
		if missing == 0 && d.DiskPath != "" {
			if _, statErr := os.Stat(d.DiskPath); statErr != nil {
				continue
			}
		}
		// Sum what we actually paid for this file by decoding the per-chunk
		// invoices we paid (cached; immutable). 0 for free / pushed files.
		var paidAtoms int64
		if len(d.Invoices) > 0 {
			if pc := s.currentLNPay(); pc != nil {
				for _, inv := range d.Invoices {
					paidAtoms += s.invoiceAtoms(r.Context(), pc, inv)
				}
			}
		}
		// Nick lookup: a contact removed mid-transfer would return
		// userNotFoundError; in that case we fall back to the UID.
		nick := c.UserLogNick(d.UID)
		out = append(out, entry{
			UID:           d.UID.String(),
			Nick:          nick,
			FID:           d.FID.String(),
			Filename:      filename,
			Size:          size,
			TotalChunks:   total,
			MissingChunks: missing,
			DiskPath:      d.DiskPath,
			// d.IsSentFile is BR's "uploader-pushed this to me" flag, not "I sent
			// it" - every ListDownloads entry is a file this node RECEIVED (an
			// outgoing SendFile creates no download record). Expose it as `pushed`
			// so the UI does not mislabel a received push as "sending".
			Pushed:      d.IsSentFile,
			Description: description,
			Cost:        cost,
			PaidAtoms:   paidAtoms,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Downloads []entry `json:"downloads"`
	}{Downloads: out})
}

// handleDownloadCancel cancels an in-flight download by FID.
func (s *StatusServer) handleDownloadCancel(w http.ResponseWriter, r *http.Request) {
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
		FID string `json:"fid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var fid zkidentity.ShortID
	if err := fid.FromString(req.FID); err != nil {
		http.Error(w, "invalid fid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.CancelDownload(fid); err != nil {
		http.Error(w, "cancel download: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDownloadDelete removes a completed download's file from disk (received
// or sent - both live under <dataDir>/downloads/<nick>/). The file is addressed
// by FID (+ optional UID), never by a client-supplied path: the path comes from
// the matching download record's DiskPath, which is then verified to resolve
// inside the downloads root before removal, so a request can never reach a file
// outside <dataDir>/downloads (e.g. shared content under db/content, which Bison
// Relay manages via unshare, stays untouchable). Body: {fid, uid?}.
func (s *StatusServer) handleDownloadDelete(w http.ResponseWriter, r *http.Request) {
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
		FID string `json:"fid"`
		UID string `json:"uid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	fidStr := strings.TrimSpace(req.FID)
	uidStr := strings.TrimSpace(req.UID)
	if fidStr == "" {
		http.Error(w, "fid is required", http.StatusBadRequest)
		return
	}
	downloads, err := c.ListDownloads()
	if err != nil {
		http.Error(w, "list downloads: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Resolve the on-disk path the SAME way handleContentFile does: by matching
	// the recorded download, never from request input.
	var diskPath string
	for _, d := range downloads {
		if d.FID.String() != fidStr {
			continue
		}
		if uidStr != "" && d.UID.String() != uidStr {
			continue
		}
		diskPath = d.DiskPath
		break
	}
	if diskPath == "" {
		http.Error(w, "no completed download for that file", http.StatusNotFound)
		return
	}
	// Defense in depth: even though DiskPath is brclientd's own value, require it
	// to live strictly inside the downloads root before removing anything.
	root := filepath.Clean(identity.PathsIn(s.DataDir).DownloadsRoot)
	clean := filepath.Clean(diskPath)
	if clean == root || !strings.HasPrefix(clean, root+string(filepath.Separator)) {
		http.Error(w, "refusing to delete outside the downloads directory", http.StatusForbidden)
		return
	}
	if err := os.Remove(clean); err != nil && !os.IsNotExist(err) {
		http.Error(w, "delete file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleContentGet initiates a download of a shared file (FID) from a remote
// user, as advertised by an --embed[download=<fid>,cost=,...]-- tag in a post
// or page. BR fetches the file metadata and pays per-chunk only when the cost
// stored on the host's share is at most max_cost_atoms (default 0 = free
// files only); a higher real cost cancels the download and emits a
// file-download-cost-rejected event carrying the actual price. Progress
// surfaces via /downloads and the file-download-progress /
// file-download-completed events. Body: {uid, fid, max_cost_atoms?}.
func (s *StatusServer) handleContentGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		UID          string `json:"uid"`
		FID          string `json:"fid"`
		MaxCostAtoms uint64 `json:"max_cost_atoms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var uid zkidentity.ShortID
	if err := uid.FromString(req.UID); err != nil {
		http.Error(w, "invalid uid: "+err.Error(), http.StatusBadRequest)
		return
	}
	var fid zkidentity.ShortID
	if err := fid.FromString(req.FID); err != nil {
		http.Error(w, "invalid fid: "+err.Error(), http.StatusBadRequest)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	// Recorded before the request goes out; the metadata reply arrives
	// async and FileDownloadConfirmer consumes the cap there.
	s.DownloadCaps.set(uid.String(), fid.String(), req.MaxCostAtoms)
	if err := c.GetUserContent(uid, fid); err != nil {
		http.Error(w, "get user content: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleContentFile streams the bytes of a fully-downloaded shared file so the
// dashboard can display it inline (image) or offer it as a download. Query:
// fid (required), uid (optional, disambiguates the same file from two peers).
// The path always comes from the matching download record's DiskPath, never
// from the request, and only completed downloads (DiskPath set) are served.
func (s *StatusServer) handleContentFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	fidStr := strings.TrimSpace(r.URL.Query().Get("fid"))
	uidStr := strings.TrimSpace(r.URL.Query().Get("uid"))
	if fidStr == "" {
		http.Error(w, "fid query param is required", http.StatusBadRequest)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	downloads, err := c.ListDownloads()
	if err != nil {
		http.Error(w, "list downloads: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var diskPath, filename string
	for _, d := range downloads {
		if d.FID.String() != fidStr {
			continue
		}
		if uidStr != "" && d.UID.String() != uidStr {
			continue
		}
		diskPath = d.DiskPath
		if d.Metadata != nil {
			// The filename is peer-supplied; run it through the same allowlist as
			// uploads before using it in the Content-Disposition header.
			filename = sanitizeUploadName(d.Metadata.Filename)
		}
		break
	}
	if diskPath == "" {
		http.Error(w, "file not downloaded yet", http.StatusNotFound)
		return
	}
	f, err := os.Open(diskPath)
	if err != nil {
		http.Error(w, "open file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	if filename != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", filename))
	}
	var modtime time.Time
	if info, err := f.Stat(); err == nil {
		modtime = info.ModTime()
	}
	// ServeContent sniffs the content type (by extension then bytes) and
	// supports range requests for large files.
	http.ServeContent(w, r, filename, modtime, f)
}

// handleBackup streams a full-state backup tarball produced by BR's
// client.Backup: the entire data dir (db, msgs, embeds, downloads, pages,
// store, ...) snapshotted under a clientdb read transaction, so it is
// consistent while the node keeps running. The tarball is written to a temp
// dir outside the data dir (so it is not walked into itself) and removed
// after serving.
func (s *StatusServer) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if s.DataDir == "" {
		http.Error(w, "data dir not configured", http.StatusInternalServerError)
		return
	}
	if !s.backupMu.TryLock() {
		http.Error(w, "a backup is already in progress", http.StatusConflict)
		return
	}
	defer s.backupMu.Unlock()

	tmpDir, err := os.MkdirTemp("", "brclientd-backup-")
	if err != nil {
		http.Error(w, "create temp dir: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(tmpDir)

	path, err := c.Backup(r.Context(), s.DataDir, tmpDir)
	if err != nil {
		http.Error(w, "backup: "+err.Error(), http.StatusInternalServerError)
		return
	}
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, "open backup: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	var modtime time.Time
	if info, err := f.Stat(); err == nil {
		modtime = info.ModTime()
	}
	name := filepath.Base(path)
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	http.ServeContent(w, r, name, modtime, f)
}

// handleRates returns the current exchange rates (USD per DCR and per BTC).
// The primary source is BR's built-in rate engine (api.decred.org / dcrdata,
// refreshed every ~10 min). When BR has no DCR rate - typically because that
// Decred infrastructure is temporarily down - we fall back to Kraken's
// DCR/USD ticker, throttled by krakenDCRUSD so a sustained BR outage cannot
// hammer Kraken.
func (s *StatusServer) handleRates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	dcrUSD, btcUSD := c.Rates().Get()
	source := "bisonrelay"
	if dcrUSD <= 0 {
		source = ""
		if kd := krakenDCRUSD(r.Context()); kd > 0 {
			dcrUSD, btcUSD, source = kd, 0, "kraken"
		}
	}

	// Stamp updated_at by change-detection: advance it only when the served
	// price actually changes. A value frozen because BR's upstreams (or Kraken)
	// are down then stays visibly stale rather than looking freshly refreshed.
	// When no source has a rate we keep serving the last-known value + its old
	// timestamp so the consumer can still tell how stale it is.
	rateState.mu.Lock()
	if dcrUSD > 0 && (dcrUSD != rateState.dcrUSD || btcUSD != rateState.btcUSD || source != rateState.source) {
		rateState.dcrUSD, rateState.btcUSD, rateState.source = dcrUSD, btcUSD, source
		rateState.updatedAt = time.Now()
	}
	out := struct {
		DCRUSD    float64 `json:"dcr_usd"`
		BTCUSD    float64 `json:"btc_usd"`
		Source    string  `json:"source"`
		UpdatedAt string  `json:"updated_at"`
	}{DCRUSD: rateState.dcrUSD, BTCUSD: rateState.btcUSD, Source: rateState.source}
	if !rateState.updatedAt.IsZero() {
		out.UpdatedAt = rateState.updatedAt.UTC().Format(time.RFC3339)
	}
	rateState.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// rateState holds the last rate brclientd served and the time that value last
// changed (see handleRates). Separate from krakenRate, which only throttles
// the Kraken HTTP call.
var rateState struct {
	mu        sync.Mutex
	dcrUSD    float64
	btcUSD    float64
	source    string
	updatedAt time.Time
}

// handleStoreMode reports (GET) or switches (POST) this node's resource-hosting
// mode: "off" (serve nothing), "pages" (static markdown) or "store" (a
// simplestore). POST body: {mode, pay_type, account, ship_charge}. Switching
// away from the store stops its invoice watcher, so orders awaiting payment will
// not auto-settle until it is re-enabled.
func (s *StatusServer) handleStoreMode(w http.ResponseWriter, r *http.Request) {
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeStoreMode(w, ctrl.Mode())
	case http.MethodPost:
		var req storeMode
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		switch req.Mode {
		case hostModeOff, hostModePages, hostModeStore:
		default:
			req.Mode = hostModeOff
		}
		if req.Mode == hostModeStore && req.PayType != "ln" && req.PayType != "onchain" {
			req.PayType = "ln"
		}
		if err := ctrl.SetMode(req); err != nil {
			http.Error(w, "set store mode: "+err.Error(), http.StatusBadGateway)
			return
		}
		writeStoreMode(w, ctrl.Mode())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func writeStoreMode(w http.ResponseWriter, m storeMode) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"mode":        m.Mode,
		"pay_type":    m.PayType,
		"account":     m.Account,
		"ship_charge": m.ShipCharge,
	})
}

// handleStoreProducts lists (GET) or upserts (POST) the storefront's products.
// Products are written to the store's products dir; the store live-reloads
// them. Works regardless of the active hosting mode so a catalog can be
// prepared before the store is switched on.
func (s *StatusServer) handleStoreProducts(w http.ResponseWriter, r *http.Request) {
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		products, err := ctrl.listProducts()
		if err != nil {
			http.Error(w, "list products: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"products": products})
	case http.MethodPost:
		var body struct {
			storeProduct
			Create bool `json:"create"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := ctrl.saveProduct(body.storeProduct, body.Create); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleStoreProductDelete removes a product by SKU. Body: {sku}.
func (s *StatusServer) handleStoreProductDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		SKU string `json:"sku"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ctrl.deleteProduct(req.SKU); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStoreOrders lists all storefront orders (across customers), newest
// first.
func (s *StatusServer) handleStoreOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	orders, err := ctrl.listOrders()
	if err != nil {
		http.Error(w, "list orders: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"orders": orders})
}

// handleStoreFileUpload stores an uploaded file under the store dir at the
// given relative path, for products to reference via sendfilename (digital
// downloads). Multipart: path (relative, e.g. ebooks/x.pdf) + file.
func (s *StatusServer) handleStoreFileUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, "parse multipart: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()
	relPath := strings.TrimSpace(r.FormValue("path"))
	overwrite := r.FormValue("overwrite") == "true"
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file part missing: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()
	if relPath == "" {
		relPath = header.Filename
	}
	// The cover (covers/<sku>.jpg) and header banner are display images shown to
	// every customer; only allow image files at those destinations.
	if isStoreImageSlot(relPath) && !isImageFilename(relPath) {
		http.Error(w, "cover and banner must be image files (jpg, png, webp, gif)", http.StatusBadRequest)
		return
	}
	saved, err := ctrl.saveStoreFile(relPath, overwrite, file)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"path": saved})
}

// handleStoreFilesList lists the user-managed media files under the store dir
// (cover images, banner, digital-download goods) - everything except templates
// and the operational subdirs.
func (s *StatusServer) handleStoreFilesList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	files, err := ctrl.listStoreFiles()
	if err != nil {
		http.Error(w, "list files: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"files": files})
}

// handleStoreFileGet streams one store file's bytes for preview/download.
// Query: path.
func (s *StatusServer) handleStoreFileGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	data, mimeType, err := ctrl.readStoreFile(r.URL.Query().Get("path"))
	if os.IsNotExist(err) {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", mimeType)
	_, _ = w.Write(data)
}

// handleStoreFileDelete removes one media file under the store dir. Body: {path}.
func (s *StatusServer) handleStoreFileDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ctrl.deleteStoreFile(req.Path); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStoreTemplates lists the storefront's *.tmpl files (the Go templates
// the store renders pages from).
func (s *StatusServer) handleStoreTemplates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	tmpls, err := ctrl.listTemplates()
	if err != nil {
		http.Error(w, "list templates: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"templates": tmpls})
}

// handleStoreTemplateFile returns one template's raw content. Query: name.
func (s *StatusServer) handleStoreTemplateFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	name := r.URL.Query().Get("name")
	content, err := ctrl.readTemplate(name)
	if os.IsNotExist(err) {
		http.Error(w, "template not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"name": name, "content": content})
}

// handleStoreTemplateSave writes (creates or overwrites) one template. Body:
// {name, content}.
func (s *StatusServer) handleStoreTemplateSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ctrl.saveTemplate(req.Name, req.Content); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStoreTemplateDelete removes one template. Body: {name}.
func (s *StatusServer) handleStoreTemplateDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ctrl.deleteTemplate(req.Name); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStoreOrderStatus updates one order's status. Body: {uid, id, status}.
func (s *StatusServer) handleStoreOrderStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		UID    string `json:"uid"`
		ID     uint64 `json:"id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ctrl.setOrderStatus(req.UID, req.ID, req.Status); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStoreOrderComment appends a merchant comment to an order and DMs the
// buyer. Body: {uid, id, comment}.
func (s *StatusServer) handleStoreOrderComment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctrl := s.currentStoreController()
	if ctrl == nil {
		http.Error(w, "store controller not yet ready", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		UID     string `json:"uid"`
		ID      uint64 `json:"id"`
		Comment string `json:"comment"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ctrl.addOrderComment(req.UID, req.ID, req.Comment); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// krakenMinInterval throttles the last-resort Kraken fallback: at most one
// request per window, no matter how often /rates is hit or how long BR's rate
// source stays down.
const krakenMinInterval = 10 * time.Minute

// krakenRate caches the last Kraken DCR/USD price and the time of the last
// attempt (success or failure) so the throttle holds even when Kraken errors.
var krakenRate struct {
	mu      sync.Mutex
	dcrUSD  float64
	lastTry time.Time
}

// krakenDCRUSD returns a DCR/USD price from Kraken, fetching at most once per
// krakenMinInterval and otherwise returning the cached value (0 if never
// fetched). Only called when BR has no rate of its own.
func krakenDCRUSD(ctx context.Context) float64 {
	krakenRate.mu.Lock()
	defer krakenRate.mu.Unlock()
	if !krakenRate.lastTry.IsZero() && time.Since(krakenRate.lastTry) < krakenMinInterval {
		return krakenRate.dcrUSD
	}
	krakenRate.lastTry = time.Now()
	price, err := fetchKrakenDCRUSD(ctx)
	if err != nil {
		// Keep any earlier value; the throttle prevents an immediate retry.
		return krakenRate.dcrUSD
	}
	krakenRate.dcrUSD = price
	return price
}

// fetchKrakenDCRUSD pulls the last-trade DCR/USD price from Kraken's public
// ticker (a clearnet call; no .onion endpoint exists).
func fetchKrakenDCRUSD(ctx context.Context) (float64, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet,
		"https://api.kraken.com/0/public/Ticker?pair=DCRUSD", nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("kraken HTTP %d", resp.StatusCode)
	}
	var kr struct {
		Error  []string `json:"error"`
		Result map[string]struct {
			C []string `json:"c"` // [last trade price, lot volume]
		} `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&kr); err != nil {
		return 0, err
	}
	if len(kr.Error) > 0 {
		return 0, fmt.Errorf("kraken: %v", kr.Error)
	}
	for _, v := range kr.Result {
		if len(v.C) > 0 {
			return strconv.ParseFloat(v.C[0], 64)
		}
	}
	return 0, fmt.Errorf("kraken: empty result")
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
		"id":          summ.ID.String(),
		"from":        summ.From.String(),
		"author_id":   summ.AuthorID.String(),
		"author_nick": summ.AuthorNick,
		"date":        summ.Date.Unix(),
		"title":       summ.Title,
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
		StatusFrom   string `json:"status_from"`
		FromNick     string `json:"from_nick"`
		Comment      string `json:"comment"`
		Parent       string `json:"parent,omitempty"`
		Timestamp    int64  `json:"timestamp"`
		Identifier   string `json:"identifier,omitempty"`
		StatusID     string `json:"status_id,omitempty"`
		Unreplicated bool   `json:"unreplicated,omitempty"`
	}
	out := make([]comment, 0, len(updates))
	for _, u := range updates {
		body, ok := u.Attributes[rpc.RMPSComment]
		if !ok || body == "" {
			continue
		}
		var ts int64
		if tsStr := u.Attributes[rpc.RMPTimestamp]; tsStr != "" {
			// Hex per BR's status-update writer (client_posts.go).
			if n, err := strconv.ParseInt(tsStr, 16, 64); err == nil {
				ts = n
			}
		}
		// The identifier attribute is the post id (shared by every
		// comment); the PMS hash is the unique status id receive
		// receipts are keyed by.
		h := u.Hash()
		var sid zkidentity.ShortID
		copy(sid[:], h[:])
		out = append(out, comment{
			StatusFrom: u.Attributes[rpc.RMPStatusFrom],
			FromNick:   u.Attributes[rpc.RMPFromNick],
			Comment:    body,
			Parent:     u.Attributes[rpc.RMPParent],
			Timestamp:  ts,
			Identifier: u.Attributes[rpc.RMPIdentifier],
			StatusID:   sid.String(),
		})
	}
	// Merge in own comments sent to the author but not yet broadcast back.
	// Dedupe by text + parent guards the race where the replicated copy
	// already landed but the tracker entry was not yet removed.
	if s.Unrepl != nil {
		myID := c.PublicID().String()
		myNick := c.LocalNick()
		for _, e := range s.Unrepl.list(uid.String(), pid.String()) {
			replicated := false
			for _, o := range out {
				if o.StatusFrom == myID && o.Comment == e.Comment && o.Parent == e.Parent {
					replicated = true
					break
				}
			}
			if replicated {
				continue
			}
			out = append(out, comment{
				StatusFrom:   myID,
				FromNick:     myNick,
				Comment:      e.Comment,
				Parent:       e.Parent,
				Timestamp:    e.Timestamp,
				Unreplicated: true,
			})
		}
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
	// Comments on the local user's own posts are added to the post directly
	// by the client (no relay round-trip), so only track comments to remote
	// authors. Normalized ShortID strings so removal in the post-status ntfn
	// (which compares against PublicID().String()/pid.String()) and the
	// merge in handlePostComments match exactly.
	if s.Unrepl != nil && uid != c.PublicID() {
		parentStr := ""
		if parent != nil {
			parentStr = parent.String()
		}
		s.Unrepl.add(uid.String(), pid.String(), req.Comment, parentStr, time.Now().Unix())
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Identifier string `json:"identifier"`
	}{Identifier: cid.String()})
}

// handlePostHearts returns the current heart count and whether the local
// identity's most recent heart status update on that post left it in the
// "hearted" state. Walks the same ListPostStatusUpdates that /stats/posts
// uses, with toggle semantics from rpc.routedrpc.go (1 adds, 0 removes).
func (s *StatusServer) handlePostHearts(w http.ResponseWriter, r *http.Request) {
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
	// Updates are time-ordered, so the last RMPSHeart status per user is
	// their current toggle state (1 adds, 0 removes; rpc.routedrpc.go).
	type heartState struct {
		mode string
		nick string
	}
	last := make(map[string]heartState)
	order := make([]string, 0, len(updates))
	for _, u := range updates {
		if u.Attributes == nil {
			continue
		}
		mode, ok := u.Attributes[rpc.RMPSHeart]
		if !ok || (mode != rpc.RMPSHeartYes && mode != rpc.RMPSHeartNo) {
			continue
		}
		from := u.Attributes[rpc.RMPStatusFrom]
		if _, seen := last[from]; !seen {
			order = append(order, from)
		}
		last[from] = heartState{mode: mode, nick: u.Attributes[rpc.RMPFromNick]}
	}
	type heartRow struct {
		User string `json:"user"`
		Nick string `json:"nick"`
	}
	myID := c.PublicID().String()
	hearts := make([]heartRow, 0, len(order))
	heartedByMe := false
	for _, from := range order {
		st := last[from]
		if st.mode != rpc.RMPSHeartYes {
			continue
		}
		if from == myID {
			heartedByMe = true
		}
		hearts = append(hearts, heartRow{User: from, Nick: st.nick})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Count       int        `json:"count"`
		HeartedByMe bool       `json:"hearted_by_me"`
		Hearts      []heartRow `json:"hearts"`
	}{Count: len(hearts), HeartedByMe: heartedByMe, Hearts: hearts})
}

// handlePostHeart toggles the local identity's heart on a remote post.
// Body: {uid, pid, heart bool}. Delegates to client.HeartPost which sends
// a status update with RMPSHeartYes / RMPSHeartNo.
func (s *StatusServer) handlePostHeart(w http.ResponseWriter, r *http.Request) {
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
		UID   string `json:"uid"`
		PID   string `json:"pid"`
		Heart bool   `json:"heart"`
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
	if err := c.HeartPost(uid, pid, req.Heart); err != nil {
		http.Error(w, "heart post: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handlePostReceiveReceipts lists the receive receipts recorded for one of
// the local user's own posts (which subscribers acknowledged receiving it).
// ListPostReceiveReceipts is keyed on the local identity, so posts authored
// by others naturally return an empty list. Timestamps are Unix milliseconds
// (clientdb.ReceiveReceipt).
func (s *StatusServer) handlePostReceiveReceipts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	pidStr := strings.TrimSpace(r.URL.Query().Get("pid"))
	if pidStr == "" {
		http.Error(w, "pid query param is required", http.StatusBadRequest)
		return
	}
	var pid zkidentity.ShortID
	if err := pid.FromString(pidStr); err != nil {
		http.Error(w, "invalid pid: "+err.Error(), http.StatusBadRequest)
		return
	}
	rrs, err := c.ListPostReceiveReceipts(pid)
	if err != nil {
		http.Error(w, "list receive receipts: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type receipt struct {
		User       string `json:"user"`
		Nick       string `json:"nick"`
		ServerTime int64  `json:"server_time"`
		ClientTime int64  `json:"client_time"`
	}
	out := make([]receipt, 0, len(rrs))
	for _, rr := range rrs {
		if rr == nil {
			continue
		}
		out = append(out, receipt{
			User:       rr.User.String(),
			Nick:       c.UserLogNick(rr.User),
			ServerTime: rr.ServerTime,
			ClientTime: rr.ClientTime,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Receipts []receipt `json:"receipts"`
	}{Receipts: out})
}

// handlePostRelay relays a known post: to a single user when to_uid is set
// (RelayPost), otherwise to all of the local client's post subscribers
// (RelayPostToSubscribers). Body: {uid (post author), pid, to_uid?}.
func (s *StatusServer) handlePostRelay(w http.ResponseWriter, r *http.Request) {
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
		UID   string `json:"uid"`
		PID   string `json:"pid"`
		ToUID string `json:"to_uid"`
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
	if strings.TrimSpace(req.ToUID) != "" {
		toUID, ok := parseUIDHex(w, "to_uid", req.ToUID)
		if !ok {
			return
		}
		if err := c.RelayPost(uid, pid, toUID); err != nil {
			http.Error(w, "relay post: "+err.Error(), http.StatusBadGateway)
			return
		}
	} else {
		if err := c.RelayPostToSubscribers(uid, pid); err != nil {
			http.Error(w, "relay post to subscribers: "+err.Error(), http.StatusBadGateway)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// handlePostCommentReceiveReceipts lists the receive receipts recorded for
// the comments on one of the local user's own posts, grouped by the
// comment's status id (the PMS hash, matching the status_id field of
// /posts/comments). Comment receipts are recorded on the post author's node
// because it relays the comments, so other users' posts return empty.
func (s *StatusServer) handlePostCommentReceiveReceipts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	pidStr := strings.TrimSpace(r.URL.Query().Get("pid"))
	if pidStr == "" {
		http.Error(w, "pid query param is required", http.StatusBadRequest)
		return
	}
	var pid zkidentity.ShortID
	if err := pid.FromString(pidStr); err != nil {
		http.Error(w, "invalid pid: "+err.Error(), http.StatusBadRequest)
		return
	}
	updates, err := c.ListPostStatusUpdates(c.PublicID(), pid)
	if err != nil {
		http.Error(w, "list status updates: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type receipt struct {
		User       string `json:"user"`
		Nick       string `json:"nick"`
		ServerTime int64  `json:"server_time"`
		ClientTime int64  `json:"client_time"`
	}
	out := make(map[string][]receipt)
	for _, u := range updates {
		if u.Attributes[rpc.RMPSComment] == "" {
			continue
		}
		h := u.Hash()
		var sid zkidentity.ShortID
		copy(sid[:], h[:])
		rrs, err := c.ListPostCommentReceiveReceipts(pid, sid)
		if err != nil || len(rrs) == 0 {
			continue
		}
		list := make([]receipt, 0, len(rrs))
		for _, rr := range rrs {
			if rr == nil {
				continue
			}
			list = append(list, receipt{
				User:       rr.User.String(),
				Nick:       c.UserLogNick(rr.User),
				ServerTime: rr.ServerTime,
				ClientTime: rr.ClientTime,
			})
		}
		out[sid.String()] = list
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Receipts map[string][]receipt `json:"receipts"`
	}{Receipts: out})
}

// tipAttemptRow is the wire shape of one tracked tip attempt. Invoices are
// omitted (large bolt11 strings the dashboard has no use for).
type tipAttemptRow struct {
	UID                  string     `json:"uid"`
	Tag                  int32      `json:"tag"`
	AmountMAtoms         uint64     `json:"amount_matoms"`
	Created              time.Time  `json:"created"`
	Attempts             int32      `json:"attempts"`
	MaxAttempts          int32      `json:"max_attempts"`
	InvoiceRequested     *time.Time `json:"invoice_requested,omitempty"`
	PaymentAttempt       *time.Time `json:"payment_attempt,omitempty"`
	PaymentAttemptCount  uint32     `json:"payment_attempt_count"`
	PaymentAttemptFailed *time.Time `json:"payment_attempt_failed,omitempty"`
	LastInvoiceError     *string    `json:"last_invoice_error,omitempty"`
	Completed            *time.Time `json:"completed,omitempty"`
}

// handleTipAttempts lists the locally tracked tip attempts to one contact:
// amounts, retry counts, invoice/payment timestamps, and completion state.
func (s *StatusServer) handleTipAttempts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	uid, ok := parseUIDHex(w, "uid", strings.TrimSpace(r.URL.Query().Get("uid")))
	if !ok {
		return
	}
	atts, err := c.ListTipUserAttempts(uid)
	if err != nil {
		http.Error(w, "list tip attempts: "+err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]tipAttemptRow, 0, len(atts))
	for _, a := range atts {
		out = append(out, tipAttemptRow{
			UID:                  a.UID.String(),
			Tag:                  a.Tag,
			AmountMAtoms:         a.MilliAtoms,
			Created:              a.Created,
			Attempts:             a.Attempts,
			MaxAttempts:          a.MaxAttempts,
			InvoiceRequested:     a.InvoiceRequested,
			PaymentAttempt:       a.PaymentAttempt,
			PaymentAttemptCount:  a.PaymentAttemptCount,
			PaymentAttemptFailed: a.PaymentAttemptFailed,
			LastInvoiceError:     a.LastInvoiceError,
			Completed:            a.Completed,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Attempts []tipAttemptRow `json:"attempts"`
	}{Attempts: out})
}

// handleRunningTipAttempts lists the tip attempts the daemon is actively
// driving, with the next scheduled action; amounts are joined from the
// per-user attempt records via the tag.
func (s *StatusServer) handleRunningTipAttempts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	running, err := c.ListRunningTipUserAttempts()
	if err != nil {
		http.Error(w, "list running tip attempts: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type runningRow struct {
		UID            string    `json:"uid"`
		Nick           string    `json:"nick"`
		Tag            int32     `json:"tag"`
		NextAction     string    `json:"next_action"`
		NextActionTime time.Time `json:"next_action_time"`
		AmountMAtoms   uint64    `json:"amount_matoms"`
	}
	amounts := make(map[string]map[int32]uint64)
	out := make([]runningRow, 0, len(running))
	for _, ra := range running {
		uidStr := ra.UID.String()
		byTag, ok := amounts[uidStr]
		if !ok {
			byTag = make(map[int32]uint64)
			if atts, err := c.ListTipUserAttempts(ra.UID); err == nil {
				for _, a := range atts {
					byTag[a.Tag] = a.MilliAtoms
				}
			}
			amounts[uidStr] = byTag
		}
		out = append(out, runningRow{
			UID:            uidStr,
			Nick:           c.UserLogNick(ra.UID),
			Tag:            ra.Tag,
			NextAction:     ra.NextAction,
			NextActionTime: ra.NextActionTime,
			AmountMAtoms:   byTag[ra.Tag],
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Running []runningRow `json:"running"`
	}{Running: out})
}

// handleNotifications streams JSONL events from the in-process notif bus to
// a long-lived subscriber (the dashboard's bisonrelay_events.go). Each line
// is one JSON object. The endpoint flushes after every event so consumers
// see them in real time. Events brclientd publishes here are ones BR has no
// upstream clientrpc stream for (e.g. OnKXSuggested) plus any other
// daemon-side notifications we surface in the future.
// handleRecentNotifications returns the persisted daemon notes (newest
// first) that power the dashboard's notification bell. Unlike the live
// /notifications stream these survive the browser being closed.
func (s *StatusServer) handleRecentNotifications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Notes == nil {
		http.Error(w, "notification store not configured", http.StatusServiceUnavailable)
		return
	}
	n := 50
	if v := strings.TrimSpace(r.URL.Query().Get("n")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 && parsed <= maxStoredNotes {
			n = parsed
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Notifications []brNote `json:"notifications"`
	}{Notifications: s.Notes.recent(n)})
}

func (s *StatusServer) handleDeleteNotification(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Notes == nil {
		http.Error(w, "notification store not configured", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.Notes.delete(req.ID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *StatusServer) handleClearNotifications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Notes == nil {
		http.Error(w, "notification store not configured", http.StatusServiceUnavailable)
		return
	}
	s.Notes.clear()
	w.WriteHeader(http.StatusNoContent)
}

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

	// Stream per-chunk relay-upload progress to live subscribers (the
	// dashboard) while the send is in flight. BR's
	// sendPreparedSendqItemListSync emits a SendProgress as the relay acks
	// each sendq item (file metadata + every chunk); a draining goroutine
	// forwards them as file-send-progress events. The progress channel was
	// unusable until the upstream fix that landed after v0.2.4.
	progressChan := make(chan client.SendProgress, 16)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		for prog := range progressChan {
			if s.Notifs == nil {
				continue
			}
			s.Notifs.Publish(NotifEvent{
				Type: "file-send-progress",
				Payload: map[string]any{
					"uid":      ru.ID().String(),
					"nick":     ru.Nick(),
					"filename": safeName,
					"size":     header.Size,
					"sent":     prog.Sent,
					"total":    prog.Total,
				},
			})
		}
	}()
	err = c.SendFile(ru.ID(), 0, storedPath, progressChan)
	close(progressChan)
	<-progressDone
	if err != nil {
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

// ----- /stats handlers ---------------------------------------------------

type quantileOut struct {
	Rel   string `json:"rel"`
	N     int64  `json:"n"`
	MaxNs int64  `json:"max_ns"`
}

func toQuantileOut(qs []timestats.Quantile) []quantileOut {
	out := make([]quantileOut, len(qs))
	for i, q := range qs {
		out[i] = quantileOut{Rel: q.Rel, N: q.N, MaxNs: q.Max}
	}
	return out
}

type policyOut struct {
	PushPayRateMAtoms    uint64 `json:"push_pay_rate_matoms"`
	PushPayRateBytes     uint64 `json:"push_pay_rate_bytes"`
	PushPayRateMinMAtoms uint64 `json:"push_pay_rate_min_matoms"`
	SubPayRate           uint64 `json:"sub_pay_rate"`
	MaxPushInvoices      int    `json:"max_push_invoices"`
	MaxMsgSize           uint   `json:"max_msg_size"`
	ExpirationDays       int    `json:"expiration_days"`
}

func policyOutFrom(p clientintf.ServerPolicy) policyOut {
	return policyOut{
		PushPayRateMAtoms:    p.PushPayRateMAtoms,
		PushPayRateBytes:     p.PushPayRateBytes,
		PushPayRateMinMAtoms: p.PushPayRateMinMAtoms,
		SubPayRate:           p.SubPayRate,
		MaxPushInvoices:      p.MaxPushInvoices,
		MaxMsgSize:           p.MaxMsgSize,
		ExpirationDays:       p.ExpirationDays,
	}
}

func policyFromSession(c *client.Client) policyOut {
	sess := c.ServerSession()
	if sess == nil {
		return policyOut{}
	}
	return policyOutFrom(sess.Policy())
}

type topContactOut struct {
	UID      string `json:"uid"`
	Nick     string `json:"nick"`
	Sent     int64  `json:"sent_matoms"`
	Received int64  `json:"received_matoms"`
}

// handleStatsOverview is the compact summary the Stats landing page renders
// as hero counters + top-contacts strip + connection-health badge. All
// figures are derived from data the BR client already has in memory, so it
// stays cheap to refresh on a 30s tick.
func (s *StatusServer) handleStatsOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	status := s.Tracker.Get()

	contacts := c.AddressBook()
	posts, _ := c.ListPosts()
	authoredCount := 0
	myID := c.PublicID()
	for _, p := range posts {
		if p.AuthorID == myID {
			authoredCount++
		}
	}
	subs, _ := c.ListPostSubscriptions()
	subscribers, _ := c.ListPostSubscribers()

	stats, _ := c.ListPaymentStats()
	var sumSent, sumRecv, sumFees int64
	for _, ps := range stats {
		sumSent += ps.TotalSent
		sumRecv += ps.TotalReceived
		sumFees += ps.TotalPayFee
	}

	// Top 5 contacts by total activity (sent+received) for the leaderboard strip.
	type rank struct {
		uid     clientintf.UserID
		sent    int64
		recv    int64
		ranking int64
	}
	ranks := make([]rank, 0, len(stats))
	for uid, ps := range stats {
		ranks = append(ranks, rank{uid: uid, sent: ps.TotalSent, recv: ps.TotalReceived,
			ranking: ps.TotalSent + ps.TotalReceived})
	}
	// Simple partial sort: we want the 5 largest by ranking.
	for i := 0; i < len(ranks); i++ {
		for j := i + 1; j < len(ranks); j++ {
			if ranks[j].ranking > ranks[i].ranking {
				ranks[i], ranks[j] = ranks[j], ranks[i]
			}
		}
		if i >= 5 {
			break
		}
	}
	if len(ranks) > 5 {
		ranks = ranks[:5]
	}
	top := make([]topContactOut, 0, len(ranks))
	for _, r := range ranks {
		nick, _ := c.UserNick(r.uid)
		top = append(top, topContactOut{
			UID:      r.uid.String(),
			Nick:     nick,
			Sent:     r.sent,
			Received: r.recv,
		})
	}

	// RMQ p50 latency (fall back to first available quantile if no p50).
	rmqQs := c.RMQTimingStat()
	var p50Ns int64
	for _, q := range rmqQs {
		if q.Rel == "50%" {
			p50Ns = q.Max
			break
		}
	}
	if p50Ns == 0 && len(rmqQs) > 0 {
		p50Ns = rmqQs[0].Max
	}

	out := struct {
		Nick             string          `json:"nick"`
		Identity         string          `json:"identity"`
		Stage            Stage           `json:"stage"`
		ConnectedAt      time.Time       `json:"connected_at,omitempty"`
		ServerNode       string          `json:"server_node,omitempty"`
		ContactsCount    int             `json:"contacts_count"`
		PostsAuthored    int             `json:"posts_authored"`
		SubscriptionsCnt int             `json:"subscriptions_count"`
		SubscribersCnt   int             `json:"subscribers_count"`
		TotalSentMAtoms  int64           `json:"total_sent_matoms"`
		TotalRecvMAtoms  int64           `json:"total_received_matoms"`
		TotalFeesMAtoms  int64           `json:"total_fees_matoms"`
		RmqP50Ns         int64           `json:"rmq_p50_ns"`
		TopContacts      []topContactOut `json:"top_contacts"`
	}{
		Nick:             status.Nick,
		Identity:         myID.String(),
		Stage:            status.Stage,
		ConnectedAt:      status.ConnectedAt,
		ServerNode:       status.ServerNode,
		ContactsCount:    len(contacts),
		PostsAuthored:    authoredCount,
		SubscriptionsCnt: len(subs),
		SubscribersCnt:   len(subscribers),
		TotalSentMAtoms:  sumSent,
		TotalRecvMAtoms:  sumRecv,
		TotalFeesMAtoms:  sumFees,
		RmqP50Ns:         p50Ns,
		TopContacts:      top,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// handleClearPayStats removes the recorded payment stats of a single user.
// Mirrors bruig's per-user trash action (CTClearPayStats); the library
// deletes the user's paystats file and summary entry, so the user drops out
// of ListPaymentStats until new payments are recorded.
func (s *StatusServer) handleClearPayStats(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.decodeUIDOnlyBody(w, r)
	if !ok {
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if err := c.ClearPayStats(&uid); err != nil {
		http.Error(w, "clear pay stats: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStatsPayments returns the per-user payment table plus the per-user
// prefix breakdowns for every user. bruig's paystats.dart shows the same
// data but only fetches breakdowns on row click; here we ship both in one
// shot so the dashboard can render the drawer instantly.
func (s *StatusServer) handleStatsPayments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	stats, err := c.ListPaymentStats()
	if err != nil {
		http.Error(w, "list payment stats: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type userRow struct {
		UID        string                     `json:"uid"`
		Nick       string                     `json:"nick"`
		Sent       int64                      `json:"sent_matoms"`
		Received   int64                      `json:"received_matoms"`
		Fees       int64                      `json:"fees_matoms"`
		Breakdowns []clientdb.PayStatsSummary `json:"breakdowns,omitempty"`
	}
	rows := make([]userRow, 0, len(stats))
	for uid, ps := range stats {
		nick, _ := c.UserNick(uid)
		summary, _ := c.SummarizeUserPayStats(uid)
		rows = append(rows, userRow{
			UID:        uid.String(),
			Nick:       nick,
			Sent:       ps.TotalSent,
			Received:   ps.TotalReceived,
			Fees:       ps.TotalPayFee,
			Breakdowns: summary,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Users    []userRow     `json:"users"`
		RmqRttQs []quantileOut `json:"rmq_rtt_quantiles"`
	}{
		Users:    rows,
		RmqRttQs: toQuantileOut(c.RMQTimingStat()),
	})
}

// handleStatsNetwork returns server-session details: server LN pubkey,
// recommended hub, full server policy (push-pay rates, retention, max
// message size), connection start time, and the RMQ RTT quantile histogram.
// All of this is hidden in bruig.
func (s *StatusServer) handleStatsNetwork(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	status := s.Tracker.Get()
	rmqWaiting, rmqSending := c.RMQLen()
	sendqItems, sendqDests := c.SendQueueLen()
	out := struct {
		ServerNode      string        `json:"server_node,omitempty"`
		RecommendedPeer string        `json:"recommended_peer,omitempty"`
		ConnectedAt     time.Time     `json:"connected_at,omitempty"`
		Stage           Stage         `json:"stage"`
		Policy          policyOut     `json:"policy"`
		RmqQuantiles    []quantileOut `json:"rmq_quantiles"`
		Queues          QueueStats    `json:"queues"`
	}{
		ServerNode:      status.ServerNode,
		RecommendedPeer: status.RecommendedPeer,
		ConnectedAt:     status.ConnectedAt,
		Stage:           status.Stage,
		Policy:          policyFromSession(c),
		RmqQuantiles:    toQuantileOut(c.RMQTimingStat()),
		Queues: QueueStats{
			RMQWaiting:  rmqWaiting,
			RMQSending:  rmqSending,
			SendQItems:  sendqItems,
			SendQDests:  sendqDests,
			RVsUpToDate: c.RVsUpToDate(),
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// handleStatsContacts returns per-contact metadata + ratchet debug info so
// the Contacts tab can paint a "ratchet health" badge per row (saved-keys
// count, will-ratchet flag, last-enc/dec ages). For non-running users (no
// in-memory RemoteUser) we still emit the addressbook row with a zero
// ratchet block so the UI can show "offline" rather than dropping them.
func (s *StatusServer) handleStatsContacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	type ratchetOut struct {
		NbSavedKeys  int       `json:"nb_saved_keys"`
		WillRatchet  bool      `json:"will_ratchet"`
		LastEncTime  time.Time `json:"last_enc_time,omitempty"`
		LastDecTime  time.Time `json:"last_dec_time,omitempty"`
		SendRVPlain  string    `json:"send_rv_plain,omitempty"`
		RecvRVPlain  string    `json:"recv_rv_plain,omitempty"`
		DrainRVPlain string    `json:"drain_rv_plain,omitempty"`
	}
	type row struct {
		UID                  string      `json:"uid"`
		Nick                 string      `json:"nick"`
		NickAlias            string      `json:"nick_alias,omitempty"`
		FirstCreated         time.Time   `json:"first_created"`
		LastCompletedKX      time.Time   `json:"last_completed_kx"`
		LastHandshakeAttempt time.Time   `json:"last_handshake_attempt,omitempty"`
		Ignored              bool        `json:"ignored"`
		Ratchet              *ratchetOut `json:"ratchet,omitempty"`
	}
	entries := c.AddressBook()
	out := make([]row, 0, len(entries))
	for _, e := range entries {
		if e.ID == nil {
			continue
		}
		r := row{
			UID:                  e.ID.Identity.String(),
			Nick:                 e.ID.Nick,
			NickAlias:            e.NickAlias,
			FirstCreated:         e.FirstCreated,
			LastCompletedKX:      e.LastCompletedKX,
			LastHandshakeAttempt: e.LastHandshakeAttempt,
			Ignored:              e.Ignored,
		}
		if ru, err := c.UserByID(e.ID.Identity); err == nil && ru != nil {
			d := ru.RatchetDebugInfo()
			r.Ratchet = &ratchetOut{
				NbSavedKeys:  d.NbSavedKeys,
				WillRatchet:  d.WillRatchet,
				LastEncTime:  d.LastEncTime,
				LastDecTime:  d.LastDecTime,
				SendRVPlain:  d.SendRVPlain,
				RecvRVPlain:  d.RecvRVPlain,
				DrainRVPlain: d.DrainRVPlain,
			}
		}
		out = append(out, r)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Contacts []row `json:"contacts"`
	}{Contacts: out})
}

// handleStatsPosts returns the local user's authored posts with engagement
// aggregates (hearts, comments) derived from ListPostStatusUpdates, plus
// counts of inbound subscribers and outbound subscriptions.
func (s *StatusServer) handleStatsPosts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c := s.currentClient()
	if c == nil {
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	myID := c.PublicID()
	posts, err := c.ListPosts()
	if err != nil {
		http.Error(w, "list posts: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type authored struct {
		PID          string    `json:"pid"`
		Title        string    `json:"title"`
		Date         time.Time `json:"date"`
		LastStatusTS time.Time `json:"last_status_ts,omitempty"`
		Hearts       int       `json:"hearts"`
		Comments     int       `json:"comments"`
	}
	out := make([]authored, 0)
	for _, p := range posts {
		if p.AuthorID != myID {
			continue
		}
		row := authored{
			PID:          p.ID.String(),
			Title:        p.Title,
			Date:         p.Date,
			LastStatusTS: p.LastStatusTS,
		}
		updates, _ := c.ListPostStatusUpdates(myID, p.ID)
		for _, u := range updates {
			if u.Attributes == nil {
				continue
			}
			// Heart toggles: "1" adds, "0" removes. Comments: any non-empty
			// body counts. Same semantics rpc.routedrpc.go uses on the
			// hash side (see HasContent at routedrpc.go:1332).
			switch u.Attributes[rpc.RMPSHeart] {
			case rpc.RMPSHeartYes:
				row.Hearts++
			case rpc.RMPSHeartNo:
				if row.Hearts > 0 {
					row.Hearts--
				}
			}
			if u.Attributes[rpc.RMPSComment] != "" {
				row.Comments++
			}
		}
		out = append(out, row)
	}
	subs, _ := c.ListPostSubscriptions()
	subscribers, _ := c.ListPostSubscribers()
	type subscriberRow struct {
		UID  string `json:"uid"`
		Nick string `json:"nick"`
	}
	subscriberRows := make([]subscriberRow, 0, len(subscribers))
	for _, uid := range subscribers {
		subscriberRows = append(subscriberRows, subscriberRow{
			UID:  uid.String(),
			Nick: c.UserLogNick(uid),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Authored         []authored      `json:"authored"`
		SubscribersCount int             `json:"subscribers_count"`
		SubscriptionsCnt int             `json:"subscriptions_count"`
		Subscribers      []subscriberRow `json:"subscribers"`
	}{
		Authored:         out,
		SubscribersCount: len(subscribers),
		SubscriptionsCnt: len(subs),
		Subscribers:      subscriberRows,
	})
}
