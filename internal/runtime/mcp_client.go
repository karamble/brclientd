// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/decred/dcrd/dcrutil/v4"
	"github.com/decred/slog"
	"github.com/karamble/brmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The BR-MCP client engine lets local MCP agents call tool services offered
// by Bison Relay bots (see github.com/karamble/brmcp). brclientd exposes a
// localhost streamable-HTTP MCP endpoint per bot (/mcp/<bot-uid>, bearer
// gated, default disabled); each endpoint mirrors the remote bot's tools and
// relays calls over the relay, settling paid tools by LN invoice or BR tip
// under the user's caps, either automatically or after explicit approval.

// mcpClientSettings is persisted as mcpclient.json in the data dir.
type mcpClientSettings struct {
	Enabled bool   `json:"enabled"`
	Bind    string `json:"bind"`
	Token   string `json:"token"`
	// Mode is "approval" (every payment waits for a human decision) or
	// "autopay" (payments under the caps run unattended).
	Mode string `json:"mode"`
	// Caps are hard ceilings on BOTH modes; zero means never pay.
	PerCallCapAtoms int64 `json:"per_call_cap_atoms"`
	PerDayCapAtoms  int64 `json:"per_day_cap_atoms"`
	// AllowedBots is the default-deny list of callable bot uids.
	AllowedBots []string `json:"allowed_bots"`
	// ApprovalTimeoutSecs bounds how long a call waits for a decision.
	ApprovalTimeoutSecs int `json:"approval_timeout_secs"`
}

func (s mcpClientSettings) withDefaults() mcpClientSettings {
	if s.Bind == "" {
		s.Bind = "127.0.0.1:8891"
	}
	if s.Mode != "autopay" {
		s.Mode = "approval"
	}
	if s.ApprovalTimeoutSecs <= 0 {
		s.ApprovalTimeoutSecs = 120
	}
	return s
}

type mcpSpendEntry struct {
	TS    int64  `json:"ts"`
	Bot   string `json:"bot"`
	Tool  string `json:"tool"`
	Rail  string `json:"rail"`
	Atoms int64  `json:"atoms"`
}

// mcpPending is one payment awaiting a human decision (approval mode). It
// exists only while the tool call blocks; a restart fails the call anyway,
// so the queue is in-memory by design.
type mcpPending struct {
	ID      string `json:"id"`
	Bot     string `json:"bot"`
	Tool    string `json:"tool"`
	Atoms   int64  `json:"atoms"`
	Invoice string `json:"invoice,omitempty"`
	Created int64  `json:"created"`

	decision chan bool
	once     sync.Once
}

func (p *mcpPending) decide(approve bool) {
	p.once.Do(func() { p.decision <- approve })
}

var mcpUIDRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

type mcpBotLink struct {
	mu      sync.Mutex
	uid     string
	conn    *brmcp.Conn
	session *mcp.ClientSession
	proxy   *mcp.Server
}

type mcpEngine struct {
	ctx     context.Context
	c       *client.Client
	pay     *client.DcrlnPaymentClient
	log     slog.Logger
	dataDir string

	mu       sync.Mutex
	settings mcpClientSettings
	router   *brmcp.Router
	bots     map[string]*mcpBotLink
	pending  map[string]*mcpPending
	spend    []mcpSpendEntry
	httpSrv  *http.Server
}

func newMCPEngine(ctx context.Context, c *client.Client, pay *client.DcrlnPaymentClient,
	dataDir string, log slog.Logger) (*mcpEngine, error) {

	e := &mcpEngine{
		ctx:     ctx,
		c:       c,
		pay:     pay,
		log:     log,
		dataDir: dataDir,
		bots:    make(map[string]*mcpBotLink),
		pending: make(map[string]*mcpPending),
	}
	if err := e.loadState(); err != nil {
		return nil, err
	}
	e.router = brmcp.NewRouter(brmcp.RouterConfig{
		Sender: mcpSender{e},
		Allow:  e.botAllowed,
		Logf: func(format string, args ...any) {
			log.Debugf(format, args...)
		},
	})
	// Feed every inbound PM through the router; non-envelope chat is
	// ignored there, so normal messaging is unaffected.
	c.NotificationManager().Register(client.OnPMNtfn(func(ru *client.RemoteUser, pm rpc.RMPrivateMessage, _ time.Time) {
		e.router.HandlePM(ru.ID().String(), pm.Message)
	}))
	if e.settings.Enabled {
		if err := e.startListenerLocked(); err != nil {
			log.Errorf("MCP listener: %v", err)
		}
	}
	go func() {
		<-ctx.Done()
		e.stopListener()
	}()
	return e, nil
}

// mcpSender resolves the peer and sends one PM through the BR client.
type mcpSender struct{ e *mcpEngine }

func (s mcpSender) SendPM(ctx context.Context, peer, text string) error {
	user, err := s.e.c.UserByNick(peer)
	if err != nil {
		return err
	}
	return s.e.c.PM(user.ID(), text)
}

func (e *mcpEngine) botAllowed(uid string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, b := range e.settings.AllowedBots {
		if strings.EqualFold(b, uid) {
			return true
		}
	}
	return false
}

// --- persistence ---

func (e *mcpEngine) settingsPath() string { return filepath.Join(e.dataDir, "mcpclient.json") }
func (e *mcpEngine) spendPath() string    { return filepath.Join(e.dataDir, "mcpspend.json") }

func (e *mcpEngine) loadState() error {
	e.settings = mcpClientSettings{}.withDefaults()
	if raw, err := os.ReadFile(e.settingsPath()); err == nil {
		var s mcpClientSettings
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("parse %s: %w", e.settingsPath(), err)
		}
		e.settings = s.withDefaults()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if raw, err := os.ReadFile(e.spendPath()); err == nil {
		if err := json.Unmarshal(raw, &e.spend); err != nil {
			return fmt.Errorf("parse %s: %w", e.spendPath(), err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (e *mcpEngine) persistSettingsLocked() error {
	raw, err := json.MarshalIndent(e.settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(e.settingsPath(), raw, 0o600)
}

func (e *mcpEngine) persistSpendLocked() error {
	// Bound the log; the dashboard only needs recent history and the
	// daily total derives from entries within the last day anyway.
	if len(e.spend) > 1000 {
		e.spend = e.spend[len(e.spend)-1000:]
	}
	raw, err := json.MarshalIndent(e.spend, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(e.spendPath(), raw, 0o600)
}

func (e *mcpEngine) recordSpendLocked(bot, tool, rail string, atoms int64) {
	e.spend = append(e.spend, mcpSpendEntry{
		TS: time.Now().Unix(), Bot: bot, Tool: tool, Rail: rail, Atoms: atoms,
	})
	if err := e.persistSpendLocked(); err != nil {
		e.log.Errorf("persist MCP spend log: %v", err)
	}
}

// spentSinceLocked sums spends after the cutoff (the rolling per-day cap).
func (e *mcpEngine) spentSinceLocked(cutoff time.Time) int64 {
	var total int64
	cut := cutoff.Unix()
	for _, s := range e.spend {
		if s.TS >= cut {
			total += s.Atoms
		}
	}
	return total
}

// --- settings surface (status REST) ---

func (e *mcpEngine) currentSettings() mcpClientSettings {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.settings
}

// applySettings persists the new settings and reconciles the listener state
// live: enabling starts it, disabling stops it, a bind/token change while
// running restarts it.
func (e *mcpEngine) applySettings(s mcpClientSettings) error {
	s = s.withDefaults()
	if s.Mode != "approval" && s.Mode != "autopay" {
		return fmt.Errorf("mode must be approval or autopay")
	}
	for _, b := range s.AllowedBots {
		if !mcpUIDRe.MatchString(strings.ToLower(b)) {
			return fmt.Errorf("allowed bot %q is not a 64-hex uid", b)
		}
	}
	if s.Enabled && s.Token == "" {
		var tok [16]byte
		if _, err := rand.Read(tok[:]); err != nil {
			return err
		}
		s.Token = hex.EncodeToString(tok[:])
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	prev := e.settings
	e.settings = s
	if err := e.persistSettingsLocked(); err != nil {
		e.settings = prev
		return err
	}
	switch {
	case s.Enabled && e.httpSrv == nil:
		return e.startListenerLocked()
	case !s.Enabled && e.httpSrv != nil:
		e.stopListenerLocked()
	case s.Enabled && (s.Bind != prev.Bind || s.Token != prev.Token):
		e.stopListenerLocked()
		return e.startListenerLocked()
	}
	return nil
}

// --- listener ---

func (e *mcpEngine) startListenerLocked() error {
	ln, err := net.Listen("tcp", e.settings.Bind)
	if err != nil {
		return err
	}
	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		uid := strings.ToLower(strings.TrimPrefix(r.URL.Path, "/mcp/"))
		srv, err := e.proxyServerFor(uid)
		if err != nil {
			e.log.Warnf("MCP proxy for %s: %v", uid, err)
			// An empty server keeps the transport handshake valid while
			// exposing nothing.
			return mcp.NewServer(&mcp.Implementation{Name: "brclientd", Version: "0"}, nil)
		}
		return srv
	}, nil)
	srv := &http.Server{
		Handler: e.authMiddleware(handler),
		// No read/write deadlines: calls legitimately block for relay
		// round trips and approval decisions. Only the header read is
		// bounded.
		ReadHeaderTimeout: 15 * time.Second,
	}
	e.httpSrv = srv
	e.log.Infof("MCP client listener on http://%s (streamable HTTP, bearer auth)", ln.Addr())
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			e.log.Errorf("MCP listener: %v", err)
		}
	}()
	return nil
}

func (e *mcpEngine) stopListenerLocked() {
	srv := e.httpSrv
	e.httpSrv = nil
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	e.log.Infof("MCP client listener stopped")
}

func (e *mcpEngine) stopListener() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stopListenerLocked()
}

func (e *mcpEngine) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.mu.Lock()
		token := e.settings.Token
		e.mu.Unlock()
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		uid := strings.ToLower(strings.TrimPrefix(r.URL.Path, "/mcp/"))
		if !mcpUIDRe.MatchString(uid) || !e.botAllowed(uid) {
			http.Error(w, "unknown bot", http.StatusNotFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- bot sessions and the tool proxy ---

// proxyServerFor returns (building if needed) the local MCP server whose
// tools mirror the remote bot's catalog 1:1, including the price metadata.
func (e *mcpEngine) proxyServerFor(uid string) (*mcp.Server, error) {
	e.mu.Lock()
	link := e.bots[uid]
	if link == nil {
		link = &mcpBotLink{uid: uid}
		e.bots[uid] = link
	}
	e.mu.Unlock()

	link.mu.Lock()
	defer link.mu.Unlock()
	if link.proxy != nil {
		return link.proxy, nil
	}
	session, err := e.dialLocked(link)
	if err != nil {
		return nil, err
	}
	lctx, cancel := context.WithTimeout(e.ctx, 2*time.Minute)
	defer cancel()
	tl, err := session.ListTools(lctx, nil)
	if err != nil {
		e.resetLocked(link)
		return nil, fmt.Errorf("list tools: %w", err)
	}
	srv := mcp.NewServer(&mcp.Implementation{Name: "brmcp-" + uid[:8], Version: "1"}, nil)
	for _, t := range tl.Tools {
		tool := *t
		srv.AddTool(&tool, e.passthrough(link, tool.Name))
	}
	link.proxy = srv
	e.log.Infof("MCP proxy for bot %s serving %d tools", uid[:8], len(tl.Tools))
	return srv, nil
}

func (e *mcpEngine) dialLocked(link *mcpBotLink) (*mcp.ClientSession, error) {
	if link.session != nil {
		return link.session, nil
	}
	conn, err := e.router.Dial(link.uid)
	if err != nil {
		return nil, err
	}
	cl := mcp.NewClient(&mcp.Implementation{Name: "brclientd", Version: "1"}, nil)
	dctx, cancel := context.WithTimeout(e.ctx, 2*time.Minute)
	defer cancel()
	session, err := cl.Connect(dctx, conn.AsTransport(), nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect to bot: %w", err)
	}
	link.conn = conn
	link.session = session
	return session, nil
}

func (e *mcpEngine) resetLocked(link *mcpBotLink) {
	if link.session != nil {
		_ = link.session.Close()
	}
	if link.conn != nil {
		link.conn.Close()
	}
	link.session = nil
	link.conn = nil
	link.proxy = nil
}

// passthrough relays one tool call to the bot, transparently settling a
// payment_required refusal when settings permit, then retrying.
func (e *mcpEngine) passthrough(link *mcpBotLink, tool string) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args json.RawMessage
		if req.Params != nil {
			args = req.Params.Arguments
		}
		paid := false
		for attempt := 0; ; attempt++ {
			link.mu.Lock()
			session, err := e.dialLocked(link)
			link.mu.Unlock()
			if err != nil {
				return nil, err
			}
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
			if err != nil {
				// One transport-level retry on a fresh session.
				link.mu.Lock()
				e.resetLocked(link)
				link.mu.Unlock()
				if attempt == 0 {
					continue
				}
				return nil, err
			}
			pr := parsePaymentRequired(res)
			if pr == nil {
				return res, nil
			}
			if paid {
				// Already settled once: the credit may still be in
				// flight (tips settle asynchronously). Poll briefly.
				if attempt < 6 {
					select {
					case <-time.After(3 * time.Second):
						continue
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return res, nil
			}
			if err := e.settle(ctx, link.uid, tool, pr); err != nil {
				e.log.Warnf("MCP payment for %s/%s refused: %v", link.uid[:8], tool, err)
				res.Content = append(res.Content, &mcp.TextContent{
					Text: "brclientd: payment not made: " + err.Error(),
				})
				return res, nil
			}
			paid = true
		}
	}
}

// parsePaymentRequired sniffs a tool error result for the brmcp
// payment_required JSON body.
func parsePaymentRequired(res *mcp.CallToolResult) *brmcp.PaymentRequired {
	if res == nil || !res.IsError {
		return nil
	}
	for _, c := range res.Content {
		tc, ok := c.(*mcp.TextContent)
		if !ok {
			continue
		}
		var pr brmcp.PaymentRequired
		if err := json.Unmarshal([]byte(tc.Text), &pr); err == nil && pr.Error == "payment_required" {
			return &pr
		}
	}
	return nil
}

// settle pays one payment_required under the configured caps and mode.
// Invoice settles synchronously and is preferred; the tip rail is the
// fallback and credits asynchronously on the bot side.
func (e *mcpEngine) settle(ctx context.Context, bot, tool string, pr *brmcp.PaymentRequired) error {
	atoms := pr.ShortfallAtoms
	if atoms <= 0 {
		atoms = pr.PriceAtoms
	}
	if atoms <= 0 {
		return fmt.Errorf("bot requested a nonpositive amount")
	}

	e.mu.Lock()
	s := e.settings
	spentToday := e.spentSinceLocked(time.Now().Add(-24 * time.Hour))
	e.mu.Unlock()

	// Caps bound BOTH modes; zero means zero, approval cannot override.
	if s.PerCallCapAtoms <= 0 || atoms > s.PerCallCapAtoms {
		return fmt.Errorf("%d atoms exceeds the per-call cap (%d)", atoms, s.PerCallCapAtoms)
	}
	if s.PerDayCapAtoms <= 0 || spentToday+atoms > s.PerDayCapAtoms {
		return fmt.Errorf("%d atoms would exceed the daily cap (%d spent of %d)",
			atoms, spentToday, s.PerDayCapAtoms)
	}
	if s.Mode == "approval" {
		if err := e.awaitApproval(ctx, bot, tool, atoms, pr.Invoice,
			time.Duration(s.ApprovalTimeoutSecs)*time.Second); err != nil {
			return err
		}
	}

	if pr.Invoice != "" && e.pay != nil {
		dec, err := e.pay.DecodeInvoice(ctx, pr.Invoice)
		if err != nil {
			return fmt.Errorf("decode invoice: %w", err)
		}
		// The invoice must ask exactly what the bot quoted; anything else
		// is a mismatch we refuse rather than trust.
		if dec.MAtoms != atoms*1000 {
			return fmt.Errorf("invoice amount %d matoms != quoted %d atoms", dec.MAtoms, atoms)
		}
		if dec.IsExpired(0) {
			return fmt.Errorf("invoice already expired")
		}
		if _, err := e.pay.PayInvoice(ctx, pr.Invoice); err != nil {
			return fmt.Errorf("pay invoice: %w", err)
		}
		e.mu.Lock()
		e.recordSpendLocked(bot, tool, "invoice", atoms)
		e.mu.Unlock()
		e.log.Infof("MCP paid %d atoms by invoice for %s/%s", atoms, bot[:8], tool)
		return nil
	}

	user, err := e.c.UserByNick(bot)
	if err != nil {
		return err
	}
	if err := e.c.TipUser(user.ID(), dcrutil.Amount(atoms).ToCoin(), 3); err != nil {
		return fmt.Errorf("tip: %w", err)
	}
	e.mu.Lock()
	e.recordSpendLocked(bot, tool, "tip", atoms)
	e.mu.Unlock()
	e.log.Infof("MCP paid %d atoms by tip for %s/%s", atoms, bot[:8], tool)
	return nil
}

// awaitApproval parks the payment in the pending queue until the user
// decides through the dashboard, the timeout passes, or the call context
// ends.
func (e *mcpEngine) awaitApproval(ctx context.Context, bot, tool string, atoms int64,
	invoice string, timeout time.Duration) error {

	var idb [8]byte
	if _, err := rand.Read(idb[:]); err != nil {
		return err
	}
	p := &mcpPending{
		ID:      hex.EncodeToString(idb[:]),
		Bot:     bot,
		Tool:    tool,
		Atoms:   atoms,
		Invoice: invoice,
		Created: time.Now().Unix(),

		decision: make(chan bool, 1),
	}
	e.mu.Lock()
	e.pending[p.ID] = p
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.pending, p.ID)
		e.mu.Unlock()
	}()

	e.log.Infof("MCP payment awaiting approval: %s atoms=%d bot=%s tool=%s",
		p.ID, atoms, bot[:8], tool)
	select {
	case ok := <-p.decision:
		if !ok {
			return fmt.Errorf("payment denied by the user")
		}
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("approval timed out after %s", timeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *mcpEngine) pendingList() []*mcpPending {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]*mcpPending, 0, len(e.pending))
	for _, p := range e.pending {
		out = append(out, p)
	}
	return out
}

func (e *mcpEngine) resolvePending(id string, approve bool) bool {
	e.mu.Lock()
	p := e.pending[id]
	e.mu.Unlock()
	if p == nil {
		return false
	}
	p.decide(approve)
	return true
}

func (e *mcpEngine) spendSummary() (entries []mcpSpendEntry, todayAtoms int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entries = append(entries, e.spend...)
	return entries, e.spentSinceLocked(time.Now().Add(-24 * time.Hour))
}
