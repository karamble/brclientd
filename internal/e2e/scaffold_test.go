// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build brclientde2e

// Package e2e runs two full brclientd instances against a real in-process
// Bison Relay server (free pay scheme, direct dial) and drives them through
// the same mTLS status API the dashboard uses. Run with:
//
//	go test -tags brclientde2e ./internal/e2e/ -count=1
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/companyzero/bisonrelay/server"
	"github.com/companyzero/bisonrelay/server/settings"
	"github.com/decred/slog"

	"github.com/karamble/brclientd/internal/certgen"
	"github.com/karamble/brclientd/internal/identity"
	"github.com/karamble/brclientd/internal/runtime"
)

// logFn returns per-subsystem loggers; silent unless BR_E2E_LOG is set.
func logFn(name string) func(string) slog.Logger {
	if os.Getenv("BR_E2E_LOG") == "" {
		return func(string) slog.Logger { return slog.Disabled }
	}
	bknd := slog.NewBackend(os.Stdout)
	return func(subsys string) slog.Logger {
		l := bknd.Logger(name + "-" + subsys)
		l.SetLevel(slog.LevelDebug)
		return l
	}
}

// newTestRelay boots a real brserver on a loopback port with its default
// free pay scheme and returns its dial address.
func newTestRelay(t *testing.T, ctx context.Context) string {
	t.Helper()
	dir := t.TempDir()
	cfg := settings.New()
	cfg.Root = dir
	cfg.RoutedMessages = filepath.Join(dir, settings.ZKSRoutedMessages)
	cfg.LogFile = filepath.Join(dir, "brserver.log")
	cfg.Listen = []string{"127.0.0.1:0"}
	cfg.InitSessTimeout = time.Second
	cfg.DebugLevel = "warn"
	cfg.LogStdOut = io.Discard
	cfg.SeederDisable = true

	s, err := server.NewServer(cfg)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	// The relay's accept loop unwinds slowly on cancel; the leaked
	// goroutines die with the test process, so do not stall teardown.
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		addrs := s.BoundAddrs()
		if len(addrs) > 0 {
			return addrs[0].String()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("relay never bound")
	return ""
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// notifEvent mirrors the NDJSON envelope of GET /notifications.
type notifEvent struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

// instance is one running brclientd with an mTLS API client and a live
// notification stream. stop and restart drive the full daemon lifecycle
// against the same data dir.
type instance struct {
	t          *testing.T
	name       string
	dataDir    string
	relayAddr  string
	parentCtx  context.Context
	certs      certgen.Triplet
	statusAddr string
	api        *http.Client
	stream     *http.Client
	events     chan notifEvent
	runCancel  context.CancelFunc
	done       chan error
}

// newInstance provisions certs and settings in a temp dir, boots the full
// brclientd runtime on the free scheme against the given relay, and creates
// the identity through the production first-boot endpoint.
func newInstance(t *testing.T, ctx context.Context, relayAddr, name string) *instance {
	t.Helper()
	dataDir := t.TempDir()

	certs := certgen.PathsIn(filepath.Join(dataDir, "rpc"))
	if err := certs.Generate([]string{"localhost", "127.0.0.1", "::1"}); err != nil {
		t.Fatalf("%s certs: %v", name, err)
	}

	// Production waits a minute after the first subscription before the tip
	// loop serves TipUser; a fresh test instance needs it immediately.
	if err := os.WriteFile(filepath.Join(dataDir, "settings.json"),
		[]byte(`{"tip_restart_secs": 1}`), 0o600); err != nil {
		t.Fatalf("%s settings: %v", name, err)
	}

	caPEM, err := os.ReadFile(certs.CACertPath)
	if err != nil {
		t.Fatalf("%s ca cert: %v", name, err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	clientCert, err := tls.LoadX509KeyPair(certs.ClientCertPath, certs.ClientKeyPath)
	if err != nil {
		t.Fatalf("%s client cert: %v", name, err)
	}
	tlsCfg := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{clientCert}}
	inst := &instance{
		t:         t,
		name:      name,
		dataDir:   dataDir,
		relayAddr: relayAddr,
		parentCtx: ctx,
		certs:     certs,
		api:       &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsCfg}},
		stream:    &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}},
	}

	inst.boot(true)
	t.Cleanup(func() {
		if inst.runCancel == nil {
			return
		}
		inst.runCancel()
		select {
		case <-inst.done:
		case <-time.After(20 * time.Second):
			t.Logf("%s runtime did not stop in time", name)
		}
	})
	return inst
}

// boot starts the runtime on fresh loopback ports with a FRESH clientdb
// handle (a clientdb may only ever Run once per handle) and brings the API
// and notification stream up. createID drives the first-boot identity path.
func (i *instance) boot(createID bool) {
	t := i.t
	t.Helper()

	idPaths := identity.PathsIn(i.dataDir)
	db, err := identity.OpenDB(idPaths, slog.Disabled)
	if err != nil {
		t.Fatalf("%s clientdb: %v", i.name, err)
	}

	i.statusAddr = freePort(t)
	rpcAddr := freePort(t)
	mcpAddr := freePort(t)

	runCtx, cancel := context.WithCancel(i.parentCtx)
	i.runCancel = cancel
	i.done = make(chan error, 1)
	dataDir := i.dataDir
	name := i.name
	go func() {
		i.done <- runtime.Run(runCtx, runtime.Config{
			Log:               logFn(name)("RUNT"),
			LogFn:             logFn(name),
			Certs:             i.certs,
			ClientRPCListen:   []string{rpcAddr},
			StatusListen:      i.statusAddr,
			MCPListen:         mcpAddr,
			AppName:           "brclientd-e2e",
			AppVersion:        "test",
			BRServer:          i.relayAddr,
			BRServerDirect:    true,
			PayScheme:         "free",
			DB:                db,
			ReplayMsgLogsRoot: filepath.Join(dataDir, "replaymsglog"),
			UploadDir:         filepath.Join(dataDir, "uploads"),
			MsgsRoot:          idPaths.MsgsRoot,
			EmbedsRoot:        idPaths.EmbedsRoot,
			SeederCachePath:   filepath.Join(dataDir, "seeder-cache.json"),
			PagesDir:          filepath.Join(dataDir, "pages"),
			StoreDir:          filepath.Join(dataDir, "store"),
			DataDir:           dataDir,
			AppDataDir:        dataDir,
			StorePayType:      "ln",
		})
	}()

	if createID {
		i.createIdentity(rpcAddr, name)
	}
	i.events = make(chan notifEvent, 256)
	i.waitReady(30 * time.Second)
	i.watchNotifications(runCtx)
}

// stop shuts the runtime down and waits for it to fully return; only then
// is the clientdb lockfile released for a subsequent boot.
func (i *instance) stop() {
	i.t.Helper()
	if i.runCancel == nil {
		return
	}
	i.runCancel()
	i.runCancel = nil
	select {
	case <-i.done:
	case <-time.After(20 * time.Second):
		i.t.Fatalf("%s runtime did not stop", i.name)
	}
}

// restart boots the daemon again on the same data dir: the persisted
// identity, contacts, and history must all carry over.
func (i *instance) restart() {
	i.t.Helper()
	i.stop()
	i.boot(false)
}

// createIdentity drives the production first-boot path: the runtime serves
// the pre-setup endpoint on the clientrpc address until an identity is
// POSTed (identities can only be written while the clientdb runs, which
// happens inside the runtime).
func (i *instance) createIdentity(rpcAddr, name string) {
	i.t.Helper()
	body, _ := json.Marshal(map[string]string{"nick": name, "name": name})
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := i.api.Post("https://"+rpcAddr+"/create-identity", "application/json", bytes.NewReader(body))
		if err == nil {
			code := resp.StatusCode
			resp.Body.Close()
			if code >= 200 && code <= 299 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	i.t.Fatalf("%s: pre-setup endpoint never accepted the identity", i.name)
}

func (i *instance) url(path string) string {
	return "https://" + i.statusAddr + path
}

func (i *instance) get(path string) ([]byte, int) {
	i.t.Helper()
	resp, err := i.api.Get(i.url(path))
	if err != nil {
		i.t.Fatalf("%s GET %s: %v", i.name, path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return body, resp.StatusCode
}

func (i *instance) post(path string, body any) ([]byte, int) {
	i.t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		i.t.Fatal(err)
	}
	resp, err := i.api.Post(i.url(path), "application/json", bytes.NewReader(raw))
	if err != nil {
		i.t.Fatalf("%s POST %s: %v", i.name, path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return out, resp.StatusCode
}

func (i *instance) mustPost(path string, body any) []byte {
	i.t.Helper()
	out, code := i.post(path, body)
	if code < 200 || code > 299 {
		i.t.Fatalf("%s POST %s: HTTP %d: %s", i.name, path, code, out)
	}
	return out
}

// waitReady polls the status API until the runtime serves requests and the
// BR client is constructed (identity was pre-created, so this only waits on
// the free-mode startup path).
func (i *instance) waitReady(timeout time.Duration) {
	i.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := i.api.Get(i.url("/public-identity"))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	i.t.Fatalf("%s never became ready", i.name)
}

// watchNotifications streams the NDJSON notification feed into i.events.
func (i *instance) watchNotifications(ctx context.Context) {
	i.t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, i.url("/notifications"), nil)
	if err != nil {
		i.t.Fatal(err)
	}
	resp, err := i.stream.Do(req)
	if err != nil {
		i.t.Fatalf("%s notifications stream: %v", i.name, err)
	}
	go func() {
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var ev notifEvent
			if json.Unmarshal([]byte(line), &ev) != nil {
				continue
			}
			select {
			case i.events <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
}

// waitEvent blocks until an event of the given type matching match arrives.
func (i *instance) waitEvent(typ string, match func(map[string]any) bool, timeout time.Duration) map[string]any {
	i.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev := <-i.events:
			if ev.Type != typ {
				continue
			}
			if match == nil || match(ev.Payload) {
				return ev.Payload
			}
		case <-deadline:
			i.t.Fatalf("%s: no %q event within %v", i.name, typ, timeout)
			return nil
		}
	}
}

// assertNoEvent asserts no event of the given type matching match arrives
// within the window.
func (i *instance) assertNoEvent(typ string, match func(map[string]any) bool, window time.Duration) {
	i.t.Helper()
	deadline := time.After(window)
	for {
		select {
		case ev := <-i.events:
			if ev.Type == typ && (match == nil || match(ev.Payload)) {
				i.t.Fatalf("%s: unexpected %q event: %v", i.name, typ, ev.Payload)
			}
		case <-deadline:
			return
		}
	}
}

var hex64RE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ownUID returns this instance's identity as 64-hex. The status endpoint
// encodes it base64 (the known uid-encoding split between surfaces).
func (i *instance) ownUID() string {
	i.t.Helper()
	body, code := i.get("/public-identity")
	if code != http.StatusOK {
		i.t.Fatalf("%s GET /public-identity: HTTP %d", i.name, code)
	}
	var resp struct {
		Identity string `json:"identity"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		i.t.Fatalf("%s identity decode: %v", i.name, err)
	}
	if hex64RE.MatchString(strings.ToLower(resp.Identity)) {
		return strings.ToLower(resp.Identity)
	}
	raw, err := base64.StdEncoding.DecodeString(resp.Identity)
	if err != nil || len(raw) != 32 {
		i.t.Fatalf("%s: identity is neither 64-hex nor a 32-byte base64 value: %q", i.name, resp.Identity)
	}
	return hex.EncodeToString(raw)
}

// kxPair connects two instances: the inviter generates a prepaid invite
// (free on this relay) and the invitee accepts it; returns once both sides
// list each other as contacts. Invite creation needs a live server session,
// so it doubles as the connectivity wait.
func kxPair(t *testing.T, inviter, invitee *instance) {
	t.Helper()
	var inviteBytes string
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		out, code := inviter.post("/invites/create", map[string]any{})
		if code == http.StatusOK {
			var resp struct {
				InviteBytes string `json:"inviteBytes"`
			}
			if err := json.Unmarshal(out, &resp); err == nil && resp.InviteBytes != "" {
				inviteBytes = resp.InviteBytes
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if inviteBytes == "" {
		t.Fatalf("%s could not create an invite (relay session never came up?)", inviter.name)
	}
	invitee.mustPost("/invites/accept", map[string]any{"inviteBytes": inviteBytes})
	waitContact(t, inviter, invitee)
	waitContact(t, invitee, inviter)
}

// waitContact blocks until a lists b in its addressbook.
func waitContact(t *testing.T, a, b *instance) {
	t.Helper()
	dl := time.Now().Add(45 * time.Second)
	for time.Now().Before(dl) {
		body, code := a.get("/contacts")
		if code == http.StatusOK && strings.Contains(string(body), fmt.Sprintf("%q", b.name)) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("%s never KXd with %s", a.name, b.name)
}

// sendFile posts a multipart /files/send from one instance to a peer nick.
func (i *instance) sendFile(user, filename string, content []byte) {
	i.t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if err := mw.WriteField("user", user); err != nil {
		i.t.Fatal(err)
	}
	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		i.t.Fatal(err)
	}
	if _, err := fw.Write(content); err != nil {
		i.t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		i.t.Fatal(err)
	}
	resp, err := i.api.Post(i.url("/files/send"), mw.FormDataContentType(), &buf)
	if err != nil {
		i.t.Fatalf("%s POST /files/send: %v", i.name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		i.t.Fatalf("%s POST /files/send: HTTP %d: %s", i.name, resp.StatusCode, body)
	}
}

// pair boots a relay and two KXd instances named alice and bob.
func pair(t *testing.T) (*instance, *instance) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	relay := newTestRelay(t, ctx)
	alice := newInstance(t, ctx, relay, "alice")
	bob := newInstance(t, ctx, relay, "bob")
	kxPair(t, alice, bob)
	return alice, bob
}

// trio boots a relay and three instances where bob is KXd with both alice
// and charlie, but alice and charlie do NOT know each other - the setup for
// mediated key exchanges.
func trio(t *testing.T) (*instance, *instance, *instance) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	relay := newTestRelay(t, ctx)
	alice := newInstance(t, ctx, relay, "alice")
	bob := newInstance(t, ctx, relay, "bob")
	charlie := newInstance(t, ctx, relay, "charlie")
	kxPair(t, alice, bob)
	kxPair(t, bob, charlie)
	return alice, bob, charlie
}
