// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package runtime hosts brclientd's long-running surfaces once identity has
// been provisioned: the BR client core (client.Run), the JSON-RPC clientrpc
// listener (BR's stock surface, currently exposing VersionService) and the
// /status HTTP endpoint that surfaces CheckLNWalletUsable + connection state.
package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/rpcserver"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/slog"
	"golang.org/x/sync/errgroup"

	"github.com/karamble/brclientd/internal/certgen"
	"github.com/karamble/brclientd/internal/identity"
	"github.com/karamble/brclientd/internal/setup"
)

// Config bundles every parameter runtime.Run needs.
type Config struct {
	Log               slog.Logger
	LogFn             func(subsys string) slog.Logger
	Certs             certgen.Triplet
	ClientRPCListen   []string
	StatusListen      string
	AppName           string
	AppVersion        string
	BRServer          string
	DB                *clientdb.DB
	DcrlndTLSCert     string
	DcrlndMacaroon    string
	DcrlndRPCHost     string
	ReplayMsgLogsRoot string
	UploadDir         string
	MsgsRoot          string
	EmbedsRoot        string
	SeederCachePath   string
	PagesDir          string
	DataDir           string
	AppDataDir        string

	// SimpleStore hosting. When StoreEnabled, the node serves a simplestore
	// from StoreDir over the relay instead of static pages (one resource
	// provider binds at the root). StorePayType is "ln" or "onchain".
	StoreEnabled    bool
	StoreDir        string
	StorePayType    string
	StoreAccount    string
	StoreShipCharge float64
}

// ErrRestartRequested is returned by Run when a settings change needs the
// daemon to restart: client.Config values are fixed at client construction,
// so the process exits cleanly and the container supervisor relaunches it
// with the persisted settings.
var ErrRestartRequested = errors.New("restart requested for settings change")

// Run brings up the /status HTTP server and clientrpc.VersionService
// immediately, polls dcrlnd until its wallet is unlocked, then starts BR
// client.Run, conditionally serves the pre-setup endpoint while
// LocalIDIniter is blocked waiting for a new identity, and finally hands
// off to the long-running goroutines. Blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	// Own cancel so the restore path below can unwind the already-running
	// goroutines before returning.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tracker := NewTracker(cfg.Log)
	notifs := newNotifBus()
	audioRouter := NewRTDTAudioRouter(cfg.LogFn("RTAU"))
	reinvites := newGCReinviteTracker()
	unrepl := newUnreplTracker()
	downloadCaps := newDownloadCapTracker()
	notes := newNotificationStore(cfg.DataDir)
	brSettings := newBRSettingsStore(cfg.DataDir)
	effectiveSRR := brSettings.sendReceiveReceipts()
	restartCh := make(chan struct{})

	g, gctx := errgroup.WithContext(ctx)

	// /status comes up before anything else so the dashboard can render
	// the gate stages while we wait for dcrlnd to unlock + the channel to
	// hub to activate. clientrpc itself is deferred until after the
	// identity check below, because the pre-setup endpoint claims port
	// 7676 in the no-identity case and would conflict with an early
	// clientrpc bind.
	statusSrv := &StatusServer{
		Log:          cfg.LogFn("STAT"),
		Certs:        cfg.Certs,
		Listen:       cfg.StatusListen,
		Tracker:      tracker,
		DB:           cfg.DB,
		UploadDir:    cfg.UploadDir,
		MsgsRoot:     cfg.MsgsRoot,
		EmbedsRoot:   cfg.EmbedsRoot,
		Notifs:       notifs,
		AudioRouter:  audioRouter,
		Reinvites:    reinvites,
		Unrepl:       unrepl,
		DownloadCaps: downloadCaps,
		Notes:        notes,
		Settings:     brSettings,
		SRREffective: effectiveSRR,
		RestartCh:    restartCh,
		PagesDir:     cfg.PagesDir,
		DataDir:      cfg.DataDir,
		AppName:      cfg.AppName,
		AppVersion:   cfg.AppVersion,
	}
	g.Go(func() error { return statusSrv.Run(gctx) })

	// Connecting to dcrlnd must come AFTER the status server kicks off:
	// on a fresh stack the dcrlnd cert doesn't exist until the user runs
	// the LN setup wizard, and waitForDcrlndConnect blocks until it
	// appears. If this ran before the status server, port 7677 would
	// never open and the dashboard / docker healthcheck would have
	// nothing to talk to.
	dcrlndPay, err := waitForDcrlndConnect(gctx, cfg.DcrlndTLSCert, cfg.DcrlndMacaroon, cfg.DcrlndRPCHost, cfg.LogFn("LNPC"))
	if err != nil {
		return err
	}

	if err := waitForDcrlndUnlocked(gctx, dcrlndPay, tracker, cfg.LogFn("LNGT")); err != nil {
		return err
	}
	if err := waitForChannelToHub(gctx, dcrlndPay, tracker, cfg.LogFn("CHGT")); err != nil {
		return err
	}

	identityChan := make(chan *zkidentity.FullIdentity, 1)

	// The store controller flips this provider between filesystem pages and a
	// simplestore at runtime; the BR client only ever sees the one wrapper.
	storeProv := &switchableProvider{}

	c, err := startBRClient(BRClientCfg{
		DB:                  cfg.DB,
		DcrlndPay:           dcrlndPay,
		BRServer:            cfg.BRServer,
		SeederCachePath:     cfg.SeederCachePath,
		Tracker:             tracker,
		Notifs:              notifs,
		AudioRouter:         audioRouter,
		Reinvites:           reinvites,
		Unrepl:              unrepl,
		DownloadCaps:        downloadCaps,
		Notes:               notes,
		SendReceiveReceipts: effectiveSRR,
		LogFn:               cfg.LogFn,
		IdentityChan:        identityChan,
		ResProvider:         storeProv,
	})
	if err != nil {
		return err
	}
	statusSrv.SetClient(c)

	// One-shot: a restore-triggered boot leaves a marker; initiate KX
	// resets with all restored contacts once the server session is up.
	kxMarker := setup.KXResetMarkerPath(cfg.AppDataDir)
	g.Go(func() error {
		return runPostRestoreKXReset(gctx, c, tracker, kxMarker, cfg.LogFn("KXRS"))
	})

	// Bind the resource provider for the persisted/default hosting mode (pages
	// or simplestore) now that the client exists, and expose the controller so
	// the dashboard can flip modes at runtime.
	defaultMode := hostModeOff
	if cfg.StoreEnabled {
		defaultMode = hostModeStore
	}
	storeCtrl := newStoreController(gctx, storeProv, c, dcrlndPay, notifs, notes, cfg.LogFn,
		cfg.PagesDir, cfg.StoreDir, storeMode{
			Mode:       defaultMode,
			PayType:    cfg.StorePayType,
			Account:    cfg.StoreAccount,
			ShipCharge: cfg.StoreShipCharge,
		})
	statusSrv.SetStoreController(storeCtrl)
	if err := storeCtrl.applyInitial(); err != nil {
		return err
	}

	g.Go(func() error {
		err := c.Run(gctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		cfg.Log.Errorf("BR client.Run exited: %v", err)
		return err
	})

	// Drive BR's exchange-rate fetcher (DCR/USD + BTC/USD from api.decred.org
	// / dcrdata). It self-throttles to a 10 minute refresh and exits with the
	// context; the /rates endpoint reads the cached value via c.Rates().Get.
	g.Go(func() error {
		c.Rates().Run(gctx)
		return nil
	})

	select {
	case <-cfg.DB.RunStarted():
	case <-gctx.Done():
		return g.Wait()
	}

	id, hasID, err := identity.Existing(gctx, cfg.DB)
	if err != nil {
		return fmt.Errorf("check local identity: %w", err)
	}

	if !hasID {
		cfg.Log.Infof("No local identity found; running pre-setup endpoint on %s", primaryListen(cfg.ClientRPCListen))
		tracker.SetNeedsIdentity()
		preSetup := &setup.Server{
			Log:          cfg.LogFn("SETP"),
			Certs:        cfg.Certs,
			Listen:       primaryListen(cfg.ClientRPCListen),
			IdentityChan: identityChan,
			AppDataDir:   cfg.AppDataDir,
		}
		if err := preSetup.Run(gctx); err != nil {
			if errors.Is(err, setup.ErrRestorePending) {
				// A restore tarball was staged. Unwind the running
				// goroutines so the clientdb closes cleanly, then
				// surface the sentinel so main exits and the
				// supervisor relaunch extracts the tarball.
				cancel()
				g.Wait()
				return setup.ErrRestorePending
			}
			return fmt.Errorf("pre-setup endpoint: %w", err)
		}
		cfg.Log.Infof("Pre-setup endpoint stopped; identity submitted")
		id, _, _ = identity.Existing(gctx, cfg.DB)
	}
	if id != nil {
		tracker.SetNick(id.Public.Nick)
		cfg.Log.Infof("Local identity ready: nick=%q", id.Public.Nick)
	}

	// clientrpc starts AFTER identity provisioning so it can share the
	// listen address with the pre-setup endpoint (handed off cleanly when
	// preSetup.Run returned) and so the chat / GC / posts / payments
	// services have a live *client.Client to bind into.
	g.Go(func() error { return runClientRPC(gctx, cfg, c, dcrlndPay) })

	return g.Wait()
}

func runClientRPC(ctx context.Context, cfg Config, c *client.Client, dcrlndPay *client.DcrlnPaymentClient) error {
	listeners, err := buildListeners(cfg.Certs, cfg.ClientRPCListen)
	if err != nil {
		return err
	}
	defer closeListeners(listeners)

	if err := os.MkdirAll(cfg.ReplayMsgLogsRoot, 0o700); err != nil {
		return fmt.Errorf("create replaymsglog dir: %w", err)
	}

	srv := rpcserver.New(rpcserver.Config{
		JSONRPCListeners: listeners,
		Log:              cfg.LogFn("RPCS"),
	})
	srv.InitVersionService(cfg.AppName, cfg.AppVersion)

	if err := srv.InitChatService(rpcserver.ChatServerCfg{
		Client:            c,
		Log:               cfg.LogFn("RPCS"),
		RootReplayMsgLogs: cfg.ReplayMsgLogsRoot,
		PayClient:         dcrlndPay,
	}); err != nil {
		return fmt.Errorf("init chat service: %w", err)
	}
	if err := srv.InitGCService(rpcserver.GCServerCfg{
		Client:            c,
		Log:               cfg.LogFn("RPCS"),
		RootReplayMsgLogs: cfg.ReplayMsgLogsRoot,
	}); err != nil {
		return fmt.Errorf("init gc service: %w", err)
	}
	if err := srv.InitPostsService(rpcserver.PostsServerCfg{
		Client:            c,
		Log:               cfg.LogFn("RPCS"),
		RootReplayMsgLogs: cfg.ReplayMsgLogsRoot,
	}); err != nil {
		return fmt.Errorf("init posts service: %w", err)
	}
	if err := srv.InitPaymentsService(rpcserver.PaymentsServerCfg{
		Client:            c,
		Log:               cfg.LogFn("RPCS"),
		RootReplayMsgLogs: cfg.ReplayMsgLogsRoot,
	}); err != nil {
		return fmt.Errorf("init payments service: %w", err)
	}
	if err := srv.InitResourcesService(rpcserver.ResourcesServerCfg{
		Client: c,
		Log:    cfg.LogFn("RPCS"),
	}); err != nil {
		return fmt.Errorf("init resources service: %w", err)
	}
	if err := srv.InitContentService(rpcserver.ContentServerCfg{
		Client:            c,
		Log:               cfg.LogFn("RPCS"),
		RootReplayMsgLogs: cfg.ReplayMsgLogsRoot,
	}); err != nil {
		return fmt.Errorf("init content service: %w", err)
	}

	for _, addr := range cfg.ClientRPCListen {
		cfg.Log.Infof("clientrpc listening on %s (mTLS, all services)", addr)
	}
	return srv.Run(ctx)
}

// ensurePagesDir creates the page-hosting root if missing and, when the
// directory is empty, seeds a starter index.md so a freshly provisioned node
// serves something at its root path instead of a 404.
func ensurePagesDir(dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return nil
	}
	const starter = "# Welcome\n\nThis is your Bison Relay page. Edit it from the dashboard Pages tab.\n"
	return os.WriteFile(filepath.Join(dir, "index.md"), []byte(starter), 0o600)
}

func buildListeners(certs certgen.Triplet, addresses []string) ([]net.Listener, error) {
	tlsCfg, err := certs.LoadServerTLSConfig()
	if err != nil {
		return nil, err
	}
	listeners := make([]net.Listener, 0, len(addresses))
	for _, addr := range addresses {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			closeListeners(listeners)
			return nil, fmt.Errorf("listen on %s: %w", addr, err)
		}
		listeners = append(listeners, tls.NewListener(l, tlsCfg))
	}
	return listeners, nil
}

func closeListeners(listeners []net.Listener) {
	for _, l := range listeners {
		l.Close()
	}
}

func primaryListen(listens []string) string {
	if len(listens) == 0 {
		return "0.0.0.0:7676"
	}
	return listens[0]
}
