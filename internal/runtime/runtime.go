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
	Log             slog.Logger
	LogFn           func(subsys string) slog.Logger
	Certs           certgen.Triplet
	ClientRPCListen []string
	StatusListen    string
	AppName         string
	AppVersion      string
	BRServer        string
	DB              *clientdb.DB
	DcrlndPay       *client.DcrlnPaymentClient
}

// Run brings up the /status HTTP server and clientrpc.VersionService
// immediately, polls dcrlnd until its wallet is unlocked, then starts BR
// client.Run, conditionally serves the pre-setup endpoint while
// LocalIDIniter is blocked waiting for a new identity, and finally hands
// off to the long-running goroutines. Blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	tracker := NewTracker(cfg.Log)

	g, gctx := errgroup.WithContext(ctx)

	// /status + clientrpc.Version come up before anything else so the
	// dashboard can render the dcrlnd-gate stage while we wait for the
	// LN wallet to be unlocked.
	g.Go(func() error {
		srv := &StatusServer{
			Log:     cfg.LogFn("STAT"),
			Certs:   cfg.Certs,
			Listen:  cfg.StatusListen,
			Tracker: tracker,
		}
		return srv.Run(gctx)
	})
	g.Go(func() error { return runClientRPC(gctx, cfg) })

	if err := waitForDcrlndUnlocked(gctx, cfg.DcrlndPay, tracker, cfg.LogFn("LNGT")); err != nil {
		return err
	}
	if err := waitForChannelToHub(gctx, cfg.DcrlndPay, tracker, cfg.LogFn("CHGT")); err != nil {
		return err
	}

	identityChan := make(chan *zkidentity.FullIdentity, 1)

	c, err := startBRClient(BRClientCfg{
		DB:           cfg.DB,
		DcrlndPay:    cfg.DcrlndPay,
		BRServer:     cfg.BRServer,
		Tracker:      tracker,
		LogFn:        cfg.LogFn,
		IdentityChan: identityChan,
	})
	if err != nil {
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
		}
		if err := preSetup.Run(gctx); err != nil {
			return fmt.Errorf("pre-setup endpoint: %w", err)
		}
		cfg.Log.Infof("Pre-setup endpoint stopped; identity submitted")
		id, _, _ = identity.Existing(gctx, cfg.DB)
	}
	if id != nil {
		tracker.SetNick(id.Public.Nick)
		cfg.Log.Infof("Local identity ready: nick=%q", id.Public.Nick)
	}

	return g.Wait()
}

func runClientRPC(ctx context.Context, cfg Config) error {
	listeners, err := buildListeners(cfg.Certs, cfg.ClientRPCListen)
	if err != nil {
		return err
	}
	defer closeListeners(listeners)

	srv := rpcserver.New(rpcserver.Config{
		JSONRPCListeners: listeners,
		Log:              cfg.LogFn("RUNT"),
	})
	srv.InitVersionService(cfg.AppName, cfg.AppVersion)

	for _, addr := range cfg.ClientRPCListen {
		cfg.Log.Infof("clientrpc listening on %s (mTLS)", addr)
	}
	return srv.Run(ctx)
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
