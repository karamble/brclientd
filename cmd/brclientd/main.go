// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/decred/dcrlnd/lnrpc"
	flags "github.com/jessevdk/go-flags"

	"github.com/karamble/brclientd/internal/certgen"
	"github.com/karamble/brclientd/internal/config"
	"github.com/karamble/brclientd/internal/identity"
	brlog "github.com/karamble/brclientd/internal/log"
	"github.com/karamble/brclientd/internal/runtime"
	"github.com/karamble/brclientd/internal/setup"
)

const Version = "0.0.1"

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flags.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "%s: %v\n", config.AppName, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.Load(args, Version)
	if err != nil {
		return err
	}

	if err := brlog.InitRotator(cfg.LogFile()); err != nil {
		return err
	}
	defer brlog.CloseRotator()
	if err := brlog.SetLevels(cfg.DebugLevel); err != nil {
		return err
	}

	brlog.BRCD.Infof("Version %s starting (%s)", Version, cfg.Network())
	brlog.BRCD.Infof("App data: %s", cfg.AppDataDir)
	brlog.BRCD.Infof("Data:     %s", cfg.DataDir)
	brlog.BRCD.Infof("Log:      %s", cfg.LogFile())

	if cfg.Dcrlnd.TLSCertPath == "" || cfg.Dcrlnd.MacaroonPath == "" {
		return errors.New("dcrlnd.tlscertpath and dcrlnd.macaroonpath are required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	certs, err := ensureCerts(cfg)
	if err != nil {
		return err
	}

	idPaths := identity.PathsIn(cfg.DataDir)
	db, err := identity.OpenDB(idPaths, brlog.BRDB)
	if err != nil {
		return fmt.Errorf("open clientdb: %w", err)
	}

	dbDone := make(chan struct{})
	go func() {
		defer close(dbDone)
		if err := db.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			brlog.BRDB.Errorf("clientdb.Run: %v", err)
		}
	}()

	id, haveIdentity, err := identity.Existing(ctx, db)
	if err != nil {
		return fmt.Errorf("check local identity: %w", err)
	}

	if !haveIdentity {
		if err := provisionIdentity(ctx, cfg, db, certs); err != nil {
			return err
		}
		id, _, err = identity.Existing(ctx, db)
		if err != nil {
			return fmt.Errorf("reload local identity: %w", err)
		}
	}
	if id != nil {
		brlog.BRCD.Infof("Local identity ready: nick=%q", id.Public.Nick)
	}

	if err := connectDcrlnd(ctx, cfg); err != nil {
		return err
	}

	runtimeErr := make(chan error, 1)
	go func() {
		runtimeErr <- runtime.Run(ctx, runtime.Config{
			Log:        brlog.RUNT,
			Certs:      certs,
			Listen:     cfg.ClientRPC.Listen,
			AppName:    config.AppName,
			AppVersion: Version,
		})
	}()

	brlog.BRCD.Infof("ready (phase 3; clientrpc VersionService active)")
	select {
	case <-ctx.Done():
	case err := <-runtimeErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			brlog.BRCD.Errorf("clientrpc exited: %v", err)
		}
	}
	brlog.BRCD.Infof("shutting down")
	<-dbDone
	return nil
}

func ensureCerts(cfg *config.Config) (certgen.Triplet, error) {
	certDir := filepath.Join(cfg.DataDir, "rpc")
	certs := certgen.PathsIn(certDir)
	present, err := certs.AllPresent()
	if err != nil {
		return certs, fmt.Errorf("check certs: %w", err)
	}
	if !present {
		brlog.BRCD.Infof("Generating mTLS cert triplet at %s", certDir)
		if err := certs.Generate(certHosts(cfg)); err != nil {
			return certs, fmt.Errorf("generate certs: %w", err)
		}
	}
	return certs, nil
}

func provisionIdentity(ctx context.Context, cfg *config.Config, db *clientdb.DB, certs certgen.Triplet) error {
	brlog.BRCD.Infof("No local identity found; entering pre-setup mode")
	srv := &setup.Server{
		Log:    brlog.SETP,
		DB:     db,
		Certs:  certs,
		Listen: primaryListen(cfg.ClientRPC.Listen),
	}
	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("pre-setup endpoint: %w", err)
	}
	brlog.BRCD.Infof("Identity provisioned; pre-setup endpoint stopped")
	return nil
}

func connectDcrlnd(ctx context.Context, cfg *config.Config) error {
	brlog.BRCD.Infof("Connecting to dcrlnd at %s", cfg.Dcrlnd.RPCHost)
	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dialCancel()

	pc, err := client.NewDcrlndPaymentClient(dialCtx, client.DcrlnPaymentClientCfg{
		TLSCertPath:  cfg.Dcrlnd.TLSCertPath,
		MacaroonPath: cfg.Dcrlnd.MacaroonPath,
		Address:      cfg.Dcrlnd.RPCHost,
		Log:          brlog.LNPC,
	})
	if err != nil {
		return fmt.Errorf("dcrlnd payment client: %w", err)
	}

	info, err := pc.LNRPC().GetInfo(dialCtx, &lnrpc.GetInfoRequest{})
	switch {
	case err == nil:
		brlog.BRCD.Infof("dcrlnd reachable: identity=%s version=%s synced_to_chain=%v",
			info.IdentityPubkey, info.Version, info.SyncedToChain)
	case strings.Contains(err.Error(), "wallet locked"):
		brlog.BRCD.Infof("dcrlnd reachable but wallet is locked; will become usable once unlocked")
	default:
		brlog.BRCD.Warnf("dcrlnd reachable but GetInfo failed: %v", err)
	}
	return nil
}

func certHosts(cfg *config.Config) []string {
	hosts := []string{"localhost", "127.0.0.1", "::1"}
	if h, err := os.Hostname(); err == nil && h != "" {
		hosts = append(hosts, h)
	}
	for _, l := range cfg.ClientRPC.Listen {
		host := l
		if idx := strings.LastIndex(host, ":"); idx > 0 {
			host = host[:idx]
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			continue
		}
		hosts = append(hosts, host)
	}
	return hosts
}

func primaryListen(listens []string) string {
	if len(listens) == 0 {
		return "0.0.0.0:" + config.DefaultClientRPCPort
	}
	return listens[0]
}
