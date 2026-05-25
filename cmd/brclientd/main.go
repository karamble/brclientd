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

	flags "github.com/jessevdk/go-flags"

	"github.com/karamble/brclientd/internal/certgen"
	"github.com/karamble/brclientd/internal/config"
	"github.com/karamble/brclientd/internal/identity"
	brlog "github.com/karamble/brclientd/internal/log"
	"github.com/karamble/brclientd/internal/runtime"
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

	brlog.BRCD.Infof("Handing off to BR runtime (status server up first; dcrlnd connect waits inside runtime)")
	err = runtime.Run(ctx, runtime.Config{
		Log:               brlog.RUNT,
		LogFn:             brlog.LoggerFn,
		Certs:             certs,
		ClientRPCListen:   cfg.ClientRPC.Listen,
		StatusListen:      cfg.Status.Listen,
		AppName:           config.AppName,
		AppVersion:        Version,
		BRServer:          cfg.BRServer,
		DB:                db,
		DcrlndTLSCert:     cfg.Dcrlnd.TLSCertPath,
		DcrlndMacaroon:    cfg.Dcrlnd.MacaroonPath,
		DcrlndRPCHost:     cfg.Dcrlnd.RPCHost,
		ReplayMsgLogsRoot: filepath.Join(cfg.DataDir, "replaymsglog"),
		UploadDir:         filepath.Join(cfg.DataDir, "uploads"),
		MsgsRoot:          idPaths.MsgsRoot,
		EmbedsRoot:        idPaths.EmbedsRoot,
		SeederCachePath:   filepath.Join(cfg.DataDir, "seeder-cache.json"),
		PagesDir:          filepath.Join(cfg.DataDir, "pages"),
		StoreEnabled:      cfg.SimpleStore.Enabled,
		StoreDir:          filepath.Join(cfg.DataDir, "store"),
		StorePayType:      cfg.SimpleStore.PayType,
		StoreAccount:      cfg.SimpleStore.Account,
		StoreShipCharge:   cfg.SimpleStore.ShipCharge,
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	brlog.BRCD.Infof("shutting down")
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
