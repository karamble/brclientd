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
	"strings"
	"syscall"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/decred/dcrlnd/lnrpc"
	flags "github.com/jessevdk/go-flags"

	"github.com/karamble/brclientd/internal/config"
	brlog "github.com/karamble/brclientd/internal/log"
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

	brlog.BRCD.Infof("ready (phase 1 skeleton; clientrpc not yet wired)")
	<-ctx.Done()
	brlog.BRCD.Infof("shutting down")
	return nil
}
