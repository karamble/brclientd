// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/decred/dcrlnd/lnrpc"
	"github.com/decred/slog"
)

// waitForDcrlndConnect blocks until both the dcrlnd TLS cert and macaroon
// files exist, then dials dcrlnd with retry. Runs AFTER the status server
// is already serving so the dashboard / docker healthcheck have a target
// to talk to during the wait (which can be hours on a fresh stack while
// the user walks through the LN setup wizard).
func waitForDcrlndConnect(ctx context.Context, tlsCertPath, macaroonPath, rpcHost string, log slog.Logger) (*client.DcrlnPaymentClient, error) {
	if tlsCertPath == "" || macaroonPath == "" {
		return nil, fmt.Errorf("dcrlnd cert/macaroon paths must be set")
	}
	if err := waitForPath(ctx, tlsCertPath, "dcrlnd TLS cert", log); err != nil {
		return nil, err
	}
	if err := waitForPath(ctx, macaroonPath, "dcrlnd macaroon", log); err != nil {
		return nil, err
	}

	log.Infof("Connecting to dcrlnd at %s", rpcHost)
	backoff := 2 * time.Second
	const maxBackoff = 30 * time.Second
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
		pc, err := client.NewDcrlndPaymentClient(dialCtx, client.DcrlnPaymentClientCfg{
			TLSCertPath:  tlsCertPath,
			MacaroonPath: macaroonPath,
			Address:      rpcHost,
			Log:          log,
		})
		if err == nil {
			info, gerr := pc.LNRPC().GetInfo(dialCtx, &lnrpc.GetInfoRequest{})
			dialCancel()
			switch {
			case gerr == nil:
				log.Infof("dcrlnd reachable: identity=%s version=%s synced_to_chain=%v",
					info.IdentityPubkey, info.Version, info.SyncedToChain)
			case strings.Contains(gerr.Error(), "wallet locked"):
				log.Infof("dcrlnd reachable but wallet is locked; will become usable once unlocked")
			default:
				log.Warnf("dcrlnd reachable but GetInfo failed: %v", gerr)
			}
			return pc, nil
		}
		dialCancel()
		log.Warnf("dcrlnd not ready (%v); retrying in %s", err, backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func waitForPath(ctx context.Context, path, label string, log slog.Logger) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	log.Infof("Waiting for %s at %s", label, path)
	const tick = 2 * time.Second
	const logEvery = 30 * time.Second
	sinceLog := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tick):
		}
		if _, err := os.Stat(path); err == nil {
			log.Infof("Found %s", label)
			return nil
		}
		sinceLog += tick
		if sinceLog >= logEvery {
			log.Infof("Still waiting for %s at %s", label, path)
			sinceLog = 0
		}
	}
}

// waitForDcrlndUnlocked polls dcrlnd's GetInfo with exponential backoff until
// the wallet is unlocked and chain access is available. brclientd must not
// start BR client.Run before this gate clears: the BR connKeeper retries
// CheckLNWalletUsable about every two seconds, and each retry triggers a
// fresh seeder query that hammers bisonrelay.org.
func waitForDcrlndUnlocked(ctx context.Context, pc *client.DcrlnPaymentClient, tracker *Tracker, log slog.Logger) error {
	const (
		probeTimeout = 10 * time.Second
		minBackoff   = 5 * time.Second
		maxBackoff   = 30 * time.Second
	)

	backoff := minBackoff
	for {
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		_, err := pc.LNRPC().GetInfo(probeCtx, &lnrpc.GetInfoRequest{})
		cancel()
		if err == nil {
			log.Infof("dcrlnd is unlocked; advancing past dcrlnd gate")
			tracker.MarkDcrlndReady()
			return nil
		}
		msg := err.Error()
		tracker.SetDcrlndWaiting(msg)
		log.Debugf("dcrlnd not ready (%s); next probe in %s", msg, backoff)

		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
