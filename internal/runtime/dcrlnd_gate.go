// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/decred/dcrlnd/lnrpc"
	"github.com/decred/slog"
)

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
