// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/decred/slog"
)

// postRestoreKXResetInterval is the ResetAllOldRatchets cutoff used after a
// restore: ratchets whose last received message is OLDER than this are
// reset, so a tiny value covers effectively every contact in the restored
// snapshot (which is by construction older than now). One minute instead of
// zero because zero means the upstream 30-day default, and instead of one
// second to tolerate clock skew between backup source and restore host.
const postRestoreKXResetInterval = time.Minute

// runPostRestoreKXReset initiates a KX reset with every contact after a
// restore-triggered boot, then exits. The reset itself is fire-and-forget:
// requests are published into each contact's reset rendezvous mailbox and
// the BR client completes the ping-pong whenever the peer comes online; no
// reset state is tracked here. The marker is removed only after the
// initiation pass succeeds so a crash retries it on the next boot
// (re-initiating a reset is idempotent).
func runPostRestoreKXReset(ctx context.Context, c *client.Client, tracker *Tracker,
	markerPath string, log slog.Logger) error {

	if _, err := os.Stat(markerPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Warnf("Stat kx-reset marker: %v", err)
		}
		return nil
	}

	log.Infof("Restore detected; waiting for server connection to initiate KX resets")
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for tracker.Get().Stage != StageReady {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}

	progr := make(chan clientintf.UserID)
	go func() {
		for uid := range progr {
			log.Infof("Initiated post-restore KX reset with %s", uid)
		}
	}()
	res, err := c.ResetAllOldRatchets(postRestoreKXResetInterval, progr)
	close(progr)
	if err != nil {
		log.Errorf("Post-restore KX reset incomplete (%d initiated): %v; will retry on next boot", len(res), err)
		return nil
	}
	log.Infof("Post-restore KX reset initiated with %d contacts", len(res))
	if err := os.Remove(markerPath); err != nil {
		log.Warnf("Remove kx-reset marker: %v", err)
	}
	return nil
}
