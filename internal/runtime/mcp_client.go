// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/decred/dcrd/dcrutil/v4"
	"github.com/decred/slog"
	"github.com/karamble/brmcp/bridge"
)

// The BR-MCP client bridge (github.com/karamble/brmcp/bridge) lets local
// MCP agents call tool services offered by Bison Relay bots: a localhost
// streamable-HTTP MCP endpoint per bot (/mcp/<bot-uid>, bearer gated,
// default disabled) mirrors the remote bot's tools and relays calls over
// the relay, settling paid tools by Bison Relay tip under the user's caps,
// either automatically or after explicit approval. This file is the host
// glue: the PM rails and the tip rail the library engine runs on.

// mcpSender resolves the peer and sends one PM through the BR client.
type mcpSender struct{ c *client.Client }

func (s mcpSender) SendPM(ctx context.Context, peer, text string) error {
	user, err := s.c.UserByNick(peer)
	if err != nil {
		return err
	}
	return s.c.PM(user.ID(), text)
}

// mcpTipPayer settles bridge payments over the BR client's native tip flow
// (the client requests an invoice from the bot's client over the relay,
// verifies the exact amount, and pays it), blocking until the attempt
// reaches a terminal state or the bridge's wait budget runs out. Tip
// attempts are asynchronous in the BR client, so payments are correlated
// with their terminal progress events; non-terminal events (willRetry) are
// ignored, matching the reference clients. Terminal events that match no
// live wait (the wait budget had passed, or the daemon restarted with the
// attempt still running) resolve the bridge's pending spend entries
// instead, so the spend log records the real outcome.
type mcpTipPayer struct {
	c       *client.Client
	matcher *bridge.TipMatcher
	// bridge is set after bridge.New (the payer is built first) and read
	// from notification goroutines.
	bridge atomic.Pointer[bridge.Bridge]
}

func newMCPTipPayer(c *client.Client) *mcpTipPayer {
	p := &mcpTipPayer{c: c, matcher: bridge.NewTipMatcher()}
	c.NotificationManager().Register(client.OnTipAttemptProgressNtfn(func(ru *client.RemoteUser,
		amtMAtoms int64, completed bool, attempt int, attemptErr error, willRetry bool) {

		if willRetry {
			return
		}
		var res error
		if !completed {
			res = attemptErr
			if res == nil {
				res = errors.New("tip attempt ended without completing")
			}
		}
		if !p.matcher.Resolve(ru.ID().String(), amtMAtoms, res) {
			// No live wait consumed the event: a late or replayed
			// outcome for a bridge payment lands on its spend entry;
			// tips from other flows match no pending entry there.
			if b := p.bridge.Load(); b != nil && amtMAtoms%1000 == 0 {
				b.ResolveSpend(ru.ID().String(), amtMAtoms/1000, res)
			}
		}
	}))
	return p
}

func (p *mcpTipPayer) Pay(ctx context.Context, payeeUID string, atoms int64) error {
	waitSecs := 0
	if dl, ok := ctx.Deadline(); ok {
		waitSecs = int(math.Round(time.Until(dl).Seconds()))
	}
	user, err := p.c.UserByNick(payeeUID)
	if err != nil {
		return err
	}
	// One attempt = one invoice request over the relay (like the reference
	// clients); LN pay retries are nested inside with exponential backoff.
	w := p.matcher.Expect(user.ID().String(), atoms*1000)
	if err := p.c.TipUser(user.ID(), dcrutil.Amount(atoms).ToCoin(), 1); err != nil {
		w.Cancel()
		return fmt.Errorf("tip: %w", err)
	}
	select {
	case err := <-w.Done():
		if err != nil {
			return fmt.Errorf("tip failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		w.Cancel()
		// The terminal event may have resolved the wait in the same
		// instant the deadline fired; it was consumed, so forward it to
		// the spend entry or the outcome would be lost.
		select {
		case res := <-w.Done():
			if b := p.bridge.Load(); b != nil {
				b.ResolveSpend(user.ID().String(), atoms, res)
			}
		default:
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("tip not confirmed within %ds; the attempt keeps "+
				"running in the background (up to 72h) and still credits the "+
				"bot's balance for a later call", waitSecs)
		}
		return ctx.Err()
	}
}

// newMCPBridge builds the bridge on this daemon's rails and feeds it every
// inbound PM; non-envelope chat is ignored there, so normal messaging is
// unaffected. A listener bind failure is logged, not fatal: the bridge
// stays usable and a later settings change retries the bind.
func newMCPBridge(ctx context.Context, c *client.Client,
	dataDir, listen string, log slog.Logger) (*bridge.Bridge, error) {

	payer := newMCPTipPayer(c)
	b, err := bridge.New(bridge.Config{
		DataDir:    dataDir,
		Sender:     mcpSender{c},
		Payer:      payer,
		ListenAddr: listen,
		Name:       "brclientd",
		Logf:       log.Infof,
	})
	if err != nil {
		return nil, err
	}
	payer.bridge.Store(b)
	c.NotificationManager().Register(client.OnPMNtfn(func(ru *client.RemoteUser, pm rpc.RMPrivateMessage, _ time.Time) {
		b.HandlePM(ru.ID().String(), pm.Message)
	}))
	if err := b.Start(ctx); err != nil {
		log.Errorf("MCP listener: %v", err)
	}
	return b, nil
}
