// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"

	"github.com/companyzero/bisonrelay/client"
	"github.com/decred/slog"
	"github.com/karamble/brmcp/brclient"
	"github.com/karamble/brmcp/bridge"
)

// The BR-MCP client bridge (github.com/karamble/brmcp/bridge) lets local
// MCP agents call tool services offered by Bison Relay bots: a localhost
// streamable-HTTP MCP endpoint per bot (/mcp/<bot-uid>, bearer gated,
// default disabled) mirrors the remote bot's tools and relays calls over
// the relay, settling paid tools by Bison Relay tip under the user's caps,
// either automatically or after explicit approval.

// newMCPBridge attaches the bridge to this daemon's BR client. A listener
// bind failure is logged, not fatal: the bridge stays usable and a later
// settings change retries the bind.
func newMCPBridge(ctx context.Context, c *client.Client,
	dataDir, listen string, log slog.Logger) (*bridge.Bridge, error) {

	b, err := brclient.Attach(c, bridge.Config{
		DataDir:    dataDir,
		ListenAddr: listen,
		Name:       "brclientd",
		Log:        log,
	})
	if err != nil {
		return nil, err
	}
	if err := b.Start(ctx); err != nil {
		log.Errorf("MCP listener: %v", err)
	}
	return b, nil
}
