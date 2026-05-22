// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/slog"
)

// BRClientCfg describes what runtime.Run needs to build a BR client. The
// concrete *client.DcrlnPaymentClient is required (not the abstract
// PaymentClient interface) so the CheckServerSession closure can hand its
// LNRPC into CheckLNWalletUsable.
type BRClientCfg struct {
	DB           *clientdb.DB
	DcrlndPay    *client.DcrlnPaymentClient
	BRServer     string
	Tracker      *Tracker
	LogFn        func(subsys string) slog.Logger
	IdentityChan <-chan *zkidentity.FullIdentity
}

// startBRClient builds the BR client config, instantiates the client, and
// returns it ready to be Run by the caller. The Tracker captures wallet
// errors from the CheckServerSession closure and connect/disconnect events
// from OnServerSessionChangedNtfn.
func startBRClient(cfg BRClientCfg) (*client.Client, error) {
	// bisonrelay.org:443 is a seeder that points at the actual BR relay
	// server; the relay serves a single self-signed cert that BR's inner
	// dialer pins. WithDialer would TLS-handshake the seeder directly and
	// reject its public-CA-issued chain.
	netDialer := &net.Dialer{}
	dialer := clientintf.WithSeeder(cfg.BRServer, cfg.LogFn("CONN"), netDialer.DialContext)

	ntfns := client.NewNotificationManager()
	ntfns.Register(client.OnServerSessionChangedNtfn(func(connected bool, _ clientintf.ServerPolicy) {
		cfg.Tracker.SetConnected(connected)
	}))

	brCfg := client.Config{
		DB:            cfg.DB,
		PayClient:     cfg.DcrlndPay,
		Dialer:        dialer,
		Notifications: ntfns,
		Logger:        cfg.LogFn,

		LocalIDIniter: func(ctx context.Context) (*zkidentity.FullIdentity, error) {
			select {
			case id := <-cfg.IdentityChan:
				return id, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},

		// First-run trust-on-first-use: accept the server cert and trust
		// future connections to honour the same identity. clientdb already
		// stores ServerCertPair entries; the connKeeper layer compares
		// against those on subsequent connects. Hardening (explicit
		// confirmation + cert pinning rotation) is a follow-up.
		CertConfirmer: func(_ context.Context, _ *tls.ConnectionState, _ *zkidentity.PublicIdentity) error {
			return nil
		},

		CheckServerSession: func(connCtx context.Context, lnNode string) error {
			cfg.Tracker.SetServerNode(lnNode)
			err := client.CheckLNWalletUsable(connCtx, cfg.DcrlndPay.LNRPC(), lnNode)
			if err != nil {
				cfg.Tracker.SetWalletErr(err.Error())
				return err
			}
			cfg.Tracker.ClearWalletErr()
			return nil
		},
	}

	c, err := client.New(brCfg)
	if err != nil {
		return nil, fmt.Errorf("client.New: %w", err)
	}
	return c, nil
}
