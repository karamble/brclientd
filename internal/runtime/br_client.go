// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

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
	DB              *clientdb.DB
	DcrlndPay       *client.DcrlnPaymentClient
	BRServer        string
	SeederCachePath string
	Tracker         *Tracker
	Notifs          *notifBus
	LogFn           func(subsys string) slog.Logger
	IdentityChan    <-chan *zkidentity.FullIdentity
}

// startBRClient builds the BR client config, instantiates the client, and
// returns it ready to be Run by the caller. The Tracker captures wallet
// errors from the CheckServerSession closure and connect/disconnect events
// from OnServerSessionChangedNtfn.
func startBRClient(cfg BRClientCfg) (*client.Client, error) {
	// bisonrelay.org:443 is a seeder that points at the actual BR relay
	// server; the relay serves a single self-signed cert that BR's inner
	// dialer pins. cachedSeederDialer is a drop-in replacement for
	// clientintf.WithSeeder that caches the resolved server address so BR's
	// connKeeper does not hammer the seeder on every reconnect attempt when
	// the BR server is briefly unreachable.
	netDialer := &net.Dialer{}
	dialer := cachedSeederDialer(cfg.BRServer, cfg.LogFn("CONN"), netDialer.DialContext, cfg.SeederCachePath)

	ntfns := client.NewNotificationManager()
	ntfns.Register(client.OnServerSessionChangedNtfn(func(connected bool, _ clientintf.ServerPolicy) {
		cfg.Tracker.SetConnected(connected)
	}))

	// OnKXSuggested fires when a contact sends us a SuggestKX. BR v0.2.4
	// does not persist these or auto-log them to PM history; we do both
	// ourselves so the suggestion survives restart and so the dashboard
	// can render it. The published live event tells the dashboard to
	// refresh the matching thread instead of waiting for the next history
	// scroll.
	nlog := cfg.LogFn("BRCD")
	ntfns.Register(client.OnKXSuggested(func(invitee *client.RemoteUser, target zkidentity.PublicIdentity) {
		targetIDHex := target.Identity.String()
		targetNick := target.Nick
		inviteeID := invitee.ID()
		inviteeNick := invitee.Nick()
		nlog.Infof("Received KX suggestion from %s for %s %q",
			inviteeNick, targetIDHex, targetNick)

		// Mirror BR's own SuggestedKXLogMsg format (clientdb/fscdb.go:96
		// in newer versions: `Suggested KX to %s %q`). Keep it identical
		// so the dashboard's parser works against both stored and live
		// entries.
		line := fmt.Sprintf("Suggested KX to %s %q", targetIDHex, targetNick)
		err := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, inviteeID, true, inviteeNick, line, time.Now())
			return err
		})
		if err != nil {
			nlog.Warnf("Log KX suggestion to PM history: %v", err)
		}

		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "kx-suggested",
				Payload: map[string]any{
					"invitee":     inviteeID.String(),
					"inviteeNick": inviteeNick,
					"target":      targetIDHex,
					"targetNick":  targetNick,
				},
			})
		}
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
