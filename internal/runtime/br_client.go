// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
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

	// OnRemoteSubscriptionChanged fires when our subscription state with a
	// remote user changes (their reply to our SubscribeToPosts /
	// UnsubscribeToPosts request landed). Wording mirrors bruig's
	// PostSubscriptionEventW (events.dart:845).
	ntfns.Register(client.OnRemoteSubscriptionChangedNtfn(func(ru *client.RemoteUser, subscribed bool) {
		uid := ru.ID()
		ruNick := ru.Nick()
		var line, typ string
		if subscribed {
			line = "Subscribed to user's posts!"
			typ = "posts-subscribed"
		} else {
			line = "Unsubscribed from user's posts"
			typ = "posts-unsubscribed"
		}
		nlog.Infof("%s (%s)", line, ruNick)
		err := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, uid, true, "", line, time.Now())
			return err
		})
		if err != nil {
			nlog.Warnf("Log subscription change to PM history: %v", err)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: typ,
				Payload: map[string]any{
					"uid":  uid.String(),
					"nick": ruNick,
					"line": line,
				},
			})
		}
	}))

	// OnPostSubscriberUpdated fires when a REMOTE user changes their
	// subscription to OUR posts (the inverse of OnRemoteSubscriptionChanged).
	// Wording mirrors bruig's PostsSubscriberUpdatedW (events.dart:865-866).
	ntfns.Register(client.OnPostSubscriberUpdated(func(ru *client.RemoteUser, subscribed bool) {
		uid := ru.ID()
		ruNick := ru.Nick()
		verb := "unsubscribed from"
		if subscribed {
			verb = "subscribed to"
		}
		line := fmt.Sprintf("%s %s the local client's posts.", ruNick, verb)
		nlog.Infof("%s", line)
		err := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, uid, true, "", line, time.Now())
			return err
		})
		if err != nil {
			nlog.Warnf("Log subscriber update to PM history: %v", err)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "posts-subscriber-updated",
				Payload: map[string]any{
					"uid":        uid.String(),
					"nick":       ruNick,
					"subscribed": subscribed,
					"line":       line,
				},
			})
		}
	}))

	// OnTipReceived fires when a remote user successfully tips the local
	// client. Log to the sender's PM thread so the recipient sees a
	// system message inline, and publish for live append.
	ntfns.Register(client.OnTipReceivedNtfn(func(ru *client.RemoteUser, amountMAtoms int64) {
		dcr := matomsToDCR(amountMAtoms)
		senderID := ru.ID()
		senderNick := ru.Nick()
		// Mirrors bruig's receiver-side string (chat/events.dart:732).
		line := fmt.Sprintf("Received %s DCR from %s!", formatDCR(dcr), senderNick)
		nlog.Infof("Received %s DCR from %s", formatDCR(dcr), senderNick)
		err := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, senderID, true, senderNick, line, time.Now())
			return err
		})
		if err != nil {
			nlog.Warnf("Log received tip to PM history: %v", err)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "tip-received",
				Payload: map[string]any{
					"sender":     senderID.String(),
					"senderNick": senderNick,
					"matoms":     amountMAtoms,
					"dcr":        dcr,
					"line":       line,
				},
			})
		}
	}))

	// OnTipAttemptProgress fires per attempt on the SENDER side. Only log
	// + publish on terminal outcomes (completed=true OR no more retries)
	// to keep the thread from being spammed with per-retry status lines.
	ntfns.Register(client.OnTipAttemptProgressNtfn(func(ru *client.RemoteUser, amtMAtoms int64, completed bool, attempt int, attemptErr error, willRetry bool) {
		if !completed && willRetry {
			return
		}
		dcr := matomsToDCR(amtMAtoms)
		recipientID := ru.ID()
		recipientNick := ru.Nick()
		var line string
		var typ string
		// Wording mirrors bruig's TipUserProgressW (chat/events.dart:1148-1156).
		if completed {
			line = fmt.Sprintf("Tip attempt of %s DCR completed successfully!", formatDCR(dcr))
			typ = "tip-sent"
		} else {
			line = fmt.Sprintf("Tip attempt of %s DCR failed due to %v. Given up on attempting to tip.", formatDCR(dcr), attemptErr)
			typ = "tip-failed"
		}
		nlog.Infof("%s to %s", line, recipientNick)
		err := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, recipientID, true, "", line, time.Now())
			return err
		})
		if err != nil {
			nlog.Warnf("Log sent tip to PM history: %v", err)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: typ,
				Payload: map[string]any{
					"recipient":     recipientID.String(),
					"recipientNick": recipientNick,
					"matoms":        amtMAtoms,
					"dcr":           dcr,
					"line":          line,
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

// matomsToDCR converts BR's internal milli-atom unit (1 DCR = 1e11 matoms)
// to a DCR float. Lossy in principle but precise enough for display since
// tip amounts are bounded by available LN capacity.
func matomsToDCR(matoms int64) float64 {
	return float64(matoms) / 1e11
}

// formatDCR renders a DCR amount with trailing-zero trimming so small tips
// don't render as "0.00100000" while large tips still show full precision.
func formatDCR(dcr float64) string {
	return strconv.FormatFloat(dcr, 'f', -1, 64)
}
