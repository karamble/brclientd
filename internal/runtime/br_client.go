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
	"sync"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/client/resources"
	"github.com/companyzero/bisonrelay/ratchet"
	"github.com/companyzero/bisonrelay/rpc"
	// rtdtclient "github.com/companyzero/bisonrelay/rtdt/client" // dormant: RTDT audio hook is fork-only (see below)
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/dcrlnd/lnrpc"
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
	AudioRouter     *RTDTAudioRouter
	Reinvites       *gcReinviteTracker
	Unrepl          *unreplTracker
	DownloadCaps    *downloadCapTracker
	Notes           *notificationStore
	LogFn           func(subsys string) slog.Logger
	IdentityChan    <-chan *zkidentity.FullIdentity

	// SendReceiveReceipts forwards into client.Config.SendReceiveReceipts:
	// send receive receipts to post authors for received posts and comments.
	SendReceiveReceipts bool
	// ResProvider is the resource provider bound at the client's root. The
	// caller passes a switchableProvider the store controller flips between
	// filesystem-hosted pages and a simplestore at runtime.
	ResProvider resources.Provider
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

	// Assigned after client.New below; notification callbacks only fire once
	// the client runs, so closures capturing brc see the live client.
	var brc *client.Client

	ntfns := client.NewNotificationManager()
	ntfns.Register(client.OnServerSessionChangedNtfn(func(connected bool, policy clientintf.ServerPolicy) {
		cfg.Tracker.SetConnected(connected)
		if connected {
			cfg.Tracker.SetServerPolicy(policy)
		}
	}))

	// OnResourceFetched fires when a page (resource) reply lands, for both
	// remote fetches (FetchResource) and our own local pages
	// (FetchLocalResource, which fires it synchronously with ru==nil). The
	// status server's /pages/fetch handler subscribes to this event to turn
	// the async fetch into a single blocking request. Response.Data carries
	// the (embed-processed) markdown; Request.Tag correlates remote replies.
	ntfns.Register(client.OnResourceFetchedNtfn(func(ru *client.RemoteUser, fr clientdb.FetchedResource, _ clientdb.PageSessionOverview) {
		if cfg.Notifs == nil {
			return
		}
		cfg.Notifs.Publish(NotifEvent{
			Type: "resource-fetched",
			Payload: map[string]any{
				"uid":             fr.UID.String(),
				"tag":             uint64(fr.Request.Tag),
				"status":          uint16(fr.Response.Status),
				"meta":            fr.Response.Meta,
				"data":            string(fr.Response.Data),
				"path":            fr.Request.Path,
				"async_target_id": fr.AsyncTargetID,
				"session_id":      uint64(fr.SessionID),
				"page_id":         uint64(fr.PageID),
				"parent_page":     uint64(fr.ParentPage),
			},
		})
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

	// OnRMReceived fires for every inbound RM before the library's own
	// handler runs; registered sync so the GC lookup below observes db
	// state from that same vantage point (client_rm_handlers.go:346).
	// BR's handleGCInvite drops any invite whose GC already exists
	// locally without storing it or firing a notification, which
	// dead-ends rejoining a GC after a restore left a stale local copy
	// (the admin re-invites because the live roster dropped us). Record
	// those blocked attempts so /gc/invites can report them and the
	// dashboard can offer recovery (leave the stale copy, request a
	// fresh invite). An invite for a GC we do not have locally is stored
	// by the library, so any blocked record for it is moot: clear it.
	// Sync handlers must not block; this is one GC file read gated
	// behind the type assertion.
	ntfns.RegisterSync(client.OnRMReceived(func(ru *client.RemoteUser, _ *rpc.RMHeader, p interface{}, _ time.Time) {
		inv, ok := p.(rpc.RMGroupInvite)
		if !ok || cfg.Reinvites == nil {
			return
		}
		gcid := inv.ID.String()
		var localName string
		exists := false
		err := cfg.DB.View(context.Background(), func(tx clientdb.ReadTx) error {
			gc, err := cfg.DB.GetGC(tx, inv.ID)
			if err == nil {
				exists = true
				localName = gc.Name()
			}
			return nil
		})
		if err != nil {
			nlog.Warnf("Check local GC for incoming invite %s: %v", gcid, err)
			return
		}
		if !exists {
			cfg.Reinvites.Clear(gcid)
			return
		}
		if localName == "" {
			localName = inv.Name
		}
		e := cfg.Reinvites.Record(gcid, localName, ru.ID().String(), ru.Nick())
		nlog.Warnf("Re-invite to GC %q (%s) from %s blocked by existing local copy",
			localName, gcid, ru.Nick())
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "gc-reinvite-blocked",
				Payload: map[string]any{
					"gcid":        e.GCID,
					"name":        e.Name,
					"from":        e.From,
					"fromNick":    e.FromNick,
					"count":       e.Count,
					"lastAttempt": e.LastAttempt,
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

	// OnPostRcvdNtfn fires when a subscribed-to user publishes a new
	// post and we receive it. Forward a lightweight summary so the
	// Feed tab can prepend a card and fetch the body on demand.
	ntfns.Register(client.OnPostRcvdNtfn(func(ru *client.RemoteUser, summary clientdb.PostSummary, _ rpc.PostMetadata) {
		nick := ru.Nick()
		nlog.Infof("Received post %s from %s", summary.ID, nick)
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "post-received",
				Payload: map[string]any{
					"id":          summary.ID.String(),
					"from":        summary.From.String(),
					"author_id":   summary.AuthorID.String(),
					"author_nick": summary.AuthorNick,
					"date":        summary.Date.Unix(),
					"title":       summary.Title,
				},
			})
		}
	}))

	// BR swallows host-side chunk-invoice failures: genInvoiceForFTUpload's
	// error is only logged and the getter receives no reply, so a buyer of
	// a paid file stalls silently when this node cannot RECEIVE the
	// payment. Observe inbound chunk requests and warn when the share's
	// cost exceeds the current LN inbound capacity. The full file cost is
	// the estimate: exact for single-chunk files (the common case), an
	// upper bound otherwise. Async registration: this queries dcrlnd and
	// must not block the RM loop. Throttled per buyer+file because getters
	// retry chunk requests.
	var saleWarnMu sync.Mutex
	saleWarned := make(map[string]time.Time)
	ntfns.Register(client.OnRMReceived(func(ru *client.RemoteUser, _ *rpc.RMHeader, p interface{}, _ time.Time) {
		req, ok := p.(rpc.RMFTGetChunk)
		if !ok || brc == nil || cfg.DcrlndPay == nil {
			return
		}
		files, err := brc.ListLocalSharedFiles()
		if err != nil {
			return
		}
		var cost uint64
		var filename string
		found := false
		for _, f := range files {
			if f.SF.FID.String() == req.FileID {
				cost = f.Cost
				filename = f.SF.Filename
				found = true
				break
			}
		}
		if !found || cost == 0 {
			return
		}
		uid := ru.ID().String()
		warnKey := uid + "/" + req.FileID
		saleWarnMu.Lock()
		last, warned := saleWarned[warnKey]
		if warned && time.Since(last) < 10*time.Minute {
			saleWarnMu.Unlock()
			return
		}
		saleWarned[warnKey] = time.Now()
		saleWarnMu.Unlock()
		bal, err := cfg.DcrlndPay.LNRPC().ChannelBalance(context.Background(), &lnrpc.ChannelBalanceRequest{})
		if err != nil {
			nlog.Warnf("ChannelBalance for sale capacity check: %v", err)
			return
		}
		if bal.MaxInboundAmount >= int64(cost) {
			return
		}
		missing := int64(cost) - bal.MaxInboundAmount
		nick := ru.Nick()
		detail := fmt.Sprintf("%s tried to buy %q (%s DCR) but this node lacks inbound capacity to receive the payment (missing %s DCR). Request an inbound channel to fix this.",
			nick, filename, formatDCR(float64(cost)/1e8), formatDCR(float64(missing)/1e8))
		nlog.Warnf("%s", detail)
		if cfg.Notes != nil {
			cfg.Notes.add("warn", "Sale blocked: "+filename, detail, uid)
		}
		logErr := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, ru.ID(), true, "", detail, time.Now())
			return err
		})
		if logErr != nil {
			nlog.Warnf("Log blocked sale to PM history: %v", logErr)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "file-invoice-capacity-low",
				Payload: map[string]any{
					"uid":               uid,
					"nick":              nick,
					"fid":               req.FileID,
					"filename":          filename,
					"cost_atoms":        cost,
					"max_inbound_atoms": bal.MaxInboundAmount,
					"missing_atoms":     missing,
					"text":              detail,
				},
			})
		}
	}))

	// OnInvoiceGenFailedNtfn fires when generating a TIP invoice for a
	// remote user fails (e.g. dcrlnd rejects it for missing inbound
	// capacity). File-chunk invoice failures do NOT reach this hook; the
	// OnRMReceived observer above covers those.
	ntfns.Register(client.OnInvoiceGenFailedNtfn(func(ru *client.RemoteUser, dcrAmount float64, genErr error) {
		uid := ru.ID().String()
		nick := ru.Nick()
		detail := fmt.Sprintf("Could not generate an invoice for %s (%s DCR): %v",
			nick, formatDCR(dcrAmount), genErr)
		nlog.Warnf("%s", detail)
		if cfg.Notes != nil {
			cfg.Notes.add("error", "Invoice failed for "+nick, detail, uid)
		}
		logErr := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, ru.ID(), true, "", detail, time.Now())
			return err
		})
		if logErr != nil {
			nlog.Warnf("Log invoice failure to PM history: %v", logErr)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "invoice-gen-failed",
				Payload: map[string]any{
					"uid":        uid,
					"nick":       nick,
					"dcr_amount": dcrAmount,
					"error":      genErr.Error(),
					"text":       detail,
				},
			})
		}
	}))

	// OnLocalClientOfflineTooLong fires after connecting when the client
	// was offline beyond the server's retention window: RVs pushed to it
	// meanwhile may have expired server-side, i.e. messages can be
	// permanently lost. Without this hook the condition is invisible.
	ntfns.Register(client.OnLocalClientOfflineTooLong(func(lastConn time.Time) {
		detail := fmt.Sprintf("This client was offline since %s, longer than the server's message retention window; messages sent to it meanwhile may have been dropped.",
			lastConn.Format(time.RFC1123))
		nlog.Warnf("%s", detail)
		if cfg.Notes != nil {
			cfg.Notes.add("warn", "Offline too long", detail, "")
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "offline-too-long",
				Payload: map[string]any{
					"since": lastConn.Unix(),
					"text":  detail,
				},
			})
		}
	}))

	// OnServerUnwelcomeError fires when the relay rejects the connection
	// in a way that hints this client version must be upgraded.
	ntfns.Register(client.OnServerUnwelcomeError(func(err error) {
		detail := fmt.Sprintf("The relay server rejected this client: %v. A daemon upgrade may be required.", err)
		nlog.Errorf("%s", detail)
		if cfg.Notes != nil {
			cfg.Notes.add("error", "Server rejected client", detail, "")
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "server-unwelcome",
				Payload: map[string]any{
					"error": err.Error(),
					"text":  detail,
				},
			})
		}
	}))

	// OnRemoteSubscriptionErrorNtfn fires when a subscribe/unsubscribe to
	// a remote user's posts errors; without it the Feed UI shows the
	// attempt as if it succeeded.
	ntfns.Register(client.OnRemoteSubscriptionErrorNtfn(func(ru *client.RemoteUser, wasSubscribing bool, errMsg string) {
		uid := ru.ID().String()
		nick := ru.Nick()
		verb := "unsubscribe from"
		if wasSubscribing {
			verb = "subscribe to"
		}
		detail := fmt.Sprintf("Could not %s %s's posts: %s", verb, nick, errMsg)
		nlog.Warnf("%s", detail)
		if cfg.Notes != nil {
			cfg.Notes.add("warn", "Post subscription failed", detail, uid)
		}
		logErr := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, ru.ID(), true, "", detail, time.Now())
			return err
		})
		if logErr != nil {
			nlog.Warnf("Log subscription error to PM history: %v", logErr)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "posts-subscribe-error",
				Payload: map[string]any{
					"uid":             uid,
					"nick":            nick,
					"was_subscribing": wasSubscribing,
					"error":           errMsg,
					"text":            detail,
				},
			})
		}
	}))

	// OnBlockNtfn fires when a contact asked to be blocked: the library
	// removes them and they cannot be messaged anymore. No PM log line
	// since the contact (and their log) is being torn down.
	ntfns.Register(client.OnBlockNtfn(func(ru *client.RemoteUser) {
		uid := ru.ID().String()
		nick := ru.Nick()
		detail := fmt.Sprintf("%s has blocked this client; messages can no longer be exchanged with them.", nick)
		nlog.Warnf("%s", detail)
		if cfg.Notes != nil {
			cfg.Notes.add("warn", "Blocked by "+nick, detail, uid)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "blocked-by-user",
				Payload: map[string]any{
					"uid":  uid,
					"nick": nick,
					"text": detail,
				},
			})
		}
	}))

	// OnProfileUpdated fires when a contact changes profile fields (e.g.
	// avatar); the dashboard refreshes its cached contact data live.
	ntfns.Register(client.OnProfileUpdated(func(ru *client.RemoteUser, _ *clientdb.AddressBookEntry, fields []client.ProfileUpdateField) {
		fieldStrs := make([]string, len(fields))
		for i, f := range fields {
			fieldStrs[i] = string(f)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "profile-updated",
				Payload: map[string]any{
					"uid":    ru.ID().String(),
					"nick":   ru.Nick(),
					"fields": fieldStrs,
				},
			})
		}
	}))

	// OnUnsubscribingIdleRemoteClient fires when this client drops an
	// idle peer's subscriptions (only when AutoRemoveIdleUsers is
	// configured); leave a trace in that peer's thread.
	ntfns.Register(client.OnUnsubscribingIdleRemoteClient(func(ru *client.RemoteUser, lastDecTime time.Time) {
		uid := ru.ID().String()
		nick := ru.Nick()
		detail := fmt.Sprintf("%s has been idle since %s; unsubscribing them from posts and GCs.",
			nick, lastDecTime.Format(time.RFC1123))
		nlog.Infof("%s", detail)
		logErr := cfg.DB.Update(context.Background(), func(tx clientdb.ReadWriteTx) error {
			_, err := cfg.DB.LogPM(tx, ru.ID(), true, "", detail, time.Now())
			return err
		})
		if logErr != nil {
			nlog.Warnf("Log idle unsubscribe to PM history: %v", logErr)
		}
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "idle-unsubscribing",
				Payload: map[string]any{
					"uid":  uid,
					"nick": nick,
					"text": detail,
				},
			})
		}
	}))

	// OnRMSent fires after the relay server acks an outbound RM. PMs get a
	// delivery event so the chat UI can upgrade its sent tick: the send
	// endpoint returns at queue time and nothing else confirms the message
	// actually reached the server.
	ntfns.Register(client.OnRMSent(func(ru *client.RemoteUser, _ ratchet.RVPoint, p interface{}) {
		if cfg.Notifs == nil {
			return
		}
		var msg string
		switch v := p.(type) {
		case rpc.RMPrivateMessage:
			msg = v.Message
		case *rpc.RMPrivateMessage:
			msg = v.Message
		default:
			return
		}
		cfg.Notifs.Publish(NotifEvent{
			Type: "pm-delivered",
			Payload: map[string]any{
				"uid":     ru.ID().String(),
				"nick":    ru.Nick(),
				"message": msg,
			},
		})
	}))

	// OnReceiveReceipt fires when a remote client acknowledges receiving
	// one of our posts (domain "posts") or a comment we relayed (domain
	// "postcomments"); lets the dashboard refresh Seen by rows live.
	ntfns.Register(client.OnReceiveReceipt(func(ru *client.RemoteUser, rr rpc.RMReceiveReceipt, serverTime time.Time) {
		if cfg.Notifs == nil {
			return
		}
		id := ""
		if rr.ID != nil {
			id = rr.ID.String()
		}
		subID := ""
		if rr.SubID != nil {
			subID = rr.SubID.String()
		}
		cfg.Notifs.Publish(NotifEvent{
			Type: "receive-receipt",
			Payload: map[string]any{
				"uid":         ru.ID().String(),
				"nick":        ru.Nick(),
				"domain":      string(rr.Domain),
				"id":          id,
				"sub_id":      subID,
				"server_time": serverTime.UnixMilli(),
			},
		})
	}))

	// OnTipUserInvoiceGeneratedNtfn fires when the remote user's invoice
	// for one of our tip attempts arrives; the visible gap between
	// "requesting invoice" and the terminal tip-sent/tip-failed can
	// otherwise last hours (invoices are re-requested daily).
	ntfns.Register(client.OnTipUserInvoiceGeneratedNtfn(func(ru *client.RemoteUser, tag uint32, _ string) {
		if cfg.Notifs == nil {
			return
		}
		cfg.Notifs.Publish(NotifEvent{
			Type: "tip-invoice-generated",
			Payload: map[string]any{
				"uid":  ru.ID().String(),
				"nick": ru.Nick(),
				"tag":  tag,
			},
		})
	}))

	// OnPostStatusRcvdNtfn fires when a status update on a post (comment,
	// heart, etc.) arrives — either ours arriving back via the relay or
	// someone else's on a post we already know about. We fan it out as
	// either post-status-received (comments) or post-heart-received
	// (hearts) so each Feed surface can react cheaply.
	ntfns.Register(client.OnPostStatusRcvdNtfn(func(ru *client.RemoteUser, pid clientintf.PostID, statusFrom client.UserID, pms rpc.PostMetadataStatus) {
		if cfg.Notifs == nil {
			return
		}
		// A status on the LOCAL user's own post round-trips back with a nil
		// remote user (there is no RemoteUser for ourselves); guard the deref
		// so it cannot panic the daemon. pid and status_from still identify it.
		var author, authorNick string
		if ru != nil {
			author = ru.ID().String()
			authorNick = ru.Nick()
		}
		commentBody := pms.Attributes[rpc.RMPSComment]
		heartVal := pms.Attributes[rpc.RMPSHeart]
		if commentBody != "" {
			// One of our own comments replicating back: the author has
			// broadcast it, so it is no longer unreplicated. The tracker
			// key is the post author; for our own posts ru is nil and the
			// author is the local identity.
			if cfg.Unrepl != nil && brc != nil && statusFrom == brc.PublicID() {
				postFrom := author
				if postFrom == "" {
					postFrom = brc.PublicID().String()
				}
				cfg.Unrepl.remove(postFrom, pid.String(), commentBody, pms.Attributes[rpc.RMPParent])
			}
			nlog.Infof("Received comment on post %s from %s", pid, statusFrom)
			var ts int64
			if tsStr := pms.Attributes[rpc.RMPTimestamp]; tsStr != "" {
				// Hex per BR's status-update writer (client_posts.go).
				if n, err := strconv.ParseInt(tsStr, 16, 64); err == nil {
					ts = n
				}
			}
			cfg.Notifs.Publish(NotifEvent{
				Type: "post-status-received",
				Payload: map[string]any{
					"author":      author,
					"author_nick": authorNick,
					"pid":         pid.String(),
					"status_from": statusFrom.String(),
					"from_nick":   pms.Attributes[rpc.RMPFromNick],
					"comment":     commentBody,
					"parent":      pms.Attributes[rpc.RMPParent],
					"identifier":  pms.Attributes[rpc.RMPIdentifier],
					"timestamp":   ts,
				},
			})
			// Bell note for a new comment on one of OUR posts. Own-post statuses
			// arrive with a nil remote user; exclude our own comments, which
			// round-trip the same way.
			if cfg.Notes != nil && ru == nil && brc != nil && statusFrom != brc.PublicID() {
				nick := pms.Attributes[rpc.RMPFromNick]
				if nick == "" {
					nick = statusFrom.String()
				}
				cfg.Notes.add("info", "New comment on your post",
					fmt.Sprintf("%s commented: %s", nick, truncateRunes(commentBody, 80)),
					statusFrom.String())
			}
			return
		}
		if heartVal != "" {
			nlog.Infof("Received heart=%s on post %s from %s", heartVal, pid, statusFrom)
			cfg.Notifs.Publish(NotifEvent{
				Type: "post-heart-received",
				Payload: map[string]any{
					"author":      author,
					"author_nick": authorNick,
					"pid":         pid.String(),
					"status_from": statusFrom.String(),
					"value":       heartVal,
				},
			})
			// Bell note when someone likes one of OUR posts (the +1 only, not an
			// un-like). Own-post statuses arrive with a nil remote user.
			if cfg.Notes != nil && ru == nil && heartVal == rpc.RMPSHeartYes &&
				brc != nil && statusFrom != brc.PublicID() {
				nick := pms.Attributes[rpc.RMPFromNick]
				if nick == "" {
					nick = statusFrom.String()
				}
				cfg.Notes.add("info", "New like on your post",
					fmt.Sprintf("%s liked your post", nick), statusFrom.String())
			}
		}
	}))

	// OnFileDownloadProgress fires per chunk-batch during a file
	// transfer (both incoming downloads and outgoing sends, per BR's
	// terminology). nbMissingChunks lets the Manage Downloads tab
	// render a progress bar without polling.
	ntfns.Register(client.OnFileDownloadProgress(func(ru *client.RemoteUser, fm rpc.FileMetadata, nbMissingChunks int) {
		if cfg.Notifs == nil {
			return
		}
		cfg.Notifs.Publish(NotifEvent{
			Type: "file-download-progress",
			Payload: map[string]any{
				"uid":            ru.ID().String(),
				"nick":           ru.Nick(),
				"filename":       fm.Filename,
				"size":           fm.Size,
				"total_chunks":   len(fm.Manifest),
				"missing_chunks": nbMissingChunks,
			},
		})
	}))

	// OnFileDownloadCompleted fires once the final chunk lands and the
	// file is fully reconstructed on disk. diskPath is the absolute
	// path BR wrote the bytes to.
	ntfns.Register(client.OnFileDownloadCompleted(func(ru *client.RemoteUser, fm rpc.FileMetadata, diskPath string) {
		nlog.Infof("Completed download %s from %s -> %s", fm.Filename, ru.Nick(), diskPath)
		if cfg.Notifs == nil {
			return
		}
		cfg.Notifs.Publish(NotifEvent{
			Type: "file-download-completed",
			Payload: map[string]any{
				"uid":       ru.ID().String(),
				"nick":      ru.Nick(),
				"fid":       clientdb.FileID(fm.MetadataHash()).String(),
				"filename":  fm.Filename,
				"size":      fm.Size,
				"disk_path": diskPath,
			},
		})
	}))

	// OnContentListReceived fires when a remote user replies to our
	// ListUserContent request with the files they have shared. Forwarded
	// verbatim to subscribers; the dashboard's modal hydrates from this.
	ntfns.Register(client.OnContentListReceived(func(ru *client.RemoteUser, files []clientdb.RemoteFile, listErr error) {
		uid := ru.ID()
		nick := ru.Nick()
		if listErr != nil {
			nlog.Warnf("Content list from %s failed: %v", nick, listErr)
			if cfg.Notifs != nil {
				cfg.Notifs.Publish(NotifEvent{
					Type: "content-list-received",
					Payload: map[string]any{
						"uid":   uid.String(),
						"nick":  nick,
						"error": listErr.Error(),
					},
				})
			}
			return
		}
		out := make([]map[string]any, 0, len(files))
		for _, f := range files {
			out = append(out, map[string]any{
				"file_id":     f.FID.String(),
				"filename":    f.Metadata.Filename,
				"size":        f.Metadata.Size,
				"directory":   f.Metadata.Directory,
				"description": f.Metadata.Description,
				"cost":        f.Metadata.Cost,
				"downloaded":  f.DiskPath != "",
			})
		}
		nlog.Infof("Received %d shared files from %s", len(out), nick)
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "content-list-received",
				Payload: map[string]any{
					"uid":   uid.String(),
					"nick":  nick,
					"files": out,
				},
			})
		}
	}))

	// OnPostsListReceived fires when a remote user replies to our
	// ListUserPosts request with their post-list. We forward the list
	// verbatim to subscribers; the dashboard's modal hydrates from this.
	ntfns.Register(client.OnPostsListReceived(func(ru *client.RemoteUser, postList rpc.RMListPostsReply) {
		uid := ru.ID()
		nick := ru.Nick()
		posts := make([]map[string]any, 0, len(postList.Posts))
		for _, p := range postList.Posts {
			posts = append(posts, map[string]any{
				"id":        p.ID.String(),
				"title":     p.Title,
				"timestamp": p.Timestamp,
			})
		}
		nlog.Infof("Received %d posts from %s", len(posts), nick)
		if cfg.Notifs != nil {
			cfg.Notifs.Publish(NotifEvent{
				Type: "posts-list-received",
				Payload: map[string]any{
					"uid":   uid.String(),
					"nick":  nick,
					"posts": posts,
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
		if cfg.Notes != nil {
			cfg.Notes.add("info", "Tip received", line, senderID.String())
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

	// ---- RTDT realtime-voice notifications ----
	// 20 OnRTDT* hooks are registered here. Each republishes a typed event
	// onto the notif bus so the dashboard's existing event consumer can
	// react. Naming convention: "rtdt-<kebab-case-source>". Payload uses
	// stringified ShortIDs / UserIDs / RTDTPeerIDs so JSON round-trips
	// cleanly to the browser.
	if cfg.Notifs != nil {
		notifs := cfg.Notifs

		ntfns.Register(client.OnInvitedToRTDTSession(func(ru *client.RemoteUser, sess *rpc.RMRTDTSessionInvite, ts time.Time) {
			notifs.Publish(NotifEvent{
				Type:      "rtdt-invited",
				Timestamp: ts,
				Payload: map[string]any{
					"inviter":     ru.ID().String(),
					"inviterNick": ru.Nick(),
					"sessRV":      sess.RV.String(),
					"size":        sess.Size,
					"description": sess.Description,
					"asPublisher": sess.AllowedAsPublisher,
					"peerID":      uint32(sess.PeerID),
					"isInstant":   sess.IsInstant,
				},
			})
		}))
		ntfns.Register(client.OnRTDTSessionInviteAccepted(func(ru *client.RemoteUser, sessID zkidentity.ShortID, asPublisher bool) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-invite-accepted",
				Payload: map[string]any{
					"acceptor":     ru.ID().String(),
					"acceptorNick": ru.Nick(),
					"sessRV":       sessID.String(),
					"asPublisher":  asPublisher,
				},
			})
		}))
		ntfns.Register(client.OnRTDTSessionInviteCanceled(func(ru *client.RemoteUser, sessID zkidentity.ShortID) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-invite-canceled",
				Payload: map[string]any{
					"by":     ru.ID().String(),
					"byNick": ru.Nick(),
					"sessRV": sessID.String(),
				},
			})
		}))
		ntfns.Register(client.OnRTDTSesssionUpdated(func(ru *client.RemoteUser, update *client.RTDTSessionUpdateNtfn) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-session-updated",
				Payload: map[string]any{
					"by":              ru.ID().String(),
					"byNick":          ru.Nick(),
					"sessRV":          update.SessionRV.String(),
					"initialJoin":     update.InitialJoin,
					"addedPublishers": len(update.NewPublishers),
					"removedPubs":     len(update.RemovedPublishers),
				},
			})
		}))
		ntfns.Register(client.OnRTDTLiveSessionJoined(func(sessRV zkidentity.ShortID) {
			notifs.Publish(NotifEvent{
				Type:    "rtdt-live-joined",
				Payload: map[string]any{"sessRV": sessRV.String()},
			})
		}))
		ntfns.Register(client.OnRTDTRefreshedSessionAllowance(func(sessRV zkidentity.ShortID, addAllowance uint64) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-allowance-refreshed",
				Payload: map[string]any{
					"sessRV":       sessRV.String(),
					"addAllowance": addAllowance,
				},
			})
		}))
		ntfns.Register(client.OnRTDTLivePeerJoined(func(sessRV zkidentity.ShortID, peerID rpc.RTDTPeerID) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-peer-joined",
				Payload: map[string]any{
					"sessRV": sessRV.String(),
					"peerID": uint32(peerID),
				},
			})
		}))
		ntfns.Register(client.OnRTDTLivePeerStalled(func(sessRV zkidentity.ShortID, peerID rpc.RTDTPeerID) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-peer-stalled",
				Payload: map[string]any{
					"sessRV": sessRV.String(),
					"peerID": uint32(peerID),
				},
			})
		}))
		ntfns.Register(client.OnRTDTLiveSessionSendErrored(func(sessRV zkidentity.ShortID, err error) {
			msg := ""
			if err != nil {
				msg = err.Error()
			}
			notifs.Publish(NotifEvent{
				Type: "rtdt-send-error",
				Payload: map[string]any{
					"sessRV": sessRV.String(),
					"error":  msg,
				},
			})
		}))
		ntfns.Register(client.OnRTDTRemadeLiveSessionHotAudio(func(sessRV zkidentity.ShortID) {
			notifs.Publish(NotifEvent{
				Type:    "rtdt-hot-audio",
				Payload: map[string]any{"sessRV": sessRV.String()},
			})
		}))
		ntfns.Register(client.OnRTDTPeerSoundChanged(func(sessRV zkidentity.ShortID, peerID rpc.RTDTPeerID, hasSoundStream, hasSound bool) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-peer-sound-changed",
				Payload: map[string]any{
					"sessRV":         sessRV.String(),
					"peerID":         uint32(peerID),
					"hasSoundStream": hasSoundStream,
					"hasSound":       hasSound,
				},
			})
		}))
		ntfns.Register(client.OnRTDTPeerExitedSession(func(ru *client.RemoteUser, sessRV zkidentity.ShortID, peerID rpc.RTDTPeerID) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-peer-exited",
				Payload: map[string]any{
					"by":     ru.ID().String(),
					"byNick": ru.Nick(),
					"sessRV": sessRV.String(),
					"peerID": uint32(peerID),
				},
			})
		}))
		ntfns.Register(client.OnRTDTKickedFromLiveSession(func(sessRV zkidentity.ShortID, peerID rpc.RTDTPeerID, banDuration time.Duration) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-kicked",
				Payload: map[string]any{
					"sessRV":     sessRV.String(),
					"peerID":     uint32(peerID),
					"banSeconds": int64(banDuration.Seconds()),
				},
			})
		}))
		ntfns.Register(client.OnRTDTSessionDissolved(func(ru *client.RemoteUser, sessRV zkidentity.ShortID, peerID rpc.RTDTPeerID) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-dissolved",
				Payload: map[string]any{
					"by":     ru.ID().String(),
					"byNick": ru.Nick(),
					"sessRV": sessRV.String(),
					"peerID": uint32(peerID),
				},
			})
		}))
		ntfns.Register(client.OnRTDTRemovedFromSession(func(ru *client.RemoteUser, sessRV zkidentity.ShortID, reason string) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-removed",
				Payload: map[string]any{
					"by":     ru.ID().String(),
					"byNick": ru.Nick(),
					"sessRV": sessRV.String(),
					"reason": reason,
				},
			})
		}))
		ntfns.Register(client.OnRTDTRotatedCookie(func(ru *client.RemoteUser, sessRV zkidentity.ShortID) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-cookies-rotated",
				Payload: map[string]any{
					"by":     ru.ID().String(),
					"byNick": ru.Nick(),
					"sessRV": sessRV.String(),
				},
			})
		}))
		ntfns.Register(client.OnRTDTChatMessageReceived(func(sessRV zkidentity.ShortID, pub rpc.RMRTDTSessionPublisher, msg string, ts uint32) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-chat",
				Payload: map[string]any{
					"sessRV":  sessRV.String(),
					"peerID":  uint32(pub.PeerID),
					"message": msg,
					"ts":      ts,
				},
			})
		}))
		ntfns.Register(client.OnRTDTAdminCookiesReceived(func(ru *client.RemoteUser, sessRV zkidentity.ShortID) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-admin-cookies",
				Payload: map[string]any{
					"by":     ru.ID().String(),
					"byNick": ru.Nick(),
					"sessRV": sessRV.String(),
				},
			})
		}))
		ntfns.Register(client.OnRTDTRTTCalculated(func(addr net.UDPAddr, rtt time.Duration) {
			notifs.Publish(NotifEvent{
				Type: "rtdt-rtt",
				Payload: map[string]any{
					"addr":  addr.String(),
					"rttNs": rtt.Nanoseconds(),
				},
			})
		}))
		ntfns.Register(client.OnRTDTJoinedInstantCall(func(sessRV zkidentity.ShortID) {
			notifs.Publish(NotifEvent{
				Type:    "rtdt-joined-instant-call",
				Payload: map[string]any{"sessRV": sessRV.String()},
			})
		}))

		// ---- PM (private message) notifications ----
		// OnPMNtfn fires once per newly-received PM (after it is logged) and is
		// never invoked for history/backlog, so the dashboard can badge from this
		// in-process event without the replay that ChatService.PMStream incurs.
		ntfns.Register(client.OnPMNtfn(func(ru *client.RemoteUser, pm rpc.RMPrivateMessage, ts time.Time) {
			notifs.Publish(NotifEvent{
				Type:      "pm",
				Timestamp: ts,
				Payload: map[string]any{
					"from":     ru.ID().String(),
					"fromNick": ru.Nick(),
					"message":  pm.Message,
					"mode":     int(pm.Mode),
				},
			})
		}))

		// ---- GC (group-chat) notifications ----
		// 12 OnGC* hooks. We republish each as gc-<kebab>. The dashboard's
		// existing ChatService.GCMStream covers message arrival as 'gcm',
		// but we also surface the higher-fidelity gc-message event here
		// (from OnGCMNtfn) so structural and message events flow over the
		// same notif bus.
		ntfns.Register(client.OnGCMNtfn(func(ru *client.RemoteUser, gcm rpc.RMGroupMessage, ts time.Time) {
			notifs.Publish(NotifEvent{
				Type:      "gc-message",
				Timestamp: ts,
				Payload: map[string]any{
					"gcid":     gcm.ID.String(),
					"from":     ru.ID().String(),
					"fromNick": ru.Nick(),
					"message":  gcm.Message,
					"mode":     int(gcm.Mode),
				},
			})
		}))
		ntfns.Register(client.OnInvitedToGCNtfn(func(ru *client.RemoteUser, iid uint64, invite rpc.RMGroupInvite) {
			notifs.Publish(NotifEvent{
				Type: "gc-invited",
				Payload: map[string]any{
					"iid":         iid,
					"gcid":        invite.ID.String(),
					"name":        invite.Name,
					"description": invite.Description,
					"from":        ru.ID().String(),
					"fromNick":    ru.Nick(),
					"expires":     invite.Expires,
					"version":     invite.Version,
				},
			})
		}))
		ntfns.Register(client.OnJoinedGCNtfn(func(gc rpc.RMGroupList) {
			notifs.Publish(NotifEvent{
				Type: "gc-joined",
				Payload: map[string]any{
					"gcid":    gc.ID.String(),
					"name":    gc.Name,
					"members": shortIDsToStrings(gc.Members),
				},
			})
		}))
		ntfns.Register(client.OnGCInviteAcceptedNtfn(func(ru *client.RemoteUser, gc rpc.RMGroupList) {
			notifs.Publish(NotifEvent{
				Type: "gc-invite-accepted",
				Payload: map[string]any{
					"by":     ru.ID().String(),
					"byNick": ru.Nick(),
					"gcid":   gc.ID.String(),
					"name":   gc.Name,
				},
			})
		}))
		ntfns.Register(client.OnAddedGCMembersNtfn(func(gc rpc.RMGroupList, uids []clientintf.UserID) {
			notifs.Publish(NotifEvent{
				Type: "gc-members-added",
				Payload: map[string]any{
					"gcid":  gc.ID.String(),
					"added": userIDsToStrings(uids),
				},
			})
		}))
		ntfns.Register(client.OnRemovedGCMembersNtfn(func(gc rpc.RMGroupList, uids []clientintf.UserID) {
			notifs.Publish(NotifEvent{
				Type: "gc-members-removed",
				Payload: map[string]any{
					"gcid":    gc.ID.String(),
					"removed": userIDsToStrings(uids),
				},
			})
		}))
		ntfns.Register(client.OnGCUserPartedNtfn(func(gcid client.GCID, uid client.UserID, reason string, kicked bool) {
			notifs.Publish(NotifEvent{
				Type: "gc-parted",
				Payload: map[string]any{
					"gcid":   gcid.String(),
					"uid":    uid.String(),
					"reason": reason,
					"kicked": kicked,
				},
			})
		}))
		ntfns.Register(client.OnGCKilledNtfn(func(ru *client.RemoteUser, gcid client.GCID, reason string) {
			notifs.Publish(NotifEvent{
				Type: "gc-killed",
				Payload: map[string]any{
					"gcid":   gcid.String(),
					"by":     ru.ID().String(),
					"byNick": ru.Nick(),
					"reason": reason,
				},
			})
		}))
		ntfns.Register(client.OnGCUpgradedNtfn(func(gc rpc.RMGroupList, oldVersion uint8) {
			notifs.Publish(NotifEvent{
				Type: "gc-upgraded",
				Payload: map[string]any{
					"gcid":       gc.ID.String(),
					"newVersion": gc.Version,
					"oldVersion": oldVersion,
				},
			})
		}))
		ntfns.Register(client.OnGCAdminsChangedNtfn(func(ru *client.RemoteUser, gc rpc.RMGroupList, added, removed []zkidentity.ShortID) {
			notifs.Publish(NotifEvent{
				Type: "gc-admins-changed",
				Payload: map[string]any{
					"gcid":    gc.ID.String(),
					"by":      ru.ID().String(),
					"byNick":  ru.Nick(),
					"added":   shortIDsToStrings(added),
					"removed": shortIDsToStrings(removed),
				},
			})
		}))
		ntfns.Register(client.OnGCVersionWarning(func(ru *client.RemoteUser, gc rpc.RMGroupList, minVersion, maxVersion uint8) {
			notifs.Publish(NotifEvent{
				Type: "gc-version-warning",
				Payload: map[string]any{
					"gcid":       gc.ID.String(),
					"name":       gc.Name,
					"from":       ru.ID().String(),
					"fromNick":   ru.Nick(),
					"minVersion": minVersion,
					"maxVersion": maxVersion,
				},
			})
		}))
		ntfns.Register(client.OnGCWithUnkxdMemberNtfn(func(gcid zkidentity.ShortID, uid clientintf.UserID,
			hasKX, hasMI bool, miCount uint32, startedMIMediator *clientintf.UserID) {
			payload := map[string]any{
				"gcid":    gcid.String(),
				"uid":     uid.String(),
				"hasKX":   hasKX,
				"hasMI":   hasMI,
				"miCount": miCount,
			}
			if startedMIMediator != nil {
				payload["mediator"] = startedMIMediator.String()
			}
			notifs.Publish(NotifEvent{Type: "gc-unkxd-member", Payload: payload})
		}))
	}

	// Audio handler: when a sink is registered for the session (Phase 3 WS
	// endpoint does this), forward the decrypted Opus packet to it.
	// Otherwise the router drops + counts the frame. Without this hook
	// stock BR would route audio into c.noterec which is a malgo native
	// device that does not exist in our container.
	// RTDT audio hook: dormant on the upstream-pinned build. Config.RTDTAudioStreamHandler
	// (set below) exists only in the karamble/bisonrelay fork, so this handler is
	// commented out here. Re-enable this block, the import above, the Config field
	// below, and the go.mod replace to build the fork variant with live RTDT audio.
	// var audioHandler rtdtclient.StreamHandler
	// if cfg.AudioRouter != nil {
	// 	router := cfg.AudioRouter
	// 	audioHandler = func(sess *rtdtclient.Session, enc *rpc.RTDTFramedPacket, plain *rpc.RTDTDataPacket) error {
	// 		rv := sess.RV()
	// 		if rv == nil {
	// 			return nil
	// 		}
	// 		router.Dispatch(*rv, enc.Source, plain.Data, plain.Timestamp)
	// 		return nil
	// 	}
	// }

	// Host our own markdown pages from PagesDir via a filesystem resource
	// bound at the root prefix. This is the brclient "pages:" equivalent and
	// is also what FetchLocalResource fulfills against when we view our own
	// pages. Without it, ResourcesProvider stays nil and both hosting and
	// local fetches are disabled.
	// The resource provider is supplied by the caller: a switchableProvider the
	// store controller flips between filesystem-hosted pages and a simplestore
	// at runtime. It is also what FetchLocalResource fulfills against for our
	// own pages. Nil disables hosting + local fetches.
	resProvider := cfg.ResProvider

	brCfg := client.Config{
		DB:            cfg.DB,
		PayClient:     cfg.DcrlndPay,
		Dialer:        dialer,
		Notifications: ntfns,
		Logger:        cfg.LogFn,
		// RTDTAudioStreamHandler: audioHandler, // fork-only; dormant on upstream BR
		ResourcesProvider: resProvider,
		// Auto-subscribe to posts on first-time KX with a new contact.
		// BR's gating in client_kx.go:277 ensures this only fires on a
		// fresh KX (updateAB && !oldUser) and defers to any prior
		// PKXActionFetchPost. Matches brclient's default (autosubposts=1)
		// and bruig's typical config.
		AutoSubscribeToPosts: true,
		SendReceiveReceipts:  cfg.SendReceiveReceipts,

		// Upstream parity (brclient defaults): compressed RMs cost less
		// in relay push fees, periodic handshakes keep ratchets fresh,
		// and contacts idle past the window are unsubscribed (each
		// removal leaves an idle-unsubscribing trace in their thread).
		// The ignore list mirrors brclient's well-known bots.
		ReconnectDelay:              5 * time.Second,
		CompressLevel:               4,
		AutoHandshakeInterval:       21 * 24 * time.Hour,
		AutoRemoveIdleUsersInterval: 60 * 24 * time.Hour,
		AutoRemoveIdleUsersIgnoreList: []string{
			"86abd31f2141b274196d481edd061a00ab7a56b61a31656775c8a590d612b966", // Oprah
			"ad716557157c1f191d8b5f8c6757ea41af49de27dc619fc87f337ca85be325ee", // GC bot
		},

		// Without this GetRTDTMessages always returns empty and in-call
		// chat history cannot be served.
		TrackRTDTChatMessages: true,

		// The cost advertised in a post embed is advisory; the host invoices
		// from the cost stored on its share, which only arrives with the
		// file metadata here. Pay only up to the cap the caller approved
		// when requesting the download (absent cap = free files only) and
		// surface the real price otherwise so the UI can re-confirm.
		FileDownloadConfirmer: func(ru *client.RemoteUser, fm rpc.FileMetadata) bool {
			fid := clientdb.FileID(fm.MetadataHash())
			maxCost := cfg.DownloadCaps.take(ru.ID().String(), fid.String())
			if fm.Cost <= maxCost {
				return true
			}
			if cfg.Notifs != nil {
				cfg.Notifs.Publish(NotifEvent{
					Type: "file-download-cost-rejected",
					Payload: map[string]any{
						"uid":            ru.ID().String(),
						"fid":            fid.String(),
						"filename":       fm.Filename,
						"size":           fm.Size,
						"cost_atoms":     fm.Cost,
						"max_cost_atoms": maxCost,
					},
				})
			}
			return false
		},

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
	brc = c
	return c, nil
}

// shortIDsToStrings + userIDsToStrings hex-encode slices of zkidentity IDs
// for inclusion in JSON notif payloads. Used by the GC notification
// republishers.
func shortIDsToStrings(ids []zkidentity.ShortID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

func userIDsToStrings(ids []clientintf.UserID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
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
