// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/decred/dcrlnd/lnrpc"
	"github.com/decred/slog"
)

const (
	seederURL = "https://bisonrelay.org/api/live"

	// Hardcoded fallback used when the seeder is unreachable. Sourced from
	// bisonrelay/client/client_onboard.go:209-210.
	fallbackHubPubkey = "03bd03386d7b2efe80ae46d6c8cfcfdfcf9c9297a465ac0d48c110d11ae58ed509"
	fallbackHubAddr   = "hub0.bisonrelay.org:9735"
)

type seederResponse struct {
	ServerGroups []struct {
		Brserver string `json:"brserver"`
		Lnd      string `json:"lnd"`
		IsMaster bool   `json:"isMaster"`
		Online   bool   `json:"online"`
	} `json:"serverGroups"`
}

// resolveHubPeer queries the seeder once for the recommended hub LN node.
// On any failure (network, rate limit, parse) it returns the hardcoded
// fallback values.
func resolveHubPeer(ctx context.Context, log slog.Logger) (pubkey, uri string) {
	fallback := fallbackHubPubkey + "@" + fallbackHubAddr

	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(queryCtx, http.MethodGet, seederURL, nil)
	if err != nil {
		log.Warnf("seeder request build failed; using hardcoded hub0: %v", err)
		return fallbackHubPubkey, fallback
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Warnf("seeder query failed; using hardcoded hub0: %v", err)
		return fallbackHubPubkey, fallback
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Warnf("seeder returned HTTP %d; using hardcoded hub0", resp.StatusCode)
		return fallbackHubPubkey, fallback
	}

	var seed seederResponse
	if err := json.NewDecoder(resp.Body).Decode(&seed); err != nil {
		log.Warnf("seeder response decode failed; using hardcoded hub0: %v", err)
		return fallbackHubPubkey, fallback
	}
	for _, g := range seed.ServerGroups {
		if !g.IsMaster {
			continue
		}
		idx := strings.Index(g.Lnd, "@")
		if idx < 0 {
			continue
		}
		log.Infof("Seeder returned hub LN node %s", g.Lnd)
		return g.Lnd[:idx], g.Lnd
	}
	log.Warnf("seeder returned no master group; using hardcoded hub0")
	return fallbackHubPubkey, fallback
}

// waitForChannelToHub polls dcrlnd's ListChannels until an active channel to
// the seeder-recommended hub exists. The seeder is queried once at the
// start; subsequent retries only touch the local dcrlnd, so this gate
// cannot trip the seeder rate limit.
func waitForChannelToHub(ctx context.Context, pc *client.DcrlnPaymentClient, tracker *Tracker, log slog.Logger) error {
	hubPubkey, hubURI := resolveHubPeer(ctx, log)
	tracker.SetRecommendedPeer(hubURI)

	const (
		probeTimeout = 5 * time.Second
		minBackoff   = 10 * time.Second
		maxBackoff   = 30 * time.Second
	)

	backoff := minBackoff
	for {
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		resp, err := pc.LNRPC().ListChannels(probeCtx, &lnrpc.ListChannelsRequest{ActiveOnly: true})
		cancel()

		if err == nil {
			for _, ch := range resp.Channels {
				if strings.EqualFold(ch.RemotePubkey, hubPubkey) {
					log.Infof("Found active channel to hub (%s)", ch.ChannelPoint)
					tracker.MarkChannelReady()
					return nil
				}
			}
			tracker.SetChannelWaiting(fmt.Sprintf("no active channel to recommended hub; %d active channels total", len(resp.Channels)))
		} else {
			tracker.SetChannelWaiting(fmt.Sprintf("ListChannels: %v", err))
		}

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
