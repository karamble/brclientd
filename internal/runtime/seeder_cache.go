// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	seederclient "github.com/companyzero/bisonrelay/brseeder/client"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/decred/slog"
)

// BR's connKeeper queries the seeder before every reconnect attempt
// (clientintf/net.go:28-35). When the BR server is unreachable the dial
// times out in ~30s and connKeeper retries the whole pair again, which
// hammers bisonrelay.org/api/live indefinitely. bisonrelay.org tightened
// rate limits after a real DDoS, so we cache the resolved server address
// here and let BR keep retrying without re-querying the seeder.
const (
	seederCacheTTL          = 10 * time.Minute
	seederCacheFailureFloor = 60 * time.Second
)

type seederCacheEntry struct {
	APIURL    string    `json:"apiURL"`
	Server    string    `json:"server"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type seederCache struct {
	mu        sync.Mutex
	path      string
	log       slog.Logger
	entry     seederCacheEntry
	lastQuery time.Time
}

func newSeederCache(path string, log slog.Logger) *seederCache {
	c := &seederCache{path: path, log: log}
	c.load()
	return c
}

func (c *seederCache) load() {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := os.ReadFile(c.path)
	if err != nil {
		return
	}
	var e seederCacheEntry
	if json.Unmarshal(data, &e) == nil && e.Server != "" {
		c.entry = e
	}
}

// saveLocked persists c.entry. Caller must hold c.mu.
func (c *seederCache) saveLocked() {
	data, err := json.Marshal(c.entry)
	if err != nil {
		return
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		c.log.Warnf("Persist seeder cache: %v", err)
		return
	}
	if err := os.Rename(tmp, c.path); err != nil {
		c.log.Warnf("Rename seeder cache: %v", err)
	}
}

// fresh returns the cached server address when the entry matches apiURL and
// is within seederCacheTTL. On false the caller falls through to either a
// fresh query or the failure-floor stale path.
func (c *seederCache) fresh(apiURL string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entry.APIURL != apiURL || c.entry.Server == "" {
		return "", false
	}
	if time.Since(c.entry.UpdatedAt) > seederCacheTTL {
		return "", false
	}
	return c.entry.Server, true
}

func (c *seederCache) stale(apiURL string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entry.APIURL != apiURL || c.entry.Server == "" {
		return "", false
	}
	return c.entry.Server, true
}

// canQuery enforces the seederCacheFailureFloor between QuerySeeder calls so
// a hot retry loop cannot DoS bisonrelay.org/api/live when the cache is
// cold.
func (c *seederCache) canQuery() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.lastQuery) >= seederCacheFailureFloor
}

func (c *seederCache) recordQuery() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastQuery = time.Now()
}

func (c *seederCache) put(apiURL, server string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entry = seederCacheEntry{
		APIURL:    apiURL,
		Server:    server,
		UpdatedAt: time.Now(),
	}
	c.lastQuery = time.Now()
	c.saveLocked()
}

// cachedSeederDialer is a drop-in replacement for clientintf.WithSeeder
// (clientintf/net.go:78) that caches the resolved server address in memory
// + on disk. The TLS handshake / cert check block mirrors tlsDialer
// (clientintf/net.go:40-59) verbatim because BR does not expose the inner
// dial path as a hook.
func cachedSeederDialer(brServer string, log slog.Logger, dialFunc clientintf.DialFunc, cachePath string) clientintf.Dialer {
	cache := newSeederCache(cachePath, log)
	apiURL := fmt.Sprintf("https://%s/api/live", brServer)

	return func(ctx context.Context) (clientintf.Conn, *tls.ConnectionState, error) {
		serverAddr, ok := cache.fresh(apiURL)
		if !ok {
			if cache.canQuery() {
				log.Infof("Querying seeder at %v", apiURL)
				cache.recordQuery()
				server, err := seederclient.QuerySeeder(ctx, apiURL, seederclient.DialFunc(dialFunc))
				if err != nil {
					if fb, hadStale := cache.stale(apiURL); hadStale {
						log.Warnf("Seeder query failed (%v); using stale cached %s", err, fb)
						serverAddr = fb
					} else {
						return nil, nil, err
					}
				} else {
					cache.put(apiURL, server)
					serverAddr = server
				}
			} else if fb, hadStale := cache.stale(apiURL); hadStale {
				serverAddr = fb
			} else {
				return nil, nil, seederclient.ErrNoServers
			}
		}

		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			},
			InsecureSkipVerify: true,
		}
		nconn, err := dialFunc(ctx, "tcp", serverAddr)
		if err != nil {
			return nil, nil, err
		}
		conn := tls.Client(nconn, tlsCfg)
		if err := conn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, nil, err
		}
		cs := conn.ConnectionState()
		if len(cs.PeerCertificates) != 1 {
			conn.Close()
			return nil, nil, errors.New("unexpected certificate chain")
		}
		log.Infof("Connected to server %s", brServer)
		return conn, &cs, nil
	}
}
