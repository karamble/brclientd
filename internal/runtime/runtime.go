// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package runtime hosts the long-running clientrpc surface. Phase 3 wires up
// only the VersionService so consumers can probe brclientd over the same
// mTLS pair the pre-setup endpoint used. The full BR client core
// (client.Run + chat / GC / posts / payments services) lands in later
// phases.
package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/companyzero/bisonrelay/client/rpcserver"
	"github.com/decred/slog"

	"github.com/karamble/brclientd/internal/certgen"
)

// Config describes a clientrpc server bound to TLS listeners derived from a
// pre-generated cert triplet.
type Config struct {
	Log        slog.Logger
	Certs      certgen.Triplet
	Listen     []string
	AppName    string
	AppVersion string
}

// Run starts the clientrpc server and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	listeners, err := buildListeners(cfg.Certs, cfg.Listen)
	if err != nil {
		return err
	}
	defer closeListeners(listeners)

	srv := rpcserver.New(rpcserver.Config{
		JSONRPCListeners: listeners,
		Log:              cfg.Log,
	})
	srv.InitVersionService(cfg.AppName, cfg.AppVersion)

	for _, addr := range cfg.Listen {
		cfg.Log.Infof("clientrpc listening on %s (mTLS)", addr)
	}
	return srv.Run(ctx)
}

func buildListeners(certs certgen.Triplet, addresses []string) ([]net.Listener, error) {
	tlsCfg, err := certs.LoadServerTLSConfig()
	if err != nil {
		return nil, err
	}
	listeners := make([]net.Listener, 0, len(addresses))
	for _, addr := range addresses {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			closeListeners(listeners)
			return nil, fmt.Errorf("listen on %s: %w", addr, err)
		}
		listeners = append(listeners, tls.NewListener(l, tlsCfg))
	}
	return listeners, nil
}

func closeListeners(listeners []net.Listener) {
	for _, l := range listeners {
		l.Close()
	}
}
