// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"net"
	"net/http"
	"time"

	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/decred/go-socks/socks"
)

// proxySettings is the SOCKS5 proxy every outbound connection uses when set
// (Tor). An empty Addr dials directly.
type proxySettings struct {
	Addr         string
	User         string
	Pass         string
	Isolation    bool
	CircuitLimit uint32
}

// proxyDialFunc returns the dialer for outbound connections: through the
// proxy when one is configured, with stream isolation drawing circuits from a
// bounded pool, and directly otherwise. Mirrors decred/dcrd and brclient.
func proxyDialFunc(p proxySettings) clientintf.DialFunc {
	if p.Addr == "" {
		return (&net.Dialer{}).DialContext
	}
	proxy := socks.Proxy{
		Addr:         p.Addr,
		Username:     p.User,
		Password:     p.Pass,
		TorIsolation: p.Isolation,
	}
	if p.Isolation {
		limit := p.CircuitLimit
		if limit == 0 {
			limit = 32
		}
		return socks.NewPool(proxy, limit).DialContext
	}
	return proxy.DialContext
}

// outboundHTTPClient is the HTTP client for brclientd's own requests to
// third parties (seeder, price fallback). It dials through dial, never an
// environment proxy, so a request goes where the Tor setting says.
func outboundHTTPClient(dial clientintf.DialFunc) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext:         dial,
		Proxy:               nil,
		TLSHandshakeTimeout: 15 * time.Second,
		IdleConnTimeout:     90 * time.Second,
	}}
}
