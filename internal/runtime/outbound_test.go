// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/decred/slog"
)

// recordingTransport stands in for the proxy: it records every host a request
// was sent to and never touches the network.
type recordingTransport struct{ hosts []string }

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.hosts = append(rt.hosts, req.URL.Host)
	return nil, errors.New("recorded, not sent")
}

// With Tor on, brclientd's own requests to third parties must leave through
// the proxy dialer, never through http.DefaultClient's direct dial.
func TestThirdPartyFetchesUseTheOutboundClient(t *testing.T) {
	rt := &recordingTransport{}
	httpc := &http.Client{Transport: rt}

	resolveHubPeer(context.Background(), httpc, slog.Disabled)
	if _, err := fetchKrakenDCRUSD(context.Background(), httpc); err == nil {
		t.Fatal("the Kraken fetch succeeded without the network")
	}
	want := []string{"bisonrelay.org", "api.kraken.com"}
	if strings.Join(rt.hosts, ",") != strings.Join(want, ",") {
		t.Fatalf("requests went to %v through the outbound client; want %v", rt.hosts, want)
	}
}

func TestOutboundClientDialsThroughTheGivenDialer(t *testing.T) {
	var dialed []string
	dial := func(_ context.Context, network, addr string) (net.Conn, error) {
		dialed = append(dialed, addr)
		return nil, errors.New("recorded, not dialed")
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.org/x", nil)
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	if _, err := outboundHTTPClient(dial).Do(req); err == nil {
		t.Fatal("request succeeded without a network")
	}
	if len(dialed) != 1 || dialed[0] != "example.org:443" {
		t.Fatalf("dialed %v; want the target host through the given dialer, not an environment proxy", dialed)
	}
}

// The BR client builds its own rate-collection HTTP client from DialFunc; with
// a proxy configured it must get the proxy dialer, without one its default.
func TestBRClientGetsTheProxyDialerOnlyWithAProxy(t *testing.T) {
	dial := proxyDialFunc(proxySettings{Addr: "127.0.0.1:9050"})
	if proxiedDial("", dial) != nil {
		t.Error("without a proxy the BR client was handed a dialer")
	}
	if proxiedDial("127.0.0.1:9050", dial) == nil {
		t.Error("with a proxy the BR client was not handed the proxy dialer")
	}
	if proxyDialFunc(proxySettings{}) == nil {
		t.Error("no direct dialer without a proxy")
	}
}
