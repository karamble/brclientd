// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const probeID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// Rows mirror the caller contract pinned in dcrpulse dashboard/internal/rpc/brclientd_url_test.go:92-275.
var routePatternRows = []struct {
	method, path, want string
}{
	{http.MethodGet, "/status", "/status"},
	{http.MethodGet, "/history/pm", "/history/pm"},
	{http.MethodPost, "/history/pm/clear", "/history/pm/clear"},
	{http.MethodGet, "/msig/history", "/msig/history"},
	{http.MethodGet, "/contacts", "/contacts"},
	{http.MethodPost, "/contacts/rename", "/contacts/rename"},
	{http.MethodGet, "/contacts/groups", "/contacts/groups"},
	{http.MethodPost, "/contacts/groups/assign", "/contacts/groups/assign"},
	{http.MethodPost, "/contacts/groups/settings", "/contacts/groups/settings"},
	{http.MethodPost, "/contacts/kx-reset", "/contacts/kx-reset"},
	{http.MethodPost, "/contacts/reset-all", "/contacts/reset-all"},
	{http.MethodPost, "/contacts/block", "/contacts/block"},
	{http.MethodGet, "/contacts/blocked", "/contacts/blocked"},
	{http.MethodPost, "/contacts/unblock", "/contacts/unblock"},
	{http.MethodPost, "/contacts/ignore", "/contacts/ignore"},
	{http.MethodPost, "/contacts/handshake", "/contacts/handshake"},
	{http.MethodPost, "/contacts/suggest-kx", "/contacts/suggest-kx"},
	{http.MethodPost, "/contacts/trans-reset", "/contacts/trans-reset"},
	{http.MethodPost, "/contacts/accept-suggestion", "/contacts/accept-suggestion"},
	{http.MethodPost, "/contacts/subscribe-posts", "/contacts/subscribe-posts"},
	{http.MethodPost, "/contacts/unsubscribe-posts", "/contacts/unsubscribe-posts"},
	{http.MethodPost, "/contacts/list-posts", "/contacts/list-posts"},
	{http.MethodPost, "/contacts/list-content", "/contacts/list-content"},
	{http.MethodPost, "/contacts/fetch-post", "/contacts/fetch-post"},
	{http.MethodGet, "/posts/feed", "/posts/feed"},
	{http.MethodGet, "/posts/body", "/posts/body"},
	{http.MethodGet, "/posts/embed-data", "/posts/embed-data"},
	{http.MethodGet, "/posts/comments", "/posts/comments"},
	{http.MethodPost, "/posts/comment", "/posts/comment"},
	{http.MethodGet, "/posts/hearts", "/posts/hearts"},
	{http.MethodPost, "/posts/heart", "/posts/heart"},
	{http.MethodGet, "/posts/receivereceipts", "/posts/receivereceipts"},
	{http.MethodGet, "/posts/comment-receivereceipts", "/posts/comment-receivereceipts"},
	{http.MethodPost, "/posts/relay", "/posts/relay"},
	{http.MethodPost, "/posts/new", "/posts/new"},
	{http.MethodGet, "/shared-files", "/shared-files"},
	{http.MethodPost, "/shared-files/add", "/shared-files/add"},
	{http.MethodPost, "/shared-files/remove", "/shared-files/remove"},
	{http.MethodGet, "/downloads", "/downloads"},
	{http.MethodPost, "/downloads/cancel", "/downloads/cancel"},
	{http.MethodPost, "/downloads/delete", "/downloads/delete"},
	{http.MethodPost, "/content/get", "/content/get"},
	{http.MethodGet, "/content/file", "/content/file"},
	{http.MethodGet, "/rates", "/rates"},
	{http.MethodGet, "/store/mode", "/store/mode"},
	{http.MethodGet, "/store/products", "/store/products"},
	{http.MethodPost, "/store/products/delete", "/store/products/delete"},
	{http.MethodGet, "/store/orders", "/store/orders"},
	{http.MethodPost, "/store/orders/status", "/store/orders/status"},
	{http.MethodPost, "/store/orders/comment", "/store/orders/comment"},
	{http.MethodPost, "/store/files/upload", "/store/files/upload"},
	{http.MethodGet, "/store/files/list", "/store/files/list"},
	{http.MethodGet, "/store/files/get", "/store/files/get"},
	{http.MethodPost, "/store/files/delete", "/store/files/delete"},
	{http.MethodGet, "/store/templates", "/store/templates"},
	{http.MethodGet, "/store/templates/file", "/store/templates/file"},
	{http.MethodPost, "/store/templates/save", "/store/templates/save"},
	{http.MethodPost, "/store/templates/delete", "/store/templates/delete"},
	{http.MethodGet, "/resources/requests", "/resources/requests"},
	{http.MethodPost, "/resources/fulfill", "/resources/fulfill"},
	{http.MethodPost, "/payments/invoice", "/payments/invoice"},
	{http.MethodGet, "/payments/invoice/wait", "/payments/invoice/wait"},
	{http.MethodGet, "/payments/invoice/status", "/payments/invoice/status"},
	{http.MethodGet, "/notifications", "/notifications"},
	{http.MethodGet, "/notifications/recent", "/notifications/recent"},
	{http.MethodPost, "/notifications/delete", "/notifications/delete"},
	{http.MethodPost, "/notifications/clear", "/notifications/clear"},
	{http.MethodGet, "/version", "/version"},
	{http.MethodGet, "/public-identity", "/public-identity"},
	{http.MethodPost, "/avatar", "/avatar"},
	{http.MethodPost, "/messages/send", "/messages/send"},
	{http.MethodPost, "/invites/create", "/invites/create"},
	{http.MethodPost, "/invites/accept", "/invites/accept"},
	{http.MethodPost, "/tip", "/tip"},
	{http.MethodGet, "/payments/tips", "/payments/tips"},
	{http.MethodGet, "/payments/tips/running", "/payments/tips/running"},
	{http.MethodPost, "/invites/redeem-key", "/invites/redeem-key"},
	{http.MethodPost, "/files/send", "/files/send"},
	{http.MethodGet, "/stats/overview", "/stats/overview"},
	{http.MethodGet, "/stats/payments", "/stats/payments"},
	{http.MethodPost, "/stats/payments/clear", "/stats/payments/clear"},
	{http.MethodGet, "/stats/network", "/stats/network"},
	{http.MethodGet, "/stats/contacts", "/stats/contacts"},
	{http.MethodGet, "/stats/posts", "/stats/posts"},
	{http.MethodGet, "/rtdt/sessions", "/rtdt/sessions"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/join", "/rtdt/sessions/"},
	{http.MethodGet, "/gc", "/gc"},
	{http.MethodPost, "/gc/" + probeID + "/kill", "/gc/"},
	{http.MethodPost, "/pages/fetch", "/pages/fetch"},
	{http.MethodGet, "/pages/local", "/pages/local"},
	{http.MethodGet, "/pages/local/file", "/pages/local/file"},
	{http.MethodPost, "/pages/local/save", "/pages/local/save"},
	{http.MethodPost, "/pages/local/import-embed", "/pages/local/import-embed"},
	{http.MethodPost, "/pages/local/delete", "/pages/local/delete"},
	{http.MethodGet, "/backup", "/backup"},
	{http.MethodGet, "/connection", "/connection"},
	{http.MethodGet, "/settings/behavior", "/settings/behavior"},
	{http.MethodGet, "/settings/mcpclient", "/settings/mcpclient"},
	{http.MethodGet, "/mcp/pending", "/mcp/pending"},
	{http.MethodPost, "/mcp/pending/resolve", "/mcp/pending/resolve"},
	{http.MethodGet, "/mcp/spend", "/mcp/spend"},
	{http.MethodGet, "/filters", "/filters"},
	{http.MethodPost, "/filters/delete", "/filters/delete"},
	{http.MethodPost, "/posts/subscribe-all", "/posts/subscribe-all"},
	{http.MethodGet, "/kx/list", "/kx/list"},
	{http.MethodGet, "/kx/searches", "/kx/searches"},
	{http.MethodGet, "/kx/mediateids", "/kx/mediateids"},
}

// TestRoutePatterns pins the full route table. Constructing the mux in-test
// turns a registration-conflict panic into a named CI failure.
func TestRoutePatterns(t *testing.T) {
	if len(routePatternRows) != 107 {
		t.Fatalf("route pattern rows = %d, want 107", len(routePatternRows))
	}
	s := &StatusServer{}
	mux := s.routes()
	for _, row := range routePatternRows {
		req := httptest.NewRequest(row.method, row.path, nil)
		_, pattern := mux.Handler(req)
		if pattern != row.want {
			t.Errorf("%s %s: matched pattern %q, want %q", row.method, row.path, pattern, row.want)
		}
	}
}

// TestRouteTableSource locks registration to string literals: the literal set in
// the package's non-test mux.Handle/HandleFunc calls must equal the want column.
func TestRouteTableSource(t *testing.T) {
	reCall := regexp.MustCompile(`mux\.(?:HandleFunc|Handle)\(`)
	reLit := regexp.MustCompile(`mux\.(?:HandleFunc|Handle)\(\s*"([^"]+)"`)

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	calls := 0
	got := make(map[string]int)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		calls += len(reCall.FindAll(data, -1))
		for _, m := range reLit.FindAllSubmatch(data, -1) {
			got[string(m[1])]++
		}
	}

	lits := 0
	for _, n := range got {
		lits += n
	}
	if calls != lits {
		t.Errorf("registration call sites = %d, literal patterns = %d; non-literal patterns are forbidden", calls, lits)
	}
	if lits != len(routePatternRows) {
		t.Errorf("registered literals = %d, want %d", lits, len(routePatternRows))
	}
	want := make(map[string]bool)
	for _, row := range routePatternRows {
		if want[row.want] {
			t.Fatalf("duplicate want pattern %q", row.want)
		}
		want[row.want] = true
	}
	for p, n := range got {
		if n != 1 {
			t.Errorf("pattern %q registered %d times", p, n)
		}
		if !want[p] {
			t.Errorf("registered pattern %q missing from want table", p)
		}
	}
	for p := range want {
		if got[p] == 0 {
			t.Errorf("want pattern %q not registered", p)
		}
	}
}

// TestRouteBehavior is the golden per-route behavior table, served through
// the exact composition Run serves.
func TestRouteBehavior(t *testing.T) {
	t.Run("encoded-slash history clear destroys files", func(t *testing.T) {
		dir := t.TempDir()
		planted := filepath.Join(dir, "groupchat.x."+probeID+".log")
		if err := os.WriteFile(planted, []byte("scrollback"), 0o600); err != nil {
			t.Fatalf("plant log: %v", err)
		}
		s := &StatusServer{MsgsRoot: dir}
		req := httptest.NewRequest(http.MethodPost, "/gc/"+probeID+"%2Fhistory%2Fclear", nil)
		if req.URL.RawPath == "" {
			t.Fatal("fixture lost the encoded slash: RawPath is empty")
		}
		rr := httptest.NewRecorder()
		s.routes().ServeHTTP(rr, req)
		if rr.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", rr.Code)
		}
		if _, err := os.Stat(planted); !os.IsNotExist(err) {
			t.Fatalf("planted log still present (stat err = %v), want removed", err)
		}
	})

	// Rows mirror the caller contract pinned in dcrpulse dashboard/internal/rpc/brclientd_url_test.go:92-275.
	rows := []struct {
		method, target string
		status         int
		bodyPrefix     string
		location       string
		wantEnc        bool
		noAllow        bool
	}{
		{http.MethodPost, "/gc/" + probeID + "%2Fkill", http.StatusServiceUnavailable, "BR client not yet running", "", true, false},
		{http.MethodGet, "/gc/" + probeID + "%2Fkill", http.StatusMethodNotAllowed, "method not allowed", "", true, false},
		{http.MethodPost, "/rtdt/sessions/" + probeID + "%2Fdissolve", http.StatusServiceUnavailable, "BR client not yet running", "", true, false},
		{http.MethodPost, "/gc/" + probeID + "/kill", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodPost, "/rtdt/sessions/" + probeID + "/dissolve", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodPost, "/gc/" + probeID + "/history/clear", http.StatusServiceUnavailable, "history paths not configured", "", false, false},
		{http.MethodPost, "/gc/" + probeID + "/../../contacts/reset-all", http.StatusTemporaryRedirect, "", "/contacts/reset-all", false, false},
		{http.MethodGet, "/gc/", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodGet, "/rtdt/sessions/", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodGet, "/gc/" + probeID + "/", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodGet, "/gc/create", http.StatusMethodNotAllowed, "method not allowed", "", false, false},
		{http.MethodGet, "/gc/create/", http.StatusBadRequest, "invalid gcid:", "", false, false},
		{http.MethodHead, "/gc", http.StatusMethodNotAllowed, "method not allowed", "", false, false},
		{http.MethodHead, "/backup", http.StatusMethodNotAllowed, "method not allowed", "", false, false},
		{http.MethodDelete, "/connection", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodPut, "/settings/mcpclient", http.StatusServiceUnavailable, "MCP engine not yet running", "", false, false},
		{http.MethodPut, "/filters", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodPut, "/contacts/groups", http.StatusServiceUnavailable, "groups store not available", "", false, false},
		{http.MethodPut, "/store/mode", http.StatusServiceUnavailable, "store controller not yet ready", "", false, false},
		{http.MethodPut, "/store/products", http.StatusServiceUnavailable, "store controller not yet ready", "", false, false},
		{http.MethodPut, "/kx/mediateids", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodPut, "/rtdt/sessions/" + probeID + "/audio", http.StatusServiceUnavailable, "BR client not yet running", "", false, false},
		{http.MethodPut, "/mcp/pending", http.StatusServiceUnavailable, "MCP engine not yet running", "", false, false},
		{http.MethodPut, "/mcp/pending/resolve", http.StatusServiceUnavailable, "MCP engine not yet running", "", false, false},
		{http.MethodPut, "/mcp/spend", http.StatusServiceUnavailable, "MCP engine not yet running", "", false, false},
		{http.MethodGet, "/contacts/reset-all", http.StatusMethodNotAllowed, "method not allowed", "", false, true},
		{http.MethodPost, "/gc/zz/kill", http.StatusBadRequest, "invalid gcid:", "", false, false},
		{http.MethodPost, "/gc/zz/nope", http.StatusBadRequest, "invalid gcid:", "", false, false},
		{http.MethodPost, "/contacts/kx-reset", http.StatusBadRequest, "decode body: EOF", "", false, false},
	}

	s := &StatusServer{Settings: &brSettingsStore{}}
	mux := s.routes()
	for _, row := range rows {
		t.Run(row.method+" "+row.target, func(t *testing.T) {
			req := httptest.NewRequest(row.method, row.target, nil)
			if row.wantEnc && req.URL.RawPath == "" {
				t.Fatal("fixture lost the encoded slash: RawPath is empty")
			}
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)
			if rr.Code != row.status {
				t.Fatalf("status = %d, want %d (body %q)", rr.Code, row.status, rr.Body.String())
			}
			if row.bodyPrefix != "" && !strings.HasPrefix(strings.TrimSpace(rr.Body.String()), row.bodyPrefix) {
				t.Errorf("body = %q, want prefix %q", rr.Body.String(), row.bodyPrefix)
			}
			if row.location != "" {
				if loc := rr.Result().Header.Get("Location"); loc != row.location {
					t.Errorf("Location = %q, want %q", loc, row.location)
				}
			}
			if row.noAllow {
				if allow := rr.Result().Header.Get("Allow"); allow != "" {
					t.Errorf("Allow = %q, want empty", allow)
				}
			}
		})
	}
}
