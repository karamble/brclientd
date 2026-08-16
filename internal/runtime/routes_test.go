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
	{http.MethodGet, "/status", "GET /status"},
	{http.MethodGet, "/history/pm", "GET /history/pm"},
	{http.MethodPost, "/history/pm/clear", "POST /history/pm/clear"},
	{http.MethodGet, "/msig/history", "GET /msig/history"},
	{http.MethodGet, "/contacts", "GET /contacts"},
	{http.MethodPost, "/contacts/rename", "POST /contacts/rename"},
	{http.MethodGet, "/contacts/groups", "GET /contacts/groups"},
	{http.MethodPost, "/contacts/groups", "POST /contacts/groups"},
	{http.MethodPost, "/contacts/groups/assign", "POST /contacts/groups/assign"},
	{http.MethodPost, "/contacts/groups/settings", "POST /contacts/groups/settings"},
	{http.MethodPost, "/contacts/kx-reset", "POST /contacts/kx-reset"},
	{http.MethodPost, "/contacts/reset-all", "POST /contacts/reset-all"},
	{http.MethodPost, "/contacts/block", "POST /contacts/block"},
	{http.MethodGet, "/contacts/blocked", "GET /contacts/blocked"},
	{http.MethodPost, "/contacts/unblock", "POST /contacts/unblock"},
	{http.MethodPost, "/contacts/ignore", "POST /contacts/ignore"},
	{http.MethodPost, "/contacts/handshake", "POST /contacts/handshake"},
	{http.MethodPost, "/contacts/suggest-kx", "POST /contacts/suggest-kx"},
	{http.MethodPost, "/contacts/trans-reset", "POST /contacts/trans-reset"},
	{http.MethodPost, "/contacts/accept-suggestion", "POST /contacts/accept-suggestion"},
	{http.MethodPost, "/contacts/subscribe-posts", "POST /contacts/subscribe-posts"},
	{http.MethodPost, "/contacts/unsubscribe-posts", "POST /contacts/unsubscribe-posts"},
	{http.MethodPost, "/contacts/list-posts", "POST /contacts/list-posts"},
	{http.MethodPost, "/contacts/list-content", "POST /contacts/list-content"},
	{http.MethodPost, "/contacts/fetch-post", "POST /contacts/fetch-post"},
	{http.MethodGet, "/posts/feed", "GET /posts/feed"},
	{http.MethodGet, "/posts/body", "GET /posts/body"},
	{http.MethodGet, "/posts/embed-data", "GET /posts/embed-data"},
	{http.MethodGet, "/posts/comments", "GET /posts/comments"},
	{http.MethodPost, "/posts/comment", "POST /posts/comment"},
	{http.MethodGet, "/posts/hearts", "GET /posts/hearts"},
	{http.MethodPost, "/posts/heart", "POST /posts/heart"},
	{http.MethodGet, "/posts/receivereceipts", "GET /posts/receivereceipts"},
	{http.MethodGet, "/posts/comment-receivereceipts", "GET /posts/comment-receivereceipts"},
	{http.MethodPost, "/posts/relay", "POST /posts/relay"},
	{http.MethodPost, "/posts/new", "POST /posts/new"},
	{http.MethodGet, "/shared-files", "GET /shared-files"},
	{http.MethodPost, "/shared-files/add", "POST /shared-files/add"},
	{http.MethodPost, "/shared-files/remove", "POST /shared-files/remove"},
	{http.MethodGet, "/downloads", "GET /downloads"},
	{http.MethodPost, "/downloads/cancel", "POST /downloads/cancel"},
	{http.MethodPost, "/downloads/delete", "POST /downloads/delete"},
	{http.MethodPost, "/content/get", "POST /content/get"},
	{http.MethodGet, "/content/file", "GET /content/file"},
	{http.MethodGet, "/rates", "GET /rates"},
	{http.MethodGet, "/store/mode", "GET /store/mode"},
	{http.MethodPost, "/store/mode", "POST /store/mode"},
	{http.MethodGet, "/store/products", "GET /store/products"},
	{http.MethodPost, "/store/products", "POST /store/products"},
	{http.MethodPost, "/store/products/delete", "POST /store/products/delete"},
	{http.MethodGet, "/store/orders", "GET /store/orders"},
	{http.MethodPost, "/store/orders/status", "POST /store/orders/status"},
	{http.MethodPost, "/store/orders/comment", "POST /store/orders/comment"},
	{http.MethodPost, "/store/files/upload", "POST /store/files/upload"},
	{http.MethodGet, "/store/files/list", "GET /store/files/list"},
	{http.MethodGet, "/store/files/get", "GET /store/files/get"},
	{http.MethodPost, "/store/files/delete", "POST /store/files/delete"},
	{http.MethodGet, "/store/templates", "GET /store/templates"},
	{http.MethodGet, "/store/templates/file", "GET /store/templates/file"},
	{http.MethodPost, "/store/templates/save", "POST /store/templates/save"},
	{http.MethodPost, "/store/templates/delete", "POST /store/templates/delete"},
	{http.MethodGet, "/resources/requests", "GET /resources/requests"},
	{http.MethodPost, "/resources/fulfill", "POST /resources/fulfill"},
	{http.MethodPost, "/payments/invoice", "POST /payments/invoice"},
	{http.MethodGet, "/payments/invoice/wait", "GET /payments/invoice/wait"},
	{http.MethodGet, "/payments/invoice/status", "GET /payments/invoice/status"},
	{http.MethodGet, "/notifications", "GET /notifications"},
	{http.MethodGet, "/notifications/recent", "GET /notifications/recent"},
	{http.MethodPost, "/notifications/delete", "POST /notifications/delete"},
	{http.MethodPost, "/notifications/clear", "POST /notifications/clear"},
	{http.MethodGet, "/version", "GET /version"},
	{http.MethodGet, "/public-identity", "GET /public-identity"},
	{http.MethodPost, "/avatar", "POST /avatar"},
	{http.MethodPost, "/messages/send", "POST /messages/send"},
	{http.MethodPost, "/invites/create", "POST /invites/create"},
	{http.MethodPost, "/invites/accept", "POST /invites/accept"},
	{http.MethodPost, "/tip", "POST /tip"},
	{http.MethodGet, "/payments/tips", "GET /payments/tips"},
	{http.MethodGet, "/payments/tips/running", "GET /payments/tips/running"},
	{http.MethodPost, "/invites/redeem-key", "POST /invites/redeem-key"},
	{http.MethodPost, "/files/send", "POST /files/send"},
	{http.MethodGet, "/stats/overview", "GET /stats/overview"},
	{http.MethodGet, "/stats/payments", "GET /stats/payments"},
	{http.MethodPost, "/stats/payments/clear", "POST /stats/payments/clear"},
	{http.MethodGet, "/stats/network", "GET /stats/network"},
	{http.MethodGet, "/stats/contacts", "GET /stats/contacts"},
	{http.MethodGet, "/stats/posts", "GET /stats/posts"},
	{http.MethodGet, "/rtdt/sessions", "GET /rtdt/sessions"},
	{http.MethodPost, "/rtdt/sessions/create", "POST /rtdt/sessions/create"},
	{http.MethodPost, "/rtdt/sessions/create-instant", "POST /rtdt/sessions/create-instant"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/invite", "POST /rtdt/sessions/{rv}/invite"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/accept", "POST /rtdt/sessions/{rv}/accept"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/join", "POST /rtdt/sessions/{rv}/join"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/leave", "POST /rtdt/sessions/{rv}/leave"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/dissolve", "POST /rtdt/sessions/{rv}/dissolve"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/kick", "POST /rtdt/sessions/{rv}/kick"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/remove", "POST /rtdt/sessions/{rv}/remove"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/rotate-cookies", "POST /rtdt/sessions/{rv}/rotate-cookies"},
	{http.MethodGet, "/rtdt/sessions/" + probeID + "/audio", "GET /rtdt/sessions/{rv}/audio"},
	{http.MethodGet, "/rtdt/sessions/" + probeID + "/messages", "GET /rtdt/sessions/{rv}/messages"},
	{http.MethodPost, "/rtdt/sessions/" + probeID + "/chat", "POST /rtdt/sessions/{rv}/chat"},
	{http.MethodGet, "/gc", "GET /gc"},
	{http.MethodPost, "/gc/create", "POST /gc/create"},
	{http.MethodGet, "/gc/invites", "GET /gc/invites"},
	{http.MethodPost, "/gc/invites/accept", "POST /gc/invites/accept"},
	{http.MethodGet, "/gc/" + probeID, "GET /gc/{gcid}"},
	{http.MethodPost, "/gc/" + probeID + "/invite", "POST /gc/{gcid}/invite"},
	{http.MethodPost, "/gc/" + probeID + "/message", "POST /gc/{gcid}/message"},
	{http.MethodGet, "/gc/" + probeID + "/history", "GET /gc/{gcid}/history"},
	{http.MethodPost, "/gc/" + probeID + "/history/clear", "POST /gc/{gcid}/history/clear"},
	{http.MethodPost, "/gc/" + probeID + "/part", "POST /gc/{gcid}/part"},
	{http.MethodPost, "/gc/" + probeID + "/kill", "POST /gc/{gcid}/kill"},
	{http.MethodPost, "/gc/" + probeID + "/kick", "POST /gc/{gcid}/kick"},
	{http.MethodPost, "/gc/" + probeID + "/block", "POST /gc/{gcid}/block"},
	{http.MethodPost, "/gc/" + probeID + "/unblock", "POST /gc/{gcid}/unblock"},
	{http.MethodPost, "/gc/" + probeID + "/admins", "POST /gc/{gcid}/admins"},
	{http.MethodPost, "/gc/" + probeID + "/owner", "POST /gc/{gcid}/owner"},
	{http.MethodPost, "/gc/" + probeID + "/upgrade", "POST /gc/{gcid}/upgrade"},
	{http.MethodPost, "/gc/" + probeID + "/alias", "POST /gc/{gcid}/alias"},
	{http.MethodPost, "/gc/" + probeID + "/resend-list", "POST /gc/{gcid}/resend-list"},
	{http.MethodPost, "/pages/fetch", "POST /pages/fetch"},
	{http.MethodGet, "/pages/local", "GET /pages/local"},
	{http.MethodGet, "/pages/local/file", "GET /pages/local/file"},
	{http.MethodPost, "/pages/local/save", "POST /pages/local/save"},
	{http.MethodPost, "/pages/local/import-embed", "POST /pages/local/import-embed"},
	{http.MethodPost, "/pages/local/delete", "POST /pages/local/delete"},
	{http.MethodGet, "/backup", "GET /backup"},
	{http.MethodGet, "/connection", "GET /connection"},
	{http.MethodPost, "/connection", "POST /connection"},
	{http.MethodGet, "/settings/behavior", "GET /settings/behavior"},
	{http.MethodPost, "/settings/behavior", "POST /settings/behavior"},
	{http.MethodGet, "/settings/mcpclient", "GET /settings/mcpclient"},
	{http.MethodPost, "/settings/mcpclient", "POST /settings/mcpclient"},
	{http.MethodGet, "/mcp/pending", "GET /mcp/pending"},
	{http.MethodPost, "/mcp/pending/resolve", "POST /mcp/pending/resolve"},
	{http.MethodGet, "/mcp/spend", "GET /mcp/spend"},
	{http.MethodGet, "/filters", "GET /filters"},
	{http.MethodPost, "/filters", "POST /filters"},
	{http.MethodPost, "/filters/delete", "POST /filters/delete"},
	{http.MethodPost, "/posts/subscribe-all", "POST /posts/subscribe-all"},
	{http.MethodGet, "/kx/list", "GET /kx/list"},
	{http.MethodGet, "/kx/searches", "GET /kx/searches"},
	{http.MethodGet, "/kx/mediateids", "GET /kx/mediateids"},
	{http.MethodPost, "/kx/mediateids", "POST /kx/mediateids"},
}

// TestRoutePatterns pins the full route table. Constructing the mux in-test
// turns a registration-conflict panic into a named CI failure.
func TestRoutePatterns(t *testing.T) {
	if len(routePatternRows) != 144 {
		t.Fatalf("route pattern rows = %d, want 144", len(routePatternRows))
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
	t.Run("encoded-slash history clear is rejected and destroys nothing", func(t *testing.T) {
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
		s.handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", rr.Code)
		}
		if allow := rr.Result().Header.Get("Allow"); allow != "GET, HEAD" {
			t.Errorf("Allow = %q, want %q", allow, "GET, HEAD")
		}
		if _, err := os.Stat(planted); err != nil {
			t.Fatalf("planted log missing (stat err = %v), want intact", err)
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
		allow          string
	}{
		{http.MethodPost, "/gc/" + probeID + "%2Fkill", http.StatusMethodNotAllowed, "Method Not Allowed", "", true, false, "GET, HEAD"},
		{http.MethodGet, "/gc/" + probeID + "%2Fkill", http.StatusBadRequest, "invalid gcid:", "", true, false, ""},
		{http.MethodPost, "/rtdt/sessions/" + probeID + "%2Fdissolve", http.StatusNotFound, "404 page not found", "", true, false, ""},
		{http.MethodPost, "/gc/" + probeID + "/kill", http.StatusBadRequest, "decode body: EOF", "", false, false, ""},
		{http.MethodPost, "/rtdt/sessions/" + probeID + "/dissolve", http.StatusServiceUnavailable, "BR client not yet running", "", false, false, ""},
		{http.MethodPost, "/gc/" + probeID + "/history/clear", http.StatusServiceUnavailable, "history paths not configured", "", false, false, ""},
		{http.MethodPost, "/gc/" + probeID + "/../../contacts/reset-all", http.StatusBadRequest, "path not canonical", "", false, false, ""},
		{http.MethodGet, "/gc/", http.StatusNotFound, "404 page not found", "", false, false, ""},
		{http.MethodGet, "/rtdt/sessions/", http.StatusNotFound, "404 page not found", "", false, false, ""},
		{http.MethodGet, "/gc/" + probeID + "/", http.StatusNotFound, "404 page not found", "", false, false, ""},
		{http.MethodGet, "/gc/create", http.StatusBadRequest, "invalid gcid:", "", false, false, ""},
		{http.MethodGet, "/gc/create/", http.StatusNotFound, "404 page not found", "", false, false, ""},
		{http.MethodHead, "/gc", http.StatusServiceUnavailable, "BR client not yet running", "", false, false, ""},
		{http.MethodHead, "/backup", http.StatusMethodNotAllowed, "method not allowed", "", false, true, ""},
		{http.MethodDelete, "/connection", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD, POST"},
		{http.MethodPut, "/settings/mcpclient", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD, POST"},
		{http.MethodPut, "/filters", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD, POST"},
		{http.MethodPut, "/contacts/groups", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD, POST"},
		{http.MethodPut, "/store/mode", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD, POST"},
		{http.MethodPut, "/store/products", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD, POST"},
		{http.MethodPut, "/kx/mediateids", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD, POST"},
		{http.MethodPut, "/rtdt/sessions/" + probeID + "/audio", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD"},
		{http.MethodPut, "/mcp/pending", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD"},
		{http.MethodPut, "/mcp/pending/resolve", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "POST"},
		{http.MethodPut, "/mcp/spend", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "GET, HEAD"},
		{http.MethodGet, "/contacts/reset-all", http.StatusMethodNotAllowed, "Method Not Allowed", "", false, false, "POST"},
		{http.MethodPost, "/gc/zz/kill", http.StatusBadRequest, "invalid gcid:", "", false, false, ""},
		{http.MethodPost, "/gc/zz/nope", http.StatusNotFound, "404 page not found", "", false, false, ""},
		{http.MethodPost, "/contacts/kx-reset", http.StatusBadRequest, "decode body: EOF", "", false, false, ""},
	}

	s := &StatusServer{Settings: &brSettingsStore{}}
	h := s.handler()
	for _, row := range rows {
		t.Run(row.method+" "+row.target, func(t *testing.T) {
			req := httptest.NewRequest(row.method, row.target, nil)
			if row.wantEnc && req.URL.RawPath == "" {
				t.Fatal("fixture lost the encoded slash: RawPath is empty")
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
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
			if row.allow != "" {
				if allow := rr.Result().Header.Get("Allow"); allow != row.allow {
					t.Errorf("Allow = %q, want %q", allow, row.allow)
				}
			}
		})
	}

	// Body rows: a request must never act on a body it could not parse, and a
	// zero value must never silently pick the action.
	bodyRows := []struct {
		target, body string
		status       int
		bodyPrefix   string
	}{
		{"/gc/" + probeID + "/part", `{"reason":`, http.StatusBadRequest, "decode body:"},
		{"/gc/" + probeID + "/kill", `{"reason":`, http.StatusBadRequest, "decode body:"},
		{"/gc/" + probeID + "/kill", `{"reason":"done"}`, http.StatusServiceUnavailable, "BR client not yet running"},
		{"/gc/" + probeID + "/resend-list", `{"uid":`, http.StatusBadRequest, "decode body:"},
		{"/gc/" + probeID + "/resend-list", `{}`, http.StatusServiceUnavailable, "BR client not yet running"},
		{"/contacts/reset-all", ``, http.StatusBadRequest, "decode body: EOF"},
		{"/contacts/reset-all", `{"age_days":`, http.StatusBadRequest, "decode body:"},
		{"/contacts/reset-all", `{"age_days":0}`, http.StatusServiceUnavailable, "BR client not yet running"},
		{"/contacts/reset-all", `{"age_days":-1}`, http.StatusBadRequest, "age_days must not be negative"},
	}
	for _, row := range bodyRows {
		t.Run("POST "+row.target+" body "+row.body, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, row.target, strings.NewReader(row.body))
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != row.status {
				t.Fatalf("status = %d, want %d (body %q)", rr.Code, row.status, rr.Body.String())
			}
			if !strings.HasPrefix(strings.TrimSpace(rr.Body.String()), row.bodyPrefix) {
				t.Errorf("body = %q, want prefix %q", rr.Body.String(), row.bodyPrefix)
			}
		})
	}
}

// TestNoURLPathRouting locks out decoded-path routing: no non-test file in
// the package may consult r.URL.Path.
func TestNoURLPathRouting(t *testing.T) {
	re := regexp.MustCompile(`\br\.URL\.Path\b`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if n := len(re.FindAll(data, -1)); n > 0 {
			t.Errorf("%s: %d r.URL.Path occurrence(s); route on mux patterns and r.PathValue instead", name, n)
		}
	}
}
