// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMethodContract sweeps every concrete route: allowed verbs must not answer
// 405, PUT/DELETE must; sweep=false rows are pinned in TestRouteBehavior instead.
func TestMethodContract(t *testing.T) {
	type row struct {
		path    string
		methods []string
		sweep   bool
	}
	get := func(p string) row { return row{p, []string{http.MethodGet}, true} }
	post := func(p string) row { return row{p, []string{http.MethodPost}, true} }

	// Rows mirror the caller contract pinned in dcrpulse dashboard/internal/rpc/brclientd_url_test.go:92-275.
	rows := []row{
		get("/status"),
		get("/history/pm"),
		post("/history/pm/clear"),
		get("/msig/history"),
		get("/contacts"),
		post("/contacts/rename"),
		post("/contacts/groups/assign"),
		post("/contacts/groups/settings"),
		post("/contacts/kx-reset"),
		post("/contacts/reset-all"),
		post("/contacts/block"),
		get("/contacts/blocked"),
		post("/contacts/unblock"),
		post("/contacts/ignore"),
		post("/contacts/handshake"),
		post("/contacts/suggest-kx"),
		post("/contacts/trans-reset"),
		post("/contacts/accept-suggestion"),
		post("/contacts/subscribe-posts"),
		post("/contacts/unsubscribe-posts"),
		post("/contacts/list-posts"),
		post("/contacts/list-content"),
		post("/contacts/fetch-post"),
		get("/posts/feed"),
		get("/posts/body"),
		get("/posts/embed-data"),
		get("/posts/comments"),
		post("/posts/comment"),
		get("/posts/hearts"),
		post("/posts/heart"),
		get("/posts/receivereceipts"),
		get("/posts/comment-receivereceipts"),
		post("/posts/relay"),
		post("/posts/new"),
		get("/shared-files"),
		post("/shared-files/add"),
		post("/shared-files/remove"),
		get("/downloads"),
		post("/downloads/cancel"),
		post("/downloads/delete"),
		post("/content/get"),
		get("/content/file"),
		get("/rates"),
		post("/store/products/delete"),
		get("/store/orders"),
		post("/store/orders/status"),
		post("/store/orders/comment"),
		post("/store/files/upload"),
		get("/store/files/list"),
		get("/store/files/get"),
		post("/store/files/delete"),
		get("/store/templates"),
		get("/store/templates/file"),
		post("/store/templates/save"),
		post("/store/templates/delete"),
		get("/resources/requests"),
		post("/resources/fulfill"),
		post("/payments/invoice"),
		get("/payments/invoice/wait"),
		get("/payments/invoice/status"),
		get("/notifications"),
		get("/notifications/recent"),
		post("/notifications/delete"),
		post("/notifications/clear"),
		get("/version"),
		get("/public-identity"),
		post("/avatar"),
		post("/messages/send"),
		post("/invites/create"),
		post("/invites/accept"),
		post("/tip"),
		get("/payments/tips"),
		get("/payments/tips/running"),
		post("/invites/redeem-key"),
		post("/files/send"),
		get("/stats/overview"),
		get("/stats/payments"),
		post("/stats/payments/clear"),
		get("/stats/network"),
		get("/stats/contacts"),
		get("/stats/posts"),
		post("/pages/fetch"),
		get("/pages/local"),
		get("/pages/local/file"),
		post("/pages/local/save"),
		post("/pages/local/import-embed"),
		post("/pages/local/delete"),
		get("/backup"),
		{"/mcp/pending", []string{http.MethodGet}, false},
		{"/mcp/pending/resolve", []string{http.MethodPost}, false},
		{"/mcp/spend", []string{http.MethodGet}, false},
		post("/filters/delete"),
		post("/posts/subscribe-all"),
		get("/kx/list"),
		get("/kx/searches"),

		{"/connection", []string{http.MethodGet, http.MethodPost}, false},
		{"/settings/behavior", []string{http.MethodGet, http.MethodPost}, true},
		{"/settings/mcpclient", []string{http.MethodGet, http.MethodPost}, false},
		{"/filters", []string{http.MethodGet, http.MethodPost}, false},
		{"/contacts/groups", []string{http.MethodGet, http.MethodPost}, false},
		{"/store/mode", []string{http.MethodGet, http.MethodPost}, false},
		{"/store/products", []string{http.MethodGet, http.MethodPost}, false},
		{"/kx/mediateids", []string{http.MethodGet, http.MethodPost}, false},

		get("/gc"),
		post("/gc/create"),
		get("/gc/invites"),
		post("/gc/invites/accept"),
		get("/gc/" + probeID),
		post("/gc/" + probeID + "/invite"),
		post("/gc/" + probeID + "/message"),
		get("/gc/" + probeID + "/history"),
		post("/gc/" + probeID + "/history/clear"),
		post("/gc/" + probeID + "/part"),
		post("/gc/" + probeID + "/kill"),
		post("/gc/" + probeID + "/kick"),
		post("/gc/" + probeID + "/block"),
		post("/gc/" + probeID + "/unblock"),
		post("/gc/" + probeID + "/admins"),
		post("/gc/" + probeID + "/owner"),
		post("/gc/" + probeID + "/upgrade"),
		post("/gc/" + probeID + "/alias"),
		post("/gc/" + probeID + "/resend-list"),
		get("/rtdt/sessions"),
		post("/rtdt/sessions/create"),
		post("/rtdt/sessions/create-instant"),
		post("/rtdt/sessions/" + probeID + "/invite"),
		post("/rtdt/sessions/" + probeID + "/accept"),
		post("/rtdt/sessions/" + probeID + "/join"),
		post("/rtdt/sessions/" + probeID + "/leave"),
		post("/rtdt/sessions/" + probeID + "/dissolve"),
		post("/rtdt/sessions/" + probeID + "/kick"),
		post("/rtdt/sessions/" + probeID + "/remove"),
		post("/rtdt/sessions/" + probeID + "/rotate-cookies"),
		{"/rtdt/sessions/" + probeID + "/audio", []string{http.MethodGet}, false},
		get("/rtdt/sessions/" + probeID + "/messages"),
		post("/rtdt/sessions/" + probeID + "/chat"),
	}
	if len(rows) != 136 {
		t.Fatalf("method contract rows = %d, want 136", len(rows))
	}

	s := &StatusServer{Settings: &brSettingsStore{}}
	mux := s.routes()
	// ServeHTTP runs under recover: a panic counts as handler-reached, since
	// the assertion is about routing, not handler health.
	probe := func(method, path string) (code int, panicked bool) {
		req := httptest.NewRequest(method, path, nil)
		rr := httptest.NewRecorder()
		func() {
			defer func() {
				if recover() != nil {
					panicked = true
				}
			}()
			mux.ServeHTTP(rr, req)
		}()
		return rr.Code, panicked
	}

	for _, row := range rows {
		for _, m := range row.methods {
			code, panicked := probe(m, row.path)
			if !panicked && code == http.StatusMethodNotAllowed {
				t.Errorf("%s %s: got 405 on the allowed verb", m, row.path)
			}
		}
		if !row.sweep {
			continue
		}
		for _, m := range []string{http.MethodPut, http.MethodDelete} {
			code, panicked := probe(m, row.path)
			if panicked {
				t.Errorf("%s %s: panicked, want 405", m, row.path)
				continue
			}
			if code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: status = %d, want 405", m, row.path, code)
			}
		}
	}
}
