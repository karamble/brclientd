// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package setup

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func setupMux() *http.ServeMux {
	s := &Server{}
	done := make(chan struct{}, 1)
	restored := make(chan struct{}, 1)
	return s.routes(done, restored)
}

// TestSetupRoutePatterns pins the setup route table; in-test construction
// turns a registration-conflict panic into a named CI failure.
func TestSetupRoutePatterns(t *testing.T) {
	// Rows mirror the caller contract pinned in dcrpulse dashboard/internal/rpc/brclientd_url_test.go:92-275.
	rows := []struct {
		method, path, want string
	}{
		{http.MethodPost, "/create-identity", "POST /create-identity"},
		{http.MethodPost, "/restore-backup", "POST /restore-backup"},
	}
	mux := setupMux()
	for _, row := range rows {
		req := httptest.NewRequest(row.method, row.path, nil)
		_, pattern := mux.Handler(req)
		if pattern != row.want {
			t.Errorf("%s %s: matched pattern %q, want %q", row.method, row.path, pattern, row.want)
		}
	}
}

// TestSetupMethodContract: POST reaches both handlers; PUT and DELETE answer
// 405; GET /create-identity answers 405 with Allow: POST.
func TestSetupMethodContract(t *testing.T) {
	mux := setupMux()
	probe := func(method, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		return rr
	}

	for _, path := range []string{"/create-identity", "/restore-backup"} {
		if rr := probe(http.MethodPost, path); rr.Code == http.StatusMethodNotAllowed {
			t.Errorf("POST %s: got 405 on the allowed verb", path)
		}
		for _, m := range []string{http.MethodPut, http.MethodDelete} {
			if rr := probe(m, path); rr.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: status = %d, want 405", m, path, rr.Code)
			}
		}
	}

	rr := probe(http.MethodGet, "/create-identity")
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /create-identity: status = %d, want 405", rr.Code)
	}
	if allow := rr.Result().Header.Get("Allow"); allow != "POST" {
		t.Errorf("GET /create-identity: Allow = %q, want %q", allow, "POST")
	}
}
