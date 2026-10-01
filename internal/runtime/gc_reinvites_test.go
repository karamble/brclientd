// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testReinviteGCID = "dd8306131ec7e67920443c706e0f704f135a07ebf71f8ec625365a15514c8c11"

func dismissReinvite(s *StatusServer, method, body string) int {
	r := httptest.NewRequest(method, "/gc/invites/blocked/dismiss", strings.NewReader(body))
	w := httptest.NewRecorder()
	s.handleGC(w, r)
	return w.Code
}

// A dismissed blocked re-invite stays gone until the next one arrives, instead
// of being listed again on every page load.
func TestDismissForgetsABlockedReinviteUntilTheNextOne(t *testing.T) {
	s := &StatusServer{Reinvites: newGCReinviteTracker()}
	s.Reinvites.Record(testReinviteGCID, "Decred Pulse", "bot", "aibot")
	s.Reinvites.Record(testReinviteGCID, "Decred Pulse", "bot", "aibot")

	for _, tc := range []struct{ method, body string }{
		{http.MethodGet, `{"gcid":"` + testReinviteGCID + `"}`},
		{http.MethodPost, `{"gcid":"not-a-gcid"}`},
		{http.MethodPost, `not json`},
	} {
		if code := dismissReinvite(s, tc.method, tc.body); code == http.StatusNoContent {
			t.Fatalf("%s %s was accepted", tc.method, tc.body)
		}
		if len(s.Reinvites.List()) != 1 {
			t.Fatalf("%s %s dropped the entry", tc.method, tc.body)
		}
	}

	if code := dismissReinvite(s, http.MethodPost, `{"gcid":"`+testReinviteGCID+`"}`); code != http.StatusNoContent {
		t.Fatalf("dismiss answered %d", code)
	}
	if got := s.Reinvites.List(); len(got) != 0 {
		t.Fatalf("still listed after dismiss: %+v", got)
	}
	if code := dismissReinvite(s, http.MethodPost, `{"gcid":"`+testReinviteGCID+`"}`); code != http.StatusNoContent {
		t.Fatalf("dismissing an unknown entry answered %d", code)
	}
	if e := s.Reinvites.Record(testReinviteGCID, "Decred Pulse", "bot", "aibot"); e.Count != 1 {
		t.Fatalf("next blocked attempt counted %d, want a fresh entry", e.Count)
	}
}
