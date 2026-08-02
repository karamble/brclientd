// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package msig

import "testing"

func TestIsEnvelope(t *testing.T) {
	envelopes := []string{
		SampleEnvelope,
		"--msig[v=1,mid=aa,exp=1]--",
		"--msig[v=9,future=key]--QQ==",
		"--msig[v=1]--aGVs\nbG8=",
		// Coarse classifier semantics shared with brmcp: trailing words
		// are base64-alphabet characters plus whitespace, so this still
		// classifies as protocol traffic; the dashboard's strict decode
		// rejects it as a frame.
		"--msig[v=1]--QQ== trailing",
	}
	for _, s := range envelopes {
		if !IsEnvelope(s) {
			t.Errorf("expected envelope: %q", s)
		}
	}

	notEnvelopes := []string{
		"",
		"hello",
		"--mcp[v=1,sid=aa,mid=bb,seq=1/1,exp=1]--QQ==",
		"look at --msig[v=1]--QQ== inside chat",
		"--msig[v=1]--abc.def",
		"--msig[v=1]--abc!!",
		"--msig[v=1]--abc--msig[v=2]--def",
		"--msig[v=1]",
	}
	for _, s := range notEnvelopes {
		if IsEnvelope(s) {
			t.Errorf("unexpected envelope: %q", s)
		}
	}
}
