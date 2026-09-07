// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"testing"

	"github.com/companyzero/bisonrelay/rpc"
)

// Every BR-derived ceiling is checked twice: against upstream's symbol, so an
// upstream bump carries here at compile time, and against a literal written out
// by hand, so a redefinition upstream cannot pass unnoticed.

func TestBRLimitsMatchUpstream(t *testing.T) {
	if want := int64(rpc.MaxPayloadSizeForVersion(rpc.MaxMsgSizeV1)); brMaxPayloadBytes != want {
		t.Errorf("payload max = %d, want upstream V1 %d", brMaxPayloadBytes, want)
	}
	if brMaxPayloadBytes != 10*1024*1024 {
		t.Errorf("payload max = %d, want 10 MiB", brMaxPayloadBytes)
	}
	if want := int64(rpc.RTDTMaxMessageSize); brRTDTMaxMessageBytes != want {
		t.Errorf("RTDT bound = %d, want upstream %d", brRTDTMaxMessageBytes, want)
	}
	if brRTDTMaxMessageBytes != 65535 {
		t.Errorf("RTDT bound = %d, want 65535", brRTDTMaxMessageBytes)
	}
}

// An imported embed is inlined into a page reply, and page replies are not
// chunked, so the whole reply has to fit one message. That is why this ceiling
// is deliberately tighter than the payload maximum rather than equal to it.
func TestImportEmbedStaysUnderAPageReply(t *testing.T) {
	if limit := int64(rpc.MaxPayloadSizeForVersion(rpc.MaxMsgSizeV0)); maxImportEmbedBytes >= limit {
		t.Errorf("import embed cap %d does not leave room under a %d page reply", maxImportEmbedBytes, limit)
	}
}
