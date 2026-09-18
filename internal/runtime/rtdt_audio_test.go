// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"testing"

	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
)

type recordingSink struct {
	calls []struct {
		peer rpc.RTDTPeerID
		opus []byte
		ts   uint32
	}
}

func (r *recordingSink) OnSpeech(peerID rpc.RTDTPeerID, opus []byte, timestamp uint32) {
	r.calls = append(r.calls, struct {
		peer rpc.RTDTPeerID
		opus []byte
		ts   uint32
	}{peerID, opus, timestamp})
}

// Inbound call audio rides the Random stream, so a frame has to reach the
// browser's sink for its own session carrying the peer, payload and timestamp
// it arrived with.
func TestDispatchCallAudioReachesTheSessionsSink(t *testing.T) {
	router := NewRTDTAudioRouter(nil)
	var rv zkidentity.ShortID
	rv[0], rv[31] = 0xab, 0xcd
	sink := &recordingSink{}
	if prev, _ := router.Register(rv, sink); prev != nil {
		t.Fatalf("a fresh router already had a sink registered")
	}

	dispatchCallAudio(router, &rv, 0x5e5ed645, []byte{0x41, 0x42, 0x43}, 340)

	if len(sink.calls) != 1 {
		t.Fatalf("sink saw %d frames, want exactly 1", len(sink.calls))
	}
	got := sink.calls[0]
	if got.peer != 0x5e5ed645 {
		t.Errorf("peer = %#x, want 0x5e5ed645", got.peer)
	}
	if string(got.opus) != "ABC" {
		t.Errorf("opus = %q, want the bytes that arrived", got.opus)
	}
	if got.ts != 340 {
		t.Errorf("timestamp = %d, want 340; the jitter buffer orders on it", got.ts)
	}
}

// A session with no RV has nowhere to route to. Dispatching it against a zero
// key would deliver one session's audio to whichever session hashed to zero.
func TestDispatchCallAudioIgnoresASessionWithNoRV(t *testing.T) {
	router := NewRTDTAudioRouter(nil)
	var zero zkidentity.ShortID
	sink := &recordingSink{}
	router.Register(zero, sink)

	dispatchCallAudio(router, nil, 0x11111111, []byte{0x01}, 20)

	if len(sink.calls) != 0 {
		t.Errorf("an unroutable frame was delivered to the zero-RV session: %+v", sink.calls)
	}
}
