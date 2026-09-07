// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import "github.com/companyzero/bisonrelay/rpc"

// A ceiling that exists because the bytes travel over Bison Relay is taken from
// upstream's own symbol rather than restated here, so a protocol bump follows at
// compile time instead of drifting.
var (
	// brMaxPayloadBytes is the largest payload one BR message carries. It bounds
	// both what a caller may hand this node to publish and what the node serves
	// back out of a message it received.
	brMaxPayloadBytes = int64(rpc.MaxPayloadSizeForVersion(rpc.MaxMsgSizeV1))

	// brRTDTMaxMessageBytes is the largest full RTDT message. The dashboard's
	// end of the audio relay reads the same symbol, so both legs agree.
	brRTDTMaxMessageBytes = int64(rpc.RTDTMaxMessageSize)
)
