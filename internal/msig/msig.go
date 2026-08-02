// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package msig recognizes dcrpulse shared-wallet coordination envelopes
// (--msig[...]--) in private messages. brclientd is only a relay for this
// traffic: it keeps the frames out of chat and serves them for replay;
// framing and the protocol itself live in the dcrpulse dashboard.
package msig

import "regexp"

// envelopeRE matches a whole PM body carrying one msig frame. The payload
// alphabet is restricted base64 so a chat message can never smuggle a
// second frame, mirroring the brmcp wire grammar. Deliberately version
// agnostic: every --msig[ frame is protocol traffic no matter its v, so
// unknown future versions also stay out of chat.
var envelopeRE = regexp.MustCompile(`^--msig\[[^\]]*\]--[A-Za-z0-9+/=\s]*$`)

// SampleEnvelope is a canonical frame used to validate that user content
// filters can never match msig traffic.
const SampleEnvelope = "--msig[v=1,mid=0011223344556677,exp=1700000000]--aGVsbG8="

// IsEnvelope reports whether a PM body is msig coordination traffic.
func IsEnvelope(text string) bool { return envelopeRE.MatchString(text) }
