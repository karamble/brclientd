// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"sync"
	"sync/atomic"

	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/slog"
)

// AudioSink receives decrypted Opus frames addressed to a live RTDT
// session. The bridge to the browser registers an AudioSink per session
// when the /rtdt/sessions/{rv}/audio WebSocket is upgraded; until then
// audio frames are dropped silently and only counted.
type AudioSink interface {
	OnSpeech(peerID rpc.RTDTPeerID, opus []byte, timestamp uint32)
}

// RTDTAudioRouter is the in-process broker between BR's audio callback
// (one process-wide handler set via Config.RTDTAudioStreamHandler) and
// the per-session WebSocket sinks the dashboard's WS proxy upgrades into.
// Routing key is the session RV. A session with no sink simply drops
// frames; that is the steady state for sessions we are joined to but
// have not opened a browser audio tab for.
type RTDTAudioRouter struct {
	log slog.Logger

	mu    sync.RWMutex
	sinks map[zkidentity.ShortID]AudioSink

	// framesSeen / framesDropped are diagnostic counters surfaced via
	// /status (Phase 1 validation) so we can verify the callback is
	// actually being invoked when a real session is up.
	framesSeen    atomic.Uint64
	framesDropped atomic.Uint64

	// loggedFirstFrame tracks (sessRV, peerID) pairs for which we've
	// already emitted the one-shot "first frame" log line. Used to
	// confirm the BR audio hook is wired without spamming logs at
	// 50 frames/sec/peer.
	loggedMu          sync.Mutex
	loggedFirstFrames map[string]struct{}
}

// NewRTDTAudioRouter returns an empty router. Construct once at runtime
// startup and share between the BR client and the status server. The log
// param is used only for the one-shot first-frame validation line.
func NewRTDTAudioRouter(log slog.Logger) *RTDTAudioRouter {
	return &RTDTAudioRouter{
		log:               log,
		sinks:             make(map[zkidentity.ShortID]AudioSink),
		loggedFirstFrames: make(map[string]struct{}),
	}
}

// Register attaches a sink to a session. Returns a cleanup that removes
// the sink and is safe to call multiple times. Replacing an existing sink
// returns the previous one so callers can decide whether to abort.
func (r *RTDTAudioRouter) Register(sessRV zkidentity.ShortID, sink AudioSink) (prev AudioSink, cleanup func()) {
	r.mu.Lock()
	prev = r.sinks[sessRV]
	r.sinks[sessRV] = sink
	r.mu.Unlock()
	cleanup = func() {
		r.mu.Lock()
		if cur, ok := r.sinks[sessRV]; ok && cur == sink {
			delete(r.sinks, sessRV)
		}
		r.mu.Unlock()
	}
	return prev, cleanup
}

// Lookup returns the currently registered sink (or nil) for the session.
// Mostly used by the WS endpoint to reject a second-tab join with 409.
func (r *RTDTAudioRouter) Lookup(sessRV zkidentity.ShortID) AudioSink {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sinks[sessRV]
}

// Dispatch is called by the BR-level audio stream handler for every
// inbound Opus frame. peerID is rpc.RTDTFramedPacket.Source (the publisher
// peer ID). When no sink is registered the frame is counted as dropped
// rather than queued anywhere; this matches Phase 1 semantics where we
// haven't built the WS pipeline yet.
func (r *RTDTAudioRouter) Dispatch(sessRV zkidentity.ShortID, peerID rpc.RTDTPeerID, opus []byte, timestamp uint32) {
	r.framesSeen.Add(1)
	r.logFirstFrame(sessRV, peerID, len(opus))
	r.mu.RLock()
	sink := r.sinks[sessRV]
	r.mu.RUnlock()
	if sink == nil {
		r.framesDropped.Add(1)
		return
	}
	sink.OnSpeech(peerID, opus, timestamp)
}

// logFirstFrame emits an Info log the first time we see audio for a given
// (session, peer) pair. Used for Phase 2 validation that the BR fork's
// RTDTAudioStreamHandler hook is actually firing in production.
func (r *RTDTAudioRouter) logFirstFrame(sessRV zkidentity.ShortID, peerID rpc.RTDTPeerID, n int) {
	if r.log == nil {
		return
	}
	key := sessRV.String() + ":" + peerIDKey(peerID)
	r.loggedMu.Lock()
	if _, ok := r.loggedFirstFrames[key]; ok {
		r.loggedMu.Unlock()
		return
	}
	r.loggedFirstFrames[key] = struct{}{}
	r.loggedMu.Unlock()
	r.log.Infof("First RTDT speech packet: session=%s peer=%d size=%dB",
		sessRV.ShortLogID(), peerID, n)
}

func peerIDKey(p rpc.RTDTPeerID) string {
	// PeerID is uint32; use a stable string form for map keys.
	var b [16]byte
	n := 0
	v := uint32(p)
	if v == 0 {
		b[0] = '0'
		n = 1
	}
	for v > 0 {
		b[n] = byte('0' + v%10)
		v /= 10
		n++
	}
	for i := 0; i < n/2; i++ {
		b[i], b[n-1-i] = b[n-1-i], b[i]
	}
	return string(b[:n])
}

// Counters returns (seen, dropped) snapshots for diagnostic exposure.
func (r *RTDTAudioRouter) Counters() (seen, dropped uint64) {
	return r.framesSeen.Load(), r.framesDropped.Load()
}
