// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/companyzero/bisonrelay/rpc"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/gorilla/websocket"
)

// Wire frame format for /rtdt/sessions/{rv}/audio:
//
//   +---------+-----------+--------------+-----------------+
//   | ver (1) | dir (1)   | peerID (4)BE | opus payload    |
//   +---------+-----------+--------------+-----------------+
//
// ver  = 0x01
// dir  = 0x01 inbound (server -> browser; peerID is the publisher)
//      = 0x02 outbound (browser -> server; peerID ignored, brclientd is
//                       always the publisher because the BR identity is
//                       single-tenant per daemon)
const (
	rtdtFrameVersion    byte = 0x01
	rtdtFrameDirInbound byte = 0x01
	rtdtFrameDirOutbnd  byte = 0x02
	rtdtFrameHeaderLen       = 6 // ver + dir + peerID(BE32)
)

// rtdtAudioUpgrader is the websocket upgrader used for the audio bridge.
// Origin is permissive because the dashboard always proxies through its
// own server; brclientd is only reachable over mTLS from a small set of
// in-cluster hosts.
var rtdtAudioUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// wsAudioSink adapts a *websocket.Conn into the AudioSink interface so
// the router can deliver inbound Opus frames to the browser. Writes are
// serialised because gorilla/websocket forbids concurrent WriteMessage
// calls on the same conn.
type wsAudioSink struct {
	conn   *websocket.Conn
	writeM sync.Mutex
	// once closed, OnSpeech becomes a no-op so a slow goroutine can't
	// write to a torn-down conn.
	closed chan struct{}
}

func newWSAudioSink(conn *websocket.Conn) *wsAudioSink {
	return &wsAudioSink{conn: conn, closed: make(chan struct{})}
}

func (s *wsAudioSink) close() {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
}

func (s *wsAudioSink) OnSpeech(peerID rpc.RTDTPeerID, opus []byte, timestamp uint32) {
	select {
	case <-s.closed:
		return
	default:
	}
	buf := make([]byte, rtdtFrameHeaderLen+len(opus))
	buf[0] = rtdtFrameVersion
	buf[1] = rtdtFrameDirInbound
	binary.BigEndian.PutUint32(buf[2:6], uint32(peerID))
	copy(buf[rtdtFrameHeaderLen:], opus)

	s.writeM.Lock()
	defer s.writeM.Unlock()
	// Best-effort write with a hard deadline; if the browser falls
	// behind, drop the frame rather than block the audio router or
	// build up backpressure on the BR socket.
	_ = s.conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	if err := s.conn.WriteMessage(websocket.BinaryMessage, buf); err != nil {
		s.close()
	}
}

// handleRTDTAudioWS upgrades to a binary WebSocket for one specific RTDT
// session. Both directions multiplex on the same conn using the frame
// format above. Cleanup teardowns:
//  - Register/Unregister the WS as the AudioRouter sink (only one per rv;
//    second tab gets 409)
//  - Outbound pump runs in a goroutine, reads WS frames, calls
//    SendSpeechPacket on the live BR session
//  - On WS close or context done, both pumps exit and the sink unregisters
func (s *StatusServer) handleRTDTAudioWS(w http.ResponseWriter, r *http.Request, rv zkidentity.ShortID) {
	if s.Log != nil {
		s.Log.Infof("RTDT audio: upgrade request rv=%s upgrade-hdr=%q", rv.ShortLogID(), r.Header.Get("Upgrade"))
	}
	c := s.currentClient()
	if c == nil {
		if s.Log != nil {
			s.Log.Warnf("RTDT audio: BR client not yet running rv=%s", rv.ShortLogID())
		}
		http.Error(w, "BR client not yet running", http.StatusServiceUnavailable)
		return
	}
	if s.AudioRouter == nil {
		if s.Log != nil {
			s.Log.Warnf("RTDT audio: audio router not configured rv=%s", rv.ShortLogID())
		}
		http.Error(w, "audio router not configured", http.StatusInternalServerError)
		return
	}
	liveSess := c.GetLiveRTSession(&rv)
	if liveSess == nil || liveSess.RTSess == nil {
		if s.Log != nil {
			s.Log.Warnf("RTDT audio: session not live rv=%s liveSessNil=%v", rv.ShortLogID(), liveSess == nil)
		}
		http.Error(w, "session not live; call /join first", http.StatusConflict)
		return
	}

	// Refuse a second simultaneous tab on the same session. The router's
	// Lookup is best-effort (we re-check under Register's lock below).
	if s.AudioRouter.Lookup(rv) != nil {
		if s.Log != nil {
			s.Log.Warnf("RTDT audio: already attached rv=%s", rv.ShortLogID())
		}
		http.Error(w, "audio session already attached in another tab", http.StatusConflict)
		return
	}

	conn, err := rtdtAudioUpgrader.Upgrade(w, r, nil)
	if err != nil {
		if s.Log != nil {
			s.Log.Warnf("RTDT audio: upgrade failed rv=%s err=%v", rv.ShortLogID(), err)
		}
		// Upgrader already wrote the error response.
		return
	}
	if s.Log != nil {
		s.Log.Infof("RTDT audio: WS upgraded + sink registered rv=%s", rv.ShortLogID())
	}

	sink := newWSAudioSink(conn)
	prev, cleanupSink := s.AudioRouter.Register(rv, sink)
	if prev != nil {
		// Race lost: another tab attached between the Lookup above and
		// Register. Put the previous sink back and tell this caller to
		// retry.
		cleanupSink()
		_, cleanupSink = s.AudioRouter.Register(rv, prev)
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "already attached"),
			time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}

	// Outbound state: a frame-counted timestamp (mirroring bruig's
	// internal/audio/streams.go:201). Starts at 0, advances by 20 per
	// SendSpeechPacket call.
	var outTS uint32
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Heartbeat goroutine: ping every 30s, close on first failure.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sink.writeM.Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				err := conn.WriteMessage(websocket.PingMessage, nil)
				sink.writeM.Unlock()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	rtSess := liveSess.RTSess
	for {
		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			// Normal close or browser refresh both land here.
			break
		}
		if msgType != websocket.BinaryMessage {
			continue
		}
		if len(payload) < rtdtFrameHeaderLen || payload[0] != rtdtFrameVersion {
			continue
		}
		if payload[1] != rtdtFrameDirOutbnd {
			// Browsers should only send outbound frames; ignore stray
			// inbound replays rather than disconnecting.
			continue
		}
		opus := payload[rtdtFrameHeaderLen:]
		if len(opus) == 0 {
			continue
		}
		if err := rtSess.SendSpeechPacket(ctx, opus, outTS); err != nil {
			// SendSpeechPacket fails on session teardown or wallet
			// allowance exhaustion. Log via the router so we have a
			// single observability surface for audio issues.
			if s.Log != nil && !errors.Is(err, context.Canceled) {
				s.Log.Warnf("SendSpeechPacket session=%s err=%v", rv.ShortLogID(), err)
			}
			break
		}
		outTS += 20
	}

	cancel()
	sink.close()
	cleanupSink()
	_ = conn.Close()
}
