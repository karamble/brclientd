// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/decred/slog"
)

// NotifEvent is a small structured event published by brclientd to live
// subscribers (e.g. the dashboard) via the /notifications stream. Mirrors
// the {type, payload} envelope used by BR's own clientrpc streams.
//
// Seq, Epoch and Missed are what let a subscriber tell a quiet stream from a
// lossy one. They are omitempty so a stream that carries no loss looks exactly
// as it did before they existed, and so the per-connection keepalive - which
// never passes through Publish and therefore has no place in the numbering -
// stays byte-identical.
type NotifEvent struct {
	Type      string         `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload"`

	// Seq counts publishes on this bus, from 1, whether or not anybody was
	// listening. A subscriber that receives a and then b with b > a+1 missed
	// exactly b-a-1 events.
	Seq uint64 `json:"seq,omitempty"`

	// Epoch identifies this process's numbering. Without it a restart looks
	// like a jump backwards, which is indistinguishable from a producer bug.
	Epoch string `json:"epoch,omitempty"`

	// Missed is how many events destined for this subscriber were dropped
	// between the previous event it received and this one.
	Missed uint64 `json:"missed,omitempty"`
}

// notifDropLogInterval bounds how often one subscriber's drops are logged. A
// single file transfer can drop hundreds of events, and a line each would bury
// the fact rather than report it. Kept at the keepalive interval so a busy
// stream says something once per heartbeat. A var so tests can shrink it.
var notifDropLogInterval = 30 * time.Second

// subscriber is one consumer of the bus. The channel used to be the whole
// identity; loss accounting needs somewhere to live, and this is it.
type subscriber struct {
	ch chan NotifEvent

	// types is the set this subscriber asked for, or nil for all of them.
	// A filtered-out event is not a drop and must never touch the counters.
	types map[string]struct{}

	// Everything below is guarded by notifBus.mu.

	// missed accumulates until an event is actually handed over, so a run of
	// drops is reported once, in full, by the event that finally lands.
	missed uint64

	// dropped is the lifetime total, for Counters.
	dropped uint64

	lastLog      time.Time
	sinceLog     uint64
	lastDropType string
}

// notifBus is a fan-out pub/sub the runtime uses for events that have no
// upstream BR clientrpc stream (e.g. OnKXSuggested). Subscribers each get
// their own buffered channel; if a subscriber is too slow we drop events for
// that subscriber rather than block publishers, and we say so rather than
// letting the loss pass for silence.
type notifBus struct {
	log slog.Logger

	mu    sync.Mutex
	seq   uint64
	epoch string
	subs  map[*subscriber]struct{}
}

// notifBufferSize is deliberately small and deliberately not the fix for
// dropping. The burst-y publishers - file transfer progress fires per chunk -
// overrun any buffer worth having, so the answer is to report the loss rather
// than to buy a little more room and still lose silently.
const notifBufferSize = 16

func newNotifBus(log slog.Logger) *notifBus {
	b := &notifBus{log: log, subs: make(map[*subscriber]struct{})}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		// An epoch nobody can generate is not a reason to refuse to run. A
		// fixed one makes every consumer treat a restart as continuous
		// numbering, which is worse than a resync and better than no daemon.
		if log != nil {
			log.Warnf("could not seed the notification epoch, restarts will not be visible: %v", err)
		}
		b.epoch = "fixed"
		return b
	}
	b.epoch = hex.EncodeToString(raw[:])
	return b
}

// Subscribe registers a subscriber that receives every event. The returned
// cleanup must be called when the subscriber is done (or disconnects); it
// removes the subscriber and closes the channel.
func (b *notifBus) Subscribe() (<-chan NotifEvent, func()) {
	return b.subscribe(nil)
}

// SubscribeFiltered registers a subscriber that receives only the named types.
//
// Two things follow from filtering, and both matter to anybody reading the
// counters. An event this subscriber did not ask for is not a drop, so it
// touches neither Missed nor the drop log. And the sequence such a subscriber
// sees is sparse by construction, so only an unfiltered subscriber may read a
// gap into it; the number is still worth carrying for correlating logs.
func (b *notifBus) SubscribeFiltered(types ...string) (<-chan NotifEvent, func()) {
	set := make(map[string]struct{}, len(types))
	for _, t := range types {
		set[t] = struct{}{}
	}
	return b.subscribe(set)
}

func (b *notifBus) subscribe(types map[string]struct{}) (<-chan NotifEvent, func()) {
	s := &subscriber{ch: make(chan NotifEvent, notifBufferSize), types: types}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s.ch, func() {
		b.mu.Lock()
		if _, ok := b.subs[s]; ok {
			delete(b.subs, s)
			close(s.ch)
		}
		b.mu.Unlock()
	}
}

// Publish fans the event out to all subscribers; drops the event for any
// subscriber whose buffer is full so a slow consumer cannot block fast ones,
// and records the drop so the loss reaches whoever is reading.
func (b *notifBus) Publish(evt NotifEvent) {
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}

	// Log lines are collected under the lock and emitted after it: slog writes
	// to a file, and the fan-out mutex is held across every subscriber.
	var lines []string

	b.mu.Lock()
	b.seq++
	evt.Seq = b.seq
	evt.Epoch = b.epoch
	now := time.Now()
	for s := range b.subs {
		if s.types != nil {
			if _, ok := s.types[evt.Type]; !ok {
				continue
			}
		}
		// A copy per subscriber, because Missed differs between them. The
		// payload map is shared, which is safe: publishers build one per call
		// and never write to it again.
		e := evt
		e.Missed = s.missed
		select {
		case s.ch <- e:
			s.missed = 0
		default:
			s.missed++
			s.dropped++
			s.sinceLog++
			s.lastDropType = evt.Type
			if now.Sub(s.lastLog) >= notifDropLogInterval {
				lines = append(lines, formatNotifDrop(s, now))
				s.lastLog = now
				s.sinceLog = 0
			}
		}
	}
	b.mu.Unlock()

	if b.log != nil {
		for _, l := range lines {
			b.log.Warnf("%s", l)
		}
	}
}

// formatNotifDrop names the most recent type dropped, which is the difference
// between a line an operator can act on and one they cannot: file transfer
// progress is noise, a gaming frame is a table losing a move.
func formatNotifDrop(s *subscriber, now time.Time) string {
	window := "since the last report"
	if !s.lastLog.IsZero() {
		window = fmt.Sprintf("in the last %s", now.Sub(s.lastLog).Truncate(time.Second))
	}
	return fmt.Sprintf("notification subscriber is not draining: dropped %d event(s) %s "+
		"(%d total); most recent %q", s.sinceLog, window, s.dropped, s.lastDropType)
}

// Counters reports lifetime drops across every live subscriber, and how many
// subscribers there are. Surfaced on /status: a counter nobody reads is a
// counter that never told anybody anything.
func (b *notifBus) Counters() (subscribers int, dropped uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs {
		dropped += s.dropped
	}
	return len(b.subs), dropped
}
