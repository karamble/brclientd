// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"sync"
	"time"
)

// NotifEvent is a small structured event published by brclientd to live
// subscribers (e.g. the dashboard) via the /notifications stream. Mirrors
// the {type, payload} envelope used by BR's own clientrpc streams.
type NotifEvent struct {
	Type      string         `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

// notifBus is a fan-out pub/sub the runtime uses for events that have no
// upstream BR clientrpc stream (e.g. OnKXSuggested). Subscribers each get
// their own buffered channel; if a subscriber is too slow we drop events
// for that subscriber rather than block publishers.
type notifBus struct {
	mu   sync.Mutex
	subs map[chan NotifEvent]struct{}
}

func newNotifBus() *notifBus {
	return &notifBus{subs: make(map[chan NotifEvent]struct{})}
}

// Subscribe registers a new subscriber with a buffered channel. The returned
// cleanup must be called when the subscriber is done (or disconnects); it
// removes the subscriber and closes the channel.
func (b *notifBus) Subscribe() (<-chan NotifEvent, func()) {
	ch := make(chan NotifEvent, 16)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
		b.mu.Unlock()
	}
}

// Publish fans the event out to all subscribers; drops the event for any
// subscriber whose buffer is full so a slow consumer cannot block fast ones.
func (b *notifBus) Publish(evt NotifEvent) {
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- evt:
		default:
		}
	}
}
