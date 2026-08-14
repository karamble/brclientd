// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/decred/slog"
)

// drain reads everything currently buffered for a subscriber.
func drain(ch <-chan NotifEvent) []NotifEvent {
	var got []NotifEvent
	for {
		select {
		case e := <-ch:
			got = append(got, e)
		default:
			return got
		}
	}
}

func TestEverySubscriberSeesTheSameNumbering(t *testing.T) {
	b := newNotifBus(nil)
	a, closeA := b.Subscribe()
	defer closeA()
	c, closeC := b.Subscribe()
	defer closeC()

	b.Publish(NotifEvent{Type: "pm"})
	b.Publish(NotifEvent{Type: "gc-message"})

	for name, ch := range map[string]<-chan NotifEvent{"a": a, "c": c} {
		got := drain(ch)
		if len(got) != 2 {
			t.Fatalf("%s received %d events", name, len(got))
		}
		if got[0].Seq != 1 || got[1].Seq != 2 {
			t.Fatalf("%s saw seq %d then %d, want 1 then 2", name, got[0].Seq, got[1].Seq)
		}
		if got[0].Epoch == "" || got[0].Epoch != got[1].Epoch {
			t.Fatalf("%s saw epochs %q and %q", name, got[0].Epoch, got[1].Epoch)
		}
	}
}

func TestTheSequenceCountsPublishesNobodyReceived(t *testing.T) {
	b := newNotifBus(nil)
	ch, done := b.Subscribe()
	defer done()

	b.Publish(NotifEvent{Type: "pm"})
	<-ch // take it, so the buffer is empty and the next ones are not drops

	// Fill and overrun.
	for i := 0; i < notifBufferSize+5; i++ {
		b.Publish(NotifEvent{Type: "gc-message"})
	}
	got := drain(ch)
	if len(got) != notifBufferSize {
		t.Fatalf("buffered %d events, want %d", len(got), notifBufferSize)
	}
	// The hole is the signal: the last buffered event is seq 17, and the five
	// that did not fit are numbered 18 to 22 and simply never arrive.
	last := got[len(got)-1]
	if last.Seq != uint64(notifBufferSize)+1 {
		t.Fatalf("last delivered seq %d, want %d", last.Seq, notifBufferSize+1)
	}
}

// TestMissedSurvivesAConsecutiveDrop is the one that matters. A naive
// implementation attaches the count and clears it, which loses everything
// after the first drop in a run.
func TestMissedSurvivesAConsecutiveDrop(t *testing.T) {
	b := newNotifBus(nil)
	ch, done := b.Subscribe()
	defer done()

	// Fill the buffer exactly.
	for i := 0; i < notifBufferSize; i++ {
		b.Publish(NotifEvent{Type: "filler"})
	}
	// Now drop a run of them.
	const dropped = 40
	for i := 0; i < dropped; i++ {
		b.Publish(NotifEvent{Type: "gc-message"})
	}
	// Make room for exactly one, then publish one that will land.
	<-ch
	b.Publish(NotifEvent{Type: "pm"})

	got := drain(ch)
	last := got[len(got)-1]
	if last.Type != "pm" {
		t.Fatalf("last event is %q, want the one that landed after the drops", last.Type)
	}
	if last.Missed != dropped {
		t.Fatalf("Missed is %d, want %d: the count must accumulate across a run of "+
			"drops and clear only on a successful handoff", last.Missed, dropped)
	}
}

func TestMissedClearsAfterItIsReported(t *testing.T) {
	b := newNotifBus(nil)
	ch, done := b.Subscribe()
	defer done()

	for i := 0; i < notifBufferSize; i++ {
		b.Publish(NotifEvent{Type: "filler"})
	}
	b.Publish(NotifEvent{Type: "dropped"})
	<-ch
	b.Publish(NotifEvent{Type: "carries-the-count"})
	<-ch // discard one more to make room again
	b.Publish(NotifEvent{Type: "clean"})

	got := drain(ch)
	last := got[len(got)-1]
	if last.Type != "clean" {
		t.Fatalf("last event is %q", last.Type)
	}
	if last.Missed != 0 {
		t.Fatalf("Missed is %d on an event after the count was reported, want 0", last.Missed)
	}
}

// TestAFilteredSubscriberCountsNoDrops kills the phantom-gap regression: if a
// filtered-out event counted as a drop, every page fetch would report loss.
func TestAFilteredSubscriberCountsNoDrops(t *testing.T) {
	b := newNotifBus(nil)
	ch, done := b.SubscribeFiltered("resource-fetched")
	defer done()

	// Far more than the buffer could hold, none of them wanted.
	for i := 0; i < notifBufferSize*4; i++ {
		b.Publish(NotifEvent{Type: "file-download-progress"})
	}
	if subs, dropped := b.Counters(); dropped != 0 {
		t.Fatalf("a filtered subscriber counted %d drops across %d subs, want 0", dropped, subs)
	}

	b.Publish(NotifEvent{Type: "resource-fetched"})
	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("filtered subscriber received %d events, want exactly the one it asked for", len(got))
	}
	if got[0].Missed != 0 {
		t.Fatalf("Missed is %d on a filtered subscriber that lost nothing", got[0].Missed)
	}
}

func TestAFilteredSubscriberCannotBeEvicted(t *testing.T) {
	b := newNotifBus(nil)
	ch, done := b.SubscribeFiltered("resource-fetched")
	defer done()

	// This is the pages.go failure: the wanted event is published first, then
	// buried under unrelated traffic. Unfiltered, it would be evicted.
	b.Publish(NotifEvent{Type: "resource-fetched"})
	for i := 0; i < notifBufferSize*4; i++ {
		b.Publish(NotifEvent{Type: "pm"})
	}
	got := drain(ch)
	if len(got) != 1 || got[0].Type != "resource-fetched" {
		t.Fatalf("the awaited event did not survive unrelated traffic: %+v", got)
	}
}

func TestCountersReportWhatWasLost(t *testing.T) {
	b := newNotifBus(nil)
	_, done := b.Subscribe()
	defer done()

	const over = 7
	for i := 0; i < notifBufferSize+over; i++ {
		b.Publish(NotifEvent{Type: "pm"})
	}
	subs, dropped := b.Counters()
	if subs != 1 {
		t.Fatalf("Counters reports %d subscribers, want 1", subs)
	}
	if dropped != over {
		t.Fatalf("Counters reports %d dropped, want %d", dropped, over)
	}
}

func TestUnsubscribingStopsTheAccounting(t *testing.T) {
	b := newNotifBus(nil)
	_, done := b.Subscribe()
	for i := 0; i < notifBufferSize*2; i++ {
		b.Publish(NotifEvent{Type: "pm"})
	}
	done()

	subs, dropped := b.Counters()
	if subs != 0 || dropped != 0 {
		t.Fatalf("after unsubscribing, Counters reports %d subs and %d dropped, want 0 and 0", subs, dropped)
	}
	// Publishing to nobody must not panic or block.
	b.Publish(NotifEvent{Type: "pm"})
}

// TestTheWireStaysQuietWhenNothingIsLost is the compatibility claim: a stream
// with no loss must not grow a "missed" key, or every consumer sees a field
// that means nothing.
func TestTheWireStaysQuietWhenNothingIsLost(t *testing.T) {
	b := newNotifBus(nil)
	ch, done := b.Subscribe()
	defer done()
	b.Publish(NotifEvent{Type: "pm", Payload: map[string]any{"x": 1}})

	raw, err := json.Marshal(<-ch)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["missed"]; ok {
		t.Fatalf("a lossless event carries a missed key: %s", raw)
	}
	for _, want := range []string{"seq", "epoch"} {
		if _, ok := m[want]; !ok {
			t.Fatalf("event is missing %q: %s", want, raw)
		}
	}

	// The keepalive is synthesized outside Publish and must stay bare, or it
	// would desync every consumer's baseline.
	raw, err = json.Marshal(NotifEvent{Type: "keepalive", Timestamp: time.Now()})
	if err != nil {
		t.Fatalf("marshal keepalive: %v", err)
	}
	// A fresh map: unmarshalling into a populated one merges into it, which
	// would carry the previous event's keys in and fail for the wrong reason.
	ka := map[string]any{}
	if err := json.Unmarshal(raw, &ka); err != nil {
		t.Fatalf("unmarshal keepalive: %v", err)
	}
	for _, absent := range []string{"seq", "epoch", "missed"} {
		if _, ok := ka[absent]; ok {
			t.Fatalf("the keepalive carries %q, which would be read as a sequence: %s", absent, raw)
		}
	}
}

func TestDropLoggingIsRateLimited(t *testing.T) {
	prev := notifDropLogInterval
	notifDropLogInterval = time.Hour
	defer func() { notifDropLogInterval = prev }()

	w := &countingWriter{}
	b := newNotifBus(slog.NewBackend(w).Logger("NOTF"))
	_, done := b.Subscribe()
	defer done()

	for i := 0; i < notifBufferSize+200; i++ {
		b.Publish(NotifEvent{Type: "file-download-progress"})
	}
	if w.lines != 1 {
		t.Fatalf("200 drops produced %d log lines, want exactly 1 inside one interval", w.lines)
	}

	// And the line has to name the type, or an operator cannot tell noise
	// from a table losing a move.
	if !strings.Contains(w.last, "file-download-progress") {
		t.Fatalf("the drop line does not name the type: %q", w.last)
	}
}

type countingWriter struct {
	lines int
	last  string
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.lines++
	w.last = string(p)
	return len(p), nil
}
