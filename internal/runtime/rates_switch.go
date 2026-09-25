// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"sync"
)

// ratesSwitch runs Bison Relay's exchange-rate fetcher while exchange rates
// are on, and stops it and forgets its prices when they are turned off.
type ratesSwitch struct {
	parent context.Context
	run    func(context.Context)
	clear  func()

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newRatesSwitch(parent context.Context, run func(context.Context), clear func()) *ratesSwitch {
	return &ratesSwitch{parent: parent, run: run, clear: clear}
}

// Set starts or stops the fetcher. Stopping waits for it to return before
// clearing the prices, so a fetch in flight cannot restore them.
func (r *ratesSwitch) Set(on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if on == (r.cancel != nil) {
		return
	}
	if on {
		ctx, cancel := context.WithCancel(r.parent)
		done := make(chan struct{})
		r.cancel, r.done = cancel, done
		go func() {
			defer close(done)
			r.run(ctx)
		}()
		return
	}
	r.cancel()
	<-r.done
	r.cancel, r.done = nil, nil
	r.clear()
}

// On reports whether the fetcher is running.
func (r *ratesSwitch) On() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cancel != nil
}
