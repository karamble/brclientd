// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"sync"
	"time"
)

// downloadCapTracker holds the per-download price cap the caller approved
// when requesting a shared-file download. The embed cost shown in a post is
// advertisory; the host invoices from the cost stored on its share, which
// only arrives with the file metadata. FileDownloadConfirmer compares that
// real cost against the cap recorded here before any chunk is paid for.
type downloadCapTracker struct {
	mu sync.Mutex
	m  map[string]capEntry
}

type capEntry struct {
	maxCostAtoms uint64
	at           time.Time
}

func newDownloadCapTracker() *downloadCapTracker {
	return &downloadCapTracker{m: make(map[string]capEntry)}
}

func downloadCapKey(uid, fid string) string {
	return uid + "/" + fid
}

// set records the cap and prunes entries whose metadata never arrived
// (e.g. host offline) so the map cannot grow unbounded.
func (t *downloadCapTracker) set(uid, fid string, maxCostAtoms uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, e := range t.m {
		if time.Since(e.at) > time.Hour {
			delete(t.m, k)
		}
	}
	t.m[downloadCapKey(uid, fid)] = capEntry{maxCostAtoms: maxCostAtoms, at: time.Now()}
}

// take returns and removes the cap for a download. Missing entries report
// a zero cap, which allows free files only.
func (t *downloadCapTracker) take(uid, fid string) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := downloadCapKey(uid, fid)
	e, ok := t.m[key]
	if !ok {
		return 0
	}
	delete(t.m, key)
	return e.maxCostAtoms
}
