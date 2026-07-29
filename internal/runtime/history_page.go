// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"github.com/companyzero/bisonrelay/client/clientdb"
)

// historyPage slices one page out of already-filtered log entries, mirroring
// clientdb's paging: entries are oldest-first and page 0 is the newest
// pageSize entries. The history handlers cannot use clientdb's own paging
// because it slices the raw log before protocol envelopes are dropped, so a
// conversation whose newest stretch is all frames would serve empty pages
// while its chat sits deeper in the log.
//
// One deliberate difference from clientdb: a page past the oldest entry is
// empty rather than a repeat of the newest page, so a pager can terminate.
func historyPage(entries []clientdb.PMLogEntry, pageSize, pageNum int) []clientdb.PMLogEntry {
	if pageSize <= 0 || pageNum < 0 {
		return entries[:0]
	}
	// Compare before multiplying: the page number comes from a query param
	// and a huge one would overflow pageSize*pageNum.
	if pageNum > len(entries)/pageSize {
		return entries[:0]
	}
	pageEnd := len(entries) - pageSize*pageNum
	pageStart := pageEnd - pageSize
	if pageStart < 0 {
		pageStart = 0
	}
	return entries[pageStart:pageEnd]
}
