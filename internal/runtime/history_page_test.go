// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"math"
	"testing"

	"github.com/companyzero/bisonrelay/client/clientdb"
)

func histEntries(n int) []clientdb.PMLogEntry {
	out := make([]clientdb.PMLogEntry, n)
	for i := range out {
		out[i] = clientdb.PMLogEntry{Timestamp: int64(i)}
	}
	return out
}

func TestHistoryPage(t *testing.T) {
	tests := []struct {
		name      string
		n         int
		pageSize  int
		pageNum   int
		wantFirst int64
		wantLen   int
	}{
		{name: "page 0 is the newest entries", n: 120, pageSize: 50, pageNum: 0, wantFirst: 70, wantLen: 50},
		{name: "middle page", n: 120, pageSize: 50, pageNum: 1, wantFirst: 20, wantLen: 50},
		{name: "last page is partial", n: 120, pageSize: 50, pageNum: 2, wantFirst: 0, wantLen: 20},
		{name: "past the end is empty", n: 120, pageSize: 50, pageNum: 3, wantLen: 0},
		{name: "exact multiple last page", n: 100, pageSize: 50, pageNum: 1, wantFirst: 0, wantLen: 50},
		{name: "exact multiple past the end", n: 100, pageSize: 50, pageNum: 2, wantLen: 0},
		{name: "page size covers everything", n: 10, pageSize: 50, pageNum: 0, wantFirst: 0, wantLen: 10},
		{name: "page size covers everything, page 1", n: 10, pageSize: 50, pageNum: 1, wantLen: 0},
		{name: "empty input", n: 0, pageSize: 50, pageNum: 0, wantLen: 0},
		{name: "huge page number does not overflow", n: 10, pageSize: 500, pageNum: math.MaxInt, wantLen: 0},
		{name: "single entry pages", n: 3, pageSize: 1, pageNum: 1, wantFirst: 1, wantLen: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := historyPage(histEntries(tc.n), tc.pageSize, tc.pageNum)
			if len(got) != tc.wantLen {
				t.Fatalf("len = %d, want %d", len(got), tc.wantLen)
			}
			if tc.wantLen == 0 {
				return
			}
			if got[0].Timestamp != tc.wantFirst {
				t.Fatalf("first = %d, want %d", got[0].Timestamp, tc.wantFirst)
			}
			if want := tc.wantFirst + int64(tc.wantLen) - 1; got[len(got)-1].Timestamp != want {
				t.Fatalf("last = %d, want %d", got[len(got)-1].Timestamp, want)
			}
		})
	}
}
