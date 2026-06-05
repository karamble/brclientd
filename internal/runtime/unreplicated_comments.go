// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import "sync"

// unreplComment is a comment the local user sent on a remote post that the
// post author has not yet broadcast back. Replicated comment status updates
// carry no unique id (rpc.RMPIdentifier holds the post id, shared by every
// comment on the post), so entries are matched by comment text + parent,
// mirroring bruig's unreplicated-comments model (bruig feed.dart).
type unreplComment struct {
	Comment   string
	Parent    string
	Timestamp int64
}

// maxUnreplPerPost bounds tracker growth for posts whose author never comes
// back online to replicate.
const maxUnreplPerPost = 50

// unreplTracker holds sent-but-unreplicated comments in memory, keyed by
// post author + post id. Entries are added when /posts/comment succeeds and
// removed when the comment round-trips back via OnPostStatusRcvdNtfn. A
// daemon restart drops the set; only the "unreplicated" badge is lost, the
// comments themselves were already queued to the author.
type unreplTracker struct {
	mu sync.Mutex
	m  map[string][]unreplComment
}

func newUnreplTracker() *unreplTracker {
	return &unreplTracker{m: make(map[string][]unreplComment)}
}

func unreplKey(postFrom, pid string) string {
	return postFrom + "/" + pid
}

func (t *unreplTracker) add(postFrom, pid, comment, parent string, ts int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := unreplKey(postFrom, pid)
	entries := t.m[key]
	for _, e := range entries {
		if e.Comment == comment && e.Parent == parent {
			return
		}
	}
	if len(entries) >= maxUnreplPerPost {
		return
	}
	t.m[key] = append(entries, unreplComment{Comment: comment, Parent: parent, Timestamp: ts})
}

func (t *unreplTracker) remove(postFrom, pid, comment, parent string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := unreplKey(postFrom, pid)
	entries := t.m[key]
	for i, e := range entries {
		if e.Comment == comment && e.Parent == parent {
			t.m[key] = append(entries[:i], entries[i+1:]...)
			if len(t.m[key]) == 0 {
				delete(t.m, key)
			}
			return
		}
	}
}

func (t *unreplTracker) list(postFrom, pid string) []unreplComment {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries := t.m[unreplKey(postFrom, pid)]
	out := make([]unreplComment, len(entries))
	copy(out, entries)
	return out
}
