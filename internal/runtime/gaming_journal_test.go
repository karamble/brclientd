// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/decred/slog"
)

const (
	journalGCA  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	journalGCB  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	journalPeer = "1111111111111111111111111111111111111111111111111111111111111111"
	journalSelf = "2222222222222222222222222222222222222222222222222222222222222222"
)

func testJournal(dir string) *gamingJournal { return newGamingJournal(dir, slog.Disabled) }

func journalAppend(t *testing.T, j *gamingJournal, gcid, from, msg string, sent bool) {
	t.Helper()
	if err := j.append(gcid, from, msg, time.Unix(1700000000, 0), sent); err != nil {
		t.Fatalf("append %s: %v", msg, err)
	}
}

func journalSeqs(t *testing.T, dir string) []uint64 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, gamingJournalFile))
	if err != nil {
		t.Fatal(err)
	}
	var out []uint64
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var rec gamingJournalRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		out = append(out, rec.Seq)
	}
	return out
}

func TestGamingJournalKeepsSendersAndSeqAcrossReload(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(dir)
	journalAppend(t, j, journalGCA, journalPeer, "a1", false)
	journalAppend(t, j, journalGCB, journalPeer, "b1", false)
	journalAppend(t, j, journalGCA, journalSelf, "a2", true)

	j = testJournal(dir)
	journalAppend(t, j, journalGCA, journalPeer, "a3", false)
	got, err := j.history(journalGCA)
	if err != nil {
		t.Fatal(err)
	}
	want := []gamingJournalEntry{
		{Message: "a1", From: journalPeer, Timestamp: 1700000000},
		{Message: "a2", From: journalSelf, Timestamp: 1700000000, Sent: true},
		{Message: "a3", From: journalPeer, Timestamp: 1700000000},
	}
	if len(got) != len(want) {
		t.Fatalf("history = %#v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d = %#v, want %#v", i, got[i], want[i])
		}
	}
	if seqs := journalSeqs(t, dir); len(seqs) != 4 || seqs[3] != 4 {
		t.Fatalf("seqs = %v", seqs)
	}
}

func TestGamingJournalDropsTornTail(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(dir)
	journalAppend(t, j, journalGCA, journalPeer, "a1", false)
	f, err := os.OpenFile(filepath.Join(dir, gamingJournalFile), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"seq":2,"gcid":"`)
	f.Close()

	j = testJournal(dir)
	journalAppend(t, j, journalGCA, journalPeer, "a2", false)
	got, err := j.history(journalGCA)
	if err != nil || len(got) != 2 || got[1].Message != "a2" {
		t.Fatalf("history = %#v, %v", got, err)
	}
	if seqs := journalSeqs(t, dir); len(seqs) != 2 || seqs[1] != 2 {
		t.Fatalf("seqs = %v", seqs)
	}
}

func TestGamingJournalRefusesDamageUntilRepaired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, gamingJournalFile)
	good := `{"seq":1,"gcid":"` + journalGCA + `","from":"` + journalPeer + `","message":"a1"}` + "\n"
	for name, body := range map[string]string{
		"garbage":    "not json\n" + good,
		"repeat seq": good + good,
		"no sender":  `{"seq":1,"gcid":"` + journalGCA + `","message":"a1"}` + "\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		j := testJournal(dir)
		if _, err := j.history(journalGCA); err == nil {
			t.Fatalf("%s: history read a damaged journal", name)
		}
		if err := j.append(journalGCA, journalPeer, "a2", time.Now(), false); err == nil {
			t.Fatalf("%s: append wrote to a damaged journal", name)
		}
		if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
			t.Fatal(err)
		}
		journalAppend(t, j, journalGCA, journalPeer, "a2", false)
		if seqs := journalSeqs(t, dir); len(seqs) != 2 || seqs[1] != 2 {
			t.Fatalf("%s: seqs after repair = %v", name, seqs)
		}
	}
}

func TestGamingJournalPruneKeepsOthersAndSeq(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(dir)
	journalAppend(t, j, journalGCB, journalPeer, "b1", false)
	journalAppend(t, j, journalGCA, journalPeer, "a1", false)
	journalAppend(t, j, journalGCB, journalPeer, "b2", false)
	journalAppend(t, j, journalGCA, journalPeer, "a2", false)

	removed, err := j.prune(journalGCA)
	if err != nil || removed != 2 {
		t.Fatalf("prune = %d, %v", removed, err)
	}
	if seqs := journalSeqs(t, dir); len(seqs) != 3 || seqs[0] != 1 || seqs[1] != 3 || seqs[2] != 4 {
		t.Fatalf("seqs after prune = %v", seqs)
	}
	if got, _ := j.history(journalGCA); len(got) != 0 {
		t.Fatalf("pruned group still has %#v", got)
	}

	j = testJournal(dir)
	journalAppend(t, j, journalGCB, journalPeer, "b3", false)
	got, err := j.history(journalGCB)
	if err != nil || len(got) != 3 || got[0].Message != "b1" || got[1].Message != "b2" || got[2].Message != "b3" {
		t.Fatalf("history = %#v, %v", got, err)
	}
	if seqs := journalSeqs(t, dir); seqs[len(seqs)-1] != 5 {
		t.Fatalf("seqs after reload = %v", seqs)
	}

	// A second prune replaces the old mark rather than stacking marks.
	if _, err := j.prune(journalGCB); err != nil {
		t.Fatal(err)
	}
	if seqs := journalSeqs(t, dir); len(seqs) != 1 || seqs[0] != 5 {
		t.Fatalf("seqs after pruning everything = %v", seqs)
	}
	journalAppend(t, testJournal(dir), journalGCA, journalPeer, "a3", false)
	if seqs := journalSeqs(t, dir); seqs[len(seqs)-1] != 6 {
		t.Fatalf("seqs after last append = %v", seqs)
	}
}

func TestGamingHistoryEndpoint(t *testing.T) {
	dir := t.TempDir()
	j := testJournal(dir)
	journalAppend(t, j, journalGCA, journalPeer, "a1", false)
	journalAppend(t, j, journalGCA, journalSelf, "a2", true)
	journalAppend(t, j, journalGCA, journalPeer, "a3", false)
	s := &StatusServer{Log: slog.Disabled, GamingJournal: j}

	rec := httptest.NewRecorder()
	s.handleGamingHistory(rec, httptest.NewRequest(http.MethodGet, "/gaming/history?gcid="+journalGCA+"&page_size=2", nil))
	var page struct {
		Entries []gamingJournalEntry `json:"entries"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 2 || page.Entries[0].Message != "a2" || !page.Entries[0].Sent ||
		page.Entries[1].From != journalPeer || page.Entries[1].Message != "a3" {
		t.Fatalf("page 0 = %#v", page.Entries)
	}

	rec = httptest.NewRecorder()
	s.handleGamingHistory(rec, httptest.NewRequest(http.MethodDelete, "/gaming/history?gcid="+journalGCA, nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete = %d", rec.Code)
	}
	if got, _ := j.history(journalGCA); len(got) != 0 {
		t.Fatalf("delete left %#v", got)
	}
	rec = httptest.NewRecorder()
	s.handleGamingHistory(rec, httptest.NewRequest(http.MethodPost, "/gaming/history?gcid="+journalGCA, nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("post = %d", rec.Code)
	}
}
