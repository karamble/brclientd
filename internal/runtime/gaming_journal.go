// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/decred/slog"
)

const gamingJournalFile = "gaming-frames.jsonl"

// gamingJournalMaxLine is the longest line the journal writes or reads.
const gamingJournalMaxLine = 8 << 20

// gamingJournalRecord is one gaming frame with the sender's authenticated
// UID. A mark record carries only the highest seq issued before a prune.
type gamingJournalRecord struct {
	Seq     uint64 `json:"seq"`
	GCID    string `json:"gcid,omitempty"`
	From    string `json:"from,omitempty"`
	Message string `json:"message,omitempty"`
	TS      int64  `json:"ts,omitempty"`
	Sent    bool   `json:"sent,omitempty"`
	Mark    bool   `json:"mark,omitempty"`
}

// gamingJournalEntry is one entry of a /gaming/history page.
type gamingJournalEntry struct {
	Message   string `json:"message"`
	From      string `json:"from"`
	Timestamp int64  `json:"timestamp"`
	Sent      bool   `json:"sent"`
}

// gamingJournal keeps every gaming frame this client sent or received, so
// history replay carries the UID BR authenticated rather than a nick. Records
// leave it only through prune, which the dashboard calls once a table's funds
// are settled.
type gamingJournal struct {
	mu     sync.Mutex
	path   string
	log    slog.Logger
	loaded bool
	next   uint64
}

func newGamingJournal(dataDir string, log slog.Logger) *gamingJournal {
	return &gamingJournal{path: filepath.Join(dataDir, gamingJournalFile), log: log}
}

// loadLocked validates the whole file once and learns the next seq. A file
// that does not read cleanly keeps the journal unloaded, so every call fails
// until it is repaired.
func (j *gamingJournal) loadLocked() error {
	if j.loaded {
		return nil
	}
	dropped, err := healGamingJournalTail(j.path)
	if errors.Is(err, os.ErrNotExist) {
		j.loaded, j.next = true, 0
		return nil
	}
	if err != nil {
		return err
	}
	if dropped > 0 {
		j.log.Warnf("Gaming journal %s: dropped an unfinished last record (%d bytes)", j.path, dropped)
	}
	var last uint64
	if err := j.scanLocked(func(rec gamingJournalRecord) { last = rec.Seq }); err != nil {
		return fmt.Errorf("gaming journal %s: %w", j.path, err)
	}
	j.loaded, j.next = true, last
	return nil
}

// scanLocked reads every record in order, rejecting any damage.
func (j *gamingJournal) scanLocked(fn func(gamingJournalRecord)) error {
	f, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64*1024), gamingJournalMaxLine)
	var prev uint64
	for line := 1; scan.Scan(); line++ {
		var rec gamingJournalRecord
		if err := json.Unmarshal(scan.Bytes(), &rec); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if rec.Seq <= prev {
			return fmt.Errorf("line %d: non-increasing sequence", line)
		}
		if !rec.Mark && (rec.GCID == "" || rec.From == "" || rec.Message == "") {
			return fmt.Errorf("line %d: incomplete gaming frame", line)
		}
		prev = rec.Seq
		fn(rec)
	}
	return scan.Err()
}

// append writes and syncs one frame.
func (j *gamingJournal) append(gcid, from, message string, ts time.Time, sent bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.loadLocked(); err != nil {
		return err
	}
	rec := gamingJournalRecord{
		Seq: j.next + 1, GCID: gcid, From: from, Message: message,
		TS: ts.Unix(), Sent: sent,
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(raw)+1 > gamingJournalMaxLine {
		return fmt.Errorf("gaming frame of %d bytes is longer than the journal keeps", len(raw))
	}
	if err := os.MkdirAll(filepath.Dir(j.path), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(raw, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	j.next = rec.Seq
	return nil
}

// history returns one group's frames oldest first.
func (j *gamingJournal) history(gcid string) ([]gamingJournalEntry, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.loadLocked(); err != nil {
		return nil, err
	}
	var out []gamingJournalEntry
	err := j.scanLocked(func(rec gamingJournalRecord) {
		if rec.GCID == gcid {
			out = append(out, gamingJournalEntry{
				Message: rec.Message, From: rec.From, Timestamp: rec.TS, Sent: rec.Sent,
			})
		}
	})
	return out, err
}

// prune removes one group's frames. The rewrite keeps a mark with the highest
// seq issued, so seq never repeats after a restart.
func (j *gamingJournal) prune(gcid string) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.loadLocked(); err != nil {
		return 0, err
	}
	var buf bytes.Buffer
	var last uint64
	removed := 0
	err := j.scanLocked(func(rec gamingJournalRecord) {
		if rec.Mark {
			return
		}
		if rec.GCID == gcid {
			removed++
			return
		}
		raw, _ := json.Marshal(rec)
		buf.Write(raw)
		buf.WriteByte('\n')
		last = rec.Seq
	})
	if err != nil {
		return 0, err
	}
	if removed == 0 {
		return 0, nil
	}
	if last < j.next {
		raw, _ := json.Marshal(gamingJournalRecord{Seq: j.next, Mark: true})
		buf.Write(raw)
		buf.WriteByte('\n')
	}
	if err := replaceGamingJournal(j.path, buf.Bytes()); err != nil {
		return 0, err
	}
	return removed, nil
}

// replaceGamingJournal swaps in new contents so a crash leaves either the old
// or the new file.
func replaceGamingJournal(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, gamingJournalFile+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Chmod(tmp.Name(), 0o600)
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// healGamingJournalTail drops a final line with no newline: an append that
// never completed. It returns how many bytes it dropped.
func healGamingJournalTail(path string) (int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	if size == 0 {
		return 0, nil
	}
	last := make([]byte, 1)
	if _, err := f.ReadAt(last, size-1); err != nil {
		return 0, err
	}
	if last[0] == '\n' {
		return 0, nil
	}
	keep := int64(0)
	buf := make([]byte, 64*1024)
	for end := size; end > 0 && keep == 0; {
		start := end - int64(len(buf))
		if start < 0 {
			start = 0
		}
		chunk := buf[:end-start]
		if _, err := f.ReadAt(chunk, start); err != nil {
			return 0, err
		}
		if i := bytes.LastIndexByte(chunk, '\n'); i >= 0 {
			keep = start + int64(i) + 1
		}
		end = start
	}
	if err := f.Truncate(keep); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	return size - keep, nil
}
