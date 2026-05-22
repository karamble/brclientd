// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package identity wraps the slice of Bison Relay's clientdb API brclientd
// needs at bootstrap: opening the DB, checking for a local identity, and
// persisting a freshly-generated one.
package identity

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/companyzero/bisonrelay/client/clientdb"
	"github.com/companyzero/bisonrelay/zkidentity"
	"github.com/decred/slog"
)

const dbChunkSize = 10 * 1024 * 1024

// Paths groups the standard clientdb sub-directories under a single data dir.
type Paths struct {
	Root          string
	MsgsRoot      string
	DownloadsRoot string
	EmbedsRoot    string
}

// PathsIn returns the canonical layout for a given data dir.
func PathsIn(dataDir string) Paths {
	return Paths{
		Root:          filepath.Join(dataDir, "db"),
		MsgsRoot:      filepath.Join(dataDir, "msgs"),
		DownloadsRoot: filepath.Join(dataDir, "downloads"),
		EmbedsRoot:    filepath.Join(dataDir, "embeds"),
	}
}

// OpenDB opens BR's clientdb at the given paths.
func OpenDB(p Paths, log slog.Logger) (*clientdb.DB, error) {
	return clientdb.New(clientdb.Config{
		Root:          p.Root,
		MsgsRoot:      p.MsgsRoot,
		DownloadsRoot: p.DownloadsRoot,
		EmbedsRoot:    p.EmbedsRoot,
		Logger:        log,
		ChunkSize:     dbChunkSize,
	})
}

// Existing reports the local identity and a flag indicating whether one was
// found. err is non-nil only on unexpected DB errors.
func Existing(ctx context.Context, db *clientdb.DB) (*zkidentity.FullIdentity, bool, error) {
	var (
		id    *zkidentity.FullIdentity
		found bool
	)
	err := db.View(ctx, func(tx clientdb.ReadTx) error {
		got, err := db.LocalID(tx)
		if err == nil {
			id = got
			found = true
			return nil
		}
		if errors.Is(err, clientdb.ErrLocalIDEmpty) {
			return nil
		}
		return err
	})
	return id, found, err
}

// Create persists a new local identity built from nick / name.
func Create(ctx context.Context, db *clientdb.DB, nick, name string) error {
	nick = strings.TrimSpace(nick)
	name = strings.TrimSpace(name)
	if nick == "" {
		return errors.New("nick is required")
	}
	id, err := zkidentity.New(name, nick)
	if err != nil {
		return fmt.Errorf("zkidentity.New: %w", err)
	}
	return db.Update(ctx, func(tx clientdb.ReadWriteTx) error {
		return db.UpdateLocalID(tx, id)
	})
}
