// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/karamble/brclientd/internal/config"
	brlog "github.com/karamble/brclientd/internal/log"
	"github.com/karamble/brclientd/internal/setup"
)

// applyPendingRestore extracts a staged restore tarball into the data dir.
// It must run before anything opens the clientdb. A failed extraction parks
// the tarball under a .failed name and lets the boot continue, so a bad
// upload cannot crash-loop the daemon under its supervisor.
func applyPendingRestore(cfg *config.Config) error {
	staging := setup.StagingPath(cfg.AppDataDir)
	if _, err := os.Stat(staging); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat restore staging: %w", err)
	}
	brlog.BRCD.Infof("Restore backup found at %s; extracting into %s", staging, cfg.DataDir)
	if err := extractRestore(staging, cfg.DataDir); err != nil {
		failed := fmt.Sprintf("%s.failed.%d", staging, time.Now().Unix())
		brlog.BRCD.Errorf("Restore extraction failed: %v; staging file parked at %s", err, failed)
		if rerr := os.Rename(staging, failed); rerr != nil {
			brlog.BRCD.Errorf("Park staging file: %v", rerr)
		}
		return nil
	}
	if err := os.Remove(staging); err != nil {
		brlog.BRCD.Warnf("Remove staging file: %v", err)
	}
	// The restored ratchet state is older than what the network last saw,
	// so every session needs a KX reset initiated once the client connects.
	// The reset itself is fire-and-forget mailbox ping-pong handled by the
	// BR client; the marker only ensures the initiation pass runs once.
	marker := setup.KXResetMarkerPath(cfg.AppDataDir)
	stamp := time.Now().UTC().Format(time.RFC3339) + "\n"
	if err := os.WriteFile(marker, []byte(stamp), 0o600); err != nil {
		brlog.BRCD.Warnf("Write kx-reset marker: %v", err)
	}
	brlog.BRCD.Infof("Restore extracted; continuing boot with restored state")
	return nil
}

// extractRestore re-roots a client.Backup tarball into dataDir. Backup tar
// entries carry the original absolute paths with the walked root as the
// first entry, so that first entry is the prefix stripped from every other
// entry. The on-disk rpc/ cert dir is kept and rpc/ entries in the tar are
// skipped, so this instance's mTLS certs stay valid for the dashboard.
func extractRestore(staging, dataDir string) error {
	f, err := os.Open(staging)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)

	first, err := tr.Next()
	if err != nil {
		return fmt.Errorf("read first entry: %w", err)
	}
	prefix := strings.TrimSuffix(filepath.ToSlash(first.Name), "/")
	if prefix == "" {
		return errors.New("backup root entry has empty name")
	}

	// Wipe the fresh-boot state so restored files do not mix with it.
	entries, err := os.ReadDir(dataDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read data dir: %w", err)
	}
	for _, e := range entries {
		if e.Name() == "rpc" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dataDir, e.Name())); err != nil {
			return fmt.Errorf("clear data dir: %w", err)
		}
	}

	for hdr := first; ; {
		if err := extractEntry(tr, hdr, prefix, dataDir); err != nil {
			return err
		}
		hdr, err = tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}
	}
}

func extractEntry(tr *tar.Reader, hdr *tar.Header, prefix, dataDir string) error {
	name := filepath.ToSlash(hdr.Name)
	if name != prefix && !strings.HasPrefix(name, prefix+"/") {
		return fmt.Errorf("entry %q outside backup root %q", hdr.Name, prefix)
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(name, prefix), "/")
	if rel == "" {
		// The root entry itself; dataDir already exists.
		return nil
	}
	clean := filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("unsafe path %q in backup", hdr.Name)
	}
	if clean == "rpc" || strings.HasPrefix(clean, "rpc"+string(os.PathSeparator)) {
		return nil
	}
	dest := filepath.Join(dataDir, clean)
	switch hdr.Typeflag {
	case tar.TypeDir:
		return os.MkdirAll(dest, 0o700)
	case tar.TypeReg:
		// MkdirAll instead of trusting dir-header ordering in the tar.
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return err
		}
		mode := os.FileMode(hdr.Mode).Perm()
		if mode == 0 {
			mode = 0o600
		}
		out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			return fmt.Errorf("write %s: %w", dest, err)
		}
		return out.Close()
	default:
		// client.Backup only emits dirs and regular files; skip anything else.
		return nil
	}
}
