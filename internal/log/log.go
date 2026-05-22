// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package log wires the brclientd subsystem loggers. Pattern mirrors dcrd's
// log.go: a single slog backend writes to both stdout and a rotating file,
// and each subsystem holds its own named logger.
package log

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/decred/slog"
	"github.com/jrick/logrotate/rotator"
)

type logWriter struct{}

func (logWriter) Write(p []byte) (n int, err error) {
	os.Stdout.Write(p)
	if logRotator != nil {
		logRotator.Write(p)
	}
	return len(p), nil
}

var (
	backendLog = slog.NewBackend(logWriter{})

	logRotator *rotator.Rotator

	BRCD = backendLog.Logger("BRCD")
	LNPC = backendLog.Logger("LNPC")
	BRDB = backendLog.Logger("BRDB")
	SETP = backendLog.Logger("SETP")
	RUNT = backendLog.Logger("RUNT")
)

var subsystems = map[string]slog.Logger{
	"BRCD": BRCD,
	"LNPC": LNPC,
	"BRDB": BRDB,
	"SETP": SETP,
	"RUNT": RUNT,
}

// InitRotator opens the log file at logFile, creating parent directories as
// needed, and wires the rotating writer. Should be called once at startup.
func InitRotator(logFile string) error {
	if err := os.MkdirAll(filepath.Dir(logFile), 0o700); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	r, err := rotator.New(logFile, 10*1024, false, 3)
	if err != nil {
		return fmt.Errorf("open log rotator: %w", err)
	}
	logRotator = r
	return nil
}

// CloseRotator flushes the rotator and releases the underlying file. Safe to
// call multiple times.
func CloseRotator() {
	if logRotator != nil {
		logRotator.Close()
		logRotator = nil
	}
}

// SetLevels sets the same level across every subsystem logger.
func SetLevels(level string) error {
	lvl, ok := slog.LevelFromString(level)
	if !ok {
		return fmt.Errorf("unknown log level %q", level)
	}
	for _, l := range subsystems {
		l.SetLevel(lvl)
	}
	return nil
}

// LoggerFn returns a slog.Logger for an arbitrary subsystem tag, registering
// it in the subsystem map the first time it's requested. BR's client.Config
// expects this shape for its Logger field; brclientd wires it directly.
func LoggerFn(subsys string) slog.Logger {
	if l, ok := subsystems[subsys]; ok {
		return l
	}
	l := backendLog.Logger(subsys)
	subsystems[subsys] = l
	return l
}

// SubsystemTags returns the registered subsystem names in stable order.
func SubsystemTags() []string {
	out := make([]string, 0, len(subsystems))
	for k := range subsystems {
		out = append(out, k)
	}
	return out
}
