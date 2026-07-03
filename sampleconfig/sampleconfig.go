// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package sampleconfig embeds brclientd's documented sample configuration file
// so tools that perform automatic configuration (including brclientd itself,
// which writes it into the application data directory on first run) can access
// it without a second copy on disk. Mirrors decred/dcrd/sampleconfig.
package sampleconfig

import _ "embed"

//go:embed sample-brclientd.conf
var sampleBrclientdConf string

// FileContents returns the string representation of the sample config file.
func FileContents() string {
	return sampleBrclientdConf
}
