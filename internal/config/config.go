// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package config parses brclientd's CLI flags and INI configuration file.
// The layout intentionally mirrors dcrd / dcrwallet / dcrlnd conventions:
// flags via go-flags, INI config via go-flags' IniParser, defaults rooted at
// the platform-appropriate application data directory.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/decred/dcrd/dcrutil/v4"
	flags "github.com/jessevdk/go-flags"
)

const (
	AppName               = "brclientd"
	DefaultConfigFilename = AppName + ".conf"
	DefaultLogFilename    = AppName + ".log"
	DefaultDebugLevel     = "info"
	DefaultBRServer       = "bisonrelay.org:443"
	DefaultClientRPCPort  = "7676"
	DefaultStatusPort     = "7677"
)

var (
	DefaultAppDataDir = dcrutil.AppDataDir(AppName, false)
	DefaultConfigFile = filepath.Join(DefaultAppDataDir, DefaultConfigFilename)
)

// DcrlndOptions are the dcrlnd connection parameters (config section `[dcrlnd]`).
type DcrlndOptions struct {
	RPCHost      string `long:"rpchost" description:"dcrlnd gRPC host:port (default: 127.0.0.1:10009)"`
	TLSCertPath  string `long:"tlscertpath" description:"Path to dcrlnd's TLS certificate"`
	MacaroonPath string `long:"macaroonpath" description:"Path to dcrlnd's admin macaroon"`
}

// ClientRPCOptions configure the JSON-RPC over WebSocket clientrpc surface
// (config section `[clientrpc]`). Reserved for phase 3+.
type ClientRPCOptions struct {
	Listen          []string `long:"listen" description:"Add an interface/port to listen on for clientrpc"`
	IssueClientCert bool     `long:"issueclientcert" description:"Auto-generate the rpc-client cert pair on first run"`
}

// StatusOptions configure the /status HTTP endpoint that surfaces BR client
// connection state and wallet-usability errors (config section `[status]`).
type StatusOptions struct {
	Listen string `long:"listen" description:"Address the /status HTTP endpoint binds to (default: 0.0.0.0:7677)"`
}

// SimpleStoreOptions configure the optional simplestore resource host (config
// section `[simplestore]`). When Enabled, this node serves a store over the
// relay instead of static pages (BR binds one resource provider at the root).
type SimpleStoreOptions struct {
	Enabled    bool    `long:"enabled" description:"Host a simplestore over the relay instead of static pages"`
	PayType    string  `long:"paytype" description:"How to charge for orders: ln or onchain (default: ln)"`
	Account    string  `long:"account" description:"Wallet account used for on-chain order addresses"`
	ShipCharge float64 `long:"shipcharge" description:"Flat shipping surcharge in USD added to each order"`
}

// Config is the parsed runtime configuration.
type Config struct {
	ShowVersion bool   `short:"V" long:"version" description:"Display version information and exit"`
	AppDataDir  string `long:"appdata" description:"Top-level application data directory"`
	ConfigFile  string `short:"C" long:"configfile" description:"Path to configuration file"`
	DataDir     string `long:"datadir" description:"Directory for Bison Relay client state (DB, messages, downloads, embeds)"`
	LogDir      string `long:"logdir" description:"Directory to log output"`
	DebugLevel  string `long:"debuglevel" description:"Logging level {trace, debug, info, warn, error, critical}"`
	TestNet     bool   `long:"testnet" description:"Use the test network"`
	SimNet      bool   `long:"simnet" description:"Use the simulation network"`
	BRServer    string `long:"brserver" description:"Bison Relay relay server address"`

	Proxy        string `long:"proxy" description:"Connect via SOCKS5 proxy (eg. 127.0.0.1:9050)"`
	ProxyUser    string `long:"proxyuser" description:"Username for proxy server"`
	ProxyPass    string `long:"proxypass" description:"Password for proxy server"`
	TorIsolation bool   `long:"torisolation" description:"Enable Tor stream isolation by randomizing user credentials for each connection"`
	CircuitLimit uint32 `long:"circuitlimit" description:"Maximum number of open connections per proxy connection (default 32)"`

	Dcrlnd      DcrlndOptions      `group:"dcrlnd options" namespace:"dcrlnd"`
	ClientRPC   ClientRPCOptions   `group:"clientrpc options" namespace:"clientrpc"`
	Status      StatusOptions      `group:"status options" namespace:"status"`
	SimpleStore SimpleStoreOptions `group:"simplestore options" namespace:"simplestore"`
}

// Network reports the active network as a string used in default sub-paths.
func (c *Config) Network() string {
	switch {
	case c.SimNet:
		return "simnet"
	case c.TestNet:
		return "testnet"
	default:
		return "mainnet"
	}
}

// Load parses CLI flags and the INI config file (if one exists). The CLI
// override semantics match dcrd: flags > INI > defaults.
func Load(args []string, version string) (*Config, error) {
	pre := defaults()
	preParser := flags.NewParser(pre, flags.HelpFlag|flags.PassDoubleDash|flags.IgnoreUnknown)
	if _, err := preParser.ParseArgs(args); err != nil {
		var fe *flags.Error
		if errors.As(err, &fe) && fe.Type == flags.ErrHelp {
			return nil, err
		}
		return nil, err
	}

	if pre.ShowVersion {
		fmt.Printf("%s version %s\n", AppName, version)
		return nil, flags.ErrHelp
	}

	if pre.AppDataDir != "" {
		pre.AppDataDir = cleanAndExpandPath(pre.AppDataDir)
	} else {
		pre.AppDataDir = DefaultAppDataDir
	}
	if pre.ConfigFile == "" {
		pre.ConfigFile = filepath.Join(pre.AppDataDir, DefaultConfigFilename)
	}
	pre.ConfigFile = cleanAndExpandPath(pre.ConfigFile)

	cfg := defaults()
	cfg.AppDataDir = pre.AppDataDir
	cfg.ConfigFile = pre.ConfigFile

	parser := flags.NewParser(cfg, flags.HelpFlag|flags.PassDoubleDash)
	if _, err := os.Stat(cfg.ConfigFile); err == nil {
		if err := flags.NewIniParser(parser).ParseFile(cfg.ConfigFile); err != nil {
			return nil, fmt.Errorf("parsing config file %q: %w", cfg.ConfigFile, err)
		}
	}

	if _, err := parser.ParseArgs(args); err != nil {
		var fe *flags.Error
		if errors.As(err, &fe) && fe.Type == flags.ErrHelp {
			return nil, err
		}
		return nil, err
	}

	if cfg.TestNet && cfg.SimNet {
		return nil, errors.New("--testnet and --simnet cannot both be set")
	}

	if cfg.AppDataDir == "" {
		cfg.AppDataDir = DefaultAppDataDir
	}
	cfg.AppDataDir = cleanAndExpandPath(cfg.AppDataDir)

	network := cfg.Network()
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join(cfg.AppDataDir, "data", network)
	}
	cfg.DataDir = cleanAndExpandPath(cfg.DataDir)

	if cfg.LogDir == "" {
		cfg.LogDir = filepath.Join(cfg.AppDataDir, "logs", network)
	}
	cfg.LogDir = cleanAndExpandPath(cfg.LogDir)

	if cfg.Dcrlnd.RPCHost == "" {
		cfg.Dcrlnd.RPCHost = "127.0.0.1:10009"
	}
	if len(cfg.ClientRPC.Listen) == 0 {
		cfg.ClientRPC.Listen = []string{"0.0.0.0:" + DefaultClientRPCPort}
	}
	if cfg.Status.Listen == "" {
		cfg.Status.Listen = "0.0.0.0:" + DefaultStatusPort
	}
	if cfg.SimpleStore.PayType == "" {
		cfg.SimpleStore.PayType = "ln"
	}

	if err := ensureDir(cfg.AppDataDir); err != nil {
		return nil, err
	}
	if err := ensureDir(cfg.DataDir); err != nil {
		return nil, err
	}
	if err := ensureDir(cfg.LogDir); err != nil {
		return nil, err
	}

	if cfg.Dcrlnd.TLSCertPath != "" {
		cfg.Dcrlnd.TLSCertPath = cleanAndExpandPath(cfg.Dcrlnd.TLSCertPath)
	}
	if cfg.Dcrlnd.MacaroonPath != "" {
		cfg.Dcrlnd.MacaroonPath = cleanAndExpandPath(cfg.Dcrlnd.MacaroonPath)
	}
	// Existence of these files is checked at runtime in connectDcrlnd, not
	// at config load. On a fresh stack the files don't exist until dcrlnd
	// runs through its wallet setup; brclientd should poll-and-wait, not
	// fail-fast (which crashloops the container and tanks Umbrel health).

	return cfg, nil
}

// LogFile returns the absolute path to the rotating log file.
func (c *Config) LogFile() string {
	return filepath.Join(c.LogDir, DefaultLogFilename)
}

func defaults() *Config {
	return &Config{
		AppDataDir: DefaultAppDataDir,
		ConfigFile: DefaultConfigFile,
		DebugLevel: DefaultDebugLevel,
		BRServer:   DefaultBRServer,
	}
}

func cleanAndExpandPath(path string) string {
	if path == "" {
		return ""
	}
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err == nil {
			path = filepath.Join(home, strings.TrimPrefix(path, "~"))
		}
	}
	return filepath.Clean(os.ExpandEnv(path))
}

func ensureDir(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists but is not a directory", path)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	return nil
}
