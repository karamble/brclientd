# brclientd

Headless [Bison Relay](https://github.com/companyzero/bisonrelay) daemon.
Wraps the BR `client` library behind a JSON-RPC surface (clientrpc) so any
application can integrate BR messaging without embedding the TUI or the
Flutter GUI.

Designed for projects that already operate an external `dcrlnd` and want
to add BR features through a clean network boundary.

## Status

Phase 1 skeleton. Connects to dcrlnd, prints `ready`, exits on SIGTERM.
clientrpc, identity creation, and the BR client core wire in later phases.

## Configuration

brclientd follows the Decred daemon conventions (dcrd / dcrwallet / dcrlnd):
CLI flags + an INI config file, parsed via
[`go-flags`](https://github.com/jessevdk/go-flags). Paths default to the
platform's standard application directory.

Default locations on Linux:

```
~/.brclientd/                          application data root
~/.brclientd/brclientd.conf            config file
~/.brclientd/data/<network>/           BR client state (DB, msgs, downloads)
~/.brclientd/logs/<network>/brclientd.log
```

See `sample-brclientd.conf` for a documented config template.

Frequently used flags:

```
brclientd \
  --appdata=/path/to/data-dir \
  --dcrlnd.rpchost=127.0.0.1:10009 \
  --dcrlnd.tlscertpath=~/.dcrlnd/tls.cert \
  --dcrlnd.macaroonpath=~/.dcrlnd/data/chain/decred/mainnet/admin.macaroon
```

Run `brclientd --help` for the full list. All long-form flags are also
valid INI keys; sections map to flag groups (`[dcrlnd options]`,
`[clientrpc options]`).

## Build

```
go build ./cmd/brclientd
```

Container image:

```
docker build -t brclientd:dev .
```

## License

ISC. Copyright (c) 2015-2026 The Decred developers.
