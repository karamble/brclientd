# brclientd

Headless [Bison Relay](https://github.com/companyzero/bisonrelay) daemon.
Wraps the BR `client` library behind two network surfaces (clientrpc and
a status REST API) so any application can integrate BR messaging without
embedding the TUI or the Flutter GUI.

Designed for projects that already operate an external `dcrlnd` and want
to add BR features through a clean network boundary.

## Features

- Full BR client core: private messages, group chats, posts and comments,
  file sharing, tips, pages and simplestore hosting.
- clientrpc: Bison Relay's stock JSON-RPC surface over mTLS WebSocket.
- Status REST API for frontends and supervisors.
- Identity bootstrap over RPC: no interactive setup, a companion app
  creates the identity (or restores a backup) with one request.
- Full-state backup and restore.
- External dcrlnd only; polls and waits for the LN wallet instead of
  crash-looping.
- Decred daemon conventions: CLI flags plus INI config file, documented
  config written on first run.

## Configuration

Default locations on Linux:

```
~/.brclientd/                          application data root
~/.brclientd/brclientd.conf            config file
~/.brclientd/data/<network>/           BR client state (DB, msgs, downloads)
~/.brclientd/logs/<network>/brclientd.log
```

On first run at the default location brclientd writes a documented
`brclientd.conf` (every option commented at its default) into the
application data directory, the same way dcrd does. The template is
`sampleconfig/sample-brclientd.conf`.

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
`[clientrpc options]`). See [docs/install.md](docs/install.md) for details.

## Build

```
go build ./cmd/brclientd
```

See [docs/build.md](docs/build.md) for version stamping and the container
image.

## Documentation

- [docs/install.md](docs/install.md) - running the daemon, data locations, configuration
- [docs/build.md](docs/build.md) - compiling from source, building the container image
- [docs/dcrlnd.md](docs/dcrlnd.md) - the external dcrlnd requirement and startup gates
- [docs/rpc.md](docs/rpc.md) - RPC glossary: clientrpc and the status REST API

## Bison Relay

brclientd builds on the Bison Relay project by Company Zero:

- Protocol, client library, brclient TUI and bruig GUI:
  https://github.com/companyzero/bisonrelay
- About Bison Relay: https://bisonrelay.org

## License

ISC. Copyright (c) 2015-2026 The Decred developers.
