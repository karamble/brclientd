# Install

brclientd is a single static binary. Run it directly or in a container.

It requires a reachable dcrlnd node with an unlocked wallet and an open
Lightning channel. See [dcrlnd.md](dcrlnd.md).

## Running the binary

Build it first ([build.md](build.md)), then:

```
brclientd \
  --dcrlnd.rpchost=127.0.0.1:10009 \
  --dcrlnd.tlscertpath=~/.dcrlnd/tls.cert \
  --dcrlnd.macaroonpath=~/.dcrlnd/data/chain/decred/mainnet/admin.macaroon
```

## Running the container

```
docker build -t brclientd:dev .
docker run -d --name brclientd \
  -v brclientd-data:/app-data/brclientd \
  -v /path/to/dcrlnd-creds:/dcrlnd:ro \
  -p 127.0.0.1:7676:7676 \
  -p 127.0.0.1:7677:7677 \
  brclientd:dev \
  --appdata=/app-data/brclientd \
  --dcrlnd.rpchost=<dcrlnd-host>:10009 \
  --dcrlnd.tlscertpath=/dcrlnd/tls.cert \
  --dcrlnd.macaroonpath=/dcrlnd/admin.macaroon
```

The image runs as an unprivileged user. Keep the published ports bound to
127.0.0.1 unless other machines need to reach the daemon.

## Data locations

Default locations on Linux:

| Path | Purpose |
| ---- | ------- |
| `~/.brclientd/` | application data root |
| `~/.brclientd/brclientd.conf` | config file |
| `~/.brclientd/data/<network>/` | BR client state (DB, messages, downloads, embeds) |
| `~/.brclientd/data/<network>/rpc/` | auto-generated mTLS certificates |
| `~/.brclientd/logs/<network>/brclientd.log` | rotating log file |

`--appdata` moves the root. On Windows and macOS the root defaults to the
platform application directory instead of `~/.brclientd`.

## Configuration

On the first run at the default location brclientd writes a documented
`brclientd.conf` into the application data directory, the same way dcrd
does. Every option is commented at its default, so the fresh file changes
no behavior. Edit it and restart the daemon to apply changes. The template
is [`sampleconfig/sample-brclientd.conf`](../sampleconfig/sample-brclientd.conf).

Every long-form CLI flag is also a valid INI key. Config sections map to
flag groups: `[dcrlnd options]` holds `dcrlnd.rpchost` and friends.
Precedence: flags override the config file, the config file overrides the
defaults.

Run `brclientd --help` for the full flag list.
