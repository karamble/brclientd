# External dcrlnd

brclientd embeds no wallet. It requires a running
[dcrlnd](https://github.com/decred/dcrlnd) node it can pay from.

## Why

Bison Relay is a paid service. The client pays the relay server over the
Decred Lightning Network for the messages it pushes and the subscriptions
it holds. Without an unlocked LN wallet and an open channel that can pay
the server, the client cannot operate.

## Configuration

Three keys, in the `[dcrlnd options]` section of `brclientd.conf` or as
flags:

```
brclientd \
  --dcrlnd.rpchost=127.0.0.1:10009 \
  --dcrlnd.tlscertpath=~/.dcrlnd/tls.cert \
  --dcrlnd.macaroonpath=~/.dcrlnd/data/chain/decred/mainnet/admin.macaroon
```

The macaroon must authorize payments; dcrlnd's admin macaroon works.

## Startup gates

brclientd starts its status endpoint immediately, then holds the Bison
Relay client back until dcrlnd is actually usable. Three gates, in order:

1. Connect: the dcrlnd gRPC endpoint answers.
2. Unlocked: the dcrlnd wallet is unlocked and synced to the chain.
3. Channel: an active channel able to reach the relay server exists.

The gates poll and wait instead of failing. Missing TLS certificate or
macaroon files at startup are not fatal either; brclientd waits for them
to appear. This lets it come up alongside a fresh dcrlnd that has not
finished its own wallet setup, without crash-looping. `GET /status` on
port 7677 reports which gate the daemon is currently waiting on.
