# RPC glossary

brclientd exposes two network surfaces. Both use TLS with the certificate
triplet auto-generated at `<datadir>/rpc/` on first run.

| Surface | Default port | Protocol |
| ------- | ------------ | -------- |
| clientrpc | 7676 | JSON-RPC 2.0 over WebSocket at `/ws`, mTLS |
| status REST | 7677 | HTTPS + JSON |

This page is a glossary. It names every endpoint and what it does, not the
request and response schemas.

## clientrpc (port 7676)

Bison Relay's stock RPC surface, served by the upstream
[clientrpc](https://github.com/companyzero/bisonrelay/tree/master/clientrpc)
package. Clients must present the client certificate (mTLS); set
`clientrpc.issueclientcert=1` to have brclientd generate the
`rpc-client.cert` / `rpc-client.key` pair on first run.

Registered services:

| Service | Purpose |
| ------- | ------- |
| VersionService | daemon and protocol version information |
| ChatService | private messages, group chat messages, KX, invites, event streams |
| GCService | group chat administration |
| PostsService | posts, comments, subscriptions |
| PaymentsService | Lightning tips |
| ContentService | file sharing and downloads |
| ResourcesService | fetching resources (pages) from peers |

Message and stream definitions live in the upstream repository under
`clientrpc/`.

### Identity bootstrap

Before an identity exists, port 7676 serves a small pre-setup API instead
of clientrpc:

| Endpoint | Purpose |
| -------- | ------- |
| `POST /create-identity` | create the Bison Relay identity (nick, name) |
| `POST /restore-backup` | stage a full-state backup archive to restore instead |

As soon as one of them succeeds, the daemon finishes startup and the
listener swaps to clientrpc.

## Status REST (port 7677)

A companion API for frontends and supervisors. TLS with the same
certificate triplet. Returns JSON; errors use plain HTTP status codes.

### Daemon and node

| Route | Purpose |
| ----- | ------- |
| `/status` | startup stage, gate state, connection state, LN wallet usability |
| `/version` | daemon version |
| `/connection` | server policy and connection state; go online or remain offline |
| `/backup` | download a consistent full-state backup archive |

### Identity and profile

| Route | Purpose |
| ----- | ------- |
| `/public-identity` | the local node's public identity |
| `/avatar` | set the profile avatar |

### Contacts

| Route | Purpose |
| ----- | ------- |
| `/contacts` | address book: identities, aliases, KX timestamps, avatars |
| `/contacts/rename` | set a local alias for a contact |
| `/contacts/block` | block a contact |
| `/contacts/unblock` | unblock a contact |
| `/contacts/blocked` | list blocked contacts |
| `/contacts/ignore` | set or clear the ignored flag on a contact |
| `/contacts/handshake` | start a handshake to verify the ratchet |
| `/contacts/kx-reset` | reset the ratchet with a contact |
| `/contacts/reset-all` | reset the ratchet with all contacts |
| `/contacts/trans-reset` | reset a broken ratchet through a mediator contact |
| `/contacts/suggest-kx` | suggest that one contact key-exchange with another |
| `/contacts/accept-suggestion` | accept a received KX suggestion |
| `/contacts/subscribe-posts` | subscribe to a contact's posts |
| `/contacts/unsubscribe-posts` | unsubscribe from a contact's posts |
| `/contacts/fetch-post` | fetch a specific post from a contact |
| `/contacts/list-content` | ask a contact for their shared file list |
| `/contacts/list-posts` | ask a contact for their post list |
| `/contacts/groups` | list and save local contact groups |
| `/contacts/groups/assign` | assign contacts to a group |
| `/contacts/groups/settings` | per-group settings |

### Key exchange

| Route | Purpose |
| ----- | ------- |
| `/kx/list` | outstanding KX attempts |
| `/kx/mediateids` | list or cancel mediated introduction requests |
| `/kx/searches` | outstanding searches for a post author across the network |

### Invites

| Route | Purpose |
| ----- | ------- |
| `/invites/create` | create an invite to connect with a peer |
| `/invites/accept` | accept an invite blob |
| `/invites/redeem-key` | fetch, decrypt and accept a prepaid invite key |

### Messaging

| Route | Purpose |
| ----- | ------- |
| `/messages/send` | send a private message |
| `/history/pm` | paginated private message history |
| `/history/pm/clear` | clear stored private message history |
| `/files/send` | send a file to a user (multipart upload) |

### Group chats

| Route | Purpose |
| ----- | ------- |
| `/gc`, `/gc/...` | group chat surface: list, create, invite, join, members, messages, admin actions |

### Posts

| Route | Purpose |
| ----- | ------- |
| `/posts/feed` | aggregated feed of subscribed posts |
| `/posts/new` | create a post |
| `/posts/body` | full body of a post |
| `/posts/relay` | relay a post to one user or to all subscribers |
| `/posts/subscribe-all` | subscribe to the posts of all contacts |
| `/posts/heart` | heart or unheart a post |
| `/posts/hearts` | list hearts on a post |
| `/posts/comment` | comment on a post |
| `/posts/comments` | list comments on a post |
| `/posts/receivereceipts` | receive receipts for a post |
| `/posts/comment-receivereceipts` | receive receipts for a comment |
| `/posts/embed-data` | decoded embedded data from a post |

### Content and downloads

| Route | Purpose |
| ----- | ------- |
| `/content/get` | request the download of a peer's shared file |
| `/content/file` | serve the bytes of a downloaded file |
| `/downloads` | list downloads with progress |
| `/downloads/cancel` | cancel a running download |
| `/downloads/delete` | delete a download record |
| `/shared-files` | list files shared by the local node |
| `/shared-files/add` | share a local file |
| `/shared-files/remove` | stop sharing a file |

### Pages

| Route | Purpose |
| ----- | ------- |
| `/pages/fetch` | fetch a page from a peer over the relay |
| `/pages/local` | list locally hosted pages |
| `/pages/local/save` | save a local page |
| `/pages/local/delete` | delete a local page |
| `/pages/local/file` | raw bytes of a local page file |

### Store

Endpoints for the optional simplestore (see `[simplestore options]` in the
config file).

| Route | Purpose |
| ----- | ------- |
| `/store/mode` | switch resource hosting between pages and the store |
| `/store/products` | list and save products |
| `/store/products/delete` | delete a product |
| `/store/orders` | list orders |
| `/store/orders/status` | update an order's status |
| `/store/orders/comment` | send a comment on an order to the buyer |
| `/store/templates` | list store templates |
| `/store/templates/save` | save a template |
| `/store/templates/delete` | delete a template |
| `/store/templates/file` | raw bytes of a template file |
| `/store/files/list` | list store media files |
| `/store/files/get` | fetch a store media file |
| `/store/files/upload` | upload a store media file |
| `/store/files/delete` | delete a store media file |

### Payments

| Route | Purpose |
| ----- | ------- |
| `/tip` | send a Lightning tip to a user |
| `/payments/tips` | tip attempt history |
| `/payments/tips/running` | in-flight tip attempts |
| `/rates` | DCR exchange rates |

### Stats

| Route | Purpose |
| ----- | ------- |
| `/stats/overview` | node activity summary |
| `/stats/contacts` | per-contact activity |
| `/stats/network` | relay network counters |
| `/stats/posts` | post activity |
| `/stats/payments` | payment totals |
| `/stats/payments/clear` | clear payment statistics |

### Settings

| Route | Purpose |
| ----- | ------- |
| `/settings/behavior` | runtime behavior settings (receipts, compression, auto-handshake) |
| `/filters` | list and save content filters |
| `/filters/delete` | delete a content filter |

### Notifications

| Route | Purpose |
| ----- | ------- |
| `/notifications` | list notifications |
| `/notifications/recent` | most recent notifications |
| `/notifications/clear` | clear notifications |
| `/notifications/delete` | delete a notification |

### Realtime sessions

| Route | Purpose |
| ----- | ------- |
| `/rtdt/sessions`, `/rtdt/sessions/...` | realtime (RTDT) sessions: list, create, invite, accept, chat |
