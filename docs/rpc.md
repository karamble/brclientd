# RPC glossary

brclientd exposes three network surfaces. clientrpc and the status REST
use TLS with the certificate triplet auto-generated at `<datadir>/rpc/`
on first run; the MCP listener is plain HTTP guarded by a bearer token.

| Surface | Default port | Protocol |
| ------- | ------------ | -------- |
| clientrpc | 7676 | JSON-RPC 2.0 over WebSocket at `/ws`, mTLS |
| status REST | 7677 | HTTPS + JSON |
| MCP client | 8891 | streamable HTTP (MCP), bearer token |

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

Routes are method-qualified. A request with a method a route does not
accept answers 405 with the body `Method Not Allowed` and an `Allow`
header listing the accepted methods, sorted and comma-space joined;
every `Allow` list that contains GET also contains HEAD, and HEAD is
accepted wherever a GET route exists, except `GET /backup`. The
trailing-slash forms `/gc/` and `/rtdt/sessions/` are gone. Paths
containing `..` or `//` answer 400 instead of redirecting to the
cleaned path.

### Daemon and node

| Route | Purpose |
| ----- | ------- |
| `GET /status` | startup stage, gate state, connection state, LN wallet usability |
| `GET /version` | daemon version |
| `GET /connection`, `POST /connection` | server policy and connection state; go online or remain offline |
| `GET /backup` | download a consistent full-state backup archive |

### Identity and profile

| Route | Purpose |
| ----- | ------- |
| `GET /public-identity` | the local node's public identity |
| `POST /avatar` | set the profile avatar |

### Contacts

| Route | Purpose |
| ----- | ------- |
| `GET /contacts` | address book: identities, aliases, KX timestamps, avatars |
| `POST /contacts/rename` | set a local alias for a contact |
| `POST /contacts/block` | block a contact |
| `POST /contacts/unblock` | unblock a contact |
| `GET /contacts/blocked` | list blocked contacts |
| `POST /contacts/ignore` | set or clear the ignored flag on a contact |
| `POST /contacts/handshake` | start a handshake to verify the ratchet |
| `POST /contacts/kx-reset` | reset the ratchet with a contact |
| `POST /contacts/reset-all` | reset the ratchet with all contacts |
| `POST /contacts/trans-reset` | reset a broken ratchet through a mediator contact |
| `POST /contacts/suggest-kx` | suggest that one contact key-exchange with another |
| `POST /contacts/accept-suggestion` | accept a received KX suggestion |
| `POST /contacts/subscribe-posts` | subscribe to a contact's posts |
| `POST /contacts/unsubscribe-posts` | unsubscribe from a contact's posts |
| `POST /contacts/fetch-post` | fetch a specific post from a contact |
| `POST /contacts/list-content` | ask a contact for their shared file list |
| `POST /contacts/list-posts` | ask a contact for their post list |
| `GET /contacts/groups`, `POST /contacts/groups` | list and save local contact groups |
| `POST /contacts/groups/assign` | assign contacts to a group |
| `POST /contacts/groups/settings` | per-group settings |

### Key exchange

| Route | Purpose |
| ----- | ------- |
| `GET /kx/list` | outstanding KX attempts |
| `GET /kx/mediateids`, `POST /kx/mediateids` | list or cancel mediated introduction requests |
| `GET /kx/searches` | outstanding searches for a post author across the network |

### Invites

| Route | Purpose |
| ----- | ------- |
| `POST /invites/create` | create an invite to connect with a peer |
| `POST /invites/accept` | accept an invite blob |
| `POST /invites/redeem-key` | fetch, decrypt and accept a prepaid invite key |

### Messaging

| Route | Purpose |
| ----- | ------- |
| `POST /messages/send` | send a private message |
| `GET /history/pm` | paginated private message history |
| `POST /history/pm/clear` | clear stored private message history |
| `POST /files/send` | send a file to a user (multipart upload) |

### Group chats

A `{gcid}` value is the group chat's 64-character hex ID.

| Route | Purpose |
| ----- | ------- |
| `GET /gc` | list group chats |
| `POST /gc/create` | create a group chat |
| `GET /gc/invites` | list received group chat invites |
| `POST /gc/invites/accept` | accept a group chat invite |
| `GET /gc/{gcid}` | group chat detail: members, version, admins |
| `POST /gc/{gcid}/invite` | invite a user to the group chat |
| `POST /gc/{gcid}/message` | send a message to the group chat |
| `GET /gc/{gcid}/history` | paginated group chat message history |
| `POST /gc/{gcid}/history/clear` | permanently delete the stored scrollback |
| `POST /gc/{gcid}/part` | leave the group chat |
| `POST /gc/{gcid}/kill` | dissolve the group chat (owner only) |
| `POST /gc/{gcid}/kick` | kick a member |
| `POST /gc/{gcid}/block` | block a member's messages in this group chat |
| `POST /gc/{gcid}/unblock` | unblock a member |
| `POST /gc/{gcid}/admins` | modify the extra admins list |
| `POST /gc/{gcid}/owner` | transfer group chat ownership |
| `POST /gc/{gcid}/upgrade` | upgrade the group chat version |
| `POST /gc/{gcid}/alias` | set a local alias for the group chat |
| `POST /gc/{gcid}/resend-list` | resend the member list to one member or all |

### Posts

| Route | Purpose |
| ----- | ------- |
| `GET /posts/feed` | aggregated feed of subscribed posts |
| `POST /posts/new` | create a post |
| `GET /posts/body` | full body of a post |
| `POST /posts/relay` | relay a post to one user or to all subscribers |
| `POST /posts/subscribe-all` | subscribe to the posts of all contacts |
| `POST /posts/heart` | heart or unheart a post |
| `GET /posts/hearts` | list hearts on a post |
| `POST /posts/comment` | comment on a post |
| `GET /posts/comments` | list comments on a post |
| `GET /posts/receivereceipts` | receive receipts for a post |
| `GET /posts/comment-receivereceipts` | receive receipts for a comment |
| `GET /posts/embed-data` | decoded embedded data from a post |

### Content and downloads

| Route | Purpose |
| ----- | ------- |
| `POST /content/get` | request the download of a peer's shared file |
| `GET /content/file` | serve the bytes of a downloaded file |
| `GET /downloads` | list downloads with progress |
| `POST /downloads/cancel` | cancel a running download |
| `POST /downloads/delete` | delete a download record |
| `GET /shared-files` | list files shared by the local node |
| `POST /shared-files/add` | share a local file |
| `POST /shared-files/remove` | stop sharing a file |

### Pages

| Route | Purpose |
| ----- | ------- |
| `POST /pages/fetch` | fetch a page from a peer over the relay |
| `GET /pages/local` | list locally hosted pages |
| `POST /pages/local/save` | save a local page |
| `POST /pages/local/delete` | delete a local page |
| `GET /pages/local/file` | raw bytes of a local page file |

### Store

Endpoints for the optional simplestore (see `[simplestore options]` in the
config file).

| Route | Purpose |
| ----- | ------- |
| `GET /store/mode`, `POST /store/mode` | switch resource hosting between pages and the store |
| `GET /store/products`, `POST /store/products` | list and save products |
| `POST /store/products/delete` | delete a product |
| `GET /store/orders` | list orders |
| `POST /store/orders/status` | update an order's status |
| `POST /store/orders/comment` | send a comment on an order to the buyer |
| `GET /store/templates` | list store templates |
| `POST /store/templates/save` | save a template |
| `POST /store/templates/delete` | delete a template |
| `GET /store/templates/file` | raw bytes of a template file |
| `GET /store/files/list` | list store media files |
| `GET /store/files/get` | fetch a store media file |
| `POST /store/files/upload` | upload a store media file |
| `POST /store/files/delete` | delete a store media file |

### Payments

| Route | Purpose |
| ----- | ------- |
| `POST /tip` | send a Lightning tip to a user |
| `GET /payments/tips` | tip attempt history |
| `GET /payments/tips/running` | in-flight tip attempts |
| `GET /rates` | DCR exchange rates |

### Stats

| Route | Purpose |
| ----- | ------- |
| `GET /stats/overview` | node activity summary |
| `GET /stats/contacts` | per-contact activity |
| `GET /stats/network` | relay network counters |
| `GET /stats/posts` | post activity |
| `GET /stats/payments` | payment totals |
| `POST /stats/payments/clear` | clear payment statistics |

### Settings

| Route | Purpose |
| ----- | ------- |
| `GET /settings/behavior`, `POST /settings/behavior` | runtime behavior settings (receipts, compression, auto-handshake) |
| `GET /filters`, `POST /filters` | list and save content filters |
| `POST /filters/delete` | delete a content filter |

### MCP client

| Route | Purpose |
| ----- | ------- |
| `GET /settings/mcpclient`, `POST /settings/mcpclient` | MCP client settings: enable, token, mode, caps, allowed bots, allowed IPs |
| `GET /mcp/pending` | payments waiting for approval |
| `POST /mcp/pending/resolve` | approve or deny a pending payment |
| `GET /mcp/spend` | recorded payments and spend totals |

### Notifications

| Route | Purpose |
| ----- | ------- |
| `GET /notifications` | list notifications |
| `GET /notifications/recent` | most recent notifications |
| `POST /notifications/clear` | clear notifications |
| `POST /notifications/delete` | delete a notification |

### Realtime sessions

A `{rv}` value is the session's 64-character hex rendezvous ID.

| Route | Purpose |
| ----- | ------- |
| `GET /rtdt/sessions` | list realtime (RTDT) sessions |
| `POST /rtdt/sessions/create` | create a session |
| `POST /rtdt/sessions/create-instant` | create an instant session |
| `POST /rtdt/sessions/{rv}/invite` | invite a user to the session |
| `POST /rtdt/sessions/{rv}/accept` | accept a session invite |
| `POST /rtdt/sessions/{rv}/join` | join the session |
| `POST /rtdt/sessions/{rv}/leave` | leave the session |
| `POST /rtdt/sessions/{rv}/dissolve` | dissolve the session |
| `POST /rtdt/sessions/{rv}/kick` | kick a member |
| `POST /rtdt/sessions/{rv}/remove` | remove a member from the appointment |
| `POST /rtdt/sessions/{rv}/rotate-cookies` | rotate the appointment cookies |
| `GET /rtdt/sessions/{rv}/audio` | binary WebSocket carrying the session's audio |
| `GET /rtdt/sessions/{rv}/messages` | chat messages tracked for a live session |
| `POST /rtdt/sessions/{rv}/chat` | send a text message into a live session |

## MCP client (port 8891)

The BR-MCP client listener is a bridge between AI agents and tool
services offered by Bison Relay bots (see
[brmcp](https://github.com/karamble/brmcp)). The agent-facing side is
standard MCP (Model Context Protocol) over the streamable HTTP
transport, so it is fully compatible with any MCP-capable agent or
client. Connecting takes nothing more than the endpoint URL and the
bearer token; no Bison Relay specifics are required on the agent side.

Each allowed bot gets its own endpoint at `/mcp/<bot-uid>`. The
endpoint mirrors the remote bot's tools as ordinary MCP tools; calls
are relayed over Bison Relay to the bot and the answers come back the
same way. Paid tools settle as Bison Relay tips under the configured
caps, automatically or after explicit approval. The agent never handles
identity, transport, or payments; it just sees tools.

Off by default. Requests must carry the bearer token from the settings,
and `allowed_ips` optionally restricts the source addresses the listener
accepts (single IPs or CIDR ranges; empty means any address) - a request
from anywhere else is answered with the same generic 401 as a bad token.
The settings reply carries `last_denied` (the most recent denied address
and time, cleared by the next successful request) so the dashboard can
offer the observed address for allowing. The listen address comes from
the `[mcp options]` config section (`mcp.mcplisten`, default
`127.0.0.1:8891`) and takes effect on restart; everything else is runtime
configuration via `/settings/mcpclient` on the status REST.
