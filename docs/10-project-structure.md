# Zeus — Project Structure

## Directory layout

```
zeus/
│
├── main.go                    ← Entry point. Wires everything together.
├── zeus.yaml                  ← Configuration file (auto-generated on first run)
├── go.mod                     ← Go module definition
│
├── config/
│   └── config.go              ← Loads + saves zeus.yaml, auto-generates token
│
├── protocol/
│   └── frame.go               ← Binary frame format, all opcodes, encode/decode
│
├── security/
│   ├── auth.go                ← Token validation, webhook HMAC signing
│   └── tls.go                 ← Auto-generates self-signed TLS cert if needed
│
├── store/
│   ├── cache.go               ← In-memory key/value store with TTL
│   ├── sharded_cache.go       ← 256-shard concurrent cache + frame pool
│   └── db.go                  ← SQLite: cache, queues, chat, receipts, polls
│
├── pubsub/
│   └── channel.go             ← Pub/sub channels, history ring, fan-out delivery
│
├── queue/
│   └── queue.go               ← Reliable delivery queues, ACK/NACK, retry, DB wiring
│
├── chat/
│   └── room.go                ← Chat rooms, presence, webhooks, all optional features
│
├── server/
│   └── handler.go             ← Per-connection goroutines, all opcode handlers
│
├── cmd/
│   └── cli.go                 ← zeus init / token / token rotate / status
│
└── docs/
    ├── 01-overview.md
    ├── 02-getting-started.md
    ├── 03-protocol.md
    ├── 04-cache.md
    ├── 05-channels.md
    ├── 06-queues.md
    ├── 07-chat.md
    ├── 08-security.md
    ├── 09-config-reference.md
    └── 10-project-structure.md  ← this file
```

---

## File-by-file breakdown

---

### `main.go`

The entry point and **wiring layer**. It:

1. Parses CLI subcommands (`init`, `token`, `status`) — delegates to `cmd/`
2. Loads `zeus.yaml` via `config.Load()`
3. Opens the SQLite DB if persistence is enabled
4. Creates the sharded cache and hooks it to the DB
5. Creates all subsystems (channels, queues, chat manager)
6. Starts the GC loop (if enabled)
7. Creates the `server.Server` with all subsystems injected
8. Starts the TCP (or TLS) listener
9. Accepts connections in a loop, spawning `srv.HandleConn()` per connection
10. Handles graceful shutdown on SIGINT / SIGTERM

**Rule:** `main.go` wires things together but contains no business logic.

---

### `config/config.go`

Loads and saves `zeus.yaml`.

- `Load(path)` — reads the file. If missing, calls `defaultConfig()`,
  generates a random token + webhook secret, writes the file, returns `(cfg, firstRun=true, nil)`
- `Save(cfg, path)` — marshals back to YAML and writes with a timestamp header
- All config structs are defined here with `yaml:"..."` tags
- Helper methods: `Addr()`, `ReadTimeout()`, `WriteTimeout()`, `AckTimeout()`

---

### `protocol/frame.go`

The binary wire format.

- `Frame` struct — one message (op, flags, requestID, key, body)
- `Frame.Encode(w)` — writes a frame to any `io.Writer`
- `Decode(r)` — reads exactly one frame from any `io.Reader`
- All OpCode constants (`OP_AUTH`, `OP_GET`, ..., `OP_PUSH_CHAT`, ...)
- All StatusCode constants (`STATUS_OK`, `STATUS_NOT_FOUND`, ...)
- Helper constructors: `OKResponse()`, `ErrorResponse()`, `PushFrame()`

**Every single network message — in both directions — goes through this file.**

---

### `security/auth.go`

- `Auth` struct — holds `Enabled` flag and `Token` string
- `Auth.Validate(token)` — constant-time comparison (prevents timing attacks)
- `SignBody(secret, body)` — HMAC-SHA256 for webhook signatures
- `VerifySignature(secret, body, sig)` — verifies a webhook signature

### `security/tls.go`

- `BuildTLSConfig(cfg)` — returns a `*tls.Config` for `tls.NewListener`
- `ensureSelfSignedCert(certFile, keyFile)` — generates an ECDSA P-256 cert
  if the files don't exist. Does nothing if they already exist (safe on restart).

---

### `store/cache.go`

The base in-memory cache.

- `CacheStore` interface — `Set`, `Get`, `Delete`, `Clear`, `Len`, `Keys`, `LoadBulk`
  Both `Cache` and `ShardedCache` implement this.
- `Cache` struct — `sync.RWMutex` + `map[string]*Entry`
- `Entry` struct — `Value []byte` + `ExpiresAt time.Time`
- Background goroutine runs `evictExpired()` every 30 seconds
- `OnChange func(ChangeEvent)` callback — called async on every write/delete

### `store/sharded_cache.go`

The high-performance 256-shard cache layer.

- `ShardedCache` — array of 256 `*Cache` instances
- `shard(key)` — FNV-1a hash → shard index (bitmask, never a modulo)
- `SetOnChange(fn)` — wires the same callback into all 256 shards
- `GetFrameBuf()` / `PutFrameBuf()` — `sync.Pool` for zero-allocation frame encoding
- `BatchWriter` — coalesces multiple SQLite writes into one transaction (75x faster at scale)

### `store/db.go`

All SQLite operations, grouped by subsystem.

**Schema tables:**
- `cache_entries` — key/value/ttl
- `queue_messages` — payload/status/attempts/retry schedule
- `chat_messages` — full message history with type/metadata/delivery_state/soft-delete
- `chat_receipts` — per-user per-message sent/delivered/read state
- `chat_polls` + `chat_poll_votes` — poll definitions and votes
- `chat_user_meta` — per-user per-room JSON metadata

Key design decisions:
- `migrate()` only uses `CREATE TABLE IF NOT EXISTS` — safe to run on every startup
- `PRAGMA journal_mode=WAL` for better concurrent read performance
- Receipts use `MAX(excluded.state, state)` — delivery state never goes backwards
- GC only deletes messages where every receipt is `state >= 2` (everyone read it)

---

### `pubsub/channel.go`

The pub/sub channel system.

- `Channel` — one named topic. Has a subscriber map + history ring + retained message.
- `Channel.Subscribe(sub, replay)` — adds subscriber, optionally replays history
- `Channel.Publish(payload, retain)` — broadcasts to all subscribers, no lock held during sends
- `Subscriber` — has a `Send chan *Message` (buffered 256). Connection handler drains this.
- `Manager` — manages all channels, enforces `max_channels` limit

**Back-pressure:** if a subscriber's channel buffer is full during `Publish()`,
the message is skipped for that subscriber via a non-blocking `select`. Other
subscribers are not affected.

---

### `queue/queue.go`

The reliable delivery queue system.

- `Queue` — pending slice + inFlight map + consumer list + ACK timers
- `Queue.Push(payload)` — adds message, calls `tryDeliver()`
- `Queue.Restore(msg)` — injects a pre-built message (used on startup from DB)
- `Queue.AddConsumer(c)` — registers worker, triggers delivery
- `Queue.Ack(msgID)` — removes from inFlight, cancels timer, notifies DB
- `Queue.Nack(msgID, err)` — schedules retry with exponential back-off
- `Queue.tryDeliver()` — round-robin delivery to available consumers
- `Manager.SetDB(db)` — wires persistence into all current + future queues
- `buildQueuePersistFn(db)` — returns the `OnPersist` callback that maps
  queue ops (enqueue/ack/nack/dead) to DB operations

**ACK timeout:** each delivery starts a `time.AfterFunc` timer. If it fires
before ACK/NACK, it calls `Nack()` with "ack timeout exceeded".

---

### `chat/room.go`

The chat room system + webhook client.

- `Room` — member map + history ring + feature flags
- `Room.Join(clientID, userID, bufSize)` — adds member, returns send channel + history snapshot
- `Room.Send(...)` — updates last-active, appends to ring, delivers to all members
- `Room.SetUserMeta(clientID, meta)` — updates in-memory metadata
- `Features` struct — all optional feature flags passed down from config

- `WebhookClient` — HTTP POST with HMAC signing + retry queue
- `webhookRetryJob` + `retryWorker()` — exponential back-off retry for failed POSTs
- `Manager` — manages all rooms, wires webhook calls, runs presence loop

---

### `server/handler.go`

The connection handler — the most complex file.

**Per connection:**
- `Client` struct — id, conn, auth state, userID, send channel, subscription maps
- `client.run()` — starts writer goroutine, reads frames in a loop
- `client.writeLoop()` — drains `send` channel, writes to TCP with write deadlines
- `client.dispatch(frame)` ��� routes on OpCode, all handlers return `error`
- `client.cleanup()` — on disconnect: unsubscribes channels, re-queues consumers, leaves chat rooms

**Handlers by group:**
- Auth: `handleAuth` — validates token, sets `c.authed`, returns connection ID
- Cache: `handleGet`, `handleSet`, `handleDelete`, `handleClear`
- Channels: `handleSubscribe`, `handleUnsubscribe`, `handlePublish`
- Queues: `handleQueuePush`, `handleQueueConsume`, `handleQueueAck`, `handleQueueNack`
- Chat core: `handleChatJoin`, `handleChatLeave`, `handleChatMessage`, `handleChatHistory`, `handleChatPresence`
- Chat receipts: `handleChatMarkDelivered`, `handleChatMarkRead`, `broadcastReceiptUpdate`
- Chat edit/delete: `handleChatEditMessage`, `handleChatDeleteMessage`
- Chat polls: `handleChatPollCreate`, `handleChatPollVote`, `handleChatPollResults`
- Chat metadata: `handleChatSetMeta`, `handleChatGetMeta`
- Smart delivery: `smartDeliveryCatchUp`
- Feature guards: `requireDB`, `requireReceiptTracking`, `requirePolls`, `requireUserMetadata`

---

### `cmd/cli.go`

The command-line interface.

- `Run(args)` — dispatches on subcommand name
- `cmdInit()` — calls `config.Load()` which creates the file on first run
- `cmdToken()` — reads and prints the current token
- `cmdTokenRotate()` — generates a new token, saves, prints old + new
- `cmdStatus()` — loads config and prints a human-readable summary

---

## Data flow diagrams

### Cache GET

```
Client TCP ──► Decode(frame) ──► dispatch(OP_GET)
                                       │
                              cache.Get(key)
                                       │
                              ◄── OKResponse(value)
                                       │
             ◄── Encode(frame) ──── send channel ──── writeLoop ──► TCP
```

### Chat message send

```
Client TCP ──► dispatch(OP_CHAT_MESSAGE)
                       │
              chat.Send(room, ...)       ← fan-out to all member send channels
                       │
              db.SaveChatMessage(...)    ← persist if enabled
                       │
              OKResponse(msgID)
                       │
              ◄── TCP

  (Meanwhile, every room member's goroutine)
  member.send ──► Encode(OP_PUSH_CHAT) ──► member's TCP connection
```

### Queue message lifecycle

```
Producer: OP_QUEUE_PUSH
    │
queue.Push() ──► db.SaveQueueMessage() ──► pending slice
    │
queue.tryDeliver() ──► consumer.Deliver channel
    │
Server: OP_PUSH_QUEUE ──► Consumer TCP

Consumer: OP_QUEUE_ACK
    │
queue.Ack() ──► db.DeleteQueueMessage() ──► message gone ✓

Consumer: OP_QUEUE_NACK
    │
queue.Nack() ──► db.MarkQueueMessageFailed(nextRetry)
    │
time.AfterFunc(delay) ──► tryDeliver() ──► retry
```
