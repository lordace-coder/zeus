<div align="center">

```
 ███████╗███████╗██╗   ██╗███████╗
 ╚══███╔╝██╔════╝██║   ██║██╔════╝
   ███╔╝ █████╗  ██║   ██║███████╗
  ███╔╝  ██╔══╝  ██║   ██║╚════██║
 ███████╗███████╗╚██████╔╝███████║
 ╚══════╝╚══════╝ ╚═════╝ ╚══════╝
```

**The realtime backbone for serious apps.**  
Cache · Channels · Queues · Chat — one binary, binary protocol, zero fluff.

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![Protocol](https://img.shields.io/badge/Protocol-Binary%20TCP-blueviolet?style=flat-square)]()
[![SQLite](https://img.shields.io/badge/Persistence-SQLite-003B57?style=flat-square&logo=sqlite)](https://sqlite.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)]()

</div>

---

## What is Zeus?

Zeus is a **self-hosted, high-performance TCP server** that gives your app four things that are genuinely hard to build:

| | Feature | In one line |
|---|---|---|
| ⚡ | **Cache** | In-memory key/value store — like Redis, but embedded in your stack |
| 📡 | **Channels** | Pub/sub fan-out — publish once, every subscriber gets it instantly |
| 📬 | **Queues** | Reliable job delivery — ACK, NACK, retry, dead-letter, crash-safe |
| 💬 | **Chat** | Full chat rooms — presence, history, receipts, polls, smart delivery |

Everything is driven by a **binary TCP protocol** and a single `zeus.yaml` config file. No HTTP overhead. No JSON parsing on the hot path. No third-party cloud dependency.

---

## Why not just use X?

| Tool | The gap Zeus fills |
|---|---|
| **Redis** | Redis doesn't do chat, queues with ACK/retry, or webhook callbacks |
| **Firebase** | You don't own your data. Firebase is a cloud lock-in by design. |
| **Pusher / Ably** | Per-message pricing. You pay more as you grow. Zeus is free at any scale. |
| **Kafka** | Kafka is a distributed log. Zeus is a lightweight realtime layer for app servers. |
| **WebSockets (DIY)** | You still need to build everything Zeus already gives you. |

Zeus is for teams that want **full control**, **zero per-message cost**, and **a simple binary to deploy**.

---

## How it fits in your stack

```
┌──────────────────────────────────────────┐
│   Your clients  (mobile / web / desktop) │
└──────────────────┬───────────────────────┘
                   │  TCP / TLS
                   │  binary protocol
                   ▼
           ┌───────────────┐
           │     ZEUS      │  ◄── zeus.yaml controls everything
           │               │
           │  cache        │
           │  channels     │
           │  queues       │
           │  chat rooms   │
           └───────┬───────┘
                   │  HTTP webhooks
                   │  HMAC-signed
                   ▼
     ┌─────────────────────────────┐
     │  Your backend / database    │
     │  (auth, business logic,     │
     │   long-term storage,        │
     │   push notifications)       │
     └─────────────────────────────┘
```

Zeus handles **realtime delivery**. Your backend handles everything else.  
They talk through **HMAC-signed webhooks** — Zeus calls your backend on every event.

**Zero lock-in.** Your data lives in your database. Zeus is a layer you can swap out.

---

## Quickstart

```bash
# 1. Build
git clone https://github.com/you/zeus
cd zeus
go build -o zeus .

# 2. First run — generates zeus.yaml with a random auth token
./zeus init

# 3. Start the server
./zeus
# → [zeus] server listening on 0.0.0.0:7878
# → [zeus] token: a3f8c2e1d4b7...  (copy this to your clients)
```

That's it. Zeus is running on port `7878`, auth token auto-generated.

---

## CLI commands

```bash
./zeus init             # create zeus.yaml with defaults + random token
./zeus token            # print the current auth token
./zeus token rotate     # generate a new token (update clients first!)
./zeus status           # show server config summary
```

---

## The binary protocol

Zeus speaks a **15-byte fixed header** followed by a key and a body:

```
┌────────┬─────────┬──────────┬───────┬─────────────┬────────┬──────────┬──────────┐
│ Magic  │ Version │  OpCode  │ Flags │  RequestID  │ KeyLen │ BodyLen  │ Reserved │
│ 1 byte │ 1 byte  │  1 byte  │ 1 byte│   4 bytes   │ 2 bytes│  4 bytes │  1 byte  │
└────────┴─────────┴──────────┴───────┴─────────────┴────────┴──────────┴──────────┘
followed by: [Key bytes][Body bytes]
```

Every message — in both directions — uses this format. A cache GET round-trip is ~20 bytes total.  
See [docs/03-protocol.md](docs/03-protocol.md) for all opcodes and body formats.

---

## Feature highlights

### ⚡ Cache — 256-shard concurrent design

```
SET  "session:alice"  →  {"role":"admin"}  TTL 3600s
GET  "session:alice"  →  {"role":"admin"}
DEL  "session:alice"
```

- **256 independent shards** — FNV-1a hash, bitmask selection, ~250x less lock contention than a single mutex
- **`sync.Pool` frame buffers** — zero allocation on the hot encode path
- **Optional SQLite persistence** — cache survives server restarts
- **TTL per key** — set expiry in milliseconds; background goroutine evicts expired keys

---

### 📡 Channels — Pub/Sub fan-out

```
SUBSCRIBE  "prices:BTC"            ← start receiving
PUBLISH    "prices:BTC"  → $67,420 ← one publisher, all subscribers get it
UNSUBSCRIBE "prices:BTC"
```

- **Named topics** — unlimited channels up to your configured limit
- **History replay** — new subscribers can receive the last N messages on join
- **Retain flag** — always deliver the last value to new subscribers (like MQTT retain)
- **Back-pressure safe** — slow subscribers are dropped silently; other subscribers are unaffected

---

### 📬 Queues — Reliable delivery with ACK/NACK

```
Producer → PUSH  "send-email"  {"to":"alice@..."}
                     │
Zeus →      DELIVER  to worker
                     │
Worker →    ACK  (processed ✓)   or   NACK  (failed, retry scheduled)
```

- **Exactly-once delivery** — message stays in-flight until ACK'd
- **Exponential back-off** — 1s → 2s → 4s → 8s → 16s (configurable)
- **ACK timeout** — worker crashes? Zeus auto-retries after `ack_timeout_sec`
- **Dead-letter** — after max attempts, message stored in SQLite for inspection
- **Round-robin workers** — spin up multiple consumers, Zeus load-balances automatically
- **Crash-safe** — all queue state persisted to SQLite; messages survive restarts

---

### 💬 Chat — Plug into your backend, not replace it

```
JOIN    "team-general"   ← subscribe to room + get history
SEND    "team-general"   → "hey team 👋"
                             ↓
                    Zeus fans out to all members
                    Zeus fires webhook → your backend
                    Your backend persists + sends push notifications
```

Everything in chat is **opt-in**:

| Feature | Enable | What it does |
|---|---|---|
| `receipt_tracking` | `features.receipt_tracking: true` | Grey/blue delivery ticks per message |
| `smart_delivery` | `features.smart_delivery: true` | On reconnect, only push messages you missed |
| `polls` | `features.polls: true` | Create polls, vote, live results broadcast |
| `user_metadata` | `features.user_metadata: true` | Display name, avatar, role per user per room |
| `gc_after_days` | `features.gc_after_days: 30` | Auto-delete old fully-read messages only |

Start with `receipt_tracking: true` and `smart_delivery: true`. Add the rest when you need them.

---

### 🔒 Security — Three layers

```
Layer 1 — Token auth      every connection must present a token (OP_AUTH)
Layer 2 — TLS             encrypt all traffic (auto self-signed cert, or bring your own)
Layer 3 — Webhook HMAC    X-Zeus-Signature: sha256=<hex> on every webhook call
```

Token is **auto-generated** on first run using `crypto/rand`.  
Webhook signatures use **HMAC-SHA256** — same convention as GitHub webhooks.

---

## Configuration

Zeus is entirely driven by `zeus.yaml`. It's generated on first run with safe defaults.  
Every field has a comment. Here's the important stuff:

```yaml
server:
  port: 7878
  max_connections: 1000

security:
  enabled: true
  token: ""           # auto-generated if blank
  tls:
    enabled: false    # set true in production

persistence:
  enabled: false      # set true to survive restarts
  db_path: "zeus.db"

chat:
  enabled: true
  features:
    receipt_tracking: false   # opt-in: delivery ticks
    smart_delivery: false     # opt-in: catch-up on reconnect
    polls: false              # opt-in: in-chat polls
    user_metadata: false      # opt-in: display names / avatars
    gc_after_days: 0          # opt-in: auto-clean old messages

webhook:
  enabled: false
  url: "https://yourapi.com/zeus/webhook"
  secret: ""          # auto-generated if blank
```

Full reference → [docs/09-config-reference.md](docs/09-config-reference.md)

---

## When to use Zeus

| You need... | Zeus is... |
|---|---|
| 💬 Chat rooms with delivery receipts | ✅ exactly this |
| ⚡ Realtime push to thousands of clients | ✅ exactly this |
| 📬 Background jobs with retry + dead-letter | ✅ exactly this |
| 🚀 Fast shared cache between services | ✅ exactly this |
| 🔌 Realtime layer that calls your existing backend | ✅ exactly this |
| ☁️ Managed cloud, zero ops | ❌ use Firebase / Ably / Pusher |
| 🗄️ Distributed log with partitions | ❌ use Kafka / NATS |

---

## Documentation

| Doc | Contents |
|---|---|
| [01 — Overview](docs/01-overview.md) | What Zeus is, use cases, stack diagram |
| [02 — Getting Started](docs/02-getting-started.md) | Build, init, run, CLI commands |
| [03 — Protocol](docs/03-protocol.md) | Binary frame format, all opcodes |
| [04 — Cache](docs/04-cache.md) | Cache ops, TTL, 256-shard design |
| [05 — Channels](docs/05-channels.md) | Pub/sub, history, retain flag |
| [06 — Queues](docs/06-queues.md) | ACK/NACK, retry, dead-letter |
| [07 — Chat](docs/07-chat.md) | Rooms, receipts, polls, webhooks |
| [08 — Security](docs/08-security.md) | Token auth, TLS, webhook verification |
| [09 — Config Reference](docs/09-config-reference.md) | Every zeus.yaml field |
| [10 — Project Structure](docs/10-project-structure.md) | Codebase layout, data flows |

---

## Project layout

```
zeus/
├── main.go              ← entry point, wires everything
├── zeus.yaml            ← your config (auto-generated)
├── config/              ← loads + saves zeus.yaml
├── protocol/            ← binary frame format, all opcodes
├── security/            ← token auth, TLS, HMAC signing
├── store/               ← 256-shard cache + SQLite persistence
├── pubsub/              ← pub/sub channels
├── queue/               ← reliable queues with ACK/NACK
├── chat/                ← chat rooms + webhooks
├── server/              ← per-connection handlers (all opcodes)
├── cmd/                 ← CLI: init / token / status
└── docs/                ← documentation (10 files)
```

---

## Production deployment

```yaml
# zeus.yaml — production example
server:
  max_connections: 5000

security:
  enabled: true
  tls:
    enabled: true
    auto_gen: false
    cert_file: /etc/ssl/zeus/fullchain.pem
    key_file:  /etc/ssl/zeus/privkey.pem

persistence:
  enabled: true
  db_path: /var/lib/zeus/zeus.db

chat:
  features:
    receipt_tracking: true
    smart_delivery: true
    gc_after_days: 90

webhook:
  enabled: true
  url: https://api.yourapp.com/zeus/webhook
  retry_on_failure: true
```

```bash
# Run as a service
./zeus          # reads zeus.yaml from working directory
                # graceful shutdown on SIGINT / SIGTERM
```

---

<div align="center">

**Built to be fast. Built to be simple. Built to stay out of your way.**

</div>
