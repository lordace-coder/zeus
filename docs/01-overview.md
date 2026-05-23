# Zeus — Overview

## What is Zeus?

Zeus is a **high-performance binary-protocol server** that handles four things
that are genuinely hard to build yourself:

| Feature | What it does |
|---|---|
| **Cache** | Blazing-fast in-memory key/value store (like Redis) |
| **Channels** | Realtime pub/sub fan-out to thousands of subscribers |
| **Queues** | Reliable command delivery with ACK, retry, and failure tracking |
| **Chat** | Full chat room system with presence, history, polls, and delivery receipts |

You connect to Zeus over a plain TCP socket (or TLS) and speak its binary
frame protocol. Zeus is **not HTTP** — it is a persistent connection server,
meaning one TCP connection stays open and Zeus pushes data to you in real time.

---

## Why binary protocol?

Most tools use text protocols (HTTP, WebSocket, Redis RESP). Text is easy to
read but wasteful — every field is serialised as ASCII with delimiters.

Zeus uses a **15-byte binary header** followed by raw bytes. This means:

- A typical cache GET response is **~20 bytes** total
- A chat message push is **30–100 bytes** depending on payload
- You can send and receive **millions of messages per second** on modest hardware
- It works great for chat apps, IoT feeds, game state sync, and microservice buses

---

## How Zeus fits into your stack

```
Your clients (mobile, web, desktop)
         │
         │  TCP / TLS  (binary protocol)
         ▼
    ┌─────────┐
    │  ZEUS   │ ◄── zeus.yaml controls everything
    └─────────┘
         │
         │  HTTP webhooks (optional, HMAC-signed)
         ▼
  Your backend API / database
```

Zeus handles **realtime delivery**. Your backend handles **auth, business
logic, and long-term storage**. They talk through webhooks — Zeus fires a
signed HTTP POST to your backend whenever something happens (message sent,
user joined, queue item failed, etc.).

This means **zero lock-in**. Your data lives in your database. Zeus is a
layer you can swap out without losing anything.

---

## Feature map

```
┌──────────────────────────────────────────────────────────────┐
│  CACHE                                                       │
│  GET / SET / DELETE / CLEAR  +  optional TTL per key         │
│  256-shard concurrent design — ~250x less lock contention    │
│  Optional SQLite backup (survives restarts)                  │
├──────────────────────────────────────────────────────────────┤
│  CHANNELS  (Pub/Sub)                                         │
│  Subscribe to a named topic → receive every published msg    │
│  History replay on join  •  Retain last message flag         │
│  Back-pressure: slow subscriber drops silently               │
├──────────────────────────────────────────────────────────────┤
│  QUEUES  (Reliable delivery)                                 │
│  Push → Deliver → ACK (done) / NACK (retry)                 │
│  Exponential back-off  •  Dead-letter after max attempts     │
│  State survives server restart (SQLite-backed)               │
├──────────────────────────────────────────────────────────────┤
│  CHAT ROOMS                                                  │
│  Join / Leave / Send / History / Presence                    │
│  Optional: receipt ticks  •  polls  •  user metadata        │
│  Optional: smart delivery (only push what you missed)        │
│  Optional: smart GC (delete only fully-read old messages)    │
│  Webhooks → your backend on every event                      │
├──────────────────────────────────────────────────────────────┤
│  SECURITY                                                    │
│  Token auth (auto-generated)  •  Optional TLS (auto cert)   │
│  HMAC-SHA256 webhook signatures                              │
└──────────────────────────────────────────────────────────────┘
```

---

## When should you use Zeus?

### ✅ Use Zeus when you need...

---

### 💬 A chat system without building one from scratch

You're building a messaging feature — group chats, support inbox, team DMs.
You don't want to wire up WebSockets, handle reconnects, track "delivered"
state, or build a fan-out engine yourself.

**Zeus gives you:** rooms, presence, history, delivery receipts, polls, and
webhooks to your backend — in one binary you run alongside your app.

> Examples: team collaboration tools, customer support chat, in-app DMs,
> community rooms, live event Q&A

---

### ⚡ Realtime updates pushed to clients

Your users need to see changes the moment they happen — no polling, no
refresh. Think live dashboards, price feeds, activity notifications, or
collaborative editing cursors.

**Zeus gives you:** named channels with fan-out. Publish once, every
subscriber gets it instantly.

> Examples: live sports scores, trading dashboards, IoT sensor feeds,
> notification bells, multiplayer game state

---

### 📬 A reliable job/task queue

You need to hand off work to background workers — send emails, process
uploads, call third-party APIs. If the worker crashes, the job must retry,
not disappear.

**Zeus gives you:** push-once queues with ACK/NACK, exponential backoff,
dead-letter storage, and round-robin delivery to multiple workers.

> Examples: email sending, image resizing, payment webhooks, report
> generation, SMS dispatch

---

### 🚀 A fast shared cache between services

Multiple services need to share state — session tokens, feature flags,
rate-limit counters, computed results — without spinning up a full Redis
deployment.

**Zeus gives you:** an in-memory key/value cache with optional TTL, optional
SQLite persistence, and a 256-shard design so it doesn't bottleneck under
concurrent load.

> Examples: session caching, feature flag store, API response cache,
> distributed rate limiting

---

### 🔌 Plugging realtime into an existing backend

You have a backend (Node, Go, Python, whatever) and want to add realtime
without rewriting it. You want events from clients, but your business logic
stays in your own code.

**Zeus gives you:** webhook callbacks on every event — HMAC-signed so your
backend can trust them. Your backend stays in control; Zeus is just the
realtime delivery layer.

> Examples: adding live notifications to a REST API, extending a CMS with
> live preview, pushing events from a mobile backend

---

### ❌ Don't use Zeus when...

| Situation | Better fit |
|---|---|
| You need HTTP/WebSocket for browser clients directly | Use a WebSocket server or SSE endpoint in your API |
| You need complex SQL queries on your chat history | Store messages in your own DB via webhooks |
| You need per-user permission checks on every message | Handle in your backend webhook handler |
| You need a message broker with topics, partitions, and consumer groups | Kafka or NATS |
| You need a managed cloud service with zero ops | Firebase, Ably, Pusher |

Zeus is a **self-hosted binary** — you run it, you own it. That's the
trade-off: full control and zero per-message cost, but you manage the
process.

---

## Documentation index

| File | Contents |
|---|---|
| **01-overview.md** | This file — what Zeus is and how it fits in |
| **02-getting-started.md** | Install, first run, config walkthrough |
| **03-protocol.md** | Binary frame format, all opcodes, status codes |
| **04-cache.md** | Cache operations, TTL, sharding, persistence |
| **05-channels.md** | Pub/Sub channels, history, retain flag |
| **06-queues.md** | Reliable queues, ACK/NACK, retry, dead-letter |
| **07-chat.md** | Chat rooms, presence, all optional features |
| **08-security.md** | Token auth, TLS, webhook signatures |
| **09-config-reference.md** | Every zeus.yaml field explained |
| **10-project-structure.md** | Codebase layout, what each file does |
