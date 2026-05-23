# Zeus — Cache

## What is the cache?

The Zeus cache is a **concurrent in-memory key/value store**. Think of it like
Redis but embedded inside Zeus alongside channels, queues, and chat.

- Keys and values are **raw bytes** — store strings, JSON, binary blobs, anything
- Every key can have an optional **TTL** (time-to-live) in seconds
- Expired keys are removed lazily on read and by a background sweep every 30 seconds
- Optional **SQLite backup** means cache data survives server restarts

---

## Operations

### GET

Retrieve a value by key.

```
Op:  OP_GET (0x10)
Key: "user:1234:session"
Body: (empty)
```

**Response:**
- `STATUS_OK` + value bytes → key exists
- `STATUS_NOT_FOUND` → key doesn't exist or has expired

---

### SET

Store a value under a key with an optional TTL.

```
Op:   OP_SET (0x11)
Key:  "user:1234:session"
Body: [ttl:4 bytes][value bytes]
```

The first 4 bytes of the body are a **uint32 big-endian TTL in seconds**.
- `0` = no expiry (key lives forever until deleted or server clears)
- `3600` = expires in 1 hour
- `86400` = expires in 1 day

The rest of the bytes are the value.

**Example — store a session token that expires in 1 hour:**
```
Body bytes: 00 00 0E 10  [then your value bytes]
            ^^^^^^^^^
            3600 in big-endian hex
```

**Response:** `STATUS_OK` (empty payload)

---

### DELETE

Remove a single key.

```
Op:  OP_DELETE (0x12)
Key: "user:1234:session"
Body: (empty)
```

**Response:** `STATUS_OK` — always succeeds, even if the key didn't exist.

---

### CLEAR

Remove **all** keys from the cache. Use carefully.

```
Op:  OP_CLEAR (0x13)
Key: (empty)
Body: (empty)
```

**Response:** `STATUS_OK`

---

## TTL behaviour

- When a key expires, Zeus does **not** immediately remove it from memory
- On the next `GET` for that key, Zeus checks the expiry, removes it, and returns `STATUS_NOT_FOUND`
- A background goroutine sweeps for expired keys every **30 seconds**, keeping memory clean

---

## Persistence (SQLite backup)

When `persistence.enabled: true` in zeus.yaml, every cache change is
**asynchronously mirrored** to SQLite:

```
Cache SET → value stored in memory
         → async callback → SQLite INSERT OR REPLACE
```

This means:
- SET does not wait for the DB write (fast path stays fast)
- If the server restarts, all keys are loaded back from SQLite on startup
- Expired keys are not restored (Zeus checks TTL during load)

**Enable it:**
```yaml
persistence:
  enabled: true
  db_path: "zeus.db"
  load_on_startup: true
```

---

## Force immediate DB write (FLAG_PERSIST)

By default, cache writes are async. If you need a specific `SET` to be
durable before the response arrives, set the `FLAG_PERSIST` bit in Flags:

```
Flags = 0x08  (FLAG_PERSIST)
Op    = OP_SET
```

Zeus will write to SQLite synchronously before sending the response.
Use only when durability matters more than speed.

---

## Performance: 256-shard design

The cache is split across **256 independent sub-caches**, each with its own
mutex. A key hashes to exactly one shard using FNV-1a.

Why this matters:
```
Single mutex: 1000 goroutines → all queue on 1 lock → ~50µs average wait
256 shards:   1000 goroutines → each shard handles ~4 → ~200ns average wait
                                                         250x improvement
```

This design means Zeus can handle **tens of thousands of cache operations per
second** without lock contention becoming a bottleneck.

You don't need to configure this — it's always on.

---

## Recommended key naming

Zeus doesn't enforce any naming convention, but these patterns work well:

```
user:{id}:session          # per-user session token
user:{id}:profile          # cached profile blob
room:{id}:last_message     # last message in a chat room
rate:{ip}:count            # rate limiting counter
config:feature_flags       # app-wide config blob
```

Using `:` as a separator makes keys easy to group and reason about.

---

## What the cache is NOT

- **Not a database.** Cache data can be lost if persistence is disabled and
  the server restarts. Use queues or your own DB for data that must survive.
- **Not distributed.** One Zeus instance = one cache. For distributed caching,
  run multiple instances and shard by key prefix in your client.
- **Not a search engine.** There is no pattern matching or scan. You must know
  the exact key. Use a naming convention to keep keys organised.
