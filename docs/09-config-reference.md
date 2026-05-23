# Zeus — Configuration Reference

Every Zeus behaviour is controlled by `zeus.yaml`. This file is
**auto-generated** on first run with all defaults filled in. Edit it and
restart Zeus to apply changes.

---

## server

Controls the TCP listener.

```yaml
server:
  host: "0.0.0.0"        # Bind address.
                          # "0.0.0.0" = all interfaces (default)
                          # "127.0.0.1" = localhost only (more secure)
  port: 7878              # TCP port to listen on.
  max_connections: 1000   # Max simultaneous connected clients.
                          # New connections above this limit get STATUS_LIMIT_HIT.
  read_timeout_sec: 30    # Seconds before an idle read times out.
                          # Prevents connections from hanging open forever.
  write_timeout_sec: 10   # Seconds before a write times out.
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | string | `"0.0.0.0"` | Bind to a specific interface if needed |
| `port` | int | `7878` | Any port not in use |
| `max_connections` | int | `1000` | Increase for high-traffic deployments |
| `read_timeout_sec` | int | `30` | Should be > your PING interval |
| `write_timeout_sec` | int | `10` | Increase on very slow networks |

---

## security

Controls authentication and TLS.

```yaml
security:
  enabled: true       # Require token auth. Set false only for local dev.
  token: ""           # Auth token. Leave blank → Zeus generates one and saves it.
                      # Copy this value to all your clients.
  tls:
    enabled: false    # Set true to encrypt traffic (recommended in production).
    cert_file: "zeus.crt"  # Path to TLS certificate file.
    key_file:  "zeus.key"  # Path to TLS private key file.
    auto_gen:  true   # true = Zeus generates a self-signed cert on startup.
                      # false = You provide the cert files yourself.
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `enabled` | bool | `true` | Always true in production |
| `token` | string | `""` | Auto-generated 64-char hex if blank |
| `tls.enabled` | bool | `false` | Enable for any non-localhost deployment |
| `tls.cert_file` | string | `"zeus.crt"` | Relative or absolute path |
| `tls.key_file` | string | `"zeus.key"` | Relative or absolute path |
| `tls.auto_gen` | bool | `true` | Set false in production with real certs |

---

## persistence

Controls SQLite backup and restore.

```yaml
persistence:
  enabled: false           # Master switch. false = memory only (default).
  db_path: "zeus.db"       # Path to SQLite file. Created if it doesn't exist.
  sync_interval_sec: 5     # How often to flush pending writes to SQLite.
                           # (Cache writes are async by default)
  load_on_startup: true    # On start, restore cache + queue state from SQLite.
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `enabled` | bool | `false` | Required for: receipt_tracking, polls, edit/delete, smart_delivery, GC |
| `db_path` | string | `"zeus.db"` | Use an absolute path in production |
| `sync_interval_sec` | int | `5` | Lower = more durable, higher = faster |
| `load_on_startup` | bool | `true` | Almost always want this true |

**What gets persisted:**
- Cache keys and values (with TTL)
- Queue messages (pending, failed, retry schedules)
- Chat message history (full log, not just ring buffer)
- Chat delivery receipts
- Poll definitions and votes
- User room metadata

---

## channels

Controls the pub/sub channel system.

```yaml
channels:
  enabled: true
  max_channels: 500        # Max number of named channels that can exist.
  max_subscribers: 200     # Max subscribers per individual channel.
  history_size: 50         # Messages to keep in memory per channel.
                           # Replayed when FLAG_RETAIN is set on subscribe.
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `enabled` | bool | `true` | Disable if you only use cache/queues/chat |
| `max_channels` | int | `500` | Each channel uses ~1KB memory |
| `max_subscribers` | int | `200` | Per channel, not total |
| `history_size` | int | `50` | Per channel, in memory only |

---

## queues

Controls the reliable command queue system.

```yaml
queues:
  enabled: true
  max_queues: 200
  max_queue_depth: 10000   # Max unprocessed messages per queue.
                           # OP_QUEUE_PUSH returns STATUS_QUEUE_FULL when hit.
  ack_timeout_sec: 30      # Consumer must ACK/NACK within this many seconds.
                           # If not, message is auto-retried.
  retry:
    max_attempts: 5        # Give up after this many failures → mark dead.
    initial_delay_sec: 1   # First retry delay.
    backoff_factor: 2.0    # Multiply delay by this each attempt.
    max_delay_sec: 60      # Cap: retry delay never exceeds this.
```

**Retry delay schedule** with defaults:
```
Attempt 1 →  ~1s
Attempt 2 →  ~2s
Attempt 3 →  ~4s
Attempt 4 →  ~8s
Attempt 5 →  ~16s  → dead-letter
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `max_queues` | int | `200` | One queue per named task type |
| `max_queue_depth` | int | `10000` | Backpressure limit per queue |
| `ack_timeout_sec` | int | `30` | Increase for slow processing tasks |
| `retry.max_attempts` | int | `5` | 0 = no retries (deliver once) |
| `retry.initial_delay_sec` | int | `1` | Starting delay |
| `retry.backoff_factor` | float | `2.0` | Exponential multiplier |
| `retry.max_delay_sec` | int | `60` | Safety cap on retry wait |

---

## chat

Controls the chat room system.

```yaml
chat:
  enabled: true
  max_rooms: 100
  max_members_per_room: 500
  history_size: 200        # Messages to keep in memory per room.
  presence_ttl_sec: 60     # Mark user offline after this seconds of silence.
                           # User sends OP_PING to refresh their presence.
  features:
    receipt_tracking: false  # WhatsApp-style delivery ticks. Needs persistence.
    smart_delivery: false    # On reconnect, only send missed messages. Needs receipts.
    polls: false             # Attach polls to messages. Needs persistence.
    user_metadata: false     # Per-user JSON metadata in rooms (display name, avatar, etc.)
    gc_after_days: 0         # Delete fully-read messages older than N days. 0 = off.
```

### chat.features details

| Feature | Requires | What it enables |
|---|---|---|
| `receipt_tracking` | `persistence.enabled` | Per-message sent/delivered/read state |
| `smart_delivery` | `receipt_tracking` | Only push unread messages on reconnect |
| `polls` | `persistence.enabled` | Create polls, vote, get live results |
| `user_metadata` | nothing | Attach display name/avatar/role to room presence |
| `gc_after_days` | `receipt_tracking` | Auto-delete old fully-read messages |

---

## webhook

Controls Zeus → your backend HTTP notifications.

```yaml
webhook:
  enabled: false           # Master switch. false = no webhooks sent.
  url: "http://localhost:3000/zeus/webhook"
  secret: ""               # HMAC-SHA256 signing key. Auto-generated if blank.
                           # Verify X-Zeus-Signature header in your backend.
  timeout_sec: 5           # How long Zeus waits for your backend to respond.
  retry_on_failure: true   # Retry failed webhooks with exponential backoff.
  events:
    chat_message: true     # Fired when a message is sent in any room.
    chat_join: true        # Fired when a user joins a room.
    chat_leave: true       # Fired when a user leaves a room.
    queue_enqueue: true    # Fired when a message is pushed to a queue.
    queue_ack: true        # Fired when a queue message is ACKed.
    queue_fail: true       # Fired when a queue message is NACKed.
    queue_expire: true     # Fired when a queue message is dead-lettered.
    channel_publish: false # Fired on every channel publish. Off by default (high volume).
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `url` | string | — | Your backend endpoint. Must respond within `timeout_sec`. |
| `secret` | string | `""` | Auto-generated 64-char hex if blank |
| `timeout_sec` | int | `5` | Low enough to not block Zeus on slow backends |
| `retry_on_failure` | bool | `true` | Retries: 2s, 4s, 8s, 16s, 32s (5 max) |
| `events.*` | bool | varies | Turn off high-volume events to reduce noise |

**Recommendation:** Start with all events on, then disable noisy ones you
don't use. `channel_publish` is off by default because it fires on every
publish — enable only if your backend needs to react to channel messages.

---

## log

Controls logging output.

```yaml
log:
  level: "info"    # debug | info | warn | error
  format: "text"   # text | json
  file: ""         # Leave blank to log to stdout.
                   # Set to a file path to write there instead.
```

| Level | What you see |
|---|---|
| `debug` | Everything, including frame-level details |
| `info` | Server start, connections, feature events (default) |
| `warn` | Non-fatal issues (buffer drops, slow clients) |
| `error` | Fatal issues only |

**JSON format** is useful when ingesting logs into Elasticsearch, Datadog,
CloudWatch, etc.:
```json
{"time":"2024-01-15T10:30:00Z","level":"info","msg":"[zeus] client c42 connected from 10.0.0.5:54321"}
```

---

## Full example: production config

```yaml
server:
  host: "0.0.0.0"
  port: 7878
  max_connections: 5000
  read_timeout_sec: 60
  write_timeout_sec: 10

security:
  enabled: true
  token: "your-64-char-token-here"
  tls:
    enabled: true
    auto_gen: false
    cert_file: "/etc/ssl/zeus/fullchain.pem"
    key_file:  "/etc/ssl/zeus/privkey.pem"

persistence:
  enabled: true
  db_path: "/var/lib/zeus/zeus.db"
  load_on_startup: true

channels:
  enabled: true
  max_channels: 1000
  max_subscribers: 500
  history_size: 100

queues:
  enabled: true
  max_queues: 500
  max_queue_depth: 50000
  ack_timeout_sec: 60
  retry:
    max_attempts: 10
    initial_delay_sec: 2
    backoff_factor: 2.0
    max_delay_sec: 300

chat:
  enabled: true
  max_rooms: 500
  max_members_per_room: 1000
  history_size: 500
  presence_ttl_sec: 120
  features:
    receipt_tracking: true
    smart_delivery: true
    polls: true
    user_metadata: true
    gc_after_days: 90

webhook:
  enabled: true
  url: "https://api.yourapp.com/zeus/webhook"
  secret: "your-webhook-secret-here"
  timeout_sec: 10
  retry_on_failure: true
  events:
    chat_message: true
    chat_join: true
    chat_leave: true
    queue_fail: true
    queue_expire: true

log:
  level: "info"
  format: "json"
  file: "/var/log/zeus/zeus.log"
```
