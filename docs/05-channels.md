# Zeus — Channels (Pub/Sub)

## What are channels?

A **channel** is a named topic. Clients subscribe to it and receive every
message published to it in real time.

```
Publisher ──► Channel "live-scores" ──► All Subscribers
```

This is **fan-out delivery** — one published message goes to everyone
subscribed at that moment. Think of it like a live broadcast feed.

---

## When to use channels

| Use case | Good fit? |
|---|---|
| Live sports score updates | ✅ |
| Stock price feed | ✅ |
| Admin broadcast to all users | ✅ |
| IoT sensor readings | ✅ |
| Guaranteed delivery to one worker | ❌ Use a Queue instead |
| Chat rooms | ❌ Use Chat rooms instead |

Channels are **fire-and-forget** — if a subscriber's buffer is full, the
message is dropped for that subscriber only. Others are unaffected.
For guaranteed delivery, use [Queues](06-queues.md).

---

## Subscribe

Register to receive messages on a channel.

```
Op:    OP_SUBSCRIBE (0x20)
Key:   "live-scores"
Body:  (empty)
Flags: 0x00 (no history replay)
       0x04 (FLAG_RETAIN — replay history on join)
```

**Response:** `STATUS_OK`

After subscribing, Zeus will start sending `OP_PUSH_CHANNEL` frames
whenever someone publishes to that channel.

---

## Receive (server push)

Zeus sends this to all subscribers when a message is published:

```
Op:  OP_PUSH_CHANNEL (0x50)
Key: "live-scores"   (the channel name)
Body: the raw message payload
```

Your client just needs to handle `OP_PUSH_CHANNEL` in its read loop.

---

## Publish

Send a message to all current subscribers.

```
Op:   OP_PUBLISH (0x22)
Key:  "live-scores"
Body: your message payload (any bytes)
```

**Response body:** 4 bytes, uint32 big-endian = number of subscribers
that received the message. (Useful for debugging — if it's 0, nobody
was listening at that moment.)

---

## Unsubscribe

Stop receiving messages from a channel.

```
Op:  OP_UNSUBSCRIBE (0x21)
Key: "live-scores"
Body: (empty)
```

**Response:** `STATUS_OK`

Unsubscribing is idempotent — calling it when not subscribed is fine.

---

## History replay on subscribe

When you set `FLAG_RETAIN (0x04)` in Flags during `OP_SUBSCRIBE`, Zeus
replays the last N messages from that channel immediately into your
subscriber queue, before any new live messages arrive.

```
Op:    OP_SUBSCRIBE
Key:   "notifications"
Flags: 0x04  ← replay history
```

The number of messages replayed is controlled by `history_size` in zeus.yaml:

```yaml
channels:
  history_size: 50  # replay last 50 messages on subscribe
```

This is useful for:
- Showing a client the last few items when they first open the app
- Catching up after a brief reconnect
- Ensuring clients don't miss anything during the window between connect and subscribe

---

## Retain flag on publish

When you publish with `FLAG_RETAIN (0x04)`, Zeus stores **that specific
message** as the "retained" message for the channel. New subscribers who
join later will always receive this one message, even without requesting history.

```
Op:    OP_PUBLISH
Key:   "server-status"
Body:  {"status": "online", "version": "1.4.2"}
Flags: 0x04  ← retain this message
```

Use this for **state messages** — the current status of something that doesn't
change often but new subscribers should always see.

---

## Back-pressure handling

Each subscriber has a buffer of **256 messages**. If a client is reading
slowly and its buffer fills up:

- The message is **dropped for that subscriber only**
- All other subscribers receive it normally
- Zeus logs a warning but does not close the connection

If your subscriber is consistently dropping messages, it means your client
is not reading fast enough. Either:
1. Increase the read rate in your client
2. Move to queues for guaranteed delivery
3. Accept that you want best-effort delivery (channels are designed for this)

---

## Channel limits

Configured in zeus.yaml:

```yaml
channels:
  max_channels: 500      # Maximum number of named channels
  max_subscribers: 200   # Max subscribers per channel
  history_size: 50       # Messages to keep in memory per channel
```

If `max_channels` is reached, new `OP_SUBSCRIBE` requests return `STATUS_LIMIT_HIT`.
Increase the limit in config if needed.

---

## Channel names

Any UTF-8 string up to the `KeyLen` limit (65535 bytes, though keep them short).
Common conventions:

```
topic:sports:nba          # hierarchical namespacing
user:1234:notifications   # per-user channels
room:general:events       # room-level event stream
system:alerts             # server-wide alerts
```

Zeus doesn't enforce naming — use whatever works for your app.

---

## Channels vs Queues vs Chat

| | Channels | Queues | Chat |
|---|---|---|---|
| **Delivery** | Fan-out to all | One consumer per message | Fan-out to room members |
| **Guarantee** | Best-effort | Guaranteed (ACK/retry) | Best-effort + optional receipts |
| **History** | Optional replay | Persisted in SQLite | Ring buffer + SQLite |
| **Use for** | Live feeds, broadcasts | Work queues, commands | Messaging between users |
