# Zeus — Chat Rooms

## Philosophy: plug in, don't replace your backend

Zeus chat is designed to handle the **hard realtime parts** of chat, while
your backend keeps ownership of users, auth, and persistent storage.

```
┌──���───────────────────────────────────────────────────┐
│  ZEUS handles:                                       │
│   ✓ Realtime push to thousands of connections        │
│   ✓ Presence (who's online, last-seen)               │
│   ✓ In-memory message history (ring buffer)          │
│   ✓ Optional: delivery receipts (grey/blue ticks)    │
│   �� Optional: polls, message metadata, smart GC      │
├────────────────────────────────────���─────────────────┤
│  YOUR BACKEND handles:                               │
│   ✓ User authentication (you issue the userID)       │
│   ✓ "Can this user join this room?" authorisation    │
│   ✓ Long-term message history (your DB)              │
│   ✓ Push notifications (FCM/APNs) for offline users  │
│   ✓ Business logic: moderation, reactions, etc.      │
└──────────────────────────────────────────────────────┘
```

The bridge is **webhooks** — Zeus fires a signed HTTP POST to your backend
for every event. Your backend can persist to its own DB and run any checks.

**Zero lock-in.** Your message data lives in your database. Zeus is a
realtime delivery layer you can swap or remove without losing anything.

---

## Core operations

### Join a room

```
Op:   OP_CHAT_JOIN (0x40)
Key:  "general"    (room name)
Body: (empty)
```

**Response body:** JSON array of recent messages (history replay).

After joining, Zeus will push `OP_PUSH_CHAT` frames to you whenever
someone sends a message in that room.

Zeus also automatically subscribes you to four side-channels for the room:
```
"general._events"    → edit/delete notifications
"general._receipts"  → delivery receipt updates
"general._polls"     → live poll vote counts
"general._presence"  → presence/metadata changes
```

---

### Leave a room

```
Op:   OP_CHAT_LEAVE (0x41)
Key:  "general"
Body: (empty)
```

Zeus removes you from the room and unsubscribes all side-channels.

---

### Send a message

```
Op:   OP_CHAT_MESSAGE (0x42)
Key:  "general"
Body: [typeLen:1][type][payload]
```

**Body format:**
```
Byte 0:     Length of the type string. Set to 0 for plain text.
Bytes 1..N: Type string. One of:
              "text"     — plain text or rich text
              "image"    — image attachment
              "video"    — video attachment
              "audio"    — audio/voice message
              "file"     — file attachment
              "poll"     — use OP_CHAT_POLL_CREATE instead
              "reaction" — emoji reaction to another message
              "system"   — server-generated event (join/leave/rename)
Bytes N+1+: Your payload (any bytes — JSON, text, binary)
```

**Response body:** 8-byte message ID (uint64 big-endian).
Store this ID if you need to reference the message later (edit, delete, poll).

---

### Receive messages (server push)

```
Op:   OP_PUSH_CHAT (0x52)
Key:  "general"       (room name)
Body: [senderLen:1][senderID][msgID:8][payload]
```

**Parsing:**
```
Byte 0:          Length of sender ID string
Bytes 1..N:      Sender's userID (the one they used in OP_AUTH)
Bytes N+1..N+8:  Message ID, uint64 big-endian
Bytes N+9+:      Message payload
```

---

### Get message history

```
Op:   OP_CHAT_HISTORY (0x43)
Key:  "general"
Body: [afterID:8][limit:2]   (optional — omit for defaults)
```

**Body (optional pagination):**
```
Bytes 0–7:  afterID — uint64. Return messages with ID > this. 0 = from start.
Bytes 8–9:  limit   — uint16. Max messages to return. 0 = server default (50).
```

**Response body:** JSON array with each message including:
```json
[
  {
    "id": 42,
    "sender": "alice",
    "type": "text",
    "payload": "<your bytes>",
    "meta": {},
    "sent_at": 1705123456,
    "delivery_state": 2
  }
]
```

`delivery_state`: `0`=sent, `1`=delivered, `2`=read

When persistence is enabled, history comes from SQLite (full log).
Without persistence, it comes from the in-memory ring buffer (last N messages).

---

### Get presence list

Who is currently in the room and when they were last active:

```
Op:   OP_CHAT_PRESENCE (0x44)
Key:  "general"
Body: (empty)
```

**Response body:** JSON array:
```json
[
  { "user_id": "alice",   "online": true,  "last_active": "2024-01-15T10:30:00Z" },
  { "user_id": "bob",     "online": false, "last_active": "2024-01-15T09:45:00Z" }
]
```

A user is marked offline after `presence_ttl_sec` seconds without a PING.

---

## Optional features

All features below are **disabled by default**. Enable them one by one in
zeus.yaml as your app needs them.

---

### Receipt tracking (WhatsApp-style ticks)

**Enable:**
```yaml
chat:
  features:
    receipt_tracking: true  # requires persistence.enabled: true
```

This adds per-message per-user delivery state to Zeus:
```
0 = sent      (Zeus received it from the sender — one grey tick)
1 = delivered (recipient's device got it — two grey ticks)
2 = read      (recipient opened the conversation — two blue ticks)
```

**Auto-mark delivered:**
When `receipt_tracking` is on, Zeus **automatically** marks every pushed
message as `delivered` the moment it's sent to the client over TCP. You
don't need to do anything for the delivered state.

**Mark as read:**
Call this when the user opens the conversation:

```
Op:   OP_CHAT_MARK_READ (0x46)
Key:  "general"
Body: [msgID:8]   — 8-byte uint64 message ID
```

**Receive receipt updates:**
When someone marks your message delivered or read, Zeus pushes this to you:

```
Op:   OP_PUSH_RECEIPT (0x53)
Key:  "general"
Body: [msgID:8][state:1][userIDLen:1][userID]
```

---

### Smart delivery (catch-up on reconnect)

**Enable:**
```yaml
chat:
  features:
    receipt_tracking: true
    smart_delivery: true
```

Without smart delivery: on join, Zeus replays the last `history_size` messages
(e.g. last 200) regardless of what you've already seen.

With smart delivery: on join, Zeus looks at what messages you've already
received (from the receipts table) and **only sends what you missed**. This
means:
- Users who reconnect after 5 minutes get exactly the 3 messages they missed
- Users who were offline for 3 days get exactly the messages from those 3 days
- No duplicate delivery, no needless data transfer

---

### Edit a message

```
Op:   OP_CHAT_EDIT_MESSAGE (0x47)
Key:  "general"
Body: [msgID:8][newPayload]
```

Zeus updates the message in SQLite and broadcasts an edit notification to
all room members:

```
OP_PUSH_CHAT pushes a frame with Body[0] = 0xED (edit marker)
followed by [msgID:8][newPayload]
```

Clients check `body[0] == 0xED` to know this is an edit, not a new message.

**Requires:** `persistence.enabled: true`

---

### Delete a message

```
Op:   OP_CHAT_DELETE_MESSAGE (0x48)
Key:  "general"
Body: [msgID:8]
```

Zeus soft-deletes the message — it's marked with `deleted_at` in SQLite
but not removed. Clients receive a delete notification:

```
OP_PUSH_CHAT Body[0] = 0xDE (delete marker), Body[1-8] = msgID
```

Clients render "This message was deleted" for the deleted message.

**Requires:** `persistence.enabled: true`

---

### Polls

**Enable:**
```yaml
chat:
  features:
    polls: true    # requires persistence.enabled: true
```

**Create a poll:**
```
Op:   OP_CHAT_POLL_CREATE (0x49)
Key:  "general"
Body: JSON
```

JSON body:
```json
{
  "question":   "What time should we meet?",
  "options":    ["2pm", "4pm", "6pm"],
  "multi_vote": false,
  "close_secs": 86400
}
```

- `multi_vote: true` allows each user to pick multiple options
- `close_secs: 0` = poll never closes

**Response body:** 8-byte poll message ID.

**Vote on a poll:**
```
Op:   OP_CHAT_POLL_VOTE (0x4A)
Key:  "general"
Body: [msgID:8][optionIndex:1]
```

`optionIndex` is 0-based (0 = first option, 1 = second, etc.).
Voting is **idempotent** — voting the same option twice is a no-op.

After each vote, Zeus broadcasts updated results to the room so all
clients see live vote counts update in real time.

**Get current results:**
```
Op:   OP_CHAT_POLL_RESULTS (0x4B)
Key:  "general"
Body: [msgID:8]
```

Response:
```json
{ "msg_id": 42, "results": { "0": 12, "1": 5, "2": 8 } }
```

---

### User metadata

**Enable:**
```yaml
chat:
  features:
    user_metadata: true
```

Lets each user attach arbitrary JSON to their presence in a room. This is
purely for your clients to display — Zeus doesn't interpret the contents.

**Set your metadata:**
```
Op:   OP_CHAT_SET_META (0x4C)
Key:  "general"
Body: JSON object (any shape you want)
```

Example:
```json
{
  "display_name": "Alice Chen",
  "avatar_url": "https://cdn.example.com/avatars/alice.jpg",
  "role": "admin",
  "status": "In a meeting until 3pm"
}
```

Zeus stores this in SQLite and broadcasts a `OP_PUSH_PRESENCE` frame to
all room members immediately:

```
Op:   OP_PUSH_PRESENCE (0x54)
Body: {"user_id": "alice", "meta": {...}}
```

**Get another user's metadata:**
```
Op:   OP_CHAT_GET_META (0x4D)
Key:  "general"
Body: "alice"   (UTF-8 userID string)
```

Response: their JSON metadata object, or `{}` if none set.

---

### Smart GC (garbage collection)

**Enable:**
```yaml
chat:
  features:
    receipt_tracking: true    # required
    gc_after_days: 30         # 0 = disabled
```

Zeus runs a nightly cleanup that deletes messages older than `gc_after_days`
days — **but only if every room member has read them**.

This is the **smart** part: Zeus never deletes a message that someone hasn't
read yet. If one member has been offline for 2 weeks, their messages stay safe.

The GC runs once per day, 1 minute after server startup, and logs what it did:
```
[zeus] gc: pruned 1,234 fully-read messages older than 30 days
```

---

## Webhooks (fire events to your backend)

**Enable:**
```yaml
webhook:
  enabled: true
  url: "https://api.yourapp.com/zeus/webhook"
  secret: "your-hmac-secret"
  timeout_sec: 5
  retry_on_failure: true
  events:
    chat_message: true
    chat_join:    true
    chat_leave:   true
```

Zeus posts JSON to your URL for every enabled event. **Always async** — your
backend being slow never delays message delivery.

**Verifying the signature in your backend:**
```
Header: X-Zeus-Signature: sha256=<hex>

Verify: HMAC-SHA256(secret, requestBody) == header value
```

Same convention as GitHub webhooks, so existing middleware works.

**Example webhook body (chat.message):**
```json
{
  "event":     "chat.message",
  "room":      "general",
  "user_id":   "alice",
  "client_id": "c42",
  "data":      <your original message bytes>,
  "at":        "2024-01-15T10:30:00Z"
}
```

**Retry on failure:**
If `retry_on_failure: true` and your backend returns a 5xx or times out, Zeus
retries with exponential back-off: 2s → 4s → 8s → 16s → 32s (5 attempts max).
