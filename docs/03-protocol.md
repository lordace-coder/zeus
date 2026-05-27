# Zeus — Binary Protocol

## Why binary?

Zeus does not use HTTP, WebSocket, or any text protocol. Every message is a
**binary frame** — a fixed-size header followed by a key and a body.

This makes Zeus:
- **Fast** — no string parsing, no delimiters, no overhead
- **Compact** — a full GET request is ~20 bytes; a chat push is ~50 bytes
- **Simple to implement** — just read N bytes, parse 4 integers, done

---

## Frame layout

Every single message — request or response — uses this exact layout:

```
Offset  Size   Field
──────  ────   ──────────────────────────────────────────────────
  0      1     Magic      — always 0x5A ('Z'). Validates the frame.
  1      1     Version    — protocol version, currently 0x01
  2      1     OpCode     — what kind of message this is
  3      1     Flags      — bitmask of optional features
  4      4     RequestID  — uint32, big-endian. Client picks this.
  8      2     KeyLen     — uint16, big-endian. Length of the Key field.
 10      4     BodyLen    — uint32, big-endian. Length of the Body field.
 14      1     Reserved   — always 0x00
─────── ───   ── HEADER = 15 bytes ──────────────────────────────
 15    KeyLen  Key        — UTF-8 string (channel name, room, cache key, etc.)
 15+KL BodyLen Body       — arbitrary bytes (your data / payload)
```

All multi-byte integers are **big-endian** (network byte order).

### RequestID

The client chooses any uint32 as a RequestID. Zeus echoes it back in the
response frame. This lets you match responses to requests when sending
multiple requests without waiting for each reply — like async HTTP.

```
Client sends:   OP_GET  RequestID=42  Key="username"
Zeus replies:   OP_RESPONSE  RequestID=42  body="alice"

Client sends:   OP_GET  RequestID=43  Key="score"
Zeus replies:   OP_RESPONSE  RequestID=43  body="9500"
```

---

## Flags bitmask

The Flags byte lets you toggle optional behaviours per-frame:

| Bit | Hex | Name | Meaning |
|---|---|---|---|
| 0 | `0x01` | `FLAG_COMPRESSED` | Body is gzip-compressed |
| 1 | `0x02` | `FLAG_BINARY` | Body is raw binary (not UTF-8) |
| 2 | `0x04` | `FLAG_RETAIN` | Channel: retain last message for late subscribers |
| 3 | `0x08` | `FLAG_PERSIST` | Cache SET: write to SQLite immediately (not async) |

Example — subscribe and replay history:
```
Flags = 0x04  (FLAG_RETAIN set)
Op    = OP_SUBSCRIBE
Key   = "live-feed"
```

---

## Response format

When Zeus responds to a request, it sends `OP_RESPONSE` or `OP_ERROR`.

**Success (`OP_RESPONSE`):**
```
Body[0]    = 0x00  (STATUS_OK)
Body[1..]  = payload (varies by opcode — may be empty)
```

**Error (`OP_ERROR`):**
```
Body[0]    = error code (see table below)
Body[1..]  = UTF-8 error message string
```

### Status codes

| Hex | Name | When you get it |
|---|---|---|
| `0x00` | `STATUS_OK` | Success |
| `0x01` | `STATUS_NOT_FOUND` | Cache key does not exist |
| `0x02` | `STATUS_AUTH_FAIL` | Wrong token, or sent a frame before OP_AUTH |
| `0x03` | `STATUS_QUEUE_FULL` | Queue has reached `max_queue_depth` |
| `0x04` | `STATUS_ROOM_FULL` | Room has reached `max_members_per_room` |
| `0x05` | `STATUS_LIMIT_HIT` | Channel/queue/room count limit reached |
| `0x06` | `STATUS_UNKNOWN_OP` | OpCode not recognised |
| `0x07` | `STATUS_INTERNAL` | Internal server error (see message body) |

---

## All opcodes

### Authentication

| Hex | Name | Direction | Key | Body |
|---|---|---|---|---|
| `0x01` | `OP_AUTH` | Client→Server | `userID` (optional) | token string |

- Must be the **first frame** sent after connecting
- `Key` = your user ID (any string, Zeus doesn't validate it — it's for receipts/presence)
- `Body` = the auth token from zeus.yaml
- On success: response body = your assigned connection ID (e.g. `"c42"`)

---

### Cache

| Hex | Name | Direction | Key | Body |
|---|---|---|---|---|
| `0x10` | `OP_GET` | Client→Server | cache key | _(empty)_ |
| `0x11` | `OP_SET` | Client→Server | cache key | `[ttl:4][value...]` |
| `0x12` | `OP_DELETE` | Client→Server | cache key | _(empty)_ |
| `0x13` | `OP_CLEAR` | Client→Server | _(empty)_ | _(empty)_ |

**SET body format:**
```
Bytes 0–3:  TTL in seconds, uint32 big-endian. 0 = no expiry.
Bytes 4+:   The value to store (any bytes you want)
```

**GET response:**
- `STATUS_OK` + value bytes on success
- `STATUS_NOT_FOUND` if the key doesn't exist or has expired

---

### Channels (Pub/Sub)

| Hex | Name | Direction | Key | Body |
|---|---|---|---|---|
| `0x20` | `OP_SUBSCRIBE` | Client→Server | channel name | _(empty)_ |
| `0x21` | `OP_UNSUBSCRIBE` | Client→Server | channel name | _(empty)_ |
| `0x22` | `OP_PUBLISH` | Client→Server | channel name | message payload |
| `0x50` | `OP_PUSH_CHANNEL` | Server→Client | channel name | message payload |

**History replay:**
Set `FLAG_RETAIN (0x04)` in Flags when subscribing to replay recent history.

**PUBLISH response body:**
4 bytes (uint32 big-endian) = number of subscribers that received the message.

---

### Queues

| Hex | Name | Direction | Key | Body |
|---|---|---|---|---|
| `0x30` | `OP_QUEUE_PUSH` | Client→Server | queue name | command payload |
| `0x31` | `OP_QUEUE_CONSUME` | Client→Server | queue name | _(empty)_ |
| `0x32` | `OP_QUEUE_ACK` | Client→Server | queue name | message ID (string) |
| `0x33` | `OP_QUEUE_NACK` | Client→Server | queue name | `[idLen:2][id][errMsg]` |
| `0x51` | `OP_PUSH_QUEUE` | Server→Client | queue name | `[idLen:2][id][payload]` |

**QUEUE_PUSH response body:** the assigned message ID (UTF-8 string)

**QUEUE_CONSUME:** registers this connection as a worker. Zeus immediately starts
pushing `OP_PUSH_QUEUE` frames as messages become available.

**NACK body format:**
```
Bytes 0–1:  Length of message ID, uint16 big-endian
Bytes 2..N: Message ID string
Bytes N+1+: Error description (any UTF-8 string)
```

---

### Chat — Core

| Hex | Name | Direction | Key | Body |
|---|---|---|---|---|
| `0x40` | `OP_CHAT_JOIN` | Client→Server | room name | _(empty)_ |
| `0x41` | `OP_CHAT_LEAVE` | Client→Server | room name | _(empty)_ |
| `0x42` | `OP_CHAT_MESSAGE` | Client→Server | room name | `[typeLen:1][type][payload]` |
| `0x43` | `OP_CHAT_HISTORY` | Client→Server | room name | `[afterID:8][limit:2]` (optional) |
| `0x44` | `OP_CHAT_PRESENCE` | Client→Server | room name | _(empty)_ |
| `0x52` | `OP_PUSH_CHAT` | Server→Client | room name | `[senderLen:1][sender][msgID:8][payload]` |

**CHAT_JOIN response body:** JSON array of recent messages (history replay)

**CHAT_MESSAGE body format:**
```
Byte 0:     Length of type string (0 = default "text")
Bytes 1..N: Message type string: "text" | "image" | "video" | "audio" | "file" | "poll" | "reaction" | "system"
Bytes N+1+: Your message payload (any bytes)
```

**CHAT_MESSAGE response body:** 8-byte message ID (uint64 big-endian)

**CHAT_HISTORY body (optional pagination):**
```
Bytes 0–7:   afterID — return messages with ID > this (0 = from beginning)
Bytes 8–9:   limit   — max messages to return (0 = server default of 50)
```

---

### Chat — Optional Features

These opcodes require the matching feature flag in `zeus.yaml`. If the feature
is disabled, Zeus returns `STATUS_INTERNAL` with an explanation message.

| Hex | Name | Requires | Key | Body |
|---|---|---|---|---|
| `0x45` | `OP_CHAT_MARK_DELIVERED` | `receipt_tracking: true` | room | `[msgID:8]` |
| `0x46` | `OP_CHAT_MARK_READ` | `receipt_tracking: true` | room | `[msgID:8]` |
| `0x47` | `OP_CHAT_EDIT_MESSAGE` | persistence | room | `[msgID:8][newPayload]` |
| `0x48` | `OP_CHAT_DELETE_MESSAGE` | persistence | room | `[msgID:8]` |
| `0x49` | `OP_CHAT_POLL_CREATE` | `polls: true` | room | JSON (see below) |
| `0x4A` | `OP_CHAT_POLL_VOTE` | `polls: true` | room | `[msgID:8][optionIndex:1]` |
| `0x4B` | `OP_CHAT_POLL_RESULTS` | `polls: true` | room | `[msgID:8]` |
| `0x4C` | `OP_CHAT_SET_META` | `user_metadata: true` | room | JSON object |
| `0x4D` | `OP_CHAT_GET_META` | `user_metadata: true` | room | userID string |

**POLL_CREATE body (JSON):**
```json
{
  "question":   "What's for lunch?",
  "options":    ["Pizza", "Sushi", "Tacos"],
  "multi_vote": false,
  "close_secs": 3600
}
```
Response body: 8-byte poll message ID.

**POLL_RESULTS response (JSON):**
```json
{ "msg_id": 42, "results": { "0": 5, "1": 3, "2": 7 } }
```

---

### RPC (Remote Procedure Call)

RPC lets a caller send a task to a worker group and await the result.
See [11-rpc.md](11-rpc.md) for the full guide.

**Client → Server:**

| Hex | Name | Key | Body |
|---|---|---|---|
| `0x60` | `OP_RPC_CALL` | group name | `[timeoutMs:4][payload]` |
| `0x61` | `OP_RPC_REPLY` | callID | `[isError:1][result bytes]` |
| `0x62` | `OP_RPC_PROGRESS` | callID | `[pct:1][message string]` |
| `0x63` | `OP_RPC_CANCEL` | callID | _(empty)_ |
| `0x64` | `OP_RPC_STATUS` | callID | _(empty)_ |
| `0x65` | `OP_RPC_CONSUME` | group name | _(empty)_ |

`OP_RPC_CALL` response body: callID string (UTF-8). **Save this** — you need it to track the call if you disconnect.

**Server push → Client:**

| Hex | Name | Body |
|---|---|---|
| `0x55` | `OP_PUSH_RPC` | `[callIDLen:1][callID][payload]` — deliver a call to a worker |
| `0x56` | `OP_PUSH_RPC_RESULT` | `[callIDLen:1][callID][result bytes]` — call succeeded |
| `0x57` | `OP_PUSH_RPC_PROGRESS` | `[callIDLen:1][callID][pct:1][message]` — progress update |
| `0x58` | `OP_PUSH_RPC_ERROR` | `[callIDLen:1][callID][error message]` — call failed/timed out |

---

### Server push frames

Zeus sends these **without** a corresponding client request. Your client
should always be ready to receive them after joining a room/channel/queue.

| Hex | Name | When sent | Body format |
|---|---|---|---|
| `0x50` | `OP_PUSH_CHANNEL` | New message published on a subscribed channel | raw payload |
| `0x51` | `OP_PUSH_QUEUE` | Queue message ready for this consumer | `[idLen:2][id][payload]` |
| `0x52` | `OP_PUSH_CHAT` | New message in a room you joined | `[senderLen:1][sender][msgID:8][payload]` |
| `0x53` | `OP_PUSH_RECEIPT` | Someone delivered/read a message you sent | `[msgID:8][state:1][userIDLen:1][userID]` |
| `0x54` | `OP_PUSH_PRESENCE` | A room member's metadata changed | JSON `{user_id, meta}` |
| `0x55` | `OP_PUSH_RPC` | A call has been dispatched to this worker | `[callIDLen:1][callID][payload]` |
| `0x56` | `OP_PUSH_RPC_RESULT` | An RPC call you made completed successfully | `[callIDLen:1][callID][result]` |
| `0x57` | `OP_PUSH_RPC_PROGRESS` | Progress update from an RPC worker | `[callIDLen:1][callID][pct:1][message]` |
| `0x58` | `OP_PUSH_RPC_ERROR` | An RPC call failed or timed out | `[callIDLen:1][callID][error message]` |

**Receipt states (in OP_PUSH_RECEIPT):**
```
0x00 = sent      (one grey tick)
0x01 = delivered (two grey ticks)
0x02 = read      (two blue ticks)
```

---

### Keepalive

| Hex | Name | Direction | Purpose |
|---|---|---|---|
| `0xFD` | `OP_PING` | Client→Server | Heartbeat / presence refresh |
| `0xFE` | `OP_PONG` | Server→Client | Reply to PING |

Send a PING every 15–30 seconds to keep the connection alive and refresh
your presence in all joined chat rooms.

---

## Minimal client implementation checklist

```
□ Open TCP connection
□ Send OP_AUTH (body = token, key = your userID)
□ Read OP_RESPONSE — check body[0] == 0x00
□ Start a read loop:
    □ Read 15 header bytes
    □ Validate byte[0] == 0x5A (magic)
    □ Parse KeyLen (bytes 8–9) and BodyLen (bytes 10–13)
    □ Read KeyLen + BodyLen more bytes
    □ Dispatch on OpCode:
        □ OP_RESPONSE         → match RequestID, resolve pending request
        □ OP_ERROR            → match RequestID, surface error
        □ OP_PONG             → keepalive confirmed
        □ OP_PUSH_CHANNEL     → channel message for a subscribed topic
        □ OP_PUSH_QUEUE       → queue item delivered to this consumer
        □ OP_PUSH_CHAT        → chat message in a joined room
        □ OP_PUSH_RECEIPT     → delivery receipt update
        □ OP_PUSH_PRESENCE    → room member metadata change
        □ OP_PUSH_RPC         → an RPC call dispatched to this worker
        □ OP_PUSH_RPC_RESULT  → RPC call you made completed
        □ OP_PUSH_RPC_PROGRESS→ progress update from an RPC worker
        □ OP_PUSH_RPC_ERROR   → RPC call failed or timed out
□ Send OP_PING every 20 seconds
```
