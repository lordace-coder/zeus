# Zeus — Queues (Reliable Delivery)

## What are queues?

A **queue** is a named list of messages that must be delivered to a consumer
exactly once, with guaranteed retry on failure.

```
Producer → Queue "send-emails" → Consumer Worker
                                 Worker must ACK each message
                                 Failed → Zeus retries automatically
```

The key difference from channels:

| | Channel | Queue |
|---|---|---|
| Delivery model | Fan-out (all subscribers) | One consumer per message |
| Guarantees | Best-effort | Exactly-once with retry |
| On failure | Message dropped | Retried with back-off |
| Order | Broadcast order | FIFO per queue |

---

## Message lifecycle

Every queue message goes through these states:

```
PUSH      → message enters the queue          (status: pending)
DELIVER   → Zeus sends it to a consumer       (status: in-flight)
ACK       → consumer marks PROCESSED          → deleted ✓
NACK      → consumer marks FAILED             → retry scheduled
TIMEOUT   → consumer didn't reply in time     → treated as NACK
DEAD      → max retries exceeded              → stored as "failed" in DB
```

---

## Push (produce a message)

Add a new message to the end of the queue:

```
Op:   OP_QUEUE_PUSH (0x30)
Key:  "send-emails"
Body: your command payload (any bytes — JSON, binary, etc.)
```

**Response body:** the assigned message ID (UTF-8 string, e.g. `"1705123456-42"`)

Save this ID if you want to track the message, but you don't have to.

---

## Consume (register as a worker)

Tell Zeus this connection is a worker for a queue:

```
Op:   OP_QUEUE_CONSUME (0x31)
Key:  "send-emails"
Body: (empty)
```

**Response:** `STATUS_OK`

After this, Zeus immediately starts pushing `OP_PUSH_QUEUE` frames to
this connection as messages become available. Your job is to:
1. Process each message
2. Send `OP_QUEUE_ACK` (success) or `OP_QUEUE_NACK` (failure)

---

## Receive (server push to consumer)

```
Op:   OP_PUSH_QUEUE (0x51)
Key:  "send-emails"   (queue name)
Body: [idLen:2][messageID][payload]
```

**Parsing the body:**
```
Bytes 0–1:  Length of the message ID, uint16 big-endian
Bytes 2..N: The message ID string (you need this for ACK/NACK)
Bytes N+1+: Your original payload
```

---

## ACK — mark as processed

Tell Zeus the message was handled successfully. Zeus deletes it.

```
Op:   OP_QUEUE_ACK (0x32)
Key:  "send-emails"
Body: the message ID string (exactly as received in OP_PUSH_QUEUE)
```

**Response:** `STATUS_OK`

After ACK, Zeus delivers the next pending message.

---

## NACK — mark as failed

Tell Zeus the message processing failed. Zeus schedules a retry.

```
Op:   OP_QUEUE_NACK (0x33)
Key:  "send-emails"
Body: [idLen:2][messageID][error description]
```

**Body format:**
```
Bytes 0–1:  Length of message ID, uint16 big-endian
Bytes 2..N: Message ID string
Bytes N+1+: Human-readable error description (why it failed)
            This is stored in the DB for debugging.
```

**Response:** `STATUS_OK`

The error description is stored in SQLite. You can query `zeus.db`
directly to see what failed and why:

```sql
SELECT * FROM queue_messages WHERE status = 'failed';
```

---

## Retry logic (exponential back-off)

After a NACK, Zeus waits before re-delivering. The wait grows exponentially
with a small random jitter to prevent thundering-herd effects:

```
Attempt 1  →  wait ~1s  (initial_delay_sec)
Attempt 2  →  wait ~2s  (× backoff_factor 2.0)
Attempt 3  →  wait ~4s
Attempt 4  →  wait ~8s
Attempt 5  →  wait ~16s  → max_attempts reached → message goes DEAD
```

Configured in zeus.yaml:
```yaml
queues:
  retry:
    max_attempts: 5
    initial_delay_sec: 1
    backoff_factor: 2.0
    max_delay_sec: 60     # cap: no retry waits longer than 60s
  ack_timeout_sec: 30     # auto-NACK if consumer doesn't reply in 30s
```

---

## ACK timeout (auto-retry on silence)

If a consumer receives a message but **never sends ACK or NACK** (e.g. the
worker crashes), Zeus automatically NACKs it after `ack_timeout_sec` seconds:

```
Consumer receives message
Zeus starts 30-second timer
  ├─ Consumer sends ACK  → timer cancelled ✓
  ├─ Consumer sends NACK → timer cancelled, retry scheduled
  └─ 30 seconds pass     → Zeus auto-NACKs: "ack timeout exceeded"
                           message is rescheduled as if NACK was sent
```

This means a crashed or disconnected worker never permanently blocks a queue.

---

## Dead-letter messages

After `max_attempts` failures, the message is marked **dead** in SQLite:

```sql
-- View all dead messages
SELECT queue_name, payload, error_msg, attempts, created_at
FROM queue_messages
WHERE status = 'failed';
```

Dead messages are kept in the DB so you can:
- Inspect what failed and why
- Manually replay them (just update `status = 'pending'` and restart Zeus)
- Alert on them (query the DB on a schedule from your monitoring system)

---

## Persistence (survives restarts)

Queue state is fully persisted to SQLite when `persistence.enabled: true`.

```
OP_QUEUE_PUSH    → message INSERT'd into queue_messages table
OP_QUEUE_ACK     → row DELETE'd
OP_QUEUE_NACK    → row UPDATE'd (attempts++, next_retry set)
max attempts hit → row UPDATE'd (status = 'failed')
```

**On restart**, Zeus reads all pending messages from SQLite and re-injects
them into their queues. The retry schedule is preserved — if a message was
scheduled to retry in 45 seconds when the server stopped, Zeus respects that
timing.

---

## Multiple consumers (round-robin)

You can register multiple workers on the same queue. Zeus delivers messages
round-robin across available workers:

```
Worker A: OP_QUEUE_CONSUME on "send-emails"
Worker B: OP_QUEUE_CONSUME on "send-emails"
Worker C: OP_QUEUE_CONSUME on "send-emails"

Message 1 → Worker A
Message 2 → Worker B
Message 3 → Worker C
Message 4 → Worker A  (wraps around)
...
```

If a worker disconnects, its in-flight messages are automatically re-queued
and delivered to the remaining workers.

---

## Practical example: email sending queue

**Producer (your API server):**
```
1. User registers → your API creates the account
2. API sends OP_QUEUE_PUSH to "send-welcome-email"
   Body: {"to":"alice@example.com","name":"Alice"}
3. API responds to user: "Account created!"
   (email sending is now Zeus's problem)
```

**Consumer (your email worker):**
```
1. Connect to Zeus, send OP_QUEUE_CONSUME on "send-welcome-email"
2. Receive OP_PUSH_QUEUE with the JSON payload
3. Call SendGrid / SES / etc.
   ├─ Success → send OP_QUEUE_ACK
   └─ Failure → send OP_QUEUE_NACK with error message
                 Zeus retries after back-off
```

---

## Queue limits

```yaml
queues:
  max_queues: 200         # Max number of named queues
  max_queue_depth: 10000  # Max pending messages per queue
```

If `max_queue_depth` is reached, new `OP_QUEUE_PUSH` returns `STATUS_QUEUE_FULL`.
