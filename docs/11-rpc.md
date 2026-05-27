# Zeus RPC — Remote Procedure Calls

## What is RPC in Zeus?

Zeus RPC lets a **caller** send a task to a named pool of **workers** and
**await the result** — like calling a function across a TCP connection.

It fills the gap between:

| Feature | Delivery | Result | Disconnect-safe |
|---|---|---|---|
| Channels | broadcast (everyone) | ❌ no reply | ❌ |
| Queues | one worker | ❌ no reply | ✅ (retry) |
| **RPC** | **one worker** | **✅ result returned** | **✅ poll status** |

---

## When should you use RPC?

Use RPC when:

- You need to **delegate work to a specific type of machine** — e.g. send a
  build job to a build server and get the compiled artifact back
- You need **progress updates** — e.g. show a "37% compiled" bar while the
  worker runs
- The caller might **disconnect and reconnect** before the work is done —
  e.g. a mobile app triggering a long export

Real examples:
- Server asks a client device to build a Flutter APK → returns the APK binary
- Offload GPU inference to a worker machine → return the prediction result
- Transcode a video on a media server → return the CDN URL when done
- Run a Docker build remotely → stream build log lines as progress updates

---

## How it works

```
  CALLER                       ZEUS                        WORKER
  ──────                       ────                        ──────
  OP_RPC_CALL                ─────────────────►  dispatches to available worker
  key="flutter-builds"                           │
  body=[30000ms][payload]                        │
                                                 ▼
                                       OP_PUSH_RPC ──────────────────►
                                       body=[callIDLen:1][callID][payload]

                                                              (worker processes...)

                                       ◄────────── OP_RPC_PROGRESS
                                       key=callID, body=[pct:1][message]
  ◄── OP_PUSH_RPC_PROGRESS ──────────────────────
  body=[callIDLen:1][callID][pct][message]

                                       ◄────────── OP_RPC_REPLY
                                       key=callID, body=[isError:1][result]
  ◄── OP_PUSH_RPC_RESULT ─────────────────────────
  body=[callIDLen:1][callID][result]
```

### Call lifecycle states

```
  PENDING ──► IN_PROGRESS ──► DONE
                    │
                    ├──► FAILED      (worker replied with error)
                    ├──► CANCELLED   (caller sent OP_RPC_CANCEL)
                    └──► TIMED_OUT   (no reply within timeout)
```

---

## Disconnect-safe design

If the **caller disconnects** mid-call:

1. The call continues running on Zeus — the worker is unaffected
2. Progress updates are stored in memory on the call object
3. When the caller reconnects, it sends `OP_RPC_STATUS key=callID`
4. Zeus returns a JSON snapshot with: current state, all progress updates, and the result (if already done)

This makes Zeus RPC safe for long-running tasks (builds, exports, transcoding)
where the client might lose connectivity.

If a **worker disconnects** mid-call:

1. Zeus detects the disconnect during cleanup
2. The in-progress call is moved back to **PENDING**
3. The next available worker in the group picks it up automatically

---

## Opcodes

### Client → Server

| OpCode | Value | Key | Body | Description |
|---|---|---|---|---|
| `OP_RPC_CONSUME` | `0x65` | group name | — | Register as a worker for this group |
| `OP_RPC_CALL` | `0x60` | group name | `[timeoutMs:4][payload]` | Initiate a call |
| `OP_RPC_REPLY` | `0x61` | callID | `[isError:1][result bytes]` | Worker: send result |
| `OP_RPC_PROGRESS` | `0x62` | callID | `[pct:1][message string]` | Worker: report progress |
| `OP_RPC_CANCEL` | `0x63` | callID | — | Caller: cancel a call |
| `OP_RPC_STATUS` | `0x64` | callID | — | Caller: get current state (after reconnect) |

### Server → Client (push)

| OpCode | Value | Body | Description |
|---|---|---|---|
| `OP_PUSH_RPC` | `0x55` | `[callIDLen:1][callID][payload]` | Server → worker: deliver a call |
| `OP_PUSH_RPC_RESULT` | `0x56` | `[callIDLen:1][callID][result]` | Server → caller: call succeeded |
| `OP_PUSH_RPC_PROGRESS` | `0x57` | `[callIDLen:1][callID][pct:1][message]` | Server → caller: progress update |
| `OP_PUSH_RPC_ERROR` | `0x58` | `[callIDLen:1][callID][error msg]` | Server → caller: call failed/timed out |

---

## Body encoding details

### `OP_RPC_CALL` body
```
[timeoutMs:4][payload bytes]
 └─ uint32 big-endian, milliseconds
    0 = use server default (5 minutes)
```

### `OP_PUSH_RPC` body (delivered to worker)
```
[callIDLen:1][callID bytes][payload bytes]
 └─ uint8, length of callID string
```

### `OP_RPC_REPLY` body (worker → server)
```
[isError:1][result bytes]
 └─ 0x00 = success, 0x01 = error
    rest of body is the result payload (or error message string)
```

### `OP_RPC_PROGRESS` body (worker → server)
```
[pct:1][message string]
 └─ uint8, 0–100 completion percentage
    rest of body is UTF-8 status message
```

### `OP_PUSH_RPC_PROGRESS` body (server → caller)
```
[callIDLen:1][callID][pct:1][message string]
```

### `OP_PUSH_RPC_RESULT` / `OP_PUSH_RPC_ERROR` body (server → caller)
```
[callIDLen:1][callID][payload bytes]
```

---

## OP_RPC_STATUS response

When the caller sends `OP_RPC_STATUS`, Zeus responds with `OP_RESPONSE`
body containing a JSON object:

```json
{
  "id": "rpc-1705123456-42",
  "group": "flutter-builds",
  "state": "in_progress",
  "progress": [
    {"pct": 10, "msg": "Fetching dependencies"},
    {"pct": 37, "msg": "Compiling lib/main.dart"},
    {"pct": 65, "msg": "Linking resources"}
  ],
  "created_at": 1705123456
}
```

When done:
```json
{
  "id": "rpc-1705123456-42",
  "group": "flutter-builds",
  "state": "done",
  "has_result": true,
  "progress": [...],
  "created_at": 1705123456
}
```

> **Note:** The result bytes are **not** included in the status snapshot to
> keep the JSON small. Use the `OP_PUSH_RPC_RESULT` frame that Zeus already
> pushed, or re-request the call (if you missed it) — the result is stored
> in memory until the call expires.

---

## Worker pools and round-robin dispatch

Multiple clients can register as workers for the same group:

```
zeus.rpcConsume("flutter-builds")   // device A
zeus.rpcConsume("flutter-builds")   // device B
zeus.rpcConsume("flutter-builds")   // device C
```

Calls are dispatched **round-robin** across all connected workers.

If **no workers** are connected when a call arrives, the call stays in
`PENDING` state and is dispatched immediately when the next worker connects.

---

## Timeouts

Every call has a timeout. The caller specifies it in the `[timeoutMs:4]`
prefix of `OP_RPC_CALL`. If `timeoutMs = 0`, the server default applies
(5 minutes by default).

When a call times out:
1. Zeus moves the call to `TIMED_OUT` state
2. Zeus pushes `OP_PUSH_RPC_ERROR` to the caller (if connected)
3. The error message is `"call timed out — no worker replied in time"`

---

## Complete SDK examples

### JavaScript / TypeScript

```typescript
import { Zeus } from 'zeus-sdk'

const zeus = new Zeus({ host: 'localhost', port: 7777, token: 'your-token' })
await zeus.connect()

// ── CALLER ─────────────────────────────────────────────────────
// Initiate a call and stream progress
const callID = await zeus.rpc.call('flutter-builds', payload, {
  timeoutMs: 120_000,
  onProgress: (pct, msg) => {
    console.log(`${pct}% — ${msg}`)
  },
})

// Await the final result (resolves when OP_PUSH_RPC_RESULT arrives)
const result = await zeus.rpc.awaitResult(callID)
console.log('Got APK bytes:', result.byteLength)


// ── WORKER ─────────────────────────────────────────────────────
// Register and process calls
zeus.rpc.consume('flutter-builds', async (call) => {
  await call.progress(10, 'Fetching dependencies')
  const apk = await buildFlutterAPK(call.payload)
  await call.progress(100, 'Build complete')
  return apk  // returned as the result
})
```

### Python

```python
import asyncio
from zeus import Zeus

async def main():
    zeus = Zeus(host='localhost', port=7777, token='your-token')
    await zeus.connect()

    # ── CALLER ─────────────────────────────────────────────────
    async def on_progress(pct, msg):
        print(f'{pct}% — {msg}')

    call_id, result = await zeus.rpc.call(
        group='flutter-builds',
        payload=b'my-task-data',
        timeout_ms=120_000,
        on_progress=on_progress,
    )
    print('Result bytes:', len(result))

    # ── WORKER ─────────────────────────────────────────────────
    @zeus.rpc.consume('flutter-builds')
    async def handle_build(call):
        await call.progress(10, 'Fetching dependencies')
        apk = await build_flutter_apk(call.payload)
        await call.progress(100, 'Build complete')
        return apk

asyncio.run(main())
```

### Go

```go
package main

import (
    "context"
    "fmt"
    zeus "github.com/yourname/zeus-go"
)

func main() {
    client, _ := zeus.Dial("localhost:7777", "your-token")
    defer client.Close()

    // ── CALLER ─────────────────────────────────────────────────
    callID, resultCh, err := client.RPCCall(context.Background(),
        "flutter-builds", payload,
        zeus.WithTimeout(120_000),
        zeus.WithProgress(func(pct uint8, msg string) {
            fmt.Printf("%d%% — %s\n", pct, msg)
        }),
    )
    result := <-resultCh
    fmt.Println("Got", len(result.Bytes), "bytes")

    // ── WORKER ─────────────────────────────────────────────────
    client.RPCConsume("flutter-builds", func(call *zeus.RPCCall) ([]byte, error) {
        call.Progress(10, "Fetching dependencies")
        apk, err := buildFlutterAPK(call.Payload)
        call.Progress(100, "Done")
        return apk, err
    })
}
```

---

## Disconnect recovery example

```typescript
const zeus = new Zeus({ ... })
await zeus.connect()

// Save the callID immediately after initiating
const callID = await zeus.rpc.call('transcoder', videoPayload, { timeoutMs: 600_000 })
localStorage.setItem('pendingCall', callID)

// ... connection drops ...

// On reconnect, recover:
const savedCallID = localStorage.getItem('pendingCall')
if (savedCallID) {
    const status = await zeus.rpc.getStatus(savedCallID)
    if (status.state === 'done') {
        // already finished while we were offline
        const result = await zeus.rpc.awaitResult(savedCallID)
        // handle result
    } else if (status.state === 'in_progress') {
        // still running — re-subscribe to updates
        const result = await zeus.rpc.awaitResult(savedCallID)
    }
    // catch up on missed progress
    console.log('Missed progress:', status.progress)
}
```

---

## Security notes

- Any authenticated client can call `OP_RPC_CALL` on any group. If you want
  to restrict who can submit tasks, validate in your worker using the
  payload (e.g. include a signed token in the payload).
- Workers receive the raw payload bytes — do **not** include secrets in
  the group name; use the payload.
- RPC calls are in-memory only. If Zeus restarts, pending/in-progress calls
  are lost. Plan for this in long-running deployments (SQLite persistence
  for RPC is on the roadmap).
