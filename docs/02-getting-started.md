# Zeus — Getting Started

## Step 1 — Build Zeus

```bash
# From the zeus/ directory
go build -o zeus .

# Or install it globally
go install .
```

You now have a `zeus` binary in your PATH (or `./zeus` locally).

---

## Step 2 — First run (init)

```bash
./zeus init
```

This creates `zeus.yaml` in the current directory with **all defaults filled in**
and a **freshly generated auth token**. Output looks like:

```
  ╔══════════════════════════════════════════════════╗
  ║          Zeus initialised successfully!          ║
  ╚══════════════════════════════════════════════════╝

  Config saved to: zeus.yaml

  ┌─ Auth Token (copy this to your clients) ─────────
  │  a3f8c2e1d4b7...9f2a1c (64 hex chars)
  └──────────────────────────────────────────────────

  Next steps:
   1. Edit zeus.yaml to configure persistence, TLS, webhooks, etc.
   2. Run 'zeus' (no args) to start the server
   3. Connect your client using the token above
```

**Copy that token.** Your clients need it to authenticate.

---

## Step 3 — Start the server

```bash
./zeus
```

Zeus reads `zeus.yaml`, starts listening, and prints:

```
2024/01/15 10:30:00 [zeus] starting on 0.0.0.0:7878
2024/01/15 10:30:00 [zeus] ready — listening on 0.0.0.0:7878
2024/01/15 10:30:00 [zeus] auth token: a3f8c2e1d4b7...
```

---

## Step 4 — Connect a client

Zeus speaks a **binary protocol** over TCP. Here is the minimal connection
sequence in pseudocode (any language):

```
1. Open TCP connection to zeus-host:7878
2. Send OP_AUTH frame  (body = your token string)
3. Receive OP_RESPONSE — body[0] == 0x00 means OK
4. Now you can send any other frames
```

See [03-protocol.md](03-protocol.md) for the exact binary layout.

---

## CLI commands

```bash
zeus              # Start the server (reads zeus.yaml)
zeus init         # Create zeus.yaml with a fresh token
zeus token        # Print the current auth token
zeus token rotate # Generate a new auth token (old one stops working)
zeus status       # Print a summary of the current configuration
zeus help         # Show all commands
```

### zeus token rotate

Use this if you suspect your token was leaked:

```bash
./zeus token rotate

  Old token: a3f8c2e1...
  New token: b7d4f9a2...

  ⚠  Update this token in all your clients — the old one is now invalid.
  ⚠  Restart Zeus for the new token to take effect.
```

### zeus status

Shows your current config at a glance:

```
  Zeus Server Configuration
  ─────────────────────────
  Listen address : 0.0.0.0:7878
  Security       : enabled
  TLS            : disabled
  Persistence    : disabled
  Channels       : enabled (max 500, history 50)
  Queues         : enabled (max 200, depth 10000, ack timeout 30s)
  Chat           : enabled (max 100 rooms, history 200)
  Webhooks       : disabled
```

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `ZEUS_CONFIG` | `zeus.yaml` | Path to config file |

Example — run two Zeus instances with different configs:

```bash
ZEUS_CONFIG=/etc/zeus/prod.yaml ./zeus
ZEUS_CONFIG=/etc/zeus/staging.yaml ./zeus
```

---

## Enabling persistence (SQLite)

By default Zeus keeps everything in memory. To survive restarts:

```yaml
# in zeus.yaml
persistence:
  enabled: true
  db_path: "zeus.db"
  load_on_startup: true
```

On restart Zeus will:
- Restore all cache keys from `zeus.db`
- Restore all pending queue messages (retrying where they left off)
- Serve chat history from the DB (longer than the in-memory ring buffer)

---

## Enabling TLS

```yaml
security:
  tls:
    enabled: true
    auto_gen: true      # Zeus generates a self-signed cert automatically
    cert_file: zeus.crt
    key_file:  zeus.key
```

For production, replace `auto_gen: true` with your real cert files from
Let's Encrypt or your CA, and set `auto_gen: false`.

---

## Graceful shutdown

Zeus catches `SIGINT` (Ctrl+C) and `SIGTERM` (from systemd/Docker) and shuts
down cleanly — the listener closes and the process exits after all in-flight
frames finish writing.

```bash
kill -TERM $(pgrep zeus)   # clean shutdown
# or
Ctrl+C                     # same thing interactively
```
