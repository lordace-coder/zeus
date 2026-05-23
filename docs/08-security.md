# Zeus — Security

## Overview

Zeus has three security layers:

| Layer | What it protects | Config key |
|---|---|---|
| **Token auth** | Who can connect to Zeus | `security.enabled` |
| **TLS** | Data in transit (encryption) | `security.tls.enabled` |
| **Webhook signatures** | Validating Zeus → your backend calls | `webhook.secret` |

---

## Token authentication

### How it works

1. Client opens a TCP connection to Zeus
2. Client sends `OP_AUTH` with the token as the body
3. Zeus compares using **constant-time comparison** (prevents timing attacks)
4. Success → connection is marked "authenticated", future frames accepted
5. Failure → Zeus sends an error and closes the connection after 200ms

Every subsequent frame from an authenticated connection is accepted without
re-checking. The connection itself is the security boundary.

### The auth token

When you run `zeus init`, Zeus generates a **64-character hex token** using
`crypto/rand` (cryptographically secure random bytes). It looks like:

```
a3f8c2e1d4b79f2a1c6e8d0b5f3a7c4e9d1b8f2e5c7a0d3f6b9e2c4a7d0f1b3
```

This token is stored in `zeus.yaml` under `security.token`. Your clients
need this token to connect.

### Token rotation

When you rotate the token, the old one stops working immediately on the next
server restart:

```bash
./zeus token rotate
```

Update the token in all your clients before restarting Zeus. Zero-downtime
rotation: deploy new client config, then restart Zeus.

### Disabling auth (development only)

```yaml
security:
  enabled: false
```

With auth disabled, any TCP client can connect and send any commands. Only
use this on localhost during development. **Never in production.**

---

## TLS encryption

### Why TLS?

Without TLS, all data (including your auth token and message payloads)
travels as plaintext. Anyone on the network path can read it.

Enable TLS in `zeus.yaml`:

```yaml
security:
  tls:
    enabled: true
    cert_file: zeus.crt
    key_file:  zeus.key
    auto_gen:  true
```

### Self-signed certificate (auto-generated)

With `auto_gen: true`, Zeus generates a self-signed ECDSA P-256 certificate
automatically on first startup. The cert is valid for 1 year.

Use this for:
- Development environments
- Internal services where you control all clients
- Quick setups where you just need encryption, not identity verification

Clients connecting to a self-signed cert must either:
- Trust the cert explicitly in their code
- Disable cert verification (acceptable for internal use)

### Production TLS (real certificate)

For production (or any service accessible from the internet):

1. Get a certificate from Let's Encrypt, your CA, or a certificate service
2. Put the cert and key files somewhere Zeus can read them
3. Set `auto_gen: false` in zeus.yaml

```yaml
security:
  tls:
    enabled: true
    auto_gen:  false
    cert_file: /etc/ssl/zeus/fullchain.pem
    key_file:  /etc/ssl/zeus/privkey.pem
```

Zeus uses **TLS 1.2 minimum** — older insecure versions are rejected.

---

## Webhook signatures

When Zeus calls your backend's webhook URL, it includes a signature header:

```
X-Zeus-Signature: sha256=a4f2c1d8e3b7...
```

This is an **HMAC-SHA256** of the request body, using your `webhook.secret`
as the key. Your backend should verify this signature to confirm the request
is genuinely from Zeus and not a fake.

### Verification example (Node.js)

```javascript
const crypto = require('crypto');

function verifyZeusWebhook(secret, body, signatureHeader) {
  // signatureHeader = "sha256=abc123..."
  const expected = 'sha256=' + crypto
    .createHmac('sha256', secret)
    .update(body)
    .digest('hex');

  // Use timingSafeEqual to prevent timing attacks
  return crypto.timingSafeEqual(
    Buffer.from(signatureHeader),
    Buffer.from(expected)
  );
}

// In your Express handler:
app.post('/zeus/webhook', (req, res) => {
  const sig = req.headers['x-zeus-signature'];
  const rawBody = req.rawBody; // make sure you have raw body middleware

  if (!verifyZeusWebhook(process.env.ZEUS_WEBHOOK_SECRET, rawBody, sig)) {
    return res.status(401).json({ error: 'invalid signature' });
  }

  // Safe to process
  const event = req.body;
  console.log('Zeus event:', event.event, 'room:', event.room);
  res.json({ ok: true });
});
```

### Verification example (Go)

```go
import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "strings"
)

func verifyZeusSignature(secret string, body []byte, header string) bool {
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write(body)
    expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))

    // Constant-time comparison
    return hmac.Equal([]byte(header), []byte(expected))
}
```

### Verification example (Python)

```python
import hmac
import hashlib

def verify_zeus_signature(secret: str, body: bytes, header: str) -> bool:
    expected = 'sha256=' + hmac.new(
        secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(header, expected)
```

---

## Security checklist

Before going to production:

```
□ security.enabled: true  (token auth on)
□ security.tls.enabled: true  (encrypt traffic)
□ Use a real TLS certificate (not self-signed for public services)
□ Store the auth token securely (env var, secrets manager)
□ Set webhook.secret to a long random string
□ Verify X-Zeus-Signature in your webhook handler
□ Firewall the Zeus port so only your servers can reach it
□ Rotate the token if it's ever exposed
```

---

## What Zeus does NOT protect against

Zeus secures the **connection and transit layer**. It does not:

- Validate user identity (you issue the userID — Zeus trusts it)
- Enforce room access control (any authenticated client can join any room)
- Rate-limit individual users (all authenticated connections are equal)

For user-level access control, use the webhook system:
1. Client asks your backend if they're allowed to join a room
2. Backend returns a short-lived room token
3. Client includes this token in the room name or message body
4. Your webhook handler validates it on every event

This keeps Zeus simple and your business logic in your backend.
