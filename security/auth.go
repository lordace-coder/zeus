// Package security handles authentication and TLS for Zeus.
//
// HOW AUTH WORKS
// ──────────────
//  1. Client connects over TCP (or TLS if enabled).
//  2. The FIRST frame the client sends MUST be OP_AUTH with body = token.
//  3. Zeus checks the token against the one in zeus.yaml.
//  4. If OK  → sends OP_RESPONSE with STATUS_OK.
//     If bad → sends OP_ERROR with STATUS_AUTH_FAIL, then closes the conn.
//  5. All subsequent frames on that connection are accepted without
//     re-checking (the connection is now "authenticated").
//
// TOKEN GENERATION
// ────────────────
//  When zeus.yaml has security.token == "", Zeus auto-generates a 32-byte
//  (64 hex chars) cryptographically secure token and writes it back into
//  the config file. Print it to stdout so the operator can copy it to
//  their clients.
//
// WEBHOOK SIGNATURE
// ─────────────────
//  Every outgoing webhook POST is signed with HMAC-SHA256 using the
//  webhook.secret from zeus.yaml. The signature is in the header:
//    X-Zeus-Signature: sha256=<hex>
//  Your backend verifies this to confirm the call is genuinely from Zeus.
package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
)

// ── Token auth ─────────────────────────────────────────────

// Auth holds the authentication configuration used at connection time.
type Auth struct {
	Enabled bool
	Token   string // expected token from config
}

// New creates an Auth checker from config values.
func New(enabled bool, token string) *Auth {
	return &Auth{Enabled: enabled, Token: token}
}

// Validate returns true if the provided token matches the configured one.
// If security is disabled, it always returns true.
func (a *Auth) Validate(token string) bool {
	if !a.Enabled {
		return true
	}
	// Use constant-time comparison to prevent timing attacks.
	// An attacker who measures response time could otherwise guess the token
	// one character at a time. hmac.Equal is constant-time.
	return hmac.Equal([]byte(token), []byte(a.Token))
}

// ── TLS ────────────────────────────────────────────────────

// TLSConfig holds TLS settings parsed from zeus.yaml.
type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
	AutoGen  bool
}

// BuildTLSConfig returns a *tls.Config ready to pass to tls.NewListener.
// If AutoGen is true and the cert files don't exist, a self-signed cert
// is generated and written to CertFile/KeyFile.
func BuildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if cfg.AutoGen {
		if err := ensureSelfSignedCert(cfg.CertFile, cfg.KeyFile); err != nil {
			return nil, fmt.Errorf("generate TLS cert: %w", err)
		}
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS cert/key: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		// Minimum TLS 1.2 — prevents downgrade attacks to older insecure versions
		MinVersion: tls.VersionTLS12,
	}, nil
}

// ── Webhook signature ──────────────────────────────────────

// SignBody computes HMAC-SHA256 of body using secret and returns
// a hex string suitable for the X-Zeus-Signature header.
//
// Format: "sha256=<hex>"  (same convention as GitHub webhooks)
func SignBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature checks that sig matches what SignBody would produce
// for the given body and secret. Constant-time to prevent timing attacks.
func VerifySignature(secret string, body []byte, sig string) bool {
	expected := SignBody(secret, body)
	return hmac.Equal([]byte(sig), []byte(expected))
}
