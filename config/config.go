// Package config handles loading, validating, and auto-generating
// the zeus.yaml configuration file.
//
// On first run (no config file found), Zeus will:
//   1. Write a default zeus.yaml to disk
//   2. Auto-generate a secure random auth token and save it back
//   3. Optionally generate a self-signed TLS certificate
package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration structure.
// Every field maps directly to a key in zeus.yaml.
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Security    SecurityConfig    `yaml:"security"`
	Persistence PersistenceConfig `yaml:"persistence"`
	Channels    ChannelsConfig    `yaml:"channels"`
	Queues      QueuesConfig      `yaml:"queues"`
	Chat        ChatConfig        `yaml:"chat"`
	Webhook     WebhookConfig     `yaml:"webhook"`
	Log         LogConfig         `yaml:"log"`
}

// ServerConfig controls the TCP listener.
type ServerConfig struct {
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	MaxConnections int    `yaml:"max_connections"`
	ReadTimeoutSec int    `yaml:"read_timeout_sec"`
	WriteTimeoutSec int   `yaml:"write_timeout_sec"`
}

// SecurityConfig controls authentication and TLS.
type SecurityConfig struct {
	Enabled bool      `yaml:"enabled"`
	Token   string    `yaml:"token"` // auto-filled if blank
	TLS     TLSConfig `yaml:"tls"`
}

// TLSConfig controls certificate settings.
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	AutoGen  bool   `yaml:"auto_gen"` // generate self-signed cert on startup
}

// PersistenceConfig controls SQLite persistence.
type PersistenceConfig struct {
	Enabled         bool   `yaml:"enabled"`
	DBPath          string `yaml:"db_path"`
	SyncIntervalSec int    `yaml:"sync_interval_sec"`
	LoadOnStartup   bool   `yaml:"load_on_startup"`
}

// ChannelsConfig controls pub/sub channel behaviour.
type ChannelsConfig struct {
	Enabled        bool `yaml:"enabled"`
	MaxChannels    int  `yaml:"max_channels"`
	MaxSubscribers int  `yaml:"max_subscribers"`
	HistorySize    int  `yaml:"history_size"` // replay last N messages on subscribe
}

// QueuesConfig controls the reliable delivery queue system.
type QueuesConfig struct {
	Enabled      bool        `yaml:"enabled"`
	MaxQueues    int         `yaml:"max_queues"`
	MaxQueueDepth int        `yaml:"max_queue_depth"`
	Retry        RetryConfig `yaml:"retry"`
	AckTimeoutSec int        `yaml:"ack_timeout_sec"`
}

// RetryConfig defines exponential back-off for unACKed queue messages.
type RetryConfig struct {
	MaxAttempts    int     `yaml:"max_attempts"`
	InitialDelaySec int    `yaml:"initial_delay_sec"`
	BackoffFactor  float64 `yaml:"backoff_factor"`
	MaxDelaySec    int     `yaml:"max_delay_sec"`
}

// ChatConfig controls built-in chat rooms.
type ChatConfig struct {
	Enabled           bool             `yaml:"enabled"`
	MaxRooms          int              `yaml:"max_rooms"`
	MaxMembersPerRoom int              `yaml:"max_members_per_room"`
	HistorySize       int              `yaml:"history_size"`
	PresenceTTLSec    int              `yaml:"presence_ttl_sec"`

	// Features holds opt-in WhatsApp-style capabilities.
	// All features default to off — plain mode is just fast pub/sub chat.
	Features ChatFeatures `yaml:"features"`
}

// ChatFeatures groups all optional advanced chat features.
// Each field maps to a key under chat.features: in zeus.yaml.
type ChatFeatures struct {
	// ReceiptTracking enables sent/delivered/read ticks per message per user.
	// Requires persistence.enabled = true (receipts are stored in SQLite).
	ReceiptTracking bool `yaml:"receipt_tracking"`

	// SmartDelivery: on reconnect, only push messages the user hasn't received.
	// Without this Zeus replays the last history_size messages.
	// Requires ReceiptTracking = true.
	SmartDelivery bool `yaml:"smart_delivery"`

	// Polls: clients can attach polls to messages.
	// Requires persistence.enabled = true.
	Polls bool `yaml:"polls"`

	// UserMetadata: clients attach arbitrary JSON to their room presence
	// (display name, avatar, role, status). Broadcast with presence updates.
	UserMetadata bool `yaml:"user_metadata"`

	// GCAfterDays: auto-delete messages everyone has read after N days.
	// 0 = disabled. Requires ReceiptTracking = true.
	GCAfterDays int `yaml:"gc_after_days"`
}

// WebhookConfig lets Zeus call your backend when chat/queue events happen.
// This is the "plug into your own system" bridge — Zeus fires webhooks so
// your backend can persist data, run auth checks, or trigger side effects.
type WebhookConfig struct {
	// Enabled turns webhooks on/off globally.
	Enabled bool `yaml:"enabled"`

	// URL is your backend endpoint. Zeus will POST JSON to this URL.
	URL string `yaml:"url"`

	// Secret is added as X-Zeus-Signature (HMAC-SHA256 of the body).
	// Your backend verifies this to confirm the call is from Zeus.
	Secret string `yaml:"secret"`

	// TimeoutSec is how long Zeus waits for your backend to respond.
	TimeoutSec int `yaml:"timeout_sec"`

	// RetryOnFailure: if your backend is down, Zeus queues and retries.
	RetryOnFailure bool `yaml:"retry_on_failure"`

	// Events lets you choose which events trigger a webhook call.
	// Leave empty to receive all events.
	Events WebhookEvents `yaml:"events"`
}

// WebhookEvents selects which Zeus events are forwarded to your backend.
type WebhookEvents struct {
	// Chat events
	ChatMessage  bool `yaml:"chat_message"`   // new message sent to a room
	ChatJoin     bool `yaml:"chat_join"`      // user joins a room
	ChatLeave    bool `yaml:"chat_leave"`     // user leaves a room

	// Queue events
	QueueEnqueue bool `yaml:"queue_enqueue"`  // message added to a queue
	QueueAck     bool `yaml:"queue_ack"`      // consumer marked processed
	QueueFail    bool `yaml:"queue_fail"`     // consumer marked failed
	QueueExpire  bool `yaml:"queue_expire"`   // message exceeded max retries

	// Channel events
	ChannelPublish bool `yaml:"channel_publish"` // message published on a channel
}

// LogConfig controls logging output.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // text | json
	File   string `yaml:"file"`   // empty = stdout
}

// ── Defaults ────────────────────────────────────────────────

// defaultConfig returns a fully populated Config with sensible defaults.
// This is written to disk when zeus.yaml does not exist yet.
func defaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            7878,
			MaxConnections:  1000,
			ReadTimeoutSec:  30,
			WriteTimeoutSec: 10,
		},
		Security: SecurityConfig{
			Enabled: true,
			Token:   "", // will be auto-generated below
			TLS: TLSConfig{
				Enabled:  false,
				CertFile: "zeus.crt",
				KeyFile:  "zeus.key",
				AutoGen:  true,
			},
		},
		Persistence: PersistenceConfig{
			Enabled:         false,
			DBPath:          "zeus.db",
			SyncIntervalSec: 5,
			LoadOnStartup:   true,
		},
		Channels: ChannelsConfig{
			Enabled:        true,
			MaxChannels:    500,
			MaxSubscribers: 200,
			HistorySize:    50,
		},
		Queues: QueuesConfig{
			Enabled:       true,
			MaxQueues:     200,
			MaxQueueDepth: 10000,
			AckTimeoutSec: 30,
			Retry: RetryConfig{
				MaxAttempts:     5,
				InitialDelaySec: 1,
				BackoffFactor:   2.0,
				MaxDelaySec:     60,
			},
		},
		Chat: ChatConfig{
			Enabled:           true,
			MaxRooms:          100,
			MaxMembersPerRoom: 500,
			HistorySize:       200,
			PresenceTTLSec:    60,
			Features: ChatFeatures{
				// All off by default — opt in per feature in zeus.yaml
				ReceiptTracking: false,
				SmartDelivery:   false,
				Polls:           false,
				UserMetadata:    false,
				GCAfterDays:     0,
			},
		},
		Webhook: WebhookConfig{
			Enabled:        false,
			URL:            "http://localhost:3000/zeus/webhook",
			Secret:         "", // auto-generated below
			TimeoutSec:     5,
			RetryOnFailure: true,
			Events: WebhookEvents{
				ChatMessage:    true,
				ChatJoin:       true,
				ChatLeave:      true,
				QueueEnqueue:   true,
				QueueAck:       true,
				QueueFail:      true,
				QueueExpire:    true,
				ChannelPublish: false, // high volume — off by default
			},
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
			File:   "",
		},
	}
}

// ── Load / Save ─────────────────────────────────────────────

// Load reads the config file at the given path.
//
// If the file does not exist, Load creates it with defaults and returns
// the default config. This is the "first run" behaviour.
func Load(path string) (*Config, bool, error) {
	firstRun := false

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		// ── First run: generate config ─────────────────────────
		firstRun = true
		cfg := defaultConfig()

		// Auto-generate auth token (32 random bytes → 64 hex chars)
		cfg.Security.Token, err = generateSecret(32)
		if err != nil {
			return nil, true, fmt.Errorf("generate auth token: %w", err)
		}

		// Auto-generate webhook signing secret
		cfg.Webhook.Secret, err = generateSecret(32)
		if err != nil {
			return nil, true, fmt.Errorf("generate webhook secret: %w", err)
		}

		// Write the generated config to disk
		if err = Save(cfg, path); err != nil {
			return nil, true, fmt.Errorf("write default config: %w", err)
		}

		return cfg, firstRun, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read config file: %w", err)
	}

	// ── Normal run: parse existing config ──────────────────
	cfg := defaultConfig() // start with defaults so missing keys are safe
	if err = yaml.Unmarshal(data, cfg); err != nil {
		return nil, false, fmt.Errorf("parse config file: %w", err)
	}

	// If user left token blank in the file, generate one now and save
	if cfg.Security.Enabled && cfg.Security.Token == "" {
		cfg.Security.Token, err = generateSecret(32)
		if err != nil {
			return nil, false, fmt.Errorf("generate missing auth token: %w", err)
		}
		if err = Save(cfg, path); err != nil {
			return nil, false, fmt.Errorf("save generated token: %w", err)
		}
	}

	return cfg, firstRun, nil
}

// Save marshals cfg back to YAML and writes it to path.
// The directory is created if needed.
func Save(cfg *Config, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	// Prepend a small header comment (yaml.Marshal strips comments)
	header := "# Zeus configuration — generated " + time.Now().Format("2006-01-02 15:04:05") + "\n" +
		"# Edit this file to customise Zeus. Changes take effect on restart.\n\n"
	return os.WriteFile(path, append([]byte(header), data...), 0o600)
}

// ── Helpers ─────────────────────────────────────────────────

// Addr returns the full "host:port" listen address.
func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

// ReadTimeout returns the read timeout as a time.Duration.
func (c *Config) ReadTimeout() time.Duration {
	return time.Duration(c.Server.ReadTimeoutSec) * time.Second
}

// WriteTimeout returns the write timeout as a time.Duration.
func (c *Config) WriteTimeout() time.Duration {
	return time.Duration(c.Server.WriteTimeoutSec) * time.Second
}

// AckTimeout returns the queue ACK deadline as a time.Duration.
func (c *Config) AckTimeout() time.Duration {
	return time.Duration(c.Queues.AckTimeoutSec) * time.Second
}

// generateSecret creates a cryptographically random hex string of byteLen bytes.
func generateSecret(byteLen int) (string, error) {
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
