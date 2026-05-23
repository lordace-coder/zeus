// Package chat implements Zeus's built-in chat room system.
//
// PHILOSOPHY — "PLUG IN, DON'T REPLACE YOUR BACKEND"
// ────────────────────────────────────────────────────
//  Zeus handles the hard realtime parts:
//   ✓ Push delivery to thousands of concurrent connections
//   ✓ In-memory presence (online, last-seen)
//   ✓ Recent message history (ring buffer)
//   ✓ Optional delivery receipts (WhatsApp-style ticks)
//   ✓ Optional polls, message metadata, smart GC
//   ✓ Binary-framed protocol (tiny, fast messages)
//
//  Your backend handles:
//   ✓ User auth — you issue the userID Zeus uses
//   ✓ Long-term message storage — your DB, your schema
//   ✓ Push notifications for offline users (FCM/APNs)
//   ✓ Business logic, moderation, reactions, etc.
//
//  The bridge is WEBHOOKS (opt-in in zeus.yaml):
//   Zeus fires HMAC-signed HTTP POSTs to your backend for every event.
//   Your backend can persist, validate, or trigger side-effects.
//   Zero lock-in — swap Zeus out and your data stays in your DB.
//
// WHATSAPP-STYLE FEATURES (all opt-in in zeus.yaml)
// ──────────────────────────────────────────────────
//  receipt_tracking  — sent/delivered/read ticks per message per user
//  smart_delivery    — on reconnect, only push messages not yet delivered
//  polls             — attach a poll to any message; clients vote via OP_CHAT_POLL_VOTE
//  user_metadata     — per-user display name, avatar URL, custom JSON
//  gc_after_days     — auto-delete messages everyone has read after N days
//
// All of these default to false/0 — plain mode is just fast, simple pub/sub chat.
package chat

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ── Feature flags (from config) ─────────────────────────────

// Features controls which optional chat features are enabled.
// Each field maps directly to a key under chat: in zeus.yaml.
type Features struct {
	// ReceiptTracking enables WhatsApp-style sent/delivered/read ticks.
	// When true, Zeus records a receipt row in SQLite for every message
	// and every recipient. Clients can query the state of their messages.
	ReceiptTracking bool

	// SmartDelivery: on reconnect, only push messages the user hasn't
	// received yet (based on receipts in SQLite). Without this, Zeus
	// just replays the last HistorySize messages in memory.
	SmartDelivery bool

	// PollsEnabled lets clients attach polls to messages.
	PollsEnabled bool

	// UserMetadata lets clients attach arbitrary JSON to their room presence
	// (display name, avatar URL, custom status, role, etc.).
	UserMetadata bool

	// GCAfterDays: auto-delete messages that everyone has read after N days.
	// 0 = disabled. Requires ReceiptTracking = true.
	GCAfterDays int
}

// ── Message ──────────────────────────────────────────────────

// MsgType tells the client how to render the message.
type MsgType string

const (
	MsgTypeText     MsgType = "text"
	MsgTypeImage    MsgType = "image"
	MsgTypeVideo    MsgType = "video"
	MsgTypeAudio    MsgType = "audio"
	MsgTypeFile     MsgType = "file"
	MsgTypePoll     MsgType = "poll"
	MsgTypeReaction MsgType = "reaction"
	MsgTypeSystem   MsgType = "system" // join/leave/rename — no user content
)

// ChatMessage is one message in a room.
type ChatMessage struct {
	ID       uint64    // monotonically increasing within the room
	RoomName string
	SenderID string    // opaque user identifier — Zeus doesn't validate this
	MsgType  MsgType
	Payload  []byte    // raw bytes — client controls the format

	// Optional metadata (only populated when Features.UserMetadata is on,
	// or when the client includes it). Clients can put anything here:
	// reply_to_id, filename, caption, duration, thumbnail_url, etc.
	Metadata map[string]interface{}

	// DB row ID — only set when persistence is enabled
	DBID int64

	SentAt time.Time
}

// ── Presence ─────────────────────────────────────────────────

// Member represents one connected user in a room.
type Member struct {
	UserID     string
	ClientID   string // Zeus connection ID (one user can have multiple tabs/devices)
	JoinedAt   time.Time
	LastActive time.Time
	Online     bool

	// UserMeta is arbitrary per-user-per-room JSON, populated when
	// Features.UserMetadata is enabled. Clients set this via OP_CHAT_SET_META.
	UserMeta map[string]interface{}

	send chan *ChatMessage // write here to push to this client's TCP conn
}

// ── Room ─────────────────────────────────────────────────────

// Room is one named chat space.
type Room struct {
	mu          sync.RWMutex
	name        string
	features    Features

	members     map[string]*Member // clientID → Member
	maxMembers  int

	history     []*ChatMessage // in-memory ring buffer
	historyMax  int
	nextID      uint64 // monotonically increasing message counter

	presenceTTL time.Duration
}

// newRoom creates an empty room with the given configuration.
func newRoom(name string, maxMembers, historySize int, presenceTTL time.Duration, feat Features) *Room {
	return &Room{
		name:        name,
		features:    feat,
		members:     make(map[string]*Member),
		maxMembers:  maxMembers,
		history:     make([]*ChatMessage, 0, historySize),
		historyMax:  historySize,
		presenceTTL: presenceTTL,
	}
}

// Join adds a client to the room.
// Returns:
//   send      — channel the connection handler drains to write push frames
//   history   — snapshot of recent messages for replay (may be nil if SmartDelivery handles it)
func (r *Room) Join(clientID, userID string, sendBuf int) (<-chan *ChatMessage, []*ChatMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.members) >= r.maxMembers {
		return nil, nil, fmt.Errorf("room %q is full (%d/%d members)", r.name, len(r.members), r.maxMembers)
	}

	send := make(chan *ChatMessage, sendBuf)
	r.members[clientID] = &Member{
		UserID:     userID,
		ClientID:   clientID,
		JoinedAt:   time.Now(),
		LastActive: time.Now(),
		Online:     true,
		send:       send,
	}

	// Snapshot history for replay (SmartDelivery will filter this further)
	snap := make([]*ChatMessage, len(r.history))
	copy(snap, r.history)
	return send, snap, nil
}

// Leave removes a client from the room.
func (r *Room) Leave(clientID string) {
	r.mu.Lock()
	delete(r.members, clientID)
	r.mu.Unlock()
}

// Send broadcasts a message to all members.
func (r *Room) Send(senderClientID, senderUserID string, msgType MsgType, payload []byte, metadata map[string]interface{}) *ChatMessage {
	msg := &ChatMessage{
		RoomName: r.name,
		SenderID: senderUserID,
		MsgType:  msgType,
		Payload:  payload,
		Metadata: metadata,
		SentAt:   time.Now(),
	}

	r.mu.Lock()
	r.nextID++
	msg.ID = r.nextID

	// Update sender last-active
	if m, ok := r.members[senderClientID]; ok {
		m.LastActive = time.Now()
	}

	// Append to history ring
	if r.historyMax > 0 {
		r.history = append(r.history, msg)
		if len(r.history) > r.historyMax {
			r.history = r.history[1:]
		}
	}

	// Snapshot targets while holding the lock
	targets := make([]*Member, 0, len(r.members))
	for _, m := range r.members {
		targets = append(targets, m)
	}
	r.mu.Unlock()

	// Deliver to each member without holding the lock (non-blocking per member)
	for _, m := range targets {
		select {
		case m.send <- msg:
		default:
			// Member buffer full — drop for this member.
			// They'll catch up via history on reconnect (or SmartDelivery).
		}
	}
	return msg
}

// SetUserMeta attaches metadata to a member's presence in the room.
// Only meaningful when Features.UserMetadata is enabled.
func (r *Room) SetUserMeta(clientID string, meta map[string]interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.members[clientID]
	if !ok {
		return fmt.Errorf("client %s not in room %s", clientID, r.name)
	}
	m.UserMeta = meta
	return nil
}

// Presence returns a snapshot of all members' presence state.
func (r *Room) Presence() []PresenceEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]PresenceEntry, 0, len(r.members))
	for _, m := range r.members {
		entry := PresenceEntry{
			UserID:     m.UserID,
			Online:     m.Online,
			LastActive: m.LastActive,
		}
		if r.features.UserMetadata && len(m.UserMeta) > 0 {
			entry.Meta = m.UserMeta
		}
		out = append(out, entry)
	}
	return out
}

// RefreshPresence updates a member's last-active timestamp (called on PING).
func (r *Room) RefreshPresence(clientID string) {
	r.mu.Lock()
	if m, ok := r.members[clientID]; ok {
		m.LastActive = time.Now()
		m.Online = true
	}
	r.mu.Unlock()
}

// evictStalePresence marks members offline if they've been silent too long.
func (r *Room) evictStalePresence(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl)
	r.mu.Lock()
	for _, m := range r.members {
		if m.LastActive.Before(cutoff) {
			m.Online = false
		}
	}
	r.mu.Unlock()
}

// MemberCount returns the current number of members.
func (r *Room) MemberCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members)
}

// History returns a copy of the in-memory ring buffer.
func (r *Room) History() []*ChatMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snap := make([]*ChatMessage, len(r.history))
	copy(snap, r.history)
	return snap
}

// PresenceEntry is one entry in the presence list.
type PresenceEntry struct {
	UserID     string                 `json:"user_id"`
	Online     bool                   `json:"online"`
	LastActive time.Time              `json:"last_active"`
	Meta       map[string]interface{} `json:"meta,omitempty"` // only when UserMetadata feature is on
}

// ── Webhook client ───────────────────────────────────────────

// webhookRetryJob is one failed POST queued for retry.
type webhookRetryJob struct {
	body     []byte        // already-marshalled JSON
	event    string        // X-Zeus-Event header value
	attempts int           // how many times we've tried
	retryAt  time.Time     // when to next attempt
}

// WebhookClient fires HTTP POST events to the developer's backend.
// This is how Zeus integrates with your system without coupling to it.
// Set webhook.enabled = true in zeus.yaml to activate.
type WebhookClient struct {
	enabled        bool
	url            string
	secret         string
	timeout        time.Duration
	retryOnFailure bool
	client         *http.Client
	events         WebhookEventFilter

	// retryQueue holds failed deliveries waiting to be re-sent.
	// Buffered at 1024 so a flapping backend doesn't block callers.
	retryQueue chan webhookRetryJob
}

// WebhookEventFilter controls which events trigger a webhook call.
// Maps to webhook.events.* in zeus.yaml.
type WebhookEventFilter struct {
	ChatMessage bool
	ChatJoin    bool
	ChatLeave   bool
}

// WebhookPayload is the JSON body POSTed to your backend.
// This format is stable — we treat breaking changes as semver majors.
type WebhookPayload struct {
	Event    string          `json:"event"`              // "chat.message" | "chat.join" | "chat.leave"
	Room     string          `json:"room"`
	UserID   string          `json:"user_id"`
	ClientID string          `json:"client_id"`
	Data     json.RawMessage `json:"data,omitempty"`     // event-specific fields (raw bytes)
	At       time.Time       `json:"at"`
}

// NewWebhookClient creates a WebhookClient from config values.
// retryOnFailure: if true, failed webhook calls are re-queued with exponential
// backoff (up to 5 attempts). Uses an in-process queue — no external deps.
func NewWebhookClient(enabled bool, url, secret string, timeoutSec int, retryOnFailure bool, filter WebhookEventFilter) *WebhookClient {
	wc := &WebhookClient{
		enabled:        enabled,
		url:            url,
		secret:         secret,
		timeout:        time.Duration(timeoutSec) * time.Second,
		retryOnFailure: retryOnFailure,
		client:         &http.Client{Timeout: time.Duration(timeoutSec) * time.Second},
		events:         filter,
		retryQueue:     make(chan webhookRetryJob, 1024),
	}
	if enabled && retryOnFailure {
		go wc.retryWorker()
	}
	return wc
}

// noopWebhook is returned when webhooks are disabled so callers never nil-check.
var noopWebhook = &WebhookClient{retryQueue: make(chan webhookRetryJob, 1)}

// FireMessage fires a "chat.message" webhook. Always async — never delays delivery.
func (w *WebhookClient) FireMessage(room, userID, clientID string, payload []byte) {
	if !w.enabled || !w.events.ChatMessage {
		return
	}
	go w.post(WebhookPayload{
		Event:    "chat.message",
		Room:     room,
		UserID:   userID,
		ClientID: clientID,
		Data:     json.RawMessage(payload),
		At:       time.Now(),
	})
}

// FireJoin fires a "chat.join" webhook.
func (w *WebhookClient) FireJoin(room, userID, clientID string) {
	if !w.enabled || !w.events.ChatJoin {
		return
	}
	go w.post(WebhookPayload{Event: "chat.join", Room: room, UserID: userID, ClientID: clientID, At: time.Now()})
}

// FireLeave fires a "chat.leave" webhook.
func (w *WebhookClient) FireLeave(room, userID, clientID string) {
	if !w.enabled || !w.events.ChatLeave {
		return
	}
	go w.post(WebhookPayload{Event: "chat.leave", Room: room, UserID: userID, ClientID: clientID, At: time.Now()})
}

// post marshals, signs, and POSTs the payload to the webhook URL.
// On failure (network error or 5xx), re-queues for retry if retryOnFailure is on.
func (w *WebhookClient) post(p WebhookPayload) {
	body, err := json.Marshal(p)
	if err != nil {
		return
	}
	w.sendRaw(body, p.Event, 0)
}

// sendRaw does the actual HTTP POST. attempts is how many times we've already
// tried (0 = first attempt). On failure, enqueues a retry job if configured.
func (w *WebhookClient) sendRaw(body []byte, event string, attempts int) {
	req, err := http.NewRequest(http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Zeus-Event", event)
	if w.secret != "" {
		req.Header.Set("X-Zeus-Signature", signBody(w.secret, body))
	}

	resp, err := w.client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode < 500 {
			return // success or client error (don't retry 4xx — that's caller's problem)
		}
	}

	// Delivery failed — enqueue retry if enabled and under the attempt limit
	const maxWebhookAttempts = 5
	if w.retryOnFailure && attempts < maxWebhookAttempts && w.retryQueue != nil {
		// Exponential backoff: 2s, 4s, 8s, 16s, 32s
		delay := time.Duration(1<<uint(attempts+1)) * time.Second
		job := webhookRetryJob{
			body:     body,
			event:    event,
			attempts: attempts + 1,
			retryAt:  time.Now().Add(delay),
		}
		// Non-blocking send — if the buffer is full, drop rather than block callers
		select {
		case w.retryQueue <- job:
		default:
		}
	}
}

// retryWorker runs in its own goroutine and drains the retry queue.
// It sleeps until each job's retryAt time, then re-sends.
func (w *WebhookClient) retryWorker() {
	for job := range w.retryQueue {
		// Sleep until the retry is due
		wait := time.Until(job.retryAt)
		if wait > 0 {
			time.Sleep(wait)
		}
		// Re-send (sendRaw will re-enqueue again if this also fails)
		w.sendRaw(job.body, job.event, job.attempts)
	}
}

// ── Manager ───────────────────────────────────────────────────

// Manager manages all chat rooms and wires features together.
type Manager struct {
	mu          sync.RWMutex
	rooms       map[string]*Room
	maxRooms    int
	maxMembers  int
	historySize int
	presenceTTL time.Duration
	features    Features

	Webhook *WebhookClient
}

// NewManager creates a chat Manager.
// features controls which optional capabilities are active (see Features struct).
// If webhookClient is nil a no-op client is used.
func NewManager(
	maxRooms, maxMembers, historySize, presenceTTLSec int,
	features Features,
	wh *WebhookClient,
) *Manager {
	if wh == nil {
		wh = noopWebhook
	}
	m := &Manager{
		rooms:       make(map[string]*Room),
		maxRooms:    maxRooms,
		maxMembers:  maxMembers,
		historySize: historySize,
		presenceTTL: time.Duration(presenceTTLSec) * time.Second,
		features:    features,
		Webhook:     wh,
	}
	go m.presenceLoop()
	return m
}

// GetOrCreate returns an existing room or creates one.
func (m *Manager) GetOrCreate(name string) (*Room, error) {
	m.mu.RLock()
	r, ok := m.rooms[name]
	m.mu.RUnlock()
	if ok {
		return r, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok = m.rooms[name]; ok {
		return r, nil
	}
	if len(m.rooms) >= m.maxRooms {
		return nil, fmt.Errorf("room limit reached (%d)", m.maxRooms)
	}
	r = newRoom(name, m.maxMembers, m.historySize, m.presenceTTL, m.features)
	m.rooms[name] = r
	return r, nil
}

// Join is the public entry point for a client joining a room.
func (m *Manager) Join(roomName, clientID, userID string) (<-chan *ChatMessage, []*ChatMessage, error) {
	room, err := m.GetOrCreate(roomName)
	if err != nil {
		return nil, nil, err
	}
	send, history, err := room.Join(clientID, userID, 512)
	if err != nil {
		return nil, nil, err
	}
	m.Webhook.FireJoin(roomName, userID, clientID)
	return send, history, nil
}

// Leave removes a client from the room and fires the webhook.
func (m *Manager) Leave(roomName, clientID, userID string) {
	m.mu.RLock()
	r, ok := m.rooms[roomName]
	m.mu.RUnlock()
	if !ok {
		return
	}
	r.Leave(clientID)
	m.Webhook.FireLeave(roomName, userID, clientID)
}

// LeaveAll removes a client from every room (on disconnect).
func (m *Manager) LeaveAll(clientID, userID string) {
	m.mu.RLock()
	rs := make([]*Room, 0, len(m.rooms))
	names := make([]string, 0, len(m.rooms))
	for name, r := range m.rooms {
		rs = append(rs, r)
		names = append(names, name)
	}
	m.mu.RUnlock()
	for i, r := range rs {
		r.Leave(clientID)
		m.Webhook.FireLeave(names[i], userID, clientID)
	}
}

// Send broadcasts a message in a room and fires the webhook.
func (m *Manager) Send(roomName, clientID, userID string, msgType MsgType, payload []byte, metadata map[string]interface{}) (*ChatMessage, error) {
	m.mu.RLock()
	r, ok := m.rooms[roomName]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("room %q not found — join first", roomName)
	}
	msg := r.Send(clientID, userID, msgType, payload, metadata)
	m.Webhook.FireMessage(roomName, userID, clientID, payload)
	return msg, nil
}

// Features returns the feature flags for this manager (read-only).
func (m *Manager) Features() Features { return m.features }

// Stats returns a map of room names → member counts.
func (m *Manager) Stats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int, len(m.rooms))
	for name, r := range m.rooms {
		out[name] = r.MemberCount()
	}
	return out
}

// presenceLoop sweeps all rooms every 10 seconds to mark stale members offline.
func (m *Manager) presenceLoop() {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for range tick.C {
		m.mu.RLock()
		rs := make([]*Room, 0, len(m.rooms))
		for _, r := range m.rooms {
			rs = append(rs, r)
		}
		m.mu.RUnlock()
		for _, r := range rs {
			r.evictStalePresence(m.presenceTTL)
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────

// signBody computes HMAC-SHA256(secret, body) and returns "sha256=<hex>".
// Same convention as GitHub webhooks so backends can reuse existing middleware.
func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
