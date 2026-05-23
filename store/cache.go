// Package store contains Zeus's in-memory key/value cache.
//
// DESIGN
// ──────
//  - Keys and values are plain byte slices so clients can store anything:
//    strings, JSON blobs, serialised protobuf, raw binary, etc.
//  - Every entry has an optional TTL (time-to-live). Expired entries are
//    lazily removed on read and periodically cleaned by a background goroutine.
//  - The store is safe for concurrent use from many goroutines (sync.RWMutex).
//  - If persistence is enabled in zeus.yaml, the store notifies a callback
//    (set via OnChange) whenever a key is written or deleted. The DB layer
//    listens on that callback to replicate changes to SQLite.
package store

import (
	"sync"
	"time"
)

// CacheStore is the interface both Cache and ShardedCache implement.
// Using an interface here means the rest of the codebase (server, main)
// doesn't need to know which implementation is in use.
type CacheStore interface {
	Set(key string, value []byte, ttl time.Duration)
	Get(key string) ([]byte, bool)
	Delete(key string)
	Clear()
	Len() int
	Keys() []string
	LoadBulk(items map[string][]byte)
}

// Entry is one cached item.
type Entry struct {
	Value     []byte    // raw stored bytes
	ExpiresAt time.Time // zero value means "no expiry"
}

// expired returns true if the entry has a TTL and it has passed.
func (e *Entry) expired() bool {
	return !e.ExpiresAt.IsZero() && time.Now().After(e.ExpiresAt)
}

// ChangeEvent is sent to the OnChange callback when the cache mutates.
// The DB layer uses these events to keep SQLite in sync.
type ChangeEvent struct {
	Op    string // "set" | "delete" | "clear"
	Key   string
	Value []byte
	TTL   time.Duration
}

// Cache is a concurrent in-memory key/value store.
type Cache struct {
	mu       sync.RWMutex
	data     map[string]*Entry

	// OnChange is called (in a goroutine) on every write/delete.
	// Set this to hook the cache into the persistence layer.
	// It is nil by default (no-op).
	OnChange func(ChangeEvent)
}

// NewCache creates an empty Cache and starts a background cleanup goroutine
// that evicts expired entries every 30 seconds.
func NewCache() *Cache {
	c := &Cache{
		data: make(map[string]*Entry),
	}
	go c.cleanupLoop()
	return c
}

// ── Core operations ────────────────────────────────────────

// Set stores value under key. If ttl > 0 the entry expires after that duration.
// If ttl == 0, the entry never expires.
func (c *Cache) Set(key string, value []byte, ttl time.Duration) {
	entry := &Entry{Value: value}
	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(ttl)
	}

	c.mu.Lock()
	c.data[key] = entry
	c.mu.Unlock()

	// Notify persistence layer asynchronously so we don't block the caller
	c.notify(ChangeEvent{Op: "set", Key: key, Value: value, TTL: ttl})
}

// Get retrieves the value for key. Returns (nil, false) if the key doesn't
// exist or has expired.
func (c *Cache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	entry, ok := c.data[key]
	c.mu.RUnlock()

	if !ok || entry.expired() {
		if ok && entry.expired() {
			// Lazily remove the expired entry
			c.mu.Lock()
			delete(c.data, key)
			c.mu.Unlock()
		}
		return nil, false
	}
	return entry.Value, true
}

// Delete removes key from the cache. No-op if key doesn't exist.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	delete(c.data, key)
	c.mu.Unlock()

	c.notify(ChangeEvent{Op: "delete", Key: key})
}

// Clear removes ALL keys from the cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	c.data = make(map[string]*Entry)
	c.mu.Unlock()

	c.notify(ChangeEvent{Op: "clear"})
}

// Len returns the number of (possibly expired) entries currently in memory.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

// Keys returns a snapshot of all non-expired keys.
func (c *Cache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, len(c.data))
	now := time.Now()
	for k, e := range c.data {
		if e.ExpiresAt.IsZero() || now.Before(e.ExpiresAt) {
			keys = append(keys, k)
		}
	}
	return keys
}

// LoadBulk populates the cache from a map (used during startup to restore
// persisted data from SQLite). It does NOT trigger OnChange callbacks to
// avoid re-writing data that already lives in the DB.
func (c *Cache) LoadBulk(items map[string][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range items {
		c.data[k] = &Entry{Value: v}
	}
}

// ── Background cleanup ─────────────────────────────────────

// cleanupLoop runs forever, evicting expired entries every 30 seconds.
// This prevents memory from growing unbounded when clients set many TTLs
// without ever reading those keys again.
func (c *Cache) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.evictExpired()
	}
}

func (c *Cache) evictExpired() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.data {
		if !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt) {
			delete(c.data, k)
		}
	}
}

// ── Internal helpers ───────────────────────────────────────

// notify fires the OnChange callback in a separate goroutine so cache
// operations never block on the persistence layer.
func (c *Cache) notify(evt ChangeEvent) {
	if c.OnChange != nil {
		go c.OnChange(evt)
	}
}
