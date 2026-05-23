// Package pubsub implements Zeus's channel (pub/sub) system.
//
// CONCEPT
// ───────
//  A Channel is a named topic. Clients subscribe to a channel to receive
//  any message published to it. This is fan-out delivery:
//    Publisher ──▶ Channel ──▶ All Subscribers
//
//  Think of it like a live event feed — stock ticks, sensor readings,
//  admin broadcasts, or any data you want to push to many clients at once.
//
// HOW IT WORKS
// ────────────
//  1. Client sends OP_SUBSCRIBE with Key = channel name.
//  2. Zeus adds the client's write channel to the subscriber list.
//  3. If history_size > 0, Zeus replays the last N messages immediately.
//  4. Client sends OP_PUBLISH with Key = channel name, Body = payload.
//  5. Zeus broadcasts the payload to every subscriber's goroutine-safe channel.
//  6. Each subscriber's connection goroutine reads from its channel and
//     writes an OP_PUSH_CHANNEL frame to the TCP connection.
//
// BACK-PRESSURE
// ─────────────
//  Each subscriber has a buffered Go channel (size 256). If a subscriber is
//  slow and its buffer fills up, Zeus drops the message for THAT subscriber
//  only — other fast subscribers are not affected. The slow subscriber gets a
//  log warning. This prevents one lagging client from stalling everyone else.
package pubsub

import (
	"fmt"
	"sync"
	"time"
)

const subscriberBufferSize = 256 // per-subscriber message buffer

// Message is one published payload on a channel.
type Message struct {
	ID        uint64    // monotonically increasing within the channel
	Payload   []byte    // raw bytes (whatever the publisher sent)
	PublishedAt time.Time
}

// Subscriber represents one connected listener on a channel.
type Subscriber struct {
	ID   string       // unique client ID
	Send chan *Message // connection handler reads from this to write to TCP
}

// Channel is one named pub/sub topic.
type Channel struct {
	mu          sync.RWMutex
	name        string
	subscribers map[string]*Subscriber // clientID → subscriber

	// Circular history ring — replayed to new subscribers
	history    []*Message
	historyMax int
	nextID     uint64 // monotonically increasing message ID

	// RETAIN: if true, the last message is stored and sent to late subscribers.
	// Set by the FLAG_RETAIN bit on OP_SUBSCRIBE.
	retained *Message
}

// newChannel creates an empty Channel with the given history size.
func newChannel(name string, historySize int) *Channel {
	return &Channel{
		name:        name,
		subscribers: make(map[string]*Subscriber),
		history:     make([]*Message, 0, historySize),
		historyMax:  historySize,
	}
}

// Subscribe adds a subscriber. If replay is true, queued history is sent
// immediately into the subscriber's Send channel.
func (c *Channel) Subscribe(sub *Subscriber, replay bool) {
	c.mu.Lock()
	c.subscribers[sub.ID] = sub

	// Snapshot history while holding the lock
	var snap []*Message
	if replay {
		snap = make([]*Message, len(c.history))
		copy(snap, c.history)
	}
	retained := c.retained
	c.mu.Unlock()

	// Replay history without holding the lock
	if replay {
		for _, msg := range snap {
			select {
			case sub.Send <- msg:
			default:
				// buffer full — skip old history, the live stream is more important
			}
		}
	} else if retained != nil {
		// Even without full replay, send the last retained message
		select {
		case sub.Send <- retained:
		default:
		}
	}
}

// Unsubscribe removes a subscriber. It does NOT close the Send channel —
// the connection handler owns that.
func (c *Channel) Unsubscribe(clientID string) {
	c.mu.Lock()
	delete(c.subscribers, clientID)
	c.mu.Unlock()
}

// Publish broadcasts payload to all current subscribers.
// Returns the number of subscribers that received the message.
func (c *Channel) Publish(payload []byte, retain bool) int {
	msg := &Message{
		Payload:     payload,
		PublishedAt: time.Now(),
	}

	c.mu.Lock()
	c.nextID++
	msg.ID = c.nextID

	// Append to history ring (drop oldest if full)
	if c.historyMax > 0 {
		c.history = append(c.history, msg)
		if len(c.history) > c.historyMax {
			c.history = c.history[1:] // evict oldest
		}
	}
	if retain {
		c.retained = msg
	}

	// Take a snapshot of subscriber list while holding the lock
	subs := make([]*Subscriber, 0, len(c.subscribers))
	for _, s := range c.subscribers {
		subs = append(subs, s)
	}
	c.mu.Unlock()

	// Deliver to each subscriber without holding the lock
	delivered := 0
	for _, sub := range subs {
		select {
		case sub.Send <- msg:
			delivered++
		default:
			// Subscriber buffer full — drop for this subscriber only
			// In production you'd increment a metric here
			_ = fmt.Sprintf("warn: channel %s subscriber %s buffer full, dropping msg %d",
				c.name, sub.ID, msg.ID)
		}
	}
	return delivered
}

// SubscriberCount returns the current number of subscribers.
func (c *Channel) SubscriberCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.subscribers)
}

// ── Manager ─────────────────────────────────────────────────

// Manager manages all named channels on the server.
type Manager struct {
	mu         sync.RWMutex
	channels   map[string]*Channel
	maxChannels int
	historySize int
}

// NewManager creates a ChannelManager with the given limits.
func NewManager(maxChannels, historySize int) *Manager {
	return &Manager{
		channels:    make(map[string]*Channel),
		maxChannels: maxChannels,
		historySize: historySize,
	}
}

// GetOrCreate returns an existing channel or creates a new one.
// Returns an error if the channel limit is exceeded.
func (m *Manager) GetOrCreate(name string) (*Channel, error) {
	// Fast path: channel already exists
	m.mu.RLock()
	ch, ok := m.channels[name]
	m.mu.RUnlock()
	if ok {
		return ch, nil
	}

	// Slow path: create it
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if ch, ok = m.channels[name]; ok {
		return ch, nil
	}
	if len(m.channels) >= m.maxChannels {
		return nil, fmt.Errorf("channel limit reached (%d)", m.maxChannels)
	}
	ch = newChannel(name, m.historySize)
	m.channels[name] = ch
	return ch, nil
}

// Publish is a convenience method to publish to a named channel.
// Returns 0 and no error if the channel doesn't exist (no-op).
func (m *Manager) Publish(name string, payload []byte, retain bool) (int, error) {
	m.mu.RLock()
	ch, ok := m.channels[name]
	m.mu.RUnlock()
	if !ok {
		return 0, nil // nobody subscribed yet — perfectly normal
	}
	return ch.Publish(payload, retain), nil
}

// RemoveSubscriber removes a client from ALL channels (called on disconnect).
func (m *Manager) RemoveSubscriber(clientID string) {
	m.mu.RLock()
	chans := make([]*Channel, 0, len(m.channels))
	for _, ch := range m.channels {
		chans = append(chans, ch)
	}
	m.mu.RUnlock()

	for _, ch := range chans {
		ch.Unsubscribe(clientID)
	}
}

// Stats returns a snapshot of channel names → subscriber counts.
func (m *Manager) Stats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int, len(m.channels))
	for name, ch := range m.channels {
		out[name] = ch.SubscriberCount()
	}
	return out
}

// NewSubscriber creates a new Subscriber with a buffered message channel.
func NewSubscriber(clientID string) *Subscriber {
	return &Subscriber{
		ID:   clientID,
		Send: make(chan *Message, subscriberBufferSize),
	}
}
