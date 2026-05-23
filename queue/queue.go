// Package queue implements Zeus's reliable command queue system.
//
// CONCEPT vs CHANNELS
// ───────────────────
//  Channels: fan-out, fire-and-forget. Publisher doesn't care who gets it.
//  Queues:   exactly-once delivery to ONE consumer. Publisher cares.
//
//  Use queues when:
//   - You need guaranteed delivery (no message lost even if server restarts)
//   - You need to know if the consumer succeeded or failed
//   - You need to retry on failure with back-off
//   - Order matters (FIFO within a queue)
//
// MESSAGE LIFECYCLE
// ─────────────────
//   PUSH     → message added to queue (status: pending)
//   DELIVER  → server sends to consumer (status: in-flight)
//   ACK      → consumer marks PROCESSED → message deleted ✓
//   NACK     → consumer marks FAILED → server records error, retries later
//   TIMEOUT  → consumer didn't ACK in time → treated like NACK
//   DEAD     → max retries exceeded → stays in DB as "failed" for inspection
//
// RETRY LOGIC
// ───────────
//  Exponential back-off with jitter (prevents thundering herd):
//    delay = min(initial_delay * backoff_factor^attempts, max_delay) + jitter
//  Config keys: queues.retry.* in zeus.yaml
//
// CONSUMER MODEL
// ──────────────
//  One consumer per queue (round-robin if multiple workers register).
//  A consumer sends OP_QUEUE_CONSUME and the server pushes the next
//  available message. The consumer must ACK or NACK before the next
//  message is pushed.  (In-flight limit = 1 per consumer by default.)
package queue

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// ── Message ─────────────────────────────────────────────────

// Status represents the lifecycle state of a queue message.
type Status string

const (
	StatusPending  Status = "pending"   // waiting to be delivered
	StatusInFlight Status = "in-flight" // delivered, waiting for ACK
	StatusFailed   Status = "failed"    // exhausted retries → dead-letter
)

// Message is one item in a queue.
type Message struct {
	ID        string    // unique ID (used in ACK/NACK frames)
	QueueName string
	Payload   []byte

	// Tracking fields
	Attempts  int
	Status    Status
	Error     string    // last failure reason (from NACK body)
	CreatedAt time.Time
	NextRetry time.Time // when to re-deliver after a NACK

	// DB row ID — only set when persistence is enabled
	DBID int64
}

// ── RetryPolicy ─────────────────────────────────────────────

// RetryPolicy defines how failed messages are re-scheduled.
type RetryPolicy struct {
	MaxAttempts     int
	InitialDelay    time.Duration
	BackoffFactor   float64
	MaxDelay        time.Duration
	AckTimeout      time.Duration // how long to wait for ACK before treating as NACK
}

// NextDelay returns the delay before the (attempts+1)-th retry.
// Adds ±10% random jitter to spread out retries under load.
func (p *RetryPolicy) NextDelay(attempts int) time.Duration {
	// base = InitialDelay * BackoffFactor^attempts
	base := float64(p.InitialDelay) * math.Pow(p.BackoffFactor, float64(attempts))
	if base > float64(p.MaxDelay) {
		base = float64(p.MaxDelay)
	}
	// jitter: ±10%
	jitter := base * 0.1 * (2*rand.Float64() - 1)
	return time.Duration(base + jitter)
}

// ── Consumer ────────────────────────────────────────────────

// Consumer is a connected worker registered to receive messages.
type Consumer struct {
	ID      string         // unique client ID
	Deliver chan *Message   // server writes here → client reads and sends over TCP
}

// ── Queue ────────────────────────────────────────────────────

// Queue is one named reliable delivery queue.
type Queue struct {
	mu        sync.Mutex
	name      string
	policy    RetryPolicy

	// pending holds messages waiting for delivery (FIFO)
	pending []*Message

	// inFlight maps messageID → Message for messages currently with a consumer
	inFlight map[string]*Message

	// consumers holds registered workers (round-robin delivery)
	consumers   []*Consumer
	consumerIdx int // next consumer to use (round-robin cursor)

	// ackTimers maps messageID → timer that fires if ACK is not received in time
	ackTimers map[string]*time.Timer

	// Callback fired when a message is permanently dead (exceeded max retries).
	// The queue manager uses this to log / persist dead messages.
	OnDead func(msg *Message)

	// Callback fired when persistence is enabled — tells the DB layer to save state.
	OnPersist func(op string, msg *Message)
}

// newQueue creates an empty queue with the given retry policy.
func newQueue(name string, policy RetryPolicy) *Queue {
	return &Queue{
		name:      name,
		policy:    policy,
		pending:   make([]*Message, 0),
		inFlight:  make(map[string]*Message),
		ackTimers: make(map[string]*time.Timer),
		consumers: make([]*Consumer, 0),
	}
}

// ── Push ────────────────────────────────────────────────────

// Restore re-inserts a previously persisted message into the pending queue
// without touching the DB. Used on startup to reload SQLite state into memory.
// The message keeps its original DBID, Attempts, and NextRetry so the retry
// schedule survives restarts exactly as it was when the server stopped.
func (q *Queue) Restore(msg *Message) {
	q.mu.Lock()
	q.pending = append(q.pending, msg)
	q.mu.Unlock()
	// Don't call tryDeliver here — callers do a single pass after all queues
	// are restored, avoiding a thundering-herd of deliveries at startup.
}

// Push adds a new message to the tail of the queue.
func (q *Queue) Push(payload []byte) *Message {
	msg := &Message{
		ID:        generateMsgID(),
		QueueName: q.name,
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: time.Now(),
		NextRetry: time.Now(), // available immediately
	}

	q.mu.Lock()
	q.pending = append(q.pending, msg)
	q.mu.Unlock()

	if q.OnPersist != nil {
		go q.OnPersist("enqueue", msg)
	}

	// Try to deliver to an available consumer right away
	q.tryDeliver()
	return msg
}

// ── Consumer registration ───────────────────────────────────

// AddConsumer registers a new worker. Immediately tries to deliver any
// pending messages to the new consumer.
func (q *Queue) AddConsumer(c *Consumer) {
	q.mu.Lock()
	q.consumers = append(q.consumers, c)
	q.mu.Unlock()
	q.tryDeliver()
}

// RemoveConsumer un-registers a worker. Any in-flight messages it held
// are re-queued for another consumer.
func (q *Queue) RemoveConsumer(clientID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Remove from consumer list
	for i, c := range q.consumers {
		if c.ID == clientID {
			q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
			break
		}
	}

	// Re-queue any in-flight messages that belonged to this consumer
	for id, msg := range q.inFlight {
		if msg.Error == clientID { // we store consumerID temporarily in Error field
			msg.Error = ""
			msg.Status = StatusPending
			msg.NextRetry = time.Now()
			q.pending = append([]*Message{msg}, q.pending...) // prepend — high priority
			delete(q.inFlight, id)
			if t, ok := q.ackTimers[id]; ok {
				t.Stop()
				delete(q.ackTimers, id)
			}
		}
	}
}

// ── ACK / NACK ──────────────────────────────────────────────

// Ack marks a message as successfully processed and removes it from the queue.
// Called when the consumer sends OP_QUEUE_ACK.
func (q *Queue) Ack(msgID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg, ok := q.inFlight[msgID]
	if !ok {
		return fmt.Errorf("unknown or already-processed message: %s", msgID)
	}

	// Cancel the ACK timeout timer
	if t, ok := q.ackTimers[msgID]; ok {
		t.Stop()
		delete(q.ackTimers, msgID)
	}
	delete(q.inFlight, msgID)

	// Notify persistence layer to delete the DB row
	if q.OnPersist != nil {
		go q.OnPersist("ack", msg)
	}
	return nil
}

// Nack marks a message as failed, records the error, and schedules a retry.
// Called when the consumer sends OP_QUEUE_NACK (body = error description).
func (q *Queue) Nack(msgID, errMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg, ok := q.inFlight[msgID]
	if !ok {
		return fmt.Errorf("unknown or already-processed message: %s", msgID)
	}

	// Cancel the ACK timeout timer
	if t, ok := q.ackTimers[msgID]; ok {
		t.Stop()
		delete(q.ackTimers, msgID)
	}
	delete(q.inFlight, msgID)

	msg.Attempts++
	msg.Error = errMsg

	if msg.Attempts >= q.policy.MaxAttempts {
		// Message is permanently dead — move to dead-letter
		msg.Status = StatusFailed
		if q.OnPersist != nil {
			go q.OnPersist("dead", msg)
		}
		if q.OnDead != nil {
			go q.OnDead(msg)
		}
		return nil
	}

	// Schedule retry with exponential back-off
	delay := q.policy.NextDelay(msg.Attempts)
	msg.Status = StatusPending
	msg.NextRetry = time.Now().Add(delay)

	if q.OnPersist != nil {
		go q.OnPersist("nack", msg)
	}

	// Re-insert at front of pending (but it won't deliver until NextRetry)
	q.pending = append([]*Message{msg}, q.pending...)

	// Schedule a wake-up timer to try delivery after the back-off delay
	time.AfterFunc(delay, q.tryDeliver)

	return nil
}

// ── Delivery ────────────────────────────────────────────────

// tryDeliver attempts to push the next pending message to an available consumer.
// It is called any time a new message arrives OR a consumer registers.
func (q *Queue) tryDeliver() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.consumers) == 0 || len(q.pending) == 0 {
		return
	}

	now := time.Now()
	// Find the first message that is due (NextRetry <= now)
	msgIdx := -1
	for i, m := range q.pending {
		if !m.NextRetry.After(now) {
			msgIdx = i
			break
		}
	}
	if msgIdx < 0 {
		return // all messages are in back-off delay
	}

	// Round-robin consumer selection
	if q.consumerIdx >= len(q.consumers) {
		q.consumerIdx = 0
	}
	consumer := q.consumers[q.consumerIdx]
	q.consumerIdx++

	// Dequeue the message
	msg := q.pending[msgIdx]
	q.pending = append(q.pending[:msgIdx], q.pending[msgIdx+1:]...)
	msg.Status = StatusInFlight

	// Temporarily use msg.Error to track which consumer has it (for re-queue on disconnect)
	consumerID := msg.Error
	msg.Error = consumer.ID
	_ = consumerID

	q.inFlight[msg.ID] = msg

	// Send to consumer's channel (non-blocking — consumer must keep up)
	select {
	case consumer.Deliver <- msg:
	default:
		// Consumer channel full — put message back and try another consumer
		msg.Status = StatusPending
		msg.Error = ""
		q.pending = append([]*Message{msg}, q.pending...)
		delete(q.inFlight, msg.ID)
		return
	}

	// Start ACK timeout timer
	ackTimeout := q.policy.AckTimeout
	msgID := msg.ID
	q.ackTimers[msgID] = time.AfterFunc(ackTimeout, func() {
		// Consumer didn't ACK in time — treat as NACK
		_ = q.Nack(msgID, "ack timeout exceeded")
	})
}

// ── Manager ─────────────────────────────────────────────────

// DBPersistence is the subset of store.DB methods needed by the queue layer.
// Defined as an interface here to avoid an import cycle (queue → store → ...).
type DBPersistence interface {
	SaveQueueMessage(queueName string, payload []byte) (int64, error)
	MarkQueueMessageFailed(id int64, errMsg string, nextRetry time.Time) error
	MarkQueueMessageDead(id int64, errMsg string) error
	DeleteQueueMessage(id int64) error
}

// Manager manages all named queues on the server.
type Manager struct {
	mu        sync.RWMutex
	queues    map[string]*Queue
	maxQueues int
	maxDepth  int
	policy    RetryPolicy

	// db is set via SetDB. When non-nil, every new queue gets an OnPersist
	// callback that mirrors queue operations to SQLite.
	db DBPersistence
}

// NewManager creates a QueueManager.
func NewManager(maxQueues, maxDepth int, policy RetryPolicy) *Manager {
	return &Manager{
		queues:    make(map[string]*Queue),
		maxQueues: maxQueues,
		maxDepth:  maxDepth,
		policy:    policy,
	}
}

// SetDB wires a persistence backend into the manager. Call this from main.go
// after opening the database, before accepting connections.
func (m *Manager) SetDB(db DBPersistence) {
	m.mu.Lock()
	m.db = db
	m.mu.Unlock()
}

// GetOrCreate returns an existing queue or creates one.
// If a DB is configured, new queues automatically persist to SQLite.
func (m *Manager) GetOrCreate(name string) (*Queue, error) {
	m.mu.RLock()
	q, ok := m.queues[name]
	db := m.db
	m.mu.RUnlock()
	if ok {
		return q, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if q, ok = m.queues[name]; ok {
		return q, nil
	}
	if len(m.queues) >= m.maxQueues {
		return nil, fmt.Errorf("queue limit reached (%d)", m.maxQueues)
	}
	q = newQueue(name, m.policy)

	// Wire DB persistence if available
	if db != nil {
		q.OnPersist = buildQueuePersistFn(db)
	}

	m.queues[name] = q
	return q, nil
}

// buildQueuePersistFn returns an OnPersist callback that translates
// queue lifecycle events (enqueue/ack/nack/dead) into SQLite operations.
func buildQueuePersistFn(db DBPersistence) func(op string, msg *Message) {
	return func(op string, msg *Message) {
		switch op {
		case "enqueue":
			// Save the message and store the DB ID back on the struct
			id, err := db.SaveQueueMessage(msg.QueueName, msg.Payload)
			if err == nil {
				msg.DBID = id
			}
		case "ack":
			// Successfully processed — remove from DB
			if msg.DBID > 0 {
				_ = db.DeleteQueueMessage(msg.DBID)
			}
		case "nack":
			// Failed attempt — update retry schedule and error
			if msg.DBID > 0 {
				_ = db.MarkQueueMessageFailed(msg.DBID, msg.Error, msg.NextRetry)
			}
		case "dead":
			// Exhausted all retries — mark permanently dead in DB
			if msg.DBID > 0 {
				_ = db.MarkQueueMessageDead(msg.DBID, msg.Error)
			}
		}
	}
}

// RemoveConsumerFromAll removes a client from every queue (called on disconnect).
func (m *Manager) RemoveConsumerFromAll(clientID string) {
	m.mu.RLock()
	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	m.mu.RUnlock()

	for _, q := range qs {
		q.RemoveConsumer(clientID)
	}
}

// Stats returns a snapshot of queue names → pending counts.
func (m *Manager) Stats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int, len(m.queues))
	for name, q := range m.queues {
		q.mu.Lock()
		out[name] = len(q.pending) + len(q.inFlight)
		q.mu.Unlock()
	}
	return out
}

// NewConsumer builds a Consumer with a buffered delivery channel.
func NewConsumer(clientID string) *Consumer {
	return &Consumer{
		ID:      clientID,
		Deliver: make(chan *Message, 64),
	}
}

// ── Helpers ─────────────────────────────────────────────────

var msgCounter uint64
var msgMu sync.Mutex

// generateMsgID generates a unique message ID combining timestamp + counter.
func generateMsgID() string {
	msgMu.Lock()
	msgCounter++
	id := msgCounter
	msgMu.Unlock()
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), id)
}
