// Package rpc implements Zeus's Remote Procedure Call system.
//
// CONCEPT
// ───────
//  RPC fills the gap between channels (fire-and-forget broadcast) and
//  queues (reliable delivery with no result). It lets a caller send a task
//  to a remote worker and AWAIT the result — like a function call across
//  a TCP connection.
//
//  Classic use cases:
//   - Server asks a client device to build a Flutter app → get the APK back
//   - Server delegates a computation to a GPU worker → get the result back
//   - Any "ask a remote peer to do X and tell me the answer" pattern
//
// CALL LIFECYCLE
// ──────────────
//
//   State: PENDING → IN_PROGRESS → DONE | FAILED | CANCELLED | TIMED_OUT
//
//   PENDING     : call created, waiting for a worker to pick it up
//   IN_PROGRESS : a worker has started; progress updates may arrive
//   DONE        : worker sent a successful reply
//   FAILED      : worker sent OP_RPC_REPLY with an error flag, OR panicked
//   CANCELLED   : caller sent OP_RPC_CANCEL
//   TIMED_OUT   : no worker replied within the call's timeout
//
// DISCONNECT-SAFE DESIGN
// ──────────────────────
//  Calls are stored in memory (and optionally SQLite). If the CALLER
//  disconnects mid-call, the call continues — the worker keeps running.
//  When the caller reconnects, it sends OP_RPC_STATUS with the callID to
//  get the current state, any accumulated progress, and the final result
//  if the call has already completed.
//
//  This makes Zeus RPC safe for long-running tasks (builds, exports,
//  transcoding) where the client might lose connectivity mid-way.
//
// PROGRESS REPORTING
// ──────────────────
//  Workers can send OP_RPC_PROGRESS frames at any time during a call.
//  Each progress frame has:
//   - pct: 0–100 completion percentage
//   - message: human-readable status string (e.g. "Compiling lib/main.dart")
//  Progress frames are forwarded to the caller in real time AND stored
//  so a reconnecting caller can catch up on missed updates.
//
// WORKER MODEL
// ────────────
//  Workers register for a "group" (e.g. "flutter-builds").
//  Zeus routes calls round-robin across all available workers in the group.
//  If no workers are available, calls queue in PENDING state and are
//  delivered when a worker connects.
package rpc

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// ── Call state ────────────────────────────────────────────────────────────

// CallState is the lifecycle state of an RPC call.
type CallState string

const (
	StatePending    CallState = "pending"     // waiting for a worker
	StateInProgress CallState = "in_progress" // worker is executing
	StateDone       CallState = "done"        // worker replied successfully
	StateFailed     CallState = "failed"      // worker failed or errored
	StateCancelled  CallState = "cancelled"   // caller cancelled
	StateTimedOut   CallState = "timed_out"   // no reply within timeout
)

// ── Progress ──────────────────────────────────────────────────────────────

// ProgressUpdate is one progress event from a worker.
type ProgressUpdate struct {
	Pct     uint8     // 0–100
	Message string    // human-readable status
	At      time.Time // when the update was sent
}

// ── Call ──────────────────────────────────────────────────────────────────

// Call represents one RPC call in flight (or completed).
type Call struct {
	mu sync.Mutex

	// Identity
	ID    string // globally unique call ID (e.g. "rpc-1705123456-42")
	Group string // worker group this call targets

	// Payload
	Payload []byte // what the caller sent

	// State
	State     CallState
	CreatedAt time.Time
	UpdatedAt time.Time
	ExpiresAt time.Time // when the call times out

	// Result (set when State == StateDone or StateFailed)
	Result []byte
	ErrMsg string

	// Progress log — all updates accumulated for reconnecting callers
	Progress []ProgressUpdate

	// Routing — which client is currently the caller / worker
	// These are connection-scoped IDs (reset when clients reconnect).
	CallerClientID string
	WorkerClientID string

	// Channel used to push live frames to the caller's write loop.
	// Nil when the caller is disconnected.
	callerSend chan<- CallEvent

	// doneCh is closed when the call reaches a terminal state.
	doneCh chan struct{}
	doneOnce sync.Once
}

// CallEvent is a delivery envelope pushed to the caller's write loop.
// The server reads Op and Payload to build the outgoing protocol frame.
type CallEvent struct {
	Op      byte   // OP_PUSH_RPC_RESULT | OP_PUSH_RPC_PROGRESS | OP_PUSH_RPC_ERROR
	Payload []byte // pre-encoded frame body
}

// newCall creates a fresh Call.
func newCall(id, group string, payload []byte, timeout time.Duration) *Call {
	return &Call{
		ID:        id,
		Group:     group,
		Payload:   payload,
		State:     StatePending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		ExpiresAt: time.Now().Add(timeout),
		doneCh:    make(chan struct{}),
	}
}

// done returns true if the call has reached a terminal state.
func (c *Call) done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.State == StateDone ||
		c.State == StateFailed ||
		c.State == StateCancelled ||
		c.State == StateTimedOut
}

// setDone marks the call as complete and unblocks anyone waiting on doneCh.
func (c *Call) setDone() {
	c.doneOnce.Do(func() { close(c.doneCh) })
}

// deliver sends an event to the caller if connected.
func (c *Call) deliver(evt CallEvent) {
	c.mu.Lock()
	ch := c.callerSend
	c.mu.Unlock()
	if ch != nil {
		select {
		case ch <- evt:
		default:
			// caller send buffer full — skip; they can poll via OP_RPC_STATUS
		}
	}
}

// StatusSnapshot returns a serialisable JSON summary of the call's current state.
// Used for OP_RPC_STATUS responses so reconnecting callers can catch up.
func (c *Call) StatusSnapshot() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return encodeStatusJSON(c)
}

// encodeStatusJSON hand-rolls a compact JSON blob for a call status.
func encodeStatusJSON(c *Call) []byte {
	// Build JSON manually to keep the rpc package dependency-free
	var b []byte
	b = append(b, `{"id":`...)
	b = appendJSONString(b, c.ID)
	b = append(b, `,"group":`...)
	b = appendJSONString(b, c.Group)
	b = append(b, `,"state":`...)
	b = appendJSONString(b, string(c.State))
	if c.ErrMsg != "" {
		b = append(b, `,"error":`...)
		b = appendJSONString(b, c.ErrMsg)
	}
	if len(c.Result) > 0 {
		b = append(b, `,"has_result":true`...)
	}
	b = append(b, `,"progress":[`...)
	for i, p := range c.Progress {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"pct":`...)
		b = appendInt(b, int(p.Pct))
		b = append(b, `,"msg":`...)
		b = appendJSONString(b, p.Message)
		b = append(b, '}')
	}
	b = append(b, `],"created_at":`...)
	b = appendInt(b, int(c.CreatedAt.Unix()))
	b = append(b, '}')
	return b
}

func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	for _, ch := range s {
		switch ch {
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		case '\n':
			b = append(b, '\\', 'n')
		default:
			b = append(b, string(ch)...)
		}
	}
	return append(b, '"')
}

func appendInt(b []byte, n int) []byte {
	return append(b, fmt.Sprintf("%d", n)...)
}

// stub to satisfy compiler — replaced by real call in encodeStatusJSON

// ── Worker ────────────────────────────────────────────────────────────────

// Worker is a connected client registered to handle calls for a group.
type Worker struct {
	ClientID string
	Deliver  chan *Call // server writes pending calls here
}

// NewWorker creates a Worker with a buffered delivery channel.
func NewWorker(clientID string) *Worker {
	return &Worker{
		ClientID: clientID,
		Deliver:  make(chan *Call, 64),
	}
}

// ── Manager ───────────────────────────────────────────────────────────────

// Manager tracks all in-flight RPC calls and registered worker groups.
type Manager struct {
	mu sync.RWMutex

	// calls maps callID → *Call
	calls map[string]*Call

	// groups maps group name → slice of registered workers
	groups map[string][]*Worker
	groupIdx map[string]int // round-robin cursor per group

	// pendingByGroup maps group name → calls waiting for a worker
	pendingByGroup map[string][]*Call

	// callCounter for generating unique IDs
	counter uint64

	// Default timeout for calls that don't specify one
	defaultTimeout time.Duration
}

// NewManager creates an RPC manager.
func NewManager(defaultTimeout time.Duration) *Manager {
	m := &Manager{
		calls:          make(map[string]*Call),
		groups:         make(map[string][]*Worker),
		groupIdx:       make(map[string]int),
		pendingByGroup: make(map[string][]*Call),
		defaultTimeout: defaultTimeout,
	}
	return m
}

// ── Caller API ─────────────────────────────────────────────────────────────

// NewCall creates a new RPC call and queues it for delivery.
// callerSend is the caller's write channel — nil if caller is fire-and-forget.
func (m *Manager) NewCall(
	group string,
	payload []byte,
	timeout time.Duration,
	callerClientID string,
	callerSend chan<- CallEvent,
) *Call {
	if timeout <= 0 {
		timeout = m.defaultTimeout
	}

	m.mu.Lock()
	m.counter++
	id := fmt.Sprintf("rpc-%d-%d", time.Now().UnixNano(), m.counter)
	c := newCall(id, group, payload, timeout)
	c.CallerClientID = callerClientID
	c.callerSend = callerSend
	m.calls[id] = c
	m.mu.Unlock()

	// Start the timeout watchdog
	time.AfterFunc(timeout, func() { m.timeoutCall(id) })

	// Try to dispatch to a worker immediately
	m.dispatch(c)

	return c
}

// NewCallEvent builds a CallEvent for delivery to a caller's write loop.
func NewCallEvent(op byte, payload []byte) CallEvent {
	return CallEvent{Op: op, Payload: payload}
}

// ── Worker API ─────────────────────────────────────────────────────────────

// AddWorker registers a new worker for a group.
// Any pending calls for this group are immediately dispatched to it.
func (m *Manager) AddWorker(group string, w *Worker) {
	m.mu.Lock()
	m.groups[group] = append(m.groups[group], w)
	// Drain pending calls for this group
	pending := m.pendingByGroup[group]
	m.pendingByGroup[group] = nil
	m.mu.Unlock()

	for _, c := range pending {
		m.dispatchToWorker(c, w)
	}
}

// RemoveWorker removes a worker (on disconnect).
// Any calls it was handling go back to pending and wait for another worker.
func (m *Manager) RemoveWorker(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for group, workers := range m.groups {
		for i, w := range workers {
			if w.ClientID == clientID {
				m.groups[group] = append(workers[:i], workers[i+1:]...)
				break
			}
		}
	}

	// Re-queue any in-progress calls assigned to this worker
	for _, c := range m.calls {
		c.mu.Lock()
		if c.WorkerClientID == clientID && c.State == StateInProgress {
			c.State = StatePending
			c.WorkerClientID = ""
			c.UpdatedAt = time.Now()
			group := c.Group
			c.mu.Unlock()
			// Put back in pending queue
			m.pendingByGroup[group] = append(m.pendingByGroup[group], c)
			// Try to dispatch to another worker
			go m.dispatchPending(group)
		} else {
			c.mu.Unlock()
		}
	}
}

// ReconnectCaller wires a reconnected caller back into a call.
// They get the current state immediately, then live updates resume.
func (m *Manager) ReconnectCaller(callID, clientID string, send chan<- CallEvent) (*Call, bool) {
	m.mu.RLock()
	c, ok := m.calls[callID]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}

	c.mu.Lock()
	c.CallerClientID = clientID
	c.callerSend = send
	c.mu.Unlock()
	return c, true
}

// DisconnectCaller removes the live send channel for a caller (on disconnect).
// The call continues running — the caller can reconnect and poll.
func (m *Manager) DisconnectCaller(clientID string) {
	m.mu.RLock()
	calls := make([]*Call, 0)
	for _, c := range m.calls {
		if c.CallerClientID == clientID {
			calls = append(calls, c)
		}
	}
	m.mu.RUnlock()

	for _, c := range calls {
		c.mu.Lock()
		c.callerSend = nil
		c.mu.Unlock()
	}
}

// Reply records a worker's result and notifies the caller.
func (m *Manager) Reply(callID string, result []byte, isError bool) error {
	m.mu.RLock()
	c, ok := m.calls[callID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown call: %s", callID)
	}

	c.mu.Lock()
	if c.done() {
		c.mu.Unlock()
		return fmt.Errorf("call %s already in terminal state %s", callID, c.State)
	}
	if isError {
		c.State = StateFailed
		c.ErrMsg = string(result)
	} else {
		c.State = StateDone
		c.Result = result
	}
	c.UpdatedAt = time.Now()
	c.mu.Unlock()

	c.setDone()

	// Push result to caller if connected
	var op byte
	if isError {
		op = 0x58 // OP_PUSH_RPC_ERROR
	} else {
		op = 0x56 // OP_PUSH_RPC_RESULT
	}
	// Body: [callIDLen:1][callID][result]
	c.deliver(CallEvent{Op: op, Payload: buildResultBody(callID, result)})
	return nil
}

// Progress records a progress update and forwards it to the caller.
func (m *Manager) Progress(callID string, pct uint8, message string) error {
	m.mu.RLock()
	c, ok := m.calls[callID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown call: %s", callID)
	}

	c.mu.Lock()
	if c.done() {
		c.mu.Unlock()
		return nil // silently ignore progress on a completed call
	}
	c.State = StateInProgress
	update := ProgressUpdate{Pct: pct, Message: message, At: time.Now()}
	c.Progress = append(c.Progress, update)
	c.UpdatedAt = time.Now()
	c.mu.Unlock()

	// Body: [callIDLen:1][callID][pct:1][message]
	body := buildProgressBody(callID, pct, message)
	c.deliver(CallEvent{Op: 0x57, Payload: body}) // OP_PUSH_RPC_PROGRESS
	return nil
}

// Cancel marks a call as cancelled and notifies the worker if possible.
func (m *Manager) Cancel(callID string) error {
	m.mu.RLock()
	c, ok := m.calls[callID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown call: %s", callID)
	}

	c.mu.Lock()
	if c.done() {
		c.mu.Unlock()
		return nil
	}
	c.State = StateCancelled
	c.UpdatedAt = time.Now()
	c.mu.Unlock()

	c.setDone()
	return nil
}

// Get returns a call by ID (for OP_RPC_STATUS polling).
func (m *Manager) Get(callID string) (*Call, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.calls[callID]
	return c, ok
}

// ── Internal ──────────────────────────────────────────────────────────────

func (m *Manager) dispatch(c *Call) {
	m.mu.Lock()
	workers := m.groups[c.Group]
	if len(workers) == 0 {
		// No workers available — enqueue
		m.pendingByGroup[c.Group] = append(m.pendingByGroup[c.Group], c)
		m.mu.Unlock()
		return
	}
	// Round-robin
	idx := m.groupIdx[c.Group] % len(workers)
	m.groupIdx[c.Group] = idx + 1
	w := workers[idx]
	m.mu.Unlock()

	m.dispatchToWorker(c, w)
}

func (m *Manager) dispatchToWorker(c *Call, w *Worker) {
	c.mu.Lock()
	if c.done() {
		c.mu.Unlock()
		return
	}
	c.State = StateInProgress
	c.WorkerClientID = w.ClientID
	c.UpdatedAt = time.Now()
	c.mu.Unlock()

	select {
	case w.Deliver <- c:
	default:
		// Worker buffer full — put back to pending
		c.mu.Lock()
		c.State = StatePending
		c.WorkerClientID = ""
		c.mu.Unlock()
		m.mu.Lock()
		m.pendingByGroup[c.Group] = append(m.pendingByGroup[c.Group], c)
		m.mu.Unlock()
	}
}

func (m *Manager) dispatchPending(group string) {
	m.mu.Lock()
	pending := m.pendingByGroup[group]
	if len(pending) == 0 || len(m.groups[group]) == 0 {
		m.mu.Unlock()
		return
	}
	c := pending[0]
	m.pendingByGroup[group] = pending[1:]
	workers := m.groups[group]
	idx := m.groupIdx[group] % len(workers)
	m.groupIdx[group] = idx + 1
	w := workers[idx]
	m.mu.Unlock()

	m.dispatchToWorker(c, w)
}

func (m *Manager) timeoutCall(callID string) {
	m.mu.RLock()
	c, ok := m.calls[callID]
	m.mu.RUnlock()
	if !ok {
		return
	}

	c.mu.Lock()
	if c.done() {
		c.mu.Unlock()
		return
	}
	c.State = StateTimedOut
	c.ErrMsg = "call timed out — no worker replied in time"
	c.UpdatedAt = time.Now()
	c.mu.Unlock()

	c.setDone()

	errBody := buildResultBody(callID, []byte(c.ErrMsg))
	c.deliver(CallEvent{Op: 0x58, Payload: errBody}) // OP_PUSH_RPC_ERROR
}

// ── Wire encoding helpers ─────────────────────────────────────────────────

// BuildWorkerDeliveryBody encodes the body of an OP_PUSH_RPC frame.
// Format: [callIDLen:1][callID][payload]
func BuildWorkerDeliveryBody(callID string, payload []byte) []byte {
	id := []byte(callID)
	body := make([]byte, 1+len(id)+len(payload))
	body[0] = byte(len(id))
	copy(body[1:], id)
	copy(body[1+len(id):], payload)
	return body
}

// ParseWorkerDelivery decodes the body of an OP_PUSH_RPC frame.
func ParseWorkerDelivery(body []byte) (callID string, payload []byte, ok bool) {
	if len(body) < 1 {
		return
	}
	idLen := int(body[0])
	if len(body) < 1+idLen {
		return
	}
	return string(body[1 : 1+idLen]), body[1+idLen:], true
}

// BuildReplyBody encodes the body of an OP_RPC_REPLY frame.
// Format: same as worker delivery — [callIDLen:1][callID][result]
func BuildReplyBody(callID string, result []byte) []byte {
	return BuildWorkerDeliveryBody(callID, result)
}

// ParseReply decodes an OP_RPC_REPLY frame body.
func ParseReply(body []byte) (callID string, result []byte, ok bool) {
	return ParseWorkerDelivery(body)
}

// BuildProgressBody encodes an OP_RPC_PROGRESS body.
// Format: [callIDLen:1][callID][pct:1][message bytes]
func BuildProgressBody(callID string, pct uint8, message string) []byte {
	return buildProgressBody(callID, pct, message)
}

func buildProgressBody(callID string, pct uint8, message string) []byte {
	id := []byte(callID)
	msg := []byte(message)
	body := make([]byte, 1+len(id)+1+len(msg))
	body[0] = byte(len(id))
	copy(body[1:], id)
	body[1+len(id)] = pct
	copy(body[2+len(id):], msg)
	return body
}

// ParseProgress decodes an OP_RPC_PROGRESS body.
func ParseProgress(body []byte) (callID string, pct uint8, message string, ok bool) {
	if len(body) < 2 {
		return
	}
	idLen := int(body[0])
	if len(body) < 1+idLen+1 {
		return
	}
	callID = string(body[1 : 1+idLen])
	pct = body[1+idLen]
	message = string(body[2+idLen:])
	ok = true
	return
}

func buildResultBody(callID string, result []byte) []byte {
	id := []byte(callID)
	body := make([]byte, 1+len(id)+len(result))
	body[0] = byte(len(id))
	copy(body[1:], id)
	copy(body[1+len(id):], result)
	return body
}

// ParseTimeoutMs parses a [timeoutMs:4] prefix from a body.
// Returns the duration and remaining bytes.
func ParseTimeoutMs(body []byte) (timeout time.Duration, rest []byte) {
	if len(body) < 4 {
		return 30 * time.Second, body
	}
	ms := binary.BigEndian.Uint32(body[:4])
	if ms == 0 {
		ms = 30000 // default 30s
	}
	return time.Duration(ms) * time.Millisecond, body[4:]
}
