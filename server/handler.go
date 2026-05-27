// Package server wires the protocol layer to all Zeus subsystems.
//
// CONNECTION LIFECYCLE
// ─────────────────────
//  1. TCP connection accepted by the listener in main.go
//  2. New Client struct created; connection assigned a unique ID
//  3. reader() goroutine starts — blocks on Decode(), dispatches frames
//  4. writer() goroutine starts — drains send channel, encodes frames to TCP
//  5. FIRST frame must be OP_AUTH; failure closes the connection immediately
//  6. After auth, any supported opcode is handled
//  7. On disconnect: all subscriptions cleaned up, queues re-queued, rooms left
//
// CONCURRENCY MODEL
// ─────────────────
//  Each client has exactly two goroutines:
//   - reader: reads from TCP → dispatches to subsystems (no shared state)
//   - writer: drains a Go channel → writes to TCP  (serialised writes)
//
//  The writer channel (Client.send) is the ONLY place all subsystems
//  (channels, queues, chat) inject push frames. This means:
//   - No mutex needed on the TCP write path
//   - Back-pressure is handled by the channel buffer size
//   - If the send buffer is full we close the connection (client too slow)
package server

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"zeus/chat"
	"zeus/config"
	"zeus/protocol"
	"zeus/pubsub"
	"zeus/queue"
	"zeus/rpc"
	"zeus/security"
	"zeus/store"
)

// ── Server ───────────────────────────────────────────────────

// Server is the Zeus TCP server. It holds references to all subsystems.
type Server struct {
	cfg      *config.Config
	auth     *security.Auth
	cache    store.CacheStore // accepts both Cache and ShardedCache
	db       *store.DB        // nil if persistence is disabled
	channels *pubsub.Manager
	queues   *queue.Manager
	chat     *chat.Manager
	rpc      *rpc.Manager     // RPC call router

	connCount int64 // atomic counter for active connections
	mu        sync.RWMutex
	clients   map[string]*Client
}

// New creates a fully wired Server. Pass nil for db if persistence is disabled.
// cache accepts either *store.Cache or *store.ShardedCache via the CacheStore interface.
func New(
	cfg *config.Config,
	auth *security.Auth,
	cache store.CacheStore,
	db *store.DB,
	channels *pubsub.Manager,
	queues *queue.Manager,
	chatMgr *chat.Manager,
	rpcMgr *rpc.Manager,
) *Server {
	return &Server{
		cfg:      cfg,
		auth:     auth,
		cache:    cache,
		db:       db,
		channels: channels,
		queues:   queues,
		chat:     chatMgr,
		rpc:      rpcMgr,
		clients:  make(map[string]*Client),
	}
}

// HandleConn is called in a goroutine for every accepted TCP connection.
func (s *Server) HandleConn(conn net.Conn) {
	// Enforce max connection limit
	count := atomic.AddInt64(&s.connCount, 1)
	defer atomic.AddInt64(&s.connCount, -1)

	if int(count) > s.cfg.Server.MaxConnections {
		_ = protocol.ErrorResponse(0, protocol.STATUS_LIMIT_HIT, "server at max capacity").
			Encode(bufio.NewWriter(conn))
		conn.Close()
		return
	}

	c := newClient(conn, s)
	s.mu.Lock()
	s.clients[c.id] = c
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, c.id)
		s.mu.Unlock()
		c.cleanup()
	}()

	log.Printf("[zeus] client %s connected from %s", c.id, conn.RemoteAddr())
	c.run()
	log.Printf("[zeus] client %s disconnected", c.id)
}

// Stats returns a snapshot of server-wide statistics.
func (s *Server) Stats() map[string]interface{} {
	return map[string]interface{}{
		"active_connections": atomic.LoadInt64(&s.connCount),
		"cache_keys":         s.cache.Len(),
		"channels":           s.channels.Stats(),
		"queues":             s.queues.Stats(),
		"rooms":              s.chat.Stats(),
	}
}

// ── Client ───────────────────────────────────────────────────

// clientIDCounter gives each connection a unique numeric ID.
var clientIDCounter uint64

// Client represents one connected Zeus client.
type Client struct {
	id      string     // unique ID for this connection
	conn    net.Conn
	server  *Server
	authed  bool       // true after successful OP_AUTH

	// userID is set during OP_AUTH (the client passes it in Key field).
	// Zeus doesn't validate this — it's an opaque string your backend knows.
	userID  string

	// send is the writer goroutine's input. All subsystems push frames here.
	// Buffer of 512 frames. If full we close the connection (client is too slow).
	send    chan *protocol.Frame

	// Track which channels this client is subscribed to (for cleanup)
	chanSubs map[string]*pubsub.Subscriber

	// Track which queues this client is consuming (for cleanup)
	queueConss map[string]*queue.Consumer

	// Track which chat rooms this client is in (for cleanup)
	chatRooms map[string]bool

	// rpcSend is the channel used by the RPC manager to push live events
	// (results, progress, errors) to this client's write loop.
	// Each item is an [op byte, payload []byte] pair encoded as rpc.CallEvent.
	rpcSend chan rpc.CallEvent

	// rpcWorkerGroups tracks which RPC groups this client is a worker for
	rpcWorkerGroups map[string]*rpc.Worker

	stopOnce sync.Once
	stopCh   chan struct{}

	writer *bufio.Writer
}

func newClient(conn net.Conn, srv *Server) *Client {
	id := atomic.AddUint64(&clientIDCounter, 1)
	return &Client{
		id:              fmt.Sprintf("c%d", id),
		conn:            conn,
		server:          srv,
		send:            make(chan *protocol.Frame, 512),
		chanSubs:        make(map[string]*pubsub.Subscriber),
		queueConss:      make(map[string]*queue.Consumer),
		chatRooms:       make(map[string]bool),
		rpcSend:         make(chan rpc.CallEvent, 256),
		rpcWorkerGroups: make(map[string]*rpc.Worker),
		stopCh:          make(chan struct{}),
		writer:          bufio.NewWriterSize(conn, 32*1024),
	}
}

// run starts the reader and writer goroutines and waits for the connection to end.
func (c *Client) run() {
	go c.writeLoop()
	go c.rpcEventLoop() // drains rpcSend → sends RPC push frames to TCP

	// Set initial read deadline for the auth frame
	c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	reader := bufio.NewReaderSize(c.conn, 32*1024)

	for {
		// Clear read deadline once authed (keepalive handled by PING/PONG)
		if c.authed {
			c.conn.SetReadDeadline(time.Now().Add(c.server.cfg.ReadTimeout()))
		}

		frame, err := protocol.Decode(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("[zeus] client %s read error: %v", c.id, err)
			}
			break
		}

		if err = c.dispatch(frame); err != nil {
			log.Printf("[zeus] client %s dispatch error: %v", c.id, err)
			break
		}
	}

	c.stop()
}

// writeLoop drains the send channel and flushes frames to TCP.
func (c *Client) writeLoop() {
	defer c.stop()

	for {
		select {
		case frame, ok := <-c.send:
			if !ok {
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(c.server.cfg.WriteTimeout()))
			if err := frame.Encode(c.writer); err != nil {
				return
			}
			// Flush after each burst (drain any queued frames without extra syscalls)
			if len(c.send) == 0 {
				if err := c.writer.Flush(); err != nil {
					return
				}
			}
		case <-c.stopCh:
			return
		}
	}
}

// dispatch routes one incoming frame to the correct handler.
func (c *Client) dispatch(f *protocol.Frame) error {
	// Auth check — ALL frames except OP_AUTH and OP_PING require auth
	if !c.authed {
		if f.Op == protocol.OP_PING {
			c.pushFrame(protocol.PushFrame(protocol.OP_PONG, nil, nil))
			return nil
		}
		if f.Op != protocol.OP_AUTH {
			c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_AUTH_FAIL,
				"send OP_AUTH first"))
			return fmt.Errorf("unauthenticated access attempt op=%x", f.Op)
		}
	}

	switch f.Op {

	// ── Auth ──────────────────────────────────────────────────
	case protocol.OP_AUTH:
		return c.handleAuth(f)

	// ── Keepalive ─────────────────────────────────────────────
	case protocol.OP_PING:
		c.pushFrame(protocol.PushFrame(protocol.OP_PONG, nil, nil))
		// Also refresh presence in all joined chat rooms
		for room := range c.chatRooms {
			if r, err := c.server.chat.GetOrCreate(room); err == nil {
				r.RefreshPresence(c.id)
			}
		}
		return nil

	// ── Cache ─────────────────────────────────────────────────
	case protocol.OP_GET:
		return c.handleGet(f)
	case protocol.OP_SET:
		return c.handleSet(f)
	case protocol.OP_DELETE:
		return c.handleDelete(f)
	case protocol.OP_CLEAR:
		return c.handleClear(f)

	// ── Channels ──────────────────────────────────────────────
	case protocol.OP_SUBSCRIBE:
		return c.handleSubscribe(f)
	case protocol.OP_UNSUBSCRIBE:
		return c.handleUnsubscribe(f)
	case protocol.OP_PUBLISH:
		return c.handlePublish(f)

	// ── Queues ────────────────────────────────────────────────
	case protocol.OP_QUEUE_PUSH:
		return c.handleQueuePush(f)
	case protocol.OP_QUEUE_CONSUME:
		return c.handleQueueConsume(f)
	case protocol.OP_QUEUE_ACK:
		return c.handleQueueAck(f)
	case protocol.OP_QUEUE_NACK:
		return c.handleQueueNack(f)

	// ── Chat (core) ───────────────────────────────────────────
	case protocol.OP_CHAT_JOIN:
		return c.handleChatJoin(f)
	case protocol.OP_CHAT_LEAVE:
		return c.handleChatLeave(f)
	case protocol.OP_CHAT_MESSAGE:
		return c.handleChatMessage(f)
	case protocol.OP_CHAT_HISTORY:
		return c.handleChatHistory(f)
	case protocol.OP_CHAT_PRESENCE:
		return c.handleChatPresence(f)

	// ── Chat (optional features) ──────────────────────────────
	case protocol.OP_CHAT_MARK_DELIVERED:
		return c.handleChatMarkDelivered(f)
	case protocol.OP_CHAT_MARK_READ:
		return c.handleChatMarkRead(f)
	case protocol.OP_CHAT_EDIT_MESSAGE:
		return c.handleChatEditMessage(f)
	case protocol.OP_CHAT_DELETE_MESSAGE:
		return c.handleChatDeleteMessage(f)
	case protocol.OP_CHAT_POLL_CREATE:
		return c.handleChatPollCreate(f)
	case protocol.OP_CHAT_POLL_VOTE:
		return c.handleChatPollVote(f)
	case protocol.OP_CHAT_POLL_RESULTS:
		return c.handleChatPollResults(f)
	case protocol.OP_CHAT_SET_META:
		return c.handleChatSetMeta(f)
	case protocol.OP_CHAT_GET_META:
		return c.handleChatGetMeta(f)

	// ── RPC ───────────────────────────────────────────────────
	case protocol.OP_RPC_CONSUME:
		return c.handleRPCConsume(f)
	case protocol.OP_RPC_CALL:
		return c.handleRPCCall(f)
	case protocol.OP_RPC_REPLY:
		return c.handleRPCReply(f)
	case protocol.OP_RPC_PROGRESS:
		return c.handleRPCProgress(f)
	case protocol.OP_RPC_CANCEL:
		return c.handleRPCCancel(f)
	case protocol.OP_RPC_STATUS:
		return c.handleRPCStatus(f)

	default:
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_UNKNOWN_OP,
			fmt.Sprintf("unknown op: 0x%02x", f.Op)))
		return nil
	}
}

// ── Auth handler ─────────────────────────────────────────────

func (c *Client) handleAuth(f *protocol.Frame) error {
	token := string(f.Body)
	if !c.server.auth.Validate(token) {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_AUTH_FAIL, "invalid token"))
		// Give the response a moment to flush before closing
		time.AfterFunc(200*time.Millisecond, c.stop)
		return nil
	}
	// Key field = optional userID provided by the client
	c.userID = f.KeyString()
	c.authed = true
	c.pushFrame(protocol.OKResponse(f.RequestID, []byte(c.id)))
	return nil
}

// ── Cache handlers ───────────────────────────────────────────

func (c *Client) handleGet(f *protocol.Frame) error {
	val, ok := c.server.cache.Get(f.KeyString())
	if !ok {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_NOT_FOUND, "key not found"))
		return nil
	}
	c.pushFrame(protocol.OKResponse(f.RequestID, val))
	return nil
}

func (c *Client) handleSet(f *protocol.Frame) error {
	// TTL is encoded as a uint32 (seconds) in the first 4 bytes of Body.
	// If body is shorter than 4 bytes, TTL = 0 (no expiry).
	var ttl time.Duration
	value := f.Body
	if len(f.Body) >= 4 {
		secs := binary.BigEndian.Uint32(f.Body[:4])
		ttl = time.Duration(secs) * time.Second
		value = f.Body[4:]
	}
	c.server.cache.Set(f.KeyString(), value, ttl)
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

func (c *Client) handleDelete(f *protocol.Frame) error {
	c.server.cache.Delete(f.KeyString())
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

func (c *Client) handleClear(f *protocol.Frame) error {
	c.server.cache.Clear()
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// ── Channel handlers ─────────────────────────────────────────

func (c *Client) handleSubscribe(f *protocol.Frame) error {
	name := f.KeyString()
	if name == "" {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "channel name required"))
		return nil
	}

	ch, err := c.server.channels.GetOrCreate(name)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_LIMIT_HIT, err.Error()))
		return nil
	}

	sub := pubsub.NewSubscriber(c.id)
	c.chanSubs[name] = sub

	// FLAG_RETAIN in Subscribe means: replay history on join
	replay := f.Flags&protocol.FLAG_RETAIN != 0
	ch.Subscribe(sub, replay)

	// Start a goroutine that forwards channel messages to our write loop
	go func() {
		for msg := range sub.Send {
			frame := protocol.PushFrame(protocol.OP_PUSH_CHANNEL,
				[]byte(name), msg.Payload)
			c.pushFrame(frame)
		}
	}()

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

func (c *Client) handleUnsubscribe(f *protocol.Frame) error {
	name := f.KeyString()
	sub, ok := c.chanSubs[name]
	if !ok {
		c.pushFrame(protocol.OKResponse(f.RequestID, nil)) // idempotent
		return nil
	}
	c.server.channels.RemoveSubscriber(c.id)
	close(sub.Send) // stops the forwarding goroutine
	delete(c.chanSubs, name)
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

func (c *Client) handlePublish(f *protocol.Frame) error {
	name := f.KeyString()
	retain := f.Flags&protocol.FLAG_RETAIN != 0
	n, err := c.server.channels.Publish(name, f.Body, retain)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}
	// Return how many subscribers received it (as 4-byte big-endian uint32)
	resp := make([]byte, 4)
	binary.BigEndian.PutUint32(resp, uint32(n))
	c.pushFrame(protocol.OKResponse(f.RequestID, resp))
	return nil
}

// ── Queue handlers ───────────────────────────────────────────

func (c *Client) handleQueuePush(f *protocol.Frame) error {
	name := f.KeyString()
	q, err := c.server.queues.GetOrCreate(name)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_LIMIT_HIT, err.Error()))
		return nil
	}
	msg := q.Push(f.Body)
	c.pushFrame(protocol.OKResponse(f.RequestID, []byte(msg.ID)))
	return nil
}

func (c *Client) handleQueueConsume(f *protocol.Frame) error {
	name := f.KeyString()
	q, err := c.server.queues.GetOrCreate(name)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_LIMIT_HIT, err.Error()))
		return nil
	}

	consumer := queue.NewConsumer(c.id)
	c.queueConss[name] = consumer
	q.AddConsumer(consumer)

	// Goroutine: forward queue deliveries to our write loop
	go func() {
		for msg := range consumer.Deliver {
			// Frame body: [msgIDLen:2][msgID][payload]
			idBytes := []byte(msg.ID)
			body := make([]byte, 2+len(idBytes)+len(msg.Payload))
			binary.BigEndian.PutUint16(body[:2], uint16(len(idBytes)))
			copy(body[2:], idBytes)
			copy(body[2+len(idBytes):], msg.Payload)
			c.pushFrame(protocol.PushFrame(protocol.OP_PUSH_QUEUE, []byte(name), body))
		}
	}()

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

func (c *Client) handleQueueAck(f *protocol.Frame) error {
	name := f.KeyString()
	msgID := string(f.Body)
	q, err := c.server.queues.GetOrCreate(name)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}
	if err = q.Ack(msgID); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_NOT_FOUND, err.Error()))
		return nil
	}
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

func (c *Client) handleQueueNack(f *protocol.Frame) error {
	name := f.KeyString()
	// Body: [msgIDLen:2][msgID][errorMsg]
	if len(f.Body) < 2 {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "malformed nack body"))
		return nil
	}
	idLen := int(binary.BigEndian.Uint16(f.Body[:2]))
	if len(f.Body) < 2+idLen {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "malformed nack body"))
		return nil
	}
	msgID := string(f.Body[2 : 2+idLen])
	errMsg := string(f.Body[2+idLen:])

	q, err := c.server.queues.GetOrCreate(name)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}
	if err = q.Nack(msgID, errMsg); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_NOT_FOUND, err.Error()))
		return nil
	}
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// ── Chat handlers ─────────────────────────────────────────────

func (c *Client) handleChatJoin(f *protocol.Frame) error {
	room := f.KeyString()
	send, history, err := c.server.chat.Join(room, c.id, c.userID)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_ROOM_FULL, err.Error()))
		return nil
	}
	c.chatRooms[room] = true

	// Goroutine: forward live chat messages to our write loop.
	// When receipt_tracking is on, also auto-mark each incoming message
	// as ReceiptDelivered right away (client received it via push).
	go func() {
		for msg := range send {
			// Auto-mark delivered if receipt tracking is on and this isn't
			// our own message (no point recording a receipt for yourself).
			if c.server.chat.Features().ReceiptTracking &&
				c.server.db != nil &&
				msg.SenderID != c.userID &&
				msg.DBID > 0 {
				_ = c.server.db.UpsertReceipt(msg.DBID, c.userID, store.ReceiptDelivered)
				// Notify the sender their message was delivered
				c.broadcastReceiptUpdate(room, msg.DBID, store.ReceiptDelivered)
			}
			b, _ := encodeChatMsg(msg)
			c.pushFrame(protocol.PushFrame(protocol.OP_PUSH_CHAT, []byte(room), b))
		}
	}()

	// Subscribe to room side-channels so this client receives edit/delete/poll
	// and presence-update pushes. These are internal channel names Zeus manages.
	for _, suffix := range []string{"._events", "._receipts", "._polls", "._presence"} {
		chName := room + suffix
		ch, chErr := c.server.channels.GetOrCreate(chName)
		if chErr != nil {
			continue
		}
		sub := pubsub.NewSubscriber(c.id + suffix)
		c.chanSubs[chName] = sub
		ch.Subscribe(sub, false)

		// Determine the push opcode based on suffix
		var pushOp protocol.OpCode
		switch suffix {
		case "._receipts":
			pushOp = protocol.OP_PUSH_RECEIPT
		case "._presence":
			pushOp = protocol.OP_PUSH_PRESENCE
		default:
			pushOp = protocol.OP_PUSH_CHAT // edit/delete/poll events piggyback on PUSH_CHAT
		}

		go func(s *pubsub.Subscriber, op protocol.OpCode, channelName string) {
			for msg := range s.Send {
				c.pushFrame(protocol.PushFrame(op, []byte(room), msg.Payload))
			}
		}(sub, pushOp, chName)
	}

	// Determine what history to replay:
	//  SmartDelivery ON  → only messages this user hasn't received yet (DB-based)
	//  SmartDelivery OFF → last history_size messages from memory ring buffer
	var histBytes []byte
	if c.server.chat.Features().SmartDelivery && c.server.db != nil {
		histBytes = c.smartDeliveryCatchUp(room)
	} else {
		histBytes = encodeChatHistory(history)
	}

	c.pushFrame(protocol.OKResponse(f.RequestID, histBytes))
	return nil
}

func (c *Client) handleChatLeave(f *protocol.Frame) error {
	room := f.KeyString()
	c.server.chat.Leave(room, c.id, c.userID)
	delete(c.chatRooms, room)

	// Unsubscribe from all side-channels for this room
	for _, suffix := range []string{"._events", "._receipts", "._polls", "._presence"} {
		chName := room + suffix
		if sub, ok := c.chanSubs[chName]; ok {
			c.server.channels.RemoveSubscriber(c.id + suffix)
			close(sub.Send)
			delete(c.chanSubs, chName)
		}
	}

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

func (c *Client) handleChatMessage(f *protocol.Frame) error {
	room := f.KeyString()
	if !c.chatRooms[room] {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_AUTH_FAIL, "join the room first"))
		return nil
	}

	// Body format: [msgTypeLen:1][msgType][payload]
	// If the client sends raw bytes with no type prefix, default to "text".
	msgType := chat.MsgTypeText
	payload := f.Body
	if len(f.Body) > 1 {
		typeLen := int(f.Body[0])
		if typeLen > 0 && len(f.Body) >= 1+typeLen {
			msgType = chat.MsgType(f.Body[1 : 1+typeLen])
			payload = f.Body[1+typeLen:]
		}
	}

	msg, err := c.server.chat.Send(room, c.id, c.userID, msgType, payload, nil)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Persist to SQLite and store the DB row ID back on the message so the
	// receipt system can reference it. This runs synchronously so the ID
	// is available before any subscribers might try to receipt-ack it.
	if c.server.db != nil {
		dbID, dbErr := c.server.db.SaveChatMessage(
			room, c.userID,
			store.MsgType(msgType),
			payload, nil,
		)
		if dbErr != nil {
			log.Printf("[zeus] persist chat message: %v", dbErr)
		} else {
			msg.DBID = dbID
		}
	}

	// Return the message's in-memory ID so the client can reference it
	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, msg.ID)
	c.pushFrame(protocol.OKResponse(f.RequestID, resp))
	return nil
}

// handleChatHistory returns message history for a room.
//
// Body format (optional): [afterID:8][limit:2]
//   afterID — pagination cursor: return messages with ID > afterID (0 = from start)
//   limit   — max messages to return (0 = server default of 50)
//
// When persistence is enabled, Zeus queries SQLite so the client gets the full
// durable history (not just the in-memory ring buffer). Each message includes
// its aggregate delivery_state so the sender can show the correct tick.
func (c *Client) handleChatHistory(f *protocol.Frame) error {
	room := f.KeyString()

	// Parse optional pagination params from body
	var afterID int64
	limit := 50 // sensible default
	if len(f.Body) >= 10 {
		afterID = int64(binary.BigEndian.Uint64(f.Body[:8]))
		limit = int(binary.BigEndian.Uint16(f.Body[8:10]))
		if limit <= 0 || limit > 500 {
			limit = 50
		}
	}

	// Prefer DB history (full durable log + delivery states) when available
	if c.server.db != nil {
		rows, err := c.server.db.LoadChatHistory(room, limit, afterID)
		if err != nil {
			c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
			return nil
		}
		c.pushFrame(protocol.OKResponse(f.RequestID, encodeDBChatHistory(rows, c.server.db)))
		return nil
	}

	// Fallback: in-memory ring buffer (no pagination, no delivery states)
	r, err := c.server.chat.GetOrCreate(room)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}
	c.pushFrame(protocol.OKResponse(f.RequestID, encodeChatHistory(r.History())))
	return nil
}

// encodeDBChatHistory serialises DB rows to JSON, annotating each message
// with its aggregate delivery state (for tick display: sent/delivered/read).
func encodeDBChatHistory(rows []*store.ChatMessageRow, db *store.DB) []byte {
	type entry struct {
		ID            int64           `json:"id"`
		Sender        string          `json:"sender"`
		Type          string          `json:"type"`
		Payload       []byte          `json:"payload"`
		Meta          json.RawMessage `json:"meta,omitempty"`
		SentAt        int64           `json:"sent_at"`
		EditedAt      *int64          `json:"edited_at,omitempty"`
		DeliveryState int             `json:"delivery_state"` // 0=sent 1=delivered 2=read
	}
	entries := make([]entry, 0, len(rows))
	for _, m := range rows {
		// Skip soft-deleted messages (client renders "deleted" placeholder instead)
		if m.DeletedAt != nil {
			continue
		}
		e := entry{
			ID:            m.ID,
			Sender:        m.SenderID,
			Type:          string(m.MsgType),
			Payload:       m.Payload,
			SentAt:        m.SentAt.Unix(),
			DeliveryState: int(m.DeliveryState),
		}
		if m.EditedAt != nil {
			t := m.EditedAt.Unix()
			e.EditedAt = &t
		}
		if len(m.Metadata) > 0 {
			if b, err := json.Marshal(m.Metadata); err == nil {
				e.Meta = b
			}
		}
		// Fetch fresh aggregate delivery state (more accurate than the cached column)
		if state, err := db.GetMessageDeliveryState(m.ID); err == nil {
			e.DeliveryState = int(state)
		}
		entries = append(entries, e)
	}
	b, _ := json.Marshal(entries)
	return b
}

func (c *Client) handleChatPresence(f *protocol.Frame) error {
	room := f.KeyString()
	r, err := c.server.chat.GetOrCreate(room)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}
	presence := r.Presence()
	b, _ := json.Marshal(presence)
	c.pushFrame(protocol.OKResponse(f.RequestID, b))
	return nil
}

// ── Cleanup ───────────────────────────────────────────────────

func (c *Client) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.conn.Close()
	})
}

func (c *Client) cleanup() {
	// Unsubscribe from all channels
	c.server.channels.RemoveSubscriber(c.id)
	for _, sub := range c.chanSubs {
		close(sub.Send)
	}

	// Remove from all queues (re-queues in-flight messages)
	c.server.queues.RemoveConsumerFromAll(c.id)

	// Leave all chat rooms
	c.server.chat.LeaveAll(c.id, c.userID)

	// RPC cleanup:
	//  - RemoveWorker: any in-progress calls assigned to us go back to pending
	//    (another worker will pick them up)
	//  - DisconnectCaller: detaches our live send channel from any calls we
	//    initiated; the calls remain alive and the caller can reconnect with
	//    OP_RPC_STATUS to resume tracking
	c.server.rpc.RemoveWorker(c.id)
	c.server.rpc.DisconnectCaller(c.id)

	// Close the rpcSend channel so rpcEventLoop exits cleanly
	close(c.rpcSend)
}

// ── Internal helpers ─────────────────────────────────────────

// pushFrame sends a frame to the write goroutine.
// If the buffer is full, the client is too slow and we drop the frame.
// Callers that need guaranteed delivery should use queues, not channels.
func (c *Client) pushFrame(f *protocol.Frame) {
	select {
	case c.send <- f:
	case <-c.stopCh:
	default:
		// Buffer full: close connection — client is too slow
		c.stop()
	}
}

// encodeChatMsg serialises a ChatMessage to binary.
// Format: [senderIDLen:1][senderID][msgID:8][payload]
func encodeChatMsg(msg *chat.ChatMessage) ([]byte, error) {
	sender := []byte(msg.SenderID)
	out := make([]byte, 1+len(sender)+8+len(msg.Payload))
	out[0] = byte(len(sender))
	copy(out[1:], sender)
	binary.BigEndian.PutUint64(out[1+len(sender):], msg.ID)
	copy(out[1+len(sender)+8:], msg.Payload)
	return out, nil
}

// encodeChatHistory serialises a slice of chat messages as a JSON array.
// JSON is fine here since history is not on the hot path.
func encodeChatHistory(msgs []*chat.ChatMessage) []byte {
	type entry struct {
		ID      uint64          `json:"id"`
		Sender  string          `json:"sender"`
		Type    string          `json:"type"`
		Payload []byte          `json:"payload"`
		Meta    json.RawMessage `json:"meta,omitempty"`
		SentAt  int64           `json:"sent_at"`
	}
	entries := make([]entry, len(msgs))
	for i, m := range msgs {
		e := entry{
			ID:      m.ID,
			Sender:  m.SenderID,
			Type:    string(m.MsgType),
			Payload: m.Payload,
			SentAt:  m.SentAt.Unix(),
		}
		if len(m.Metadata) > 0 {
			if b, err := json.Marshal(m.Metadata); err == nil {
				e.Meta = b
			}
		}
		entries[i] = e
	}
	b, _ := json.Marshal(entries)
	return b
}

// ── helpers: feature guard ────────────────────────────────────

// requireFeature returns false and sends an error frame if the named feature
// is not enabled. Keeps handler code clean.
func (c *Client) requireDB(requestID uint32) bool {
	if c.server.db == nil {
		c.pushFrame(protocol.ErrorResponse(requestID, protocol.STATUS_INTERNAL,
			"persistence is disabled — enable persistence.enabled in zeus.yaml"))
		return false
	}
	return true
}

func (c *Client) requireReceiptTracking(requestID uint32) bool {
	if !c.server.chat.Features().ReceiptTracking {
		c.pushFrame(protocol.ErrorResponse(requestID, protocol.STATUS_INTERNAL,
			"receipt_tracking is disabled — enable chat.features.receipt_tracking in zeus.yaml"))
		return false
	}
	return c.requireDB(requestID)
}

func (c *Client) requirePolls(requestID uint32) bool {
	if !c.server.chat.Features().PollsEnabled {
		c.pushFrame(protocol.ErrorResponse(requestID, protocol.STATUS_INTERNAL,
			"polls are disabled — enable chat.features.polls in zeus.yaml"))
		return false
	}
	return c.requireDB(requestID)
}

func (c *Client) requireUserMetadata(requestID uint32) bool {
	if !c.server.chat.Features().UserMetadata {
		c.pushFrame(protocol.ErrorResponse(requestID, protocol.STATUS_INTERNAL,
			"user_metadata is disabled — enable chat.features.user_metadata in zeus.yaml"))
		return false
	}
	return true
}

// ── helpers: read a uint64 from the start of a byte slice ────

func readUint64(b []byte) (uint64, []byte, bool) {
	if len(b) < 8 {
		return 0, nil, false
	}
	return binary.BigEndian.Uint64(b[:8]), b[8:], true
}

// ── Receipt handlers ─────────────────────────────────────────
//
// Receipt tracking must be enabled in zeus.yaml (chat.features.receipt_tracking).
// When disabled these opcodes return an error so clients know immediately.

// handleChatMarkDelivered records that the client's device received the message.
// This is the "two grey ticks" moment — client calls this right after receiving
// an OP_PUSH_CHAT frame. Key=room, Body=[msgID:8]
func (c *Client) handleChatMarkDelivered(f *protocol.Frame) error {
	if !c.requireReceiptTracking(f.RequestID) {
		return nil
	}
	msgID, _, ok := readUint64(f.Body)
	if !ok {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "body must be 8-byte msgID"))
		return nil
	}

	if err := c.server.db.UpsertReceipt(int64(msgID), c.userID, store.ReceiptDelivered); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Push a receipt update back to the original sender so they can show ticks.
	// We broadcast it on the room so anyone who cares (sender) gets notified.
	// Format: [msgID:8][userIDLen:1][userID][state:1]
	c.broadcastReceiptUpdate(f.KeyString(), int64(msgID), store.ReceiptDelivered)

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// handleChatMarkRead records that the user actually opened/read the message.
// This is the "two blue ticks" moment. Key=room, Body=[msgID:8]
func (c *Client) handleChatMarkRead(f *protocol.Frame) error {
	if !c.requireReceiptTracking(f.RequestID) {
		return nil
	}
	msgID, _, ok := readUint64(f.Body)
	if !ok {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "body must be 8-byte msgID"))
		return nil
	}

	if err := c.server.db.UpsertReceipt(int64(msgID), c.userID, store.ReceiptRead); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Notify sender of the read receipt
	c.broadcastReceiptUpdate(f.KeyString(), int64(msgID), store.ReceiptRead)

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// broadcastReceiptUpdate pushes an OP_PUSH_RECEIPT frame to everyone in the room.
// The sender of the original message uses this to update their tick display.
// Frame body: [msgID:8][state:1][userIDLen:1][userID]
func (c *Client) broadcastReceiptUpdate(roomName string, msgID int64, state store.ReceiptState) {
	userIDBytes := []byte(c.userID)
	body := make([]byte, 8+1+1+len(userIDBytes))
	binary.BigEndian.PutUint64(body[:8], uint64(msgID))
	body[8] = byte(state)
	body[9] = byte(len(userIDBytes))
	copy(body[10:], userIDBytes)

	// Publish as a push frame on the room channel so all room members see it.
	// We reuse the channels system — the room name is used as the channel name
	// with a "_receipt" suffix so it doesn't interfere with chat messages.
	_, _ = c.server.channels.Publish(roomName+"._receipts", body, false)
}

// ── Edit / Delete handlers ────────────────────────────────────
//
// Any user can edit or delete their OWN messages. Zeus enforces ownership.
// Persistence is always used (DB required) so the edit survives restarts.

// handleChatEditMessage lets a user replace the content of their own message.
// Key=room, Body=[msgID:8][newPayload...]
// Zeus broadcasts an OP_PUSH_CHAT frame to the room with the updated content
// so all connected clients can update their local message in real-time.
func (c *Client) handleChatEditMessage(f *protocol.Frame) error {
	if !c.requireDB(f.RequestID) {
		return nil
	}
	msgID, newPayload, ok := readUint64(f.Body)
	if !ok || len(newPayload) == 0 {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL,
			"body must be [msgID:8][newPayload]"))
		return nil
	}
	if err := c.server.db.EditMessage(int64(msgID), newPayload); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Push an "edit" push frame to all room members so they can update in-place.
	// Frame key = room, Body = [0xED "edit marker"][msgID:8][newPayload]
	// Clients check byte[0] == 0xED to know this is an edit, not a new message.
	room := f.KeyString()
	editBody := make([]byte, 1+8+len(newPayload))
	editBody[0] = 0xED // edit marker
	binary.BigEndian.PutUint64(editBody[1:9], msgID)
	copy(editBody[9:], newPayload)
	_, _ = c.server.channels.Publish(room+"._events", editBody, false)

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// handleChatDeleteMessage soft-deletes a message (marks deleted_at in DB).
// The message is NOT removed from the DB — it stays for history integrity.
// Clients receive a push frame so they can render "This message was deleted".
// Key=room, Body=[msgID:8]
func (c *Client) handleChatDeleteMessage(f *protocol.Frame) error {
	if !c.requireDB(f.RequestID) {
		return nil
	}
	msgID, _, ok := readUint64(f.Body)
	if !ok {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "body must be 8-byte msgID"))
		return nil
	}
	if err := c.server.db.SoftDeleteMessage(int64(msgID)); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Notify room members of the deletion.
	// Body = [0xDE "delete marker"][msgID:8]
	room := f.KeyString()
	delBody := make([]byte, 9)
	delBody[0] = 0xDE // delete marker
	binary.BigEndian.PutUint64(delBody[1:], msgID)
	_, _ = c.server.channels.Publish(room+"._events", delBody, false)

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// ── Poll handlers ─────────────────────────────────────────────
//
// Polls require chat.features.polls: true + persistence.enabled: true.
// A poll is a special message (MsgType="poll") with a structured JSON body.

// PollCreateRequest is the JSON body clients send for OP_CHAT_POLL_CREATE.
// Example:
//   {"question":"Lunch?","options":["Pizza","Sushi","Tacos"],"multi_vote":false}
type PollCreateRequest struct {
	Question  string   `json:"question"`
	Options   []string `json:"options"`
	MultiVote bool     `json:"multi_vote"`
	CloseSecs int      `json:"close_secs"` // 0 = never closes
}

// handleChatPollCreate creates a poll in a room.
// Key=room, Body=JSON PollCreateRequest
func (c *Client) handleChatPollCreate(f *protocol.Frame) error {
	if !c.requirePolls(f.RequestID) {
		return nil
	}
	room := f.KeyString()
	if !c.chatRooms[room] {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_AUTH_FAIL, "join the room first"))
		return nil
	}

	var req PollCreateRequest
	if err := json.Unmarshal(f.Body, &req); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL,
			"body must be JSON: {question, options[], multi_vote, close_secs}"))
		return nil
	}
	if req.Question == "" || len(req.Options) < 2 {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL,
			"question required; at least 2 options required"))
		return nil
	}

	// Save a "poll" message to DB first to get an ID
	msgID, err := c.server.db.SaveChatMessage(room, c.userID, store.MsgTypePoll, f.Body, nil)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Determine optional close time
	var closesAt *time.Time
	if req.CloseSecs > 0 {
		t := time.Now().Add(time.Duration(req.CloseSecs) * time.Second)
		closesAt = &t
	}
	if err = c.server.db.CreatePoll(msgID, req.Question, req.Options, req.MultiVote, closesAt); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Broadcast the poll message to the room (same path as a regular message)
	_, _ = c.server.chat.Send(room, c.id, c.userID, chat.MsgTypePoll, f.Body, nil)

	// Return the poll's message ID so the client can reference it for votes
	resp := make([]byte, 8)
	binary.BigEndian.PutUint64(resp, uint64(msgID))
	c.pushFrame(protocol.OKResponse(f.RequestID, resp))
	return nil
}

// handleChatPollVote records a vote from the current user.
// Key=room, Body=[msgID:8][optionIndex:1]
// A user can only vote once per option (idempotent). The server broadcasts
// updated results to the room so live vote counts work without polling.
func (c *Client) handleChatPollVote(f *protocol.Frame) error {
	if !c.requirePolls(f.RequestID) {
		return nil
	}
	if len(f.Body) < 9 {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL,
			"body must be [msgID:8][optionIndex:1]"))
		return nil
	}
	msgID, rest, _ := readUint64(f.Body)
	optIdx := int(rest[0])

	if err := c.server.db.VotePoll(int64(msgID), c.userID, optIdx); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Fetch updated results and broadcast to room
	results, err := c.server.db.GetPollResults(int64(msgID))
	if err == nil {
		resultsJSON, _ := json.Marshal(map[string]interface{}{
			"msg_id":  msgID,
			"results": results,
		})
		room := f.KeyString()
		_, _ = c.server.channels.Publish(room+"._polls", resultsJSON, false)
	}

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// handleChatPollResults returns the current vote tally for a poll.
// Key=room, Body=[msgID:8]
// Response body: JSON {"msg_id":N,"results":{"0":5,"1":3}}
func (c *Client) handleChatPollResults(f *protocol.Frame) error {
	if !c.requirePolls(f.RequestID) {
		return nil
	}
	msgID, _, ok := readUint64(f.Body)
	if !ok {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "body must be 8-byte msgID"))
		return nil
	}
	results, err := c.server.db.GetPollResults(int64(msgID))
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}
	resp, _ := json.Marshal(map[string]interface{}{
		"msg_id":  msgID,
		"results": results,
	})
	c.pushFrame(protocol.OKResponse(f.RequestID, resp))
	return nil
}

// ── User metadata handlers ────────────────────────────────────
//
// User metadata lets clients attach arbitrary JSON to their presence in a room.
// Think: display name, avatar URL, role badge, custom status message.
// Requires chat.features.user_metadata: true in zeus.yaml.

// handleChatSetMeta stores the caller's metadata in the room and broadcasts
// a presence update so other members see the change immediately.
// Key=room, Body=JSON object (any key/value pairs you want)
// The metadata is also stored to SQLite if persistence is enabled.
func (c *Client) handleChatSetMeta(f *protocol.Frame) error {
	if !c.requireUserMetadata(f.RequestID) {
		return nil
	}
	room := f.KeyString()

	// Parse as a generic JSON object — Zeus doesn't care about the shape
	var meta map[string]interface{}
	if err := json.Unmarshal(f.Body, &meta); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL,
			"body must be a JSON object"))
		return nil
	}

	// Update in-memory room presence
	r, err := c.server.chat.GetOrCreate(room)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}
	if err = r.SetUserMeta(c.id, meta); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}

	// Persist to SQLite if available
	if c.server.db != nil {
		_ = c.server.db.SetUserMeta(room, c.userID, meta)
	}

	// Broadcast a presence-change push frame to the room so other clients
	// can update their member list without needing to request it.
	presenceBytes, _ := json.Marshal(map[string]interface{}{
		"user_id": c.userID,
		"meta":    meta,
	})
	_, _ = c.server.channels.Publish(room+"._presence", presenceBytes, false)

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// handleChatGetMeta fetches another user's metadata in a room.
// Key=room, Body=userID (UTF-8 string)
// Response body: JSON object (the stored metadata, or {} if none set)
func (c *Client) handleChatGetMeta(f *protocol.Frame) error {
	if !c.requireUserMetadata(f.RequestID) {
		return nil
	}
	room := f.KeyString()
	targetUserID := string(f.Body)

	// Try DB first (most up-to-date, survives restarts)
	if c.server.db != nil {
		meta, err := c.server.db.GetUserMeta(room, targetUserID)
		if err != nil {
			c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
			return nil
		}
		resp, _ := json.Marshal(meta)
		c.pushFrame(protocol.OKResponse(f.RequestID, resp))
		return nil
	}

	// Fallback: read from in-memory presence list
	r, err := c.server.chat.GetOrCreate(room)
	if err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, err.Error()))
		return nil
	}
	for _, p := range r.Presence() {
		if p.UserID == targetUserID {
			resp, _ := json.Marshal(p.Meta)
			c.pushFrame(protocol.OKResponse(f.RequestID, resp))
			return nil
		}
	}
	// Not found — return empty object
	c.pushFrame(protocol.OKResponse(f.RequestID, []byte("{}")))
	return nil
}

// ── Smart delivery: catch-up on join ─────────────────────────
//
// When smart_delivery is enabled, handleChatJoin replaces the in-memory
// history replay with a DB-based catch-up: only messages the user hasn't
// received yet are sent, and all past messages are marked delivered.

// smartDeliveryCatchUp is called inside handleChatJoin when SmartDelivery
// is on. It:
//   1. Marks all existing room messages as ReceiptDelivered for this user
//   2. Returns only the messages the user hasn't received yet (as JSON)
func (c *Client) smartDeliveryCatchUp(room string) []byte {
	if c.server.db == nil {
		return nil
	}
	// Load messages this user hasn't had delivered yet
	msgs, err := c.server.db.LoadUndeliveredMessages(room, c.userID)
	if err != nil {
		log.Printf("[zeus] smart delivery load error for user %s room %s: %v", c.userID, room, err)
		return nil
	}

	// Bulk-mark them all as delivered now (they're about to be sent)
	_ = c.server.db.BulkMarkDelivered(room, c.userID)

	// Encode as the same JSON format as regular history
	type entry struct {
		ID      int64           `json:"id"`
		Sender  string          `json:"sender"`
		Type    string          `json:"type"`
		Payload []byte          `json:"payload"`
		Meta    json.RawMessage `json:"meta,omitempty"`
		SentAt  int64           `json:"sent_at"`
	}
	entries := make([]entry, len(msgs))
	for i, m := range msgs {
		e := entry{
			ID:      m.ID,
			Sender:  m.SenderID,
			Type:    string(m.MsgType),
			Payload: m.Payload,
			SentAt:  m.SentAt.Unix(),
		}
		if len(m.Metadata) > 0 {
			if b, err2 := json.Marshal(m.Metadata); err2 == nil {
				e.Meta = b
			}
		}
		entries[i] = e
	}
	b, _ := json.Marshal(entries)
	return b
}

// ── RPC handlers ──────────────────────────────────────────────
//
// RPC lets a caller send a task to a named worker group and await the result.
// The key design goal is disconnect-safety: callers can reconnect and resume
// tracking a call using OP_RPC_STATUS + the callID they received on initiation.
//
// Call lifecycle:
//   pending → in_progress → done | failed | cancelled | timed_out
//
// Worker lifecycle:
//   OP_RPC_CONSUME  → worker registers; Zeus starts delivering calls
//   worker disconnect → in-progress call re-queued to pending

// rpcEventLoop drains the rpcSend channel and turns each event into a TCP push.
// This runs as a separate goroutine so RPC pushes never block the main writeLoop.
func (c *Client) rpcEventLoop() {
	defer c.stop()
	for {
		select {
		case evt, ok := <-c.rpcSend:
			if !ok {
				return
			}
			c.pushFrame(&protocol.Frame{
				Op:   protocol.OpCode(evt.Op),
				Body: evt.Payload,
			})
		case <-c.stopCh:
			return
		}
	}
}

// handleRPCConsume registers the client as a worker for a named group.
// Key = group name. After this call Zeus will deliver OP_PUSH_RPC frames
// to this client when callers submit work to the group.
//
// A single client can be a worker for multiple groups (call multiple times).
func (c *Client) handleRPCConsume(f *protocol.Frame) error {
	group := f.KeyString()
	if group == "" {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "group name required"))
		return nil
	}
	if _, already := c.rpcWorkerGroups[group]; already {
		c.pushFrame(protocol.OKResponse(f.RequestID, nil)) // idempotent
		return nil
	}

	// Build a Worker struct with its own delivery channel, register it, then
	// start a goroutine that turns Call deliveries into OP_PUSH_RPC frames.
	worker := rpc.NewWorker(c.id)
	c.server.rpc.AddWorker(group, worker)
	c.rpcWorkerGroups[group] = worker

	// Goroutine: forward calls delivered to this worker → OP_PUSH_RPC push frames
	go func() {
		for call := range worker.Deliver {
			body := rpc.BuildWorkerDeliveryBody(call.ID, call.Payload)
			c.pushFrame(&protocol.Frame{Op: protocol.OP_PUSH_RPC, Body: body})
		}
	}()

	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// handleRPCCall initiates a new RPC call.
// Key  = group name (which worker pool to route to)
// Body = [timeoutMs:4][payload...]
//
// On success the response body is the callID (UTF-8 string) that the caller
// must save. The caller will receive OP_PUSH_RPC_RESULT (or OP_PUSH_RPC_ERROR)
// asynchronously when the worker finishes.
func (c *Client) handleRPCCall(f *protocol.Frame) error {
	group := f.KeyString()
	if group == "" {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "group name required"))
		return nil
	}
	if len(f.Body) < 4 {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL,
			"body must be [timeoutMs:4][payload...]"))
		return nil
	}

	timeout, payload := rpc.ParseTimeoutMs(f.Body)
	call := c.server.rpc.NewCall(group, payload, timeout, c.id, c.rpcSend)

	// Return the callID to the caller — they must save this!
	c.pushFrame(protocol.OKResponse(f.RequestID, []byte(call.ID)))
	return nil
}

// handleRPCReply is called by a worker to deliver the result of a call.
// Key  = callID
// Body = result bytes (arbitrary; caller receives them verbatim)
func (c *Client) handleRPCReply(f *protocol.Frame) error {
	callID := f.KeyString()
	if callID == "" {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "callID required in key"))
		return nil
	}
	// Body format: [isError:1][result bytes]
	// isError byte 0x01 = worker reports failure, 0x00 = success
	var isError bool
	result := f.Body
	if len(f.Body) >= 1 {
		isError = f.Body[0] == 0x01
		result = f.Body[1:]
	}
	if err := c.server.rpc.Reply(callID, result, isError); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_NOT_FOUND, err.Error()))
		return nil
	}
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// handleRPCProgress is called by a worker to report incremental progress.
// Key  = callID
// Body = [percent:1][message string...]
//   percent — 0–100
//   message — human-readable status (e.g. "compiling step 3/10")
//
// Progress events are stored in the call so reconnecting callers catch up.
func (c *Client) handleRPCProgress(f *protocol.Frame) error {
	callID := f.KeyString()
	if callID == "" {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "callID required in key"))
		return nil
	}
	if len(f.Body) < 1 {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL,
			"body must be [percent:1][message...]"))
		return nil
	}
	// Body = [pct:1][message...] — callID comes from the Key field, not the body
	pct := f.Body[0]
	msg := string(f.Body[1:])
	if err := c.server.rpc.Progress(callID, pct, msg); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_NOT_FOUND, err.Error()))
		return nil
	}
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// handleRPCCancel cancels an in-flight call.
// Key = callID
// The worker receives no further messages; the call transitions to "cancelled".
// If the call is already done/failed, this is a no-op and returns OK.
func (c *Client) handleRPCCancel(f *protocol.Frame) error {
	callID := f.KeyString()
	if callID == "" {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "callID required in key"))
		return nil
	}
	if err := c.server.rpc.Cancel(callID); err != nil {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_NOT_FOUND, err.Error()))
		return nil
	}
	c.pushFrame(protocol.OKResponse(f.RequestID, nil))
	return nil
}

// handleRPCStatus allows a reconnected caller to query the current state of a call.
// Key = callID
//
// Response body: JSON snapshot containing:
//   {
//     "state":    "pending|in_progress|done|failed|cancelled|timed_out",
//     "result":   <base64 bytes, if done>,
//     "error":    <string, if failed>,
//     "progress": [{"pct":N,"msg":"...","ts":unix}, ...],
//     "created_at": unix,
//     "updated_at": unix
//   }
//
// This is the disconnect-safe recovery path. A caller that dropped before
// receiving the result can reconnect and call OP_RPC_STATUS to get the
// full picture including all progress updates so far.
func (c *Client) handleRPCStatus(f *protocol.Frame) error {
	callID := f.KeyString()
	if callID == "" {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_INTERNAL, "callID required in key"))
		return nil
	}
	call, ok := c.server.rpc.Get(callID)
	if !ok {
		c.pushFrame(protocol.ErrorResponse(f.RequestID, protocol.STATUS_NOT_FOUND,
			fmt.Sprintf("call %s not found", callID)))
		return nil
	}

	// Re-register the caller's live channel so future events are delivered again.
	// If the call is already terminal (done/failed/etc) this is harmless.
	c.server.rpc.ReconnectCaller(callID, c.id, c.rpcSend)

	snap := call.StatusSnapshot()
	c.pushFrame(protocol.OKResponse(f.RequestID, snap))
	return nil
}

// cleanup is extended to remove this client from all RPC roles:
//   - Deregister all worker groups (in-progress calls are re-queued)
//   - Disconnect as caller (live channel detached; state preserved for reconnect)
