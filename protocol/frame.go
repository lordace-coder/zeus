// Package protocol defines Zeus's binary wire format.
//
// WHY BINARY?
//   Text protocols (like Redis's RESP or raw HTTP) are easy to debug but
//   wasteful — every field is serialised as ASCII bytes with delimiters.
//   Binary framing lets us pack a full message into a few dozen bytes,
//   making Zeus fast over high-throughput connections (chat, live feeds).
//
// ── FRAME LAYOUT ────────────────────────────────────────────
//
//   All multi-byte integers are BIG-ENDIAN (network byte order).
//
//   Offset  Size  Field
//   ──────  ────  ───────────────────────────────────────────
//     0     1     Magic byte — always 0xZE (validates frame)
//     1     1     Version   — protocol version (currently 1)
//     2     1     OpCode    — what kind of message this is
//     3     1     Flags     — bitmask of optional features
//     4     4     RequestID — client-chosen ID for req/reply matching
//     8     2     KeyLen    — length of the key/topic/room field
//    10     4     BodyLen   — length of the body/payload field
//    14     1     Reserved  — always 0x00, reserved for future use
//   ─────── ── HEADER TOTAL = 15 bytes ─────────────────────
//    15    KeyLen  Key       — UTF-8 name (channel, queue, room, cache key)
//   15+KL BodyLen Body      — arbitrary bytes (your data / command payload)
//
// ── RESPONSE FRAME ──────────────────────────────────────────
//
//   Responses use the same layout. The OpCode is set to OP_RESPONSE or
//   OP_ERROR. The RequestID echoes back the request's ID so the client
//   can match async replies.
//
// ── STATUS CODES ────────────────────────────────────────────
//
//   When OpCode == OP_RESPONSE or OP_ERROR, byte 0 of the Body carries
//   a StatusCode. The rest of the body is the payload or error message.

package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// ── Constants ──────────────────────────────────────────────

const (
	Magic   = 0x5A // 'Z' — first byte of every Zeus frame
	Version = 0x01 // current protocol version

	HeaderSize = 15 // fixed header size in bytes
)

// ── OpCodes ────────────────────────────────────────────────
// OpCodes tell Zeus what the client wants to do.

type OpCode byte

const (
	// ── Auth ──────────────────────────────────────────
	OP_AUTH OpCode = 0x01 // Authenticate: body = token string

	// ── Cache ─────────────────────────────────────────
	OP_GET    OpCode = 0x10 // Get a cache value by key
	OP_SET    OpCode = 0x11 // Set a cache key/value
	OP_DELETE OpCode = 0x12 // Delete a cache key
	OP_CLEAR  OpCode = 0x13 // Clear entire cache

	// ── Channels (Pub/Sub) ────────────────────────────
	OP_SUBSCRIBE   OpCode = 0x20 // Subscribe to a channel
	OP_UNSUBSCRIBE OpCode = 0x21 // Unsubscribe from a channel
	OP_PUBLISH     OpCode = 0x22 // Publish a message to a channel

	// ── Queues ────────────────────────────────────────
	OP_QUEUE_PUSH    OpCode = 0x30 // Push a command onto a queue
	OP_QUEUE_CONSUME OpCode = 0x31 // Start consuming a queue (register as worker)
	OP_QUEUE_ACK     OpCode = 0x32 // Mark message as processed (success)
	OP_QUEUE_NACK    OpCode = 0x33 // Mark message as failed (triggers retry)

	// ── Chat (client → server) ────────────────────────
	OP_CHAT_JOIN     OpCode = 0x40 // Join a chat room; Key=room, Body=userID (optional)
	OP_CHAT_LEAVE    OpCode = 0x41 // Leave a chat room; Key=room
	OP_CHAT_MESSAGE  OpCode = 0x42 // Send a message; Key=room, Body=[typeLen:1][type][payload]
	OP_CHAT_HISTORY  OpCode = 0x43 // Request history; Key=room, Body=[afterID:8][limit:2]
	OP_CHAT_PRESENCE OpCode = 0x44 // Request presence list; Key=room

	// ── Chat: optional features (client → server) ─────
	// These opcodes are only processed when the matching feature flag is
	// enabled in zeus.yaml. If the feature is off, Zeus returns OP_ERROR.

	// Receipt ops — require chat.features.receipt_tracking: true
	OP_CHAT_MARK_DELIVERED OpCode = 0x45 // Mark a message delivered; Key=room, Body=[msgID:8]
	OP_CHAT_MARK_READ      OpCode = 0x46 // Mark a message read; Key=room, Body=[msgID:8]

	// Message management — always available
	OP_CHAT_EDIT_MESSAGE   OpCode = 0x47 // Edit own message; Key=room, Body=[msgID:8][newPayload]
	OP_CHAT_DELETE_MESSAGE OpCode = 0x48 // Soft-delete own message; Key=room, Body=[msgID:8]

	// Polls — require chat.features.polls: true
	OP_CHAT_POLL_CREATE  OpCode = 0x49 // Create a poll; Key=room, Body=JSON PollCreate
	OP_CHAT_POLL_VOTE    OpCode = 0x4A // Vote on a poll; Key=room, Body=[msgID:8][optIdx:1]
	OP_CHAT_POLL_RESULTS OpCode = 0x4B // Get poll results; Key=room, Body=[msgID:8]

	// User metadata — require chat.features.user_metadata: true
	OP_CHAT_SET_META OpCode = 0x4C // Set my metadata in a room; Key=room, Body=JSON object
	OP_CHAT_GET_META OpCode = 0x4D // Get a user's metadata; Key=room, Body=[userID string]

	// ── Server → Client push ──────────────────────────
	// These OpCodes are sent BY the server to push data to clients.
	OP_PUSH_CHANNEL  OpCode = 0x50 // Server pushing a channel message to subscriber
	OP_PUSH_QUEUE    OpCode = 0x51 // Server delivering a queue item to consumer
	OP_PUSH_CHAT     OpCode = 0x52 // Server pushing a chat message to room member
	OP_PUSH_RECEIPT  OpCode = 0x53 // Server pushing a receipt update to message sender
	OP_PUSH_PRESENCE OpCode = 0x54 // Server pushing a presence change to room members

	// ── Responses ─────────────────────────────────────
	OP_RESPONSE OpCode = 0xF0 // Successful response
	OP_ERROR    OpCode = 0xFF // Error response
	OP_PONG     OpCode = 0xFE // Keepalive reply to OP_PING
	OP_PING     OpCode = 0xFD // Keepalive ping (client → server)
)

// ── Flags ─────────────────────────────────────────────────
// Flags is a bitmask packed into 1 byte of the header.

type Flags byte

const (
	FLAG_NONE      Flags = 0x00
	FLAG_COMPRESSED Flags = 0x01 // body is gzip-compressed
	FLAG_BINARY    Flags = 0x02 // body is raw binary (not UTF-8)
	FLAG_RETAIN    Flags = 0x04 // for channels: retain last message for late subscribers
	FLAG_PERSIST   Flags = 0x08 // for cache SET: persist to SQLite immediately
)

// ── Status codes ──────────────────────────────────────────
// When Op == OP_RESPONSE, byte[0] of body is a StatusCode.

type StatusCode byte

const (
	STATUS_OK          StatusCode = 0x00
	STATUS_NOT_FOUND   StatusCode = 0x01
	STATUS_AUTH_FAIL   StatusCode = 0x02
	STATUS_QUEUE_FULL  StatusCode = 0x03
	STATUS_ROOM_FULL   StatusCode = 0x04
	STATUS_LIMIT_HIT   StatusCode = 0x05
	STATUS_UNKNOWN_OP  StatusCode = 0x06
	STATUS_INTERNAL    StatusCode = 0x07
)

// ── Frame ─────────────────────────────────────────────────

// Frame is one Zeus protocol message, parsed from the wire.
type Frame struct {
	Op        OpCode // what to do
	Flags     Flags  // bitmask options
	RequestID uint32 // echoed in response so client matches async replies
	Key       []byte // topic / channel / queue / room name / cache key
	Body      []byte // payload — meaning depends on OpCode
}

// ── Encoder ────────────────────────────────────────────────

// Encode serialises the frame into binary and writes it to w.
// Layout: [Magic][Version][Op][Flags][RequestID:4][KeyLen:2][BodyLen:4][Reserved][Key][Body]
func (f *Frame) Encode(w io.Writer) error {
	keyLen := len(f.Key)
	bodyLen := len(f.Body)

	// Build the 15-byte fixed header
	hdr := make([]byte, HeaderSize)
	hdr[0] = Magic
	hdr[1] = Version
	hdr[2] = byte(f.Op)
	hdr[3] = byte(f.Flags)
	binary.BigEndian.PutUint32(hdr[4:8], f.RequestID)
	binary.BigEndian.PutUint16(hdr[8:10], uint16(keyLen))
	binary.BigEndian.PutUint32(hdr[10:14], uint32(bodyLen))
	hdr[14] = 0x00 // reserved

	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if keyLen > 0 {
		if _, err := w.Write(f.Key); err != nil {
			return err
		}
	}
	if bodyLen > 0 {
		if _, err := w.Write(f.Body); err != nil {
			return err
		}
	}
	return nil
}

// ── Decoder ────────────────────────────────────────────────

var (
	ErrBadMagic   = errors.New("zeus: bad magic byte — not a Zeus frame")
	ErrBadVersion = errors.New("zeus: unsupported protocol version")
)

// Decode reads exactly one frame from r.
// It blocks until a full frame is available or an error occurs.
func Decode(r io.Reader) (*Frame, error) {
	// Read fixed-size header first
	hdr := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}

	// Validate magic byte
	if hdr[0] != Magic {
		return nil, ErrBadMagic
	}
	// Validate version — we can add upgrade negotiation here later
	if hdr[1] != Version {
		return nil, ErrBadVersion
	}

	f := &Frame{
		Op:        OpCode(hdr[2]),
		Flags:     Flags(hdr[3]),
		RequestID: binary.BigEndian.Uint32(hdr[4:8]),
	}
	keyLen := binary.BigEndian.Uint16(hdr[8:10])
	bodyLen := binary.BigEndian.Uint32(hdr[10:14])
	// hdr[14] is reserved — ignore for now

	// Read variable-length key
	if keyLen > 0 {
		f.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(r, f.Key); err != nil {
			return nil, err
		}
	}

	// Read variable-length body
	if bodyLen > 0 {
		f.Body = make([]byte, bodyLen)
		if _, err := io.ReadFull(r, f.Body); err != nil {
			return nil, err
		}
	}

	return f, nil
}

// ── Helpers ────────────────────────────────────────────────

// OKResponse builds a success response frame for the given request.
// payload is appended after the STATUS_OK byte.
func OKResponse(requestID uint32, payload []byte) *Frame {
	body := make([]byte, 1+len(payload))
	body[0] = byte(STATUS_OK)
	copy(body[1:], payload)
	return &Frame{Op: OP_RESPONSE, RequestID: requestID, Body: body}
}

// ErrorResponse builds an error response frame with a status code and message.
func ErrorResponse(requestID uint32, status StatusCode, msg string) *Frame {
	msgBytes := []byte(msg)
	body := make([]byte, 1+len(msgBytes))
	body[0] = byte(status)
	copy(body[1:], msgBytes)
	return &Frame{Op: OP_ERROR, RequestID: requestID, Body: body}
}

// PushFrame builds a server→client push frame (channel / queue / chat delivery).
func PushFrame(op OpCode, key, body []byte) *Frame {
	return &Frame{Op: op, Key: key, Body: body}
}

// KeyString returns the Key field as a UTF-8 string.
func (f *Frame) KeyString() string { return string(f.Key) }

// BodyString returns the Body field as a UTF-8 string.
func (f *Frame) BodyString() string { return string(f.Body) }
