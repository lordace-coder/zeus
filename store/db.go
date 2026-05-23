// db.go — SQLite persistence layer for Zeus.
//
// WHY SQLITE?
// ───────────
//  Zeus is a cache + message broker first, so the primary data lives in
//  memory for speed. SQLite is used as a durable backing store:
//   - Cache snapshots: so a server restart doesn't lose hot data.
//   - Queue state: unACKed messages survive crashes.
//   - Chat history + delivery receipts: full WhatsApp-style message tracking.
//
// The DB is only created/used when persistence.enabled = true in zeus.yaml.
//
// SCHEMA
// ──────
//  Table: cache_entries     — key/value/ttl snapshots
//  Table: queue_messages    — pending/failed queue items
//  Table: chat_messages     — room message history with metadata + delivery state
//  Table: chat_receipts     — per-user delivery receipts (sent/delivered/read)
//  Table: chat_polls        — poll definitions attached to messages
//  Table: chat_poll_votes   — individual poll votes
//  Table: chat_user_meta    — per-user-per-room metadata (display name, avatar, etc.)
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no cgo required)
)

// DB wraps the SQLite connection and exposes Zeus-specific methods.
type DB struct {
	conn *sql.DB
}

// OpenDB opens (or creates) a SQLite database at path and initialises the schema.
func OpenDB(path string) (*DB, error) {
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Use WAL mode for better concurrent read performance
	if _, err = conn.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}

	db := &DB{conn: conn}
	if err = db.migrate(); err != nil {
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	return db, nil
}

// Close releases the database connection.
func (db *DB) Close() error { return db.conn.Close() }

// ── Schema migration ───────────────────────────────────────

// migrate creates all required tables if they don't already exist.
// Zeus never drops or alters existing columns, so old databases are safe.
func (db *DB) migrate() error {
	_, err := db.conn.Exec(`
		-- ── Cache snapshots ─────────────────────────────────
		CREATE TABLE IF NOT EXISTS cache_entries (
			key        TEXT PRIMARY KEY,
			value      BLOB NOT NULL,
			expires_at INTEGER          -- Unix timestamp; 0 = no expiry
		);

		-- ── Queue messages (pending + failed) ───────────────
		CREATE TABLE IF NOT EXISTS queue_messages (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			queue_name  TEXT    NOT NULL,
			payload     BLOB    NOT NULL,
			status      TEXT    NOT NULL DEFAULT 'pending', -- pending | failed
			attempts    INTEGER NOT NULL DEFAULT 0,
			error_msg   TEXT,                               -- last failure reason
			created_at  INTEGER NOT NULL,                   -- Unix timestamp
			next_retry  INTEGER NOT NULL DEFAULT 0          -- when to retry
		);
		CREATE INDEX IF NOT EXISTS idx_queue_messages_queue
			ON queue_messages (queue_name, status, next_retry);

		-- ── Chat messages ────────────────────────────────────
		-- Full message history with WhatsApp-style delivery state.
		-- 'type' tells the client how to render: text | image | video |
		--   audio | file | poll | reaction | system
		-- 'metadata' is a JSON blob the client controls (reply_to_id,
		--   filename, duration, caption, etc.)
		-- 'delivery_state' tracks the furthest state of any recipient:
		--   0=sent  1=delivered  2=read
		CREATE TABLE IF NOT EXISTS chat_messages (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			room            TEXT    NOT NULL,
			sender_id       TEXT    NOT NULL,
			msg_type        TEXT    NOT NULL DEFAULT 'text',
			payload         BLOB    NOT NULL,
			metadata        TEXT,           -- JSON; client-defined
			delivery_state  INTEGER NOT NULL DEFAULT 0,
			sent_at         INTEGER NOT NULL,
			edited_at       INTEGER,        -- NULL unless message was edited
			deleted_at      INTEGER         -- NULL unless soft-deleted
		);
		CREATE INDEX IF NOT EXISTS idx_chat_messages_room
			ON chat_messages (room, sent_at)
			WHERE deleted_at IS NULL;

		-- ── Delivery receipts ────────────────────────────────
		-- Per-user, per-message receipt so the sender knows exactly
		-- who received/read their message (like WhatsApp blue ticks).
		-- state: 0=sent(server has it)  1=delivered(client got it)  2=read
		CREATE TABLE IF NOT EXISTS chat_receipts (
			msg_id      INTEGER NOT NULL REFERENCES chat_messages(id),
			user_id     TEXT    NOT NULL,
			state       INTEGER NOT NULL DEFAULT 0,
			updated_at  INTEGER NOT NULL,
			PRIMARY KEY (msg_id, user_id)
		);
		CREATE INDEX IF NOT EXISTS idx_chat_receipts_user
			ON chat_receipts (user_id, state, msg_id);

		-- ── Polls ────────────────────────────────────────────
		-- A poll is a special message (msg_type='poll').
		-- The options JSON is an array of strings: ["Yes","No","Maybe"]
		CREATE TABLE IF NOT EXISTS chat_polls (
			msg_id      INTEGER PRIMARY KEY REFERENCES chat_messages(id),
			question    TEXT    NOT NULL,
			options     TEXT    NOT NULL, -- JSON array of strings
			multi_vote  INTEGER NOT NULL DEFAULT 0, -- 1 = allow multiple choices
			closes_at   INTEGER          -- NULL = open forever
		);

		-- ── Poll votes ───────────────────────────────────────
		CREATE TABLE IF NOT EXISTS chat_poll_votes (
			msg_id      INTEGER NOT NULL REFERENCES chat_polls(msg_id),
			user_id     TEXT    NOT NULL,
			option_idx  INTEGER NOT NULL, -- 0-based index into options array
			voted_at    INTEGER NOT NULL,
			PRIMARY KEY (msg_id, user_id, option_idx)
		);

		-- ── User room metadata ───────────────────────────────
		-- Devs can attach arbitrary JSON metadata to a user's presence
		-- in a room: display name, avatar URL, role, custom status, etc.
		-- Zeus doesn't interpret this — it's purely for clients.
		CREATE TABLE IF NOT EXISTS chat_user_meta (
			room        TEXT NOT NULL,
			user_id     TEXT NOT NULL,
			meta        TEXT NOT NULL DEFAULT '{}', -- JSON object
			updated_at  INTEGER NOT NULL,
			PRIMARY KEY (room, user_id)
		);
	`)
	return err
}

// ── Cache persistence ──────────────────────────────────────

// SaveCacheEntry upserts a cache entry into SQLite.
// expiresAt == zero means no expiry (stored as 0 in the DB).
func (db *DB) SaveCacheEntry(key string, value []byte, expiresAt time.Time) error {
	exp := int64(0)
	if !expiresAt.IsZero() {
		exp = expiresAt.Unix()
	}
	_, err := db.conn.Exec(
		`INSERT INTO cache_entries (key, value, expires_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at`,
		key, value, exp,
	)
	return err
}

// DeleteCacheEntry removes a key from the SQLite cache table.
func (db *DB) DeleteCacheEntry(key string) error {
	_, err := db.conn.Exec(`DELETE FROM cache_entries WHERE key = ?`, key)
	return err
}

// ClearCache removes ALL cache entries from SQLite.
func (db *DB) ClearCache() error {
	_, err := db.conn.Exec(`DELETE FROM cache_entries`)
	return err
}

// LoadCache reads all non-expired cache entries from SQLite and returns
// them as a map[key]value, ready to be passed to Cache.LoadBulk().
func (db *DB) LoadCache() (map[string][]byte, error) {
	now := time.Now().Unix()
	rows, err := db.conn.Query(
		`SELECT key, value FROM cache_entries
		 WHERE expires_at = 0 OR expires_at > ?`, now,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]byte)
	for rows.Next() {
		var key string
		var value []byte
		if err = rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, rows.Err()
}

// ── Queue persistence ──────────────────────────────────────

// QueueMessage is a queue item loaded from SQLite.
type QueueMessage struct {
	ID        int64
	QueueName string
	Payload   []byte
	Status    string // "pending" | "failed"
	Attempts  int
	ErrorMsg  string
	CreatedAt time.Time
	NextRetry time.Time
}

// SaveQueueMessage inserts a new queue message into SQLite and returns its ID.
func (db *DB) SaveQueueMessage(queueName string, payload []byte) (int64, error) {
	now := time.Now().Unix()
	res, err := db.conn.Exec(
		`INSERT INTO queue_messages (queue_name, payload, status, created_at, next_retry)
		 VALUES (?, ?, 'pending', ?, 0)`,
		queueName, payload, now,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// MarkQueueMessageFailed updates a message's status to "failed", records the
// error, increments attempts, and schedules the next retry.
func (db *DB) MarkQueueMessageFailed(id int64, errMsg string, nextRetry time.Time) error {
	_, err := db.conn.Exec(
		`UPDATE queue_messages
		 SET status='pending', attempts=attempts+1, error_msg=?, next_retry=?
		 WHERE id=?`,
		errMsg, nextRetry.Unix(), id,
	)
	return err
}

// MarkQueueMessageDead permanently marks a message as "failed" after max retries.
func (db *DB) MarkQueueMessageDead(id int64, errMsg string) error {
	_, err := db.conn.Exec(
		`UPDATE queue_messages SET status='failed', error_msg=? WHERE id=?`,
		errMsg, id,
	)
	return err
}

// DeleteQueueMessage removes a successfully processed message from SQLite.
func (db *DB) DeleteQueueMessage(id int64) error {
	_, err := db.conn.Exec(`DELETE FROM queue_messages WHERE id=?`, id)
	return err
}

// LoadAllPendingQueueNames returns the distinct names of every queue that has
// at least one pending message in SQLite. Used on startup to know which queues
// need to be restored into memory.
func (db *DB) LoadAllPendingQueueNames() ([]string, error) {
	rows, err := db.conn.Query(
		`SELECT DISTINCT queue_name FROM queue_messages WHERE status='pending'`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// LoadPendingQueueMessages loads ALL pending messages (regardless of next_retry)
// for a given queue name. Used on startup to restore full queue state —
// the retry schedule is preserved via the NextRetry field.
func (db *DB) LoadPendingQueueMessages(queueName string) ([]*QueueMessage, error) {
	rows, err := db.conn.Query(
		`SELECT id, queue_name, payload, status, attempts, COALESCE(error_msg,''), created_at, next_retry
		 FROM queue_messages
		 WHERE queue_name=? AND status='pending'
		 ORDER BY id ASC`,
		queueName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*QueueMessage
	for rows.Next() {
		var m QueueMessage
		var createdAt, nextRetry int64
		if err = rows.Scan(&m.ID, &m.QueueName, &m.Payload, &m.Status,
			&m.Attempts, &m.ErrorMsg, &createdAt, &nextRetry); err != nil {
			return nil, err
		}
		m.CreatedAt = time.Unix(createdAt, 0)
		m.NextRetry = time.Unix(nextRetry, 0)
		msgs = append(msgs, &m)
	}
	return msgs, rows.Err()
}

// ── Chat: full WhatsApp-style message store ────────────────

// MsgType categorises a chat message for client rendering.
type MsgType string

const (
	MsgTypeText     MsgType = "text"
	MsgTypeImage    MsgType = "image"
	MsgTypeVideo    MsgType = "video"
	MsgTypeAudio    MsgType = "audio"
	MsgTypeFile     MsgType = "file"
	MsgTypePoll     MsgType = "poll"
	MsgTypeReaction MsgType = "reaction"
	MsgTypeSystem   MsgType = "system" // join/leave/rename events
)

// ReceiptState mirrors WhatsApp tick system.
type ReceiptState int

const (
	ReceiptSent      ReceiptState = 0 // server received it (one grey tick)
	ReceiptDelivered ReceiptState = 1 // client device got it (two grey ticks)
	ReceiptRead      ReceiptState = 2 // client opened it   (two blue ticks)
)

// ChatMessageRow is a full DB row for a chat message.
type ChatMessageRow struct {
	ID            int64
	Room          string
	SenderID      string
	MsgType       MsgType
	Payload       []byte
	Metadata      map[string]interface{} // parsed from JSON
	DeliveryState ReceiptState
	SentAt        time.Time
	EditedAt      *time.Time
	DeletedAt     *time.Time
}

// SaveChatMessage inserts a new chat message and returns its DB ID.
// metadata is any JSON-serialisable map — pass nil for plain text messages.
func (db *DB) SaveChatMessage(room, senderID string, msgType MsgType, payload []byte, metadata map[string]interface{}) (int64, error) {
	metaJSON := "{}"
	if metadata != nil {
		b, err := json.Marshal(metadata)
		if err == nil {
			metaJSON = string(b)
		}
	}
	res, err := db.conn.Exec(
		`INSERT INTO chat_messages (room, sender_id, msg_type, payload, metadata, sent_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		room, senderID, string(msgType), payload, metaJSON, time.Now().Unix(),
	)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	// Every new message starts with ReceiptSent for the sender themselves
	_ = db.UpsertReceipt(id, senderID, ReceiptSent)
	return id, nil
}

// SoftDeleteMessage marks a message as deleted (it stays in DB for history
// integrity but clients will render it as "This message was deleted").
func (db *DB) SoftDeleteMessage(msgID int64) error {
	_, err := db.conn.Exec(
		`UPDATE chat_messages SET deleted_at=? WHERE id=?`,
		time.Now().Unix(), msgID,
	)
	return err
}

// EditMessage updates message payload and records edit timestamp.
func (db *DB) EditMessage(msgID int64, newPayload []byte) error {
	_, err := db.conn.Exec(
		`UPDATE chat_messages SET payload=?, edited_at=? WHERE id=?`,
		newPayload, time.Now().Unix(), msgID,
	)
	return err
}

// LoadChatHistory returns the last `limit` non-deleted messages for a room,
// oldest-first. If afterID > 0, only returns messages with id > afterID
// (pagination cursor).
func (db *DB) LoadChatHistory(room string, limit int, afterID int64) ([]*ChatMessageRow, error) {
	query := `
		SELECT id, sender_id, msg_type, payload, COALESCE(metadata,'{}'),
		       delivery_state, sent_at, edited_at, deleted_at
		FROM chat_messages
		WHERE room=? AND deleted_at IS NULL AND id > ?
		ORDER BY sent_at DESC
		LIMIT ?`
	rows, err := db.conn.Query(query, room, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*ChatMessageRow
	for rows.Next() {
		var m ChatMessageRow
		var sentAt int64
		var editedAt, deletedAt sql.NullInt64
		var metaJSON string
		var msgType string
		if err = rows.Scan(&m.ID, &m.SenderID, &msgType, &m.Payload, &metaJSON,
			&m.DeliveryState, &sentAt, &editedAt, &deletedAt); err != nil {
			return nil, err
		}
		m.Room = room
		m.MsgType = MsgType(msgType)
		m.SentAt = time.Unix(sentAt, 0)
		if editedAt.Valid {
			t := time.Unix(editedAt.Int64, 0)
			m.EditedAt = &t
		}
		if deletedAt.Valid {
			t := time.Unix(deletedAt.Int64, 0)
			m.DeletedAt = &t
		}
		_ = json.Unmarshal([]byte(metaJSON), &m.Metadata)
		msgs = append(msgs, &m)
	}
	// Reverse: DB gave us newest-first, callers expect oldest-first
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	return msgs, rows.Err()
}

// LoadUndeliveredMessages returns all messages in a room that the given userID
// has NOT yet marked as ReceiptDelivered. This is the smart GC / catch-up
// mechanism — when a user reconnects, Zeus only sends them what they missed.
func (db *DB) LoadUndeliveredMessages(room, userID string) ([]*ChatMessageRow, error) {
	// Find the max msg_id this user has already delivered, then load everything after
	var maxDelivered int64
	row := db.conn.QueryRow(
		`SELECT COALESCE(MAX(msg_id), 0) FROM chat_receipts
		 WHERE user_id=? AND state >= ? AND msg_id IN (
		   SELECT id FROM chat_messages WHERE room=?
		 )`,
		userID, int(ReceiptDelivered), room,
	)
	_ = row.Scan(&maxDelivered)

	return db.LoadChatHistory(room, 1000, maxDelivered)
}

// ── Delivery receipts ──────────────────────────────────────

// UpsertReceipt updates or inserts a delivery receipt for a user/message pair.
// Receipts only move FORWARD: delivered→read is ok, read→delivered is ignored.
func (db *DB) UpsertReceipt(msgID int64, userID string, state ReceiptState) error {
	_, err := db.conn.Exec(
		`INSERT INTO chat_receipts (msg_id, user_id, state, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(msg_id, user_id) DO UPDATE
		 SET state = MAX(excluded.state, state),  -- never downgrade
		     updated_at = excluded.updated_at
		 WHERE excluded.state > state`,
		msgID, userID, int(state), time.Now().Unix(),
	)
	return err
}

// GetMessageDeliveryState returns the aggregate delivery state for a message:
// the minimum across all receipts (weakest-link — all must read for "read").
func (db *DB) GetMessageDeliveryState(msgID int64) (ReceiptState, error) {
	var minState sql.NullInt64
	err := db.conn.QueryRow(
		`SELECT MIN(state) FROM chat_receipts WHERE msg_id=?`, msgID,
	).Scan(&minState)
	if err != nil || !minState.Valid {
		return ReceiptSent, err
	}
	return ReceiptState(minState.Int64), nil
}

// BulkMarkDelivered marks all messages in a room as ReceiptDelivered for userID
// where the current state is still ReceiptSent. Called when a user reconnects.
func (db *DB) BulkMarkDelivered(room, userID string) error {
	_, err := db.conn.Exec(
		`INSERT INTO chat_receipts (msg_id, user_id, state, updated_at)
		 SELECT cm.id, ?, ?, ?
		 FROM chat_messages cm
		 WHERE cm.room=? AND cm.deleted_at IS NULL
		   AND cm.sender_id != ?
		 ON CONFLICT(msg_id, user_id) DO UPDATE
		 SET state = MAX(excluded.state, state),
		     updated_at = excluded.updated_at
		 WHERE excluded.state > state`,
		userID, int(ReceiptDelivered), time.Now().Unix(), room, userID,
	)
	return err
}

// ── Polls ──────────────────────────────────────────────────

// PollRow holds poll data.
type PollRow struct {
	MsgID     int64
	Question  string
	Options   []string
	MultiVote bool
	ClosesAt  *time.Time
}

// CreatePoll inserts a poll linked to a message.
func (db *DB) CreatePoll(msgID int64, question string, options []string, multiVote bool, closesAt *time.Time) error {
	optJSON, _ := json.Marshal(options)
	var closeUnix interface{}
	if closesAt != nil {
		closeUnix = closesAt.Unix()
	}
	_, err := db.conn.Exec(
		`INSERT INTO chat_polls (msg_id, question, options, multi_vote, closes_at)
		 VALUES (?, ?, ?, ?, ?)`,
		msgID, question, string(optJSON), boolToInt(multiVote), closeUnix,
	)
	return err
}

// VotePoll casts a vote. Idempotent — voting the same option twice is a no-op.
func (db *DB) VotePoll(msgID int64, userID string, optionIdx int) error {
	_, err := db.conn.Exec(
		`INSERT OR IGNORE INTO chat_poll_votes (msg_id, user_id, option_idx, voted_at)
		 VALUES (?, ?, ?, ?)`,
		msgID, userID, optionIdx, time.Now().Unix(),
	)
	return err
}

// GetPollResults returns vote counts per option. Returns map[optionIdx]count.
func (db *DB) GetPollResults(msgID int64) (map[int]int, error) {
	rows, err := db.conn.Query(
		`SELECT option_idx, COUNT(*) FROM chat_poll_votes
		 WHERE msg_id=? GROUP BY option_idx`, msgID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[int]int)
	for rows.Next() {
		var idx, count int
		if err = rows.Scan(&idx, &count); err != nil {
			return nil, err
		}
		result[idx] = count
	}
	return result, rows.Err()
}

// ── User room metadata ─────────────────────────────────────

// SetUserMeta upserts per-user-per-room metadata (display name, avatar, etc.).
// meta should be a JSON-serialisable map.
func (db *DB) SetUserMeta(room, userID string, meta map[string]interface{}) error {
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = db.conn.Exec(
		`INSERT INTO chat_user_meta (room, user_id, meta, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(room, user_id) DO UPDATE SET meta=excluded.meta, updated_at=excluded.updated_at`,
		room, userID, string(b), time.Now().Unix(),
	)
	return err
}

// GetUserMeta retrieves metadata for a user in a room. Returns empty map if none set.
func (db *DB) GetUserMeta(room, userID string) (map[string]interface{}, error) {
	var metaJSON string
	err := db.conn.QueryRow(
		`SELECT meta FROM chat_user_meta WHERE room=? AND user_id=?`, room, userID,
	).Scan(&metaJSON)
	if err == sql.ErrNoRows {
		return map[string]interface{}{}, nil
	}
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	_ = json.Unmarshal([]byte(metaJSON), &out)
	return out, nil
}

// GCOldMessages deletes messages older than `keepDays` that have been
// read by ALL members of the room. This mirrors WhatsApp's approach:
// delete only what everyone has seen.
func (db *DB) GCOldMessages(room string, keepDays int) (int64, error) {
	cutoff := time.Now().AddDate(0, 0, -keepDays).Unix()

	// A message is safe to GC if:
	//  1. It's older than keepDays
	//  2. Every receipt for it is in state ReceiptRead
	//  3. OR it has no pending members (room is empty)
	res, err := db.conn.Exec(`
		DELETE FROM chat_messages
		WHERE room=? AND sent_at < ?
		  AND id NOT IN (
		    SELECT DISTINCT msg_id FROM chat_receipts
		    WHERE state < ? -- any receipt not yet "read" blocks GC
		  )
	`, room, cutoff, int(ReceiptRead))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ── Helpers ────────────────────────────────────────────────

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
