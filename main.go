// Zeus — High-performance binary-protocol cache + realtime server
//
// FEATURES AT A GLANCE
// ─────────────────────
//  ┌─────────────────┬──────────────────────────────────────────────────┐
//  │  Cache          │  In-memory GET/SET/DELETE/CLEAR with optional TTL │
//  │  Persistence    │  SQLite backup (opt-in via zeus.yaml)             │
//  │  Channels       │  Pub/sub fan-out with history replay              │
//  │  Queues         │  Reliable delivery with ACK/NACK/retry backoff   │
//  │  Chat           │  Built-in chat rooms with presence + history      │
//  │  Webhooks       │  Fire events to your backend — zero lock-in       │
//  │  Security       │  Token auth + optional TLS (auto self-signed)     │
//  └─────────────────┴──────────────────────────────────────────────────┘
//
// FIRST RUN
// ─────────
//  1. Run: zeus init        ← creates zeus.yaml + generates auth token
//  2. Edit zeus.yaml        ← turn on persistence, webhooks, TLS, etc.
//  3. Run: zeus             ← starts the server
//  4. Copy the token to your clients
//
// BINARY PROTOCOL
// ───────────────
//  Zeus uses a compact binary frame protocol (see protocol/frame.go).
//  This makes it suitable for high-throughput use cases like:
//   - Live chat at scale
//   - IoT sensor streams
//   - Game state synchronisation
//   - Event-driven microservice buses
//
// ENVIRONMENT VARIABLES
// ─────────────────────
//  ZEUS_CONFIG   Path to config file (default: zeus.yaml)

package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"zeus/chat"
	"zeus/cmd"
	"zeus/config"
	"zeus/pubsub"
	"zeus/queue"
	"zeus/rpc"
	"zeus/security"
	"zeus/server"
	"zeus/store"
)

func main() {
	// ── CLI subcommands ──────────────────────────────────────
	// If the first argument is a known subcommand, handle it and exit.
	// Otherwise fall through to start the server.
	if len(os.Args) > 1 {
		subcommands := map[string]bool{
			"init": true, "token": true, "status": true, "help": true,
			"--help": true, "-h": true, "start": true,
		}
		if subcommands[os.Args[1]] {
			os.Exit(cmd.Run(os.Args[1:]))
		}
	}

	// ── Load config ──────────────────────────────────────────
	configPath := "zeus.yaml"
	if p := os.Getenv("ZEUS_CONFIG"); p != "" {
		configPath = p
	}

	cfg, firstRun, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("[zeus] failed to load config: %v", err)
	}

	// On first run, show the generated token prominently so the operator
	// can copy it to their clients immediately.
	if firstRun {
		cmd.PrintStartupBanner(cfg.Addr(), cfg.Security.Token, cfg.Security.Enabled, cfg.Security.TLS.Enabled)
	}

	// ── Setup logger ─────────────────────────────────────────
	setupLogger(cfg.Log.Level, cfg.Log.File)
	log.Printf("[zeus] starting on %s", cfg.Addr())

	// ── Persistence (SQLite) ─────────────────────────────────
	// Only opened if persistence.enabled = true in zeus.yaml.
	var db *store.DB
	if cfg.Persistence.Enabled {
		db, err = store.OpenDB(cfg.Persistence.DBPath)
		if err != nil {
			log.Fatalf("[zeus] open database: %v", err)
		}
		defer db.Close()
		log.Printf("[zeus] persistence enabled → %s", cfg.Persistence.DBPath)
	}

	// ── In-memory cache (sharded for max throughput) ─────────
	// ShardedCache splits the keyspace across 256 independent sub-caches,
	// each with its own mutex. At high concurrency this gives ~250x less
	// lock contention vs a single global mutex.
	shardedCache := store.NewShardedCache()

	// Hook cache changes into the DB layer so every write is mirrored to SQLite.
	// All 256 shards share the same callback — it's called asynchronously
	// so cache operations are never blocked by the DB write.
	if db != nil {
		onChangeFn := func(evt store.ChangeEvent) {
			switch evt.Op {
			case "set":
				var exp time.Time
				if evt.TTL > 0 {
					exp = time.Now().Add(evt.TTL)
				}
				if err := db.SaveCacheEntry(evt.Key, evt.Value, exp); err != nil {
					log.Printf("[zeus] db save cache entry: %v", err)
				}
			case "delete":
				if err := db.DeleteCacheEntry(evt.Key); err != nil {
					log.Printf("[zeus] db delete cache entry: %v", err)
				}
			case "clear":
				if err := db.ClearCache(); err != nil {
					log.Printf("[zeus] db clear cache: %v", err)
				}
			}
		}
		shardedCache.SetOnChange(onChangeFn)
	}

	if db != nil && cfg.Persistence.LoadOnStartup {
		// Restore persisted data from SQLite into all shards
		items, err := db.LoadCache()
		if err != nil {
			log.Printf("[zeus] warn: could not load cache from db: %v", err)
		} else {
			shardedCache.LoadBulk(items)
			log.Printf("[zeus] restored %d cache entries from SQLite", len(items))
		}
	}

	// The server uses the Cache interface; ShardedCache satisfies it
	cache := shardedCache

	// ── Security (auth + TLS) ────────────────────────────────
	auth := security.New(cfg.Security.Enabled, cfg.Security.Token)

	var tlsCfg *tls.Config
	if cfg.Security.TLS.Enabled {
		tlsCfg, err = security.BuildTLSConfig(security.TLSConfig{
			Enabled:  cfg.Security.TLS.Enabled,
			CertFile: cfg.Security.TLS.CertFile,
			KeyFile:  cfg.Security.TLS.KeyFile,
			AutoGen:  cfg.Security.TLS.AutoGen,
		})
		if err != nil {
			log.Fatalf("[zeus] TLS setup: %v", err)
		}
		log.Printf("[zeus] TLS enabled (cert: %s)", cfg.Security.TLS.CertFile)
	}

	// ── Channels ─────────────────────────────────────────────
	channels := pubsub.NewManager(cfg.Channels.MaxChannels, cfg.Channels.HistorySize)

	// ── Queues ───────────────────────────────────────────────
	retryPolicy := queue.RetryPolicy{
		MaxAttempts:   cfg.Queues.Retry.MaxAttempts,
		InitialDelay:  time.Duration(cfg.Queues.Retry.InitialDelaySec) * time.Second,
		BackoffFactor: cfg.Queues.Retry.BackoffFactor,
		MaxDelay:      time.Duration(cfg.Queues.Retry.MaxDelaySec) * time.Second,
		AckTimeout:    cfg.AckTimeout(),
	}
	queues := queue.NewManager(cfg.Queues.MaxQueues, cfg.Queues.MaxQueueDepth, retryPolicy)

	// Wire queue persistence: every enqueue/ack/nack/dead is mirrored to SQLite.
	// Also restore any pending messages from the DB that survived a restart —
	// they're re-injected into their queues so delivery picks up where it left off.
	if db != nil {
		queues.SetDB(db)
		log.Printf("[zeus] queue persistence enabled")

		if cfg.Persistence.LoadOnStartup {
			restored, restoreErr := restoreQueuesFromDB(db, queues)
			if restoreErr != nil {
				log.Printf("[zeus] warn: queue restore failed: %v", restoreErr)
			} else if restored > 0 {
				log.Printf("[zeus] restored %d pending queue messages from SQLite", restored)
			}
		}
	}

	// ── Chat ─────────────────────────────────────────────────
	webhookClient := chat.NewWebhookClient(
		cfg.Webhook.Enabled,
		cfg.Webhook.URL,
		cfg.Webhook.Secret,
		cfg.Webhook.TimeoutSec,
		cfg.Webhook.RetryOnFailure,
		chat.WebhookEventFilter{
			ChatMessage: cfg.Webhook.Events.ChatMessage,
			ChatJoin:    cfg.Webhook.Events.ChatJoin,
			ChatLeave:   cfg.Webhook.Events.ChatLeave,
		},
	)
	// Map config feature flags → chat.Features struct
	chatFeatures := chat.Features{
		ReceiptTracking: cfg.Chat.Features.ReceiptTracking,
		SmartDelivery:   cfg.Chat.Features.SmartDelivery,
		PollsEnabled:    cfg.Chat.Features.Polls,
		UserMetadata:    cfg.Chat.Features.UserMetadata,
		GCAfterDays:     cfg.Chat.Features.GCAfterDays,
	}
	chatMgr := chat.NewManager(
		cfg.Chat.MaxRooms,
		cfg.Chat.MaxMembersPerRoom,
		cfg.Chat.HistorySize,
		cfg.Chat.PresenceTTLSec,
		chatFeatures,
		webhookClient,
	)

	// ── Chat GC loop ─────────────────────────────────────────
	// If gc_after_days > 0 AND receipt_tracking is on, run nightly GC to prune
	// messages that everyone in a room has read. Zeus never deletes unread messages.
	if db != nil && cfg.Chat.Features.GCAfterDays > 0 && cfg.Chat.Features.ReceiptTracking {
		gcDays := cfg.Chat.Features.GCAfterDays
		go func() {
			// First run 1 minute after startup, then every 24 hours
			time.Sleep(1 * time.Minute)
			ticker := time.NewTicker(24 * time.Hour)
			defer ticker.Stop()
			for {
				stats := chatMgr.Stats()
				total := int64(0)
				for room := range stats {
					n, err := db.GCOldMessages(room, gcDays)
					if err != nil {
						log.Printf("[zeus] gc room %q: %v", room, err)
					} else {
						total += n
					}
				}
				if total > 0 {
					log.Printf("[zeus] gc: pruned %d fully-read messages older than %d days", total, gcDays)
				}
				<-ticker.C
			}
		}()
		log.Printf("[zeus] chat GC enabled: delete read messages after %d days", gcDays)
	}

	// ── RPC manager ──────────────────────────────────────────────────────────
	// Default call timeout is 5 minutes; callers can override per-call via
	// the [timeoutMs:4] prefix in OP_RPC_CALL body.
	rpcMgr := rpc.NewManager(5 * time.Minute)

	// ── Server ───────────────────────────────────────────────
	srv := server.New(cfg, auth, cache, db, channels, queues, chatMgr, rpcMgr)

	// ── TCP listener ─────────────────────────────────────────
	var listener net.Listener
	if tlsCfg != nil {
		listener, err = tls.Listen("tcp", cfg.Addr(), tlsCfg)
	} else {
		listener, err = net.Listen("tcp", cfg.Addr())
	}
	if err != nil {
		log.Fatalf("[zeus] listen on %s: %v", cfg.Addr(), err)
	}
	defer listener.Close()

	// Print the animated ASCII banner with thunder effect
	cmd.PrintStartupBanner(cfg.Addr(), cfg.Security.Token, cfg.Security.Enabled, cfg.Security.TLS.Enabled)
	log.Printf("[zeus] ready — listening on %s", cfg.Addr())

	// ── Graceful shutdown ─────────────────────────────────────
	// Listen for SIGINT / SIGTERM so we can flush writes before exiting.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[zeus] received %s — shutting down", sig)
		listener.Close() // causes listener.Accept() to return an error → loop exits
	}()

	// ── Accept loop ───────────────────────────────────────────
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Accept returns an error when the listener is closed (shutdown).
			// Any other error is transient — log and continue.
			select {
			case <-sigCh:
				// Already shutting down — break cleanly
			default:
				if isClosedErr(err) {
					break
				}
				log.Printf("[zeus] accept error: %v", err)
				continue
			}
			break
		}
		go srv.HandleConn(conn)
	}

	log.Println("[zeus] goodbye")
}

// ── Helpers ───────────────────────────────────────────────────

// setupLogger configures the standard logger with appropriate flags.
func setupLogger(level, file string) {
	flags := log.LstdFlags
	if level == "debug" {
		flags |= log.Lshortfile
	}
	log.SetFlags(flags)

	if file != "" {
		f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("[zeus] warn: cannot open log file %s: %v", file, err)
			return
		}
		log.SetOutput(f)
	}
}

// restoreQueuesFromDB re-injects pending queue messages from SQLite into
// their in-memory queues after a server restart.
//
// How it works:
//  1. We don't know which queue names exist yet (no clients connected), so we
//     query the DB for all distinct queue names that have pending messages.
//  2. For each name we call GetOrCreate (which wires up the OnPersist callback)
//     then inject each pending message directly into the pending slice.
//
// The injected messages keep their original DBID so ACK/NACK still hits the
// correct DB row. Their NextRetry is already set correctly by the DB so
// back-off is preserved across restarts.
func restoreQueuesFromDB(db *store.DB, mgr *queue.Manager) (int, error) {
	// Pull all pending queue names from the DB
	queueNames, err := db.LoadAllPendingQueueNames()
	if err != nil {
		return 0, err
	}

	total := 0
	for _, name := range queueNames {
		msgs, err := db.LoadPendingQueueMessages(name)
		if err != nil {
			log.Printf("[zeus] restore queue %q: %v", name, err)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		q, err := mgr.GetOrCreate(name)
		if err != nil {
			log.Printf("[zeus] restore queue %q create: %v", name, err)
			continue
		}

		for _, m := range msgs {
			q.Restore(&queue.Message{
				ID:        fmt.Sprintf("%d-%d", m.CreatedAt.UnixNano(), m.ID),
				QueueName: name,
				Payload:   m.Payload,
				Status:    queue.StatusPending,
				Attempts:  m.Attempts,
				Error:     m.ErrorMsg,
				CreatedAt: m.CreatedAt,
				NextRetry: m.NextRetry,
				DBID:      m.ID,
			})
			total++
		}
	}
	return total, nil
}

// isClosedErr returns true if err indicates the listener was closed.
func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	return contains(err.Error(), "use of closed network connection")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsRaw(s, sub))
}

func containsRaw(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
