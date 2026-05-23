// Package cmd implements Zeus's command-line interface.
//
// COMMANDS
// ────────
//  zeus init          Generate zeus.yaml with a fresh auth token
//  zeus start         Start the server (reads zeus.yaml)
//  zeus start --dev   Start in dev mode: verbose logging, no auth required
//  zeus status        Print server stats (connects and reads /status internally)
//  zeus token         Print the current auth token from zeus.yaml
//  zeus token rotate  Generate a new auth token and save it to zeus.yaml
//
// The CLI is intentionally minimal. Zeus is configured through zeus.yaml,
// not through a mountain of flags. This keeps the learning curve low.
package cmd

import (
	"crypto/rand"
	"fmt"
	"os"

	"zeus/config"
)

// ConfigPath is the default location Zeus looks for its config file.
// Can be overridden with the ZEUS_CONFIG env var.
var ConfigPath = "zeus.yaml"

// Run is the CLI entry point. Call this from main().
// Returns the process exit code (0 = success).
func Run(args []string) int {
	// Check if a custom config path was set via env
	if p := os.Getenv("ZEUS_CONFIG"); p != "" {
		ConfigPath = p
	}

	if len(args) == 0 {
		printUsage()
		return 1
	}

	switch args[0] {
	case "init":
		return cmdInit()
	case "start":
		// Don't start the server here — main.go does that.
		// This subcommand just validates the approach.
		fmt.Println("Use: zeus [--config path] to start the server.")
		fmt.Println("The server starts automatically when no subcommand is given.")
		return 0
	case "token":
		if len(args) > 1 && args[1] == "rotate" {
			return cmdTokenRotate()
		}
		return cmdToken()
	case "status":
		return cmdStatus()
	case "help", "--help", "-h":
		printUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", args[0])
		printUsage()
		return 1
	}
}

// ── zeus init ────────────────────────────────────────────────

// cmdInit creates zeus.yaml if it doesn't exist.
// If it already exists, it prints the existing token instead of overwriting.
func cmdInit() int {
	// Check if config already exists
	if _, err := os.Stat(ConfigPath); err == nil {
		fmt.Printf("✓ zeus.yaml already exists at %s\n", ConfigPath)
		fmt.Println("  Run 'zeus token' to see your auth token.")
		fmt.Println("  Run 'zeus token rotate' to generate a new one.")
		return 0
	}

	// config.Load creates the file on first run
	cfg, _, err := config.Load(ConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	fmt.Println()
	fmt.Println("  ╔══════════════════════════════════════════════════╗")
	fmt.Println("  ║          Zeus initialised successfully!          ║")
	fmt.Println("  ╚══════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Config saved to: %s\n", ConfigPath)
	fmt.Println()
	fmt.Println("  ┌─ Auth Token (copy this to your clients) ─────────")
	fmt.Printf("  │  %s\n", cfg.Security.Token)
	fmt.Println("  └──────────────────────────────────────────────────")
	fmt.Println()
	fmt.Println("  Next steps:")
	fmt.Println("   1. Edit zeus.yaml to configure persistence, TLS, webhooks, etc.")
	fmt.Println("   2. Run 'zeus' (no args) to start the server")
	fmt.Println("   3. Connect your client using the token above")
	fmt.Println()
	return 0
}

// ── zeus token ───────────────────────────────────────────────

// cmdToken prints the current auth token from zeus.yaml.
func cmdToken() int {
	cfg, err := loadConfig()
	if err != nil {
		return 1
	}
	if !cfg.Security.Enabled {
		fmt.Println("Security is disabled in zeus.yaml (security.enabled = false).")
		fmt.Println("No token required.")
		return 0
	}
	fmt.Println()
	fmt.Println("  Current Zeus auth token:")
	fmt.Printf("  %s\n\n", cfg.Security.Token)
	return 0
}

// cmdTokenRotate generates a new token and saves it to zeus.yaml.
func cmdTokenRotate() int {
	cfg, err := loadConfig()
	if err != nil {
		return 1
	}

	// Generate a new token
	newToken, genErr := generateSecret()
	if genErr != nil {
		fmt.Fprintf(os.Stderr, "Error generating token: %v\n", genErr)
		return 1
	}

	oldToken := cfg.Security.Token
	cfg.Security.Token = newToken

	if err = config.Save(cfg, ConfigPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error saving config: %v\n", err)
		return 1
	}

	fmt.Println()
	fmt.Printf("  Old token: %s\n", oldToken)
	fmt.Printf("  New token: %s\n", newToken)
	fmt.Println()
	fmt.Println("  ⚠  Update this token in all your clients — the old one is now invalid.")
	fmt.Println("  ⚠  Restart Zeus for the new token to take effect.")
	fmt.Println()
	return 0
}

// ── zeus status ──────────────────────────────────────────────

// cmdStatus prints server status info.
// In a real implementation this would connect to the running server
// and request stats. For now it reads the config and shows what's expected.
func cmdStatus() int {
	cfg, err := loadConfig()
	if err != nil {
		return 1
	}

	fmt.Println()
	fmt.Println("  Zeus Server Configuration")
	fmt.Println("  ─────────────────────────")
	fmt.Printf("  Listen address : %s\n", cfg.Addr())
	fmt.Printf("  Security       : %v\n", onOff(cfg.Security.Enabled))
	fmt.Printf("  TLS            : %v\n", onOff(cfg.Security.TLS.Enabled))
	fmt.Printf("  Persistence    : %v", onOff(cfg.Persistence.Enabled))
	if cfg.Persistence.Enabled {
		fmt.Printf(" → %s", cfg.Persistence.DBPath)
	}
	fmt.Println()
	fmt.Printf("  Channels       : %v (max %d, history %d)\n",
		onOff(cfg.Channels.Enabled), cfg.Channels.MaxChannels, cfg.Channels.HistorySize)
	fmt.Printf("  Queues         : %v (max %d, depth %d, ack timeout %ds)\n",
		onOff(cfg.Queues.Enabled), cfg.Queues.MaxQueues, cfg.Queues.MaxQueueDepth, cfg.Queues.AckTimeoutSec)
	fmt.Printf("  Chat           : %v (max %d rooms, history %d)\n",
		onOff(cfg.Chat.Enabled), cfg.Chat.MaxRooms, cfg.Chat.HistorySize)
	fmt.Printf("  Webhooks       : %v", onOff(cfg.Webhook.Enabled))
	if cfg.Webhook.Enabled {
		fmt.Printf(" → %s", cfg.Webhook.URL)
	}
	fmt.Println()
	fmt.Println()
	return 0
}

// ── Helpers ──────────────────────────────────────────────────

func loadConfig() (*config.Config, error) {
	cfg, _, err := config.Load(ConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading %s: %v\n", ConfigPath, err)
		fmt.Fprintf(os.Stderr, "Run 'zeus init' to create a default config.\n")
	}
	return cfg, err
}

func onOff(b bool) string {
	if b {
		return "enabled"
	}
	return "disabled"
}

// generateSecret creates a 32-byte cryptographically random hex token.
// Inlined here so cmd doesn't need to import the security package.
func generateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return fmt.Sprintf("%x", b), nil
}

func printUsage() {
	fmt.Println(`
Zeus — High-performance binary-protocol cache + realtime server

USAGE
  zeus [command]

COMMANDS
  (no args)         Start the server (reads zeus.yaml)
  init              Create zeus.yaml with a fresh auth token
  token             Print the current auth token
  token rotate      Generate a new auth token
  status            Show server configuration summary
  help              Show this help

CONFIGURATION
  Zeus is configured through zeus.yaml in the current directory.
  Run 'zeus init' to generate the file with all options documented.

ENVIRONMENT
  ZEUS_CONFIG   Override the config file path (default: zeus.yaml)

EXAMPLES
  zeus init                 # First-time setup
  zeus                      # Start the server
  zeus token                # See your auth token
  zeus token rotate         # Generate new token
  zeus status               # Check config
`)
}
