// Package cmd implements Zeus's command-line interface.
//
// COMMANDS
// ────────
//  zeus init          Generate zeus.yaml with a fresh auth token
//  zeus start         Start the server (reads zeus.yaml)
//  zeus token         Print the current auth token from zeus.yaml
//  zeus token rotate  Generate a new auth token and save it to zeus.yaml
//  zeus status        Print server configuration summary
//  zeus help          Show help
//
// The CLI is intentionally minimal. Zeus is configured through zeus.yaml,
// not through a mountain of flags. This keeps the learning curve low.
package cmd

import (
	"crypto/rand"
	"fmt"
	"os"
	"runtime"
	"time"

	"zeus/config"
)

// ── ANSI colour helpers ───────────────────────────────────────────────────────

const (
	reset      = "\033[0m"
	bold       = "\033[1m"
	dim        = "\033[2m"
	// Blues / cyans
	blue       = "\033[34m"
	boldBlue   = "\033[1;34m"
	cyan       = "\033[36m"
	boldCyan   = "\033[1;36m"
	lightBlue  = "\033[94m"
	brightCyan = "\033[96m"
	// Accent
	white      = "\033[97m"
	boldWhite  = "\033[1;97m"
	yellow     = "\033[33m"
	boldYellow = "\033[1;33m"
	gray       = "\033[90m"
	green      = "\033[32m"
	boldGreen  = "\033[1;32m"
	red        = "\033[31m"
)

// colour wraps text in the given ANSI colour code.
// On Windows (no ANSI) we skip colouring.
func colour(code, text string) string {
	if runtime.GOOS == "windows" {
		return text
	}
	return code + text + reset
}

// ── ANSI cursor / screen helpers ──────────────────────────────────────────────

const (
	cursorUp1    = "\033[1A"
	cursorUp     = "\033[%dA"
	eraseLine    = "\r\033[2K"
	hideCursor   = "\033[?25l"
	showCursor   = "\033[?25h"
	saveCursor   = "\033[s"
	restCursor   = "\033[u"
)

func up(n int) string { return fmt.Sprintf("\033[%dA", n) }

// ── Logo lines ────────────────────────────────────────────────────────────────
//
// Wider 6-row logo — every character is a full block so colour gradients work.

var logoLines = []string{
	`  ███████╗███████╗██╗   ██╗███████╗`,
	`  ╚══███╔╝██╔════╝██║   ██║██╔════╝`,
	`    ███╔╝ █████╗  ██║   ██║███████╗`,
	`   ███╔╝  ██╔══╝  ██║   ██║╚════██║`,
	`  ███████╗███████╗╚██████╔╝███████║`,
	`  ╚══════╝╚══════╝ ╚═════╝ ╚══════╝`,
}

// Colour gradient applied per logo row (top → bottom: white → cyan → blue)
var logoColours = []string{
	boldWhite,
	brightCyan,
	boldCyan,
	cyan,
	boldBlue,
	blue,
}

// ── Lightning animation ───────────────────────────────────────────────────────
//
// Three layers of animation:
//   1. Logo scan-in   — rows fade in from dim→white one by one
//   2. Strike phase   — multi-line lightning bolt slams through the logo
//   3. Afterglow      — logo pulses back to normal colour, sparks settle
//   4. Boot sequence  — subsystems print in one at a time
//   5. Ready state    — clean final display

// strikeFrames are printed OVER the logo during the lightning strike.
// Each frame is a full replacement of all 6 logo rows + 1 spark row below.
// "\033[7m" = reverse video (inverted colours — gives a "flash" effect).
var strikeFrames = [][]string{
	// Frame 0 — bolt hits top
	{
		"\033[7m" + `  ███████╗███████╗██╗   ██╗███████╗` + reset,
		colour(boldCyan, `  ╚══███╔╝██╔════╝██║   ██║██╔════╝`),
		colour(cyan,     `    ███╔╝ █████╗  ██║   ██║███████╗`),
		colour(cyan,     `   ███╔╝  ██╔══╝  ██║   ██║╚════██║`),
		colour(blue,     `  ███████╗███████╗╚██████╔╝███████║`),
		colour(blue,     `  ╚══════╝╚══════╝ ╚═════╝ ╚══════╝`),
		colour(boldYellow, `          ⚡`),
	},
	// Frame 1 — bolt mid
	{
		colour(boldWhite, `  ███████╗███████╗██╗   ██╗███████╗`),
		"\033[7m" + `  ╚══███╔╝██╔════╝██║   ██║██╔════╝` + reset,
		"\033[7m" + `    ███╔╝ █████╗  ██║   ██║███████╗` + reset,
		colour(cyan,     `   ███╔╝  ██╔══╝  ██║   ██║╚════██║`),
		colour(blue,     `  ███████╗███████╗╚██████╔╝███████║`),
		colour(blue,     `  ╚══════╝╚══════╝ ╚═════╝ ╚══════╝`),
		colour(boldYellow, `          ⚡⚡  ─────────────────`),
	},
	// Frame 2 — full flash (whole logo inverted)
	{
		"\033[7m" + `  ███████╗███████╗██╗   ██╗███████╗` + reset,
		"\033[7m" + `  ╚══███╔╝██╔════╝██║   ██║██╔════╝` + reset,
		"\033[7m" + `    ███╔╝ █████╗  ██║   ██║███████╗` + reset,
		"\033[7m" + `   ███╔╝  ██╔══╝  ██║   ██║╚════██║` + reset,
		"\033[7m" + `  ███████╗███████╗╚██████╔╝███████║` + reset,
		"\033[7m" + `  ╚══════╝╚══════╝ ╚═════╝ ╚══════╝` + reset,
		colour(boldYellow, `      ⚡⚡⚡⚡  ─────────────────────`),
	},
	// Frame 3 — bolt exits bottom, afterglow
	{
		colour(boldWhite,  `  ███████╗███████╗██╗   ██╗███████╗`),
		colour(brightCyan, `  ╚══███╔╝██╔════╝██║   ██║██╔════╝`),
		colour(boldCyan,   `    ███╔╝ █████╗  ██║   ██║███████╗`),
		"\033[7m" + `   ███╔╝  ██╔══╝  ██║   ██║╚════██║` + reset,
		"\033[7m" + `  ███████╗███████╗╚██████╔╝███████║` + reset,
		"\033[7m" + `  ╚══════╝╚══════╝ ╚═════╝ ╚══════╝` + reset,
		colour(boldYellow, `        ⚡⚡  ──── ─ ───────────────`),
	},
	// Frame 4 — sparks spreading
	{
		colour(boldWhite,  `  ███████╗███████╗██╗   ██╗███████╗`),
		colour(brightCyan, `  ╚══███╔╝██╔════╝██║   ██║██╔════╝`),
		colour(boldCyan,   `    ███╔╝ █████╗  ██║   ██║███████╗`),
		colour(cyan,       `   ███╔╝  ██╔══╝  ██║   ██║╚════██║`),
		colour(boldBlue,   `  ███████╗███████╗╚██████╔╝███████║`),
		colour(blue,       `  ╚══════╝╚══════╝ ╚═════╝ ╚══════╝`),
		colour(boldYellow, `   ─ ─ ⚡ ─ ─ ⚡ ─ ─ ⚡ ─ ─ ─ ⚡ ─ ─`),
	},
	// Frame 5 — sparks fading
	{
		colour(boldWhite,  `  ███████╗███████╗██╗   ██╗███████╗`),
		colour(brightCyan, `  ╚══███╔╝██╔════╝██║   ██║██╔════╝`),
		colour(boldCyan,   `    ███╔╝ █████╗  ██║   ██║███████╗`),
		colour(cyan,       `   ███╔╝  ██╔══╝  ██║   ██║╚════██║`),
		colour(boldBlue,   `  ███████╗███████╗╚██████╔╝███████║`),
		colour(blue,       `  ╚══════╝╚══════╝ ╚═════╝ ╚══════╝`),
		colour(yellow,     `   ─ ─ · ─ ─ · ─ ─ · ─ ─ ─ · ─ ─`),
	},
	// Frame 6 — settled
	{
		colour(boldWhite,  `  ███████╗███████╗██╗   ██╗███████╗`),
		colour(brightCyan, `  ╚══███╔╝██╔════╝██║   ██║██╔════╝`),
		colour(boldCyan,   `    ███╔╝ █████╗  ██║   ██║███████╗`),
		colour(cyan,       `   ███╔╝  ██╔══╝  ██║   ██║╚════██║`),
		colour(boldBlue,   `  ███████╗███████╗╚██████╔╝███████║`),
		colour(blue,       `  ╚══════╝╚══════╝ ╚═════╝ ╚══════╝`),
		colour(gray,       `   ── ── ── ── ── ── ── ── ── ── ──`),
	},
}

// subsystems listed during the boot sequence
var subsystems = []struct {
	label string
	icon  string
}{
	{"cache          ", "💾"},
	{"channels       ", "📡"},
	{"queues         ", "📬"},
	{"chat           ", "💬"},
	{"rpc            ", "⚡"},
	{"security       ", "🔒"},
}

// printBanner is the full startup animation. Total runtime ~2.5 s.
func printBanner() {
	fmt.Print(hideCursor)
	defer fmt.Print(showCursor)

	// ── Phase 1: scan-in logo dim → colour ───────────────────────────────────
	fmt.Println()
	// Print all rows dim first
	for _, line := range logoLines {
		fmt.Println(colour(dim+blue, line))
		time.Sleep(18 * time.Millisecond)
	}
	// Blank spark row placeholder
	fmt.Println()

	// Re-colour rows from top one by one (cursor jump back up each time)
	for i, line := range logoLines {
		// Jump to row i
		rows := len(logoLines) + 1 - i // rows from current position to target
		fmt.Print(up(rows))
		fmt.Println(eraseLine + colour(logoColours[i], line))
		// Jump back to bottom
		fmt.Print(fmt.Sprintf("\033[%dB", rows-1))
		time.Sleep(35 * time.Millisecond)
	}
	time.Sleep(80 * time.Millisecond)

	// ── Phase 2: lightning strike ─────────────────────────────────────────────
	// Each strike frame overwrites all 7 rows (6 logo + 1 spark)
	frameDurations := []time.Duration{60, 50, 80, 70, 90, 100, 120}
	for fi, frame := range strikeFrames {
		// Jump up to overwrite all rows
		fmt.Print(up(7))
		for _, row := range frame {
			fmt.Println(eraseLine + row)
		}
		time.Sleep(frameDurations[fi] * time.Millisecond)
	}

	// ── Phase 3: second strike (faster, more intense) ─────────────────────────
	time.Sleep(60 * time.Millisecond)
	quick := []int{2, 0, 2, 1, 4, 5, 6} // frame indices for second strike
	quickDurations := []time.Duration{40, 30, 50, 40, 60, 80, 100}
	for qi, fi := range quick {
		fmt.Print(up(7))
		for _, row := range strikeFrames[fi] {
			fmt.Println(eraseLine + row)
		}
		time.Sleep(quickDurations[qi] * time.Millisecond)
	}

	// ── Phase 4: tagline fades in ─────────────────────────────────────────────
	time.Sleep(50 * time.Millisecond)
	// Overwrite spark row with separator
	fmt.Print(up(1))
	fmt.Println(eraseLine + colour(boldBlue, `  ══════════════════════════════════════`))
	fmt.Println()

	// Tagline types in character-by-character
	tagline := "  ⚡ Zeus  —  binary-protocol realtime server"
	fmt.Print(colour(boldWhite, ""))
	for i, ch := range tagline {
		if i == 0 {
			fmt.Print(colour(boldYellow, string(ch)))
		} else if i < 4 {
			fmt.Print(colour(boldYellow, string(ch)))
		} else {
			fmt.Print(colour(boldWhite, string(ch)))
		}
		time.Sleep(12 * time.Millisecond)
	}
	fmt.Println(reset)
	time.Sleep(40 * time.Millisecond)

	// ── Phase 5: subsystem boot sequence ─────────────────────────────────────
	fmt.Println(colour(dim, `  ────────────────────────────────────────`))
	for _, s := range subsystems {
		fmt.Printf("  %s  %s ", s.icon, colour(dim, s.label))
		time.Sleep(30 * time.Millisecond)
		// Animate dots
		for d := 0; d < 3; d++ {
			fmt.Print(colour(blue, "·"))
			time.Sleep(25 * time.Millisecond)
		}
		fmt.Println("  " + colour(boldGreen, "ready"))
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Println(colour(dim, `  ────────────────────────────────────────`))
	fmt.Println()
}

// printInitBanner is shown after `zeus init` and on first run.
func printInitBanner(token, configPath string) {
	fmt.Print(hideCursor)
	defer fmt.Print(showCursor)

	fmt.Println()

	// Mini logo scan-in (faster than the full startup)
	for i, line := range logoLines {
		fmt.Println(colour(logoColours[i], line))
		time.Sleep(25 * time.Millisecond)
	}
	fmt.Println()

	// Flash the "initialised" header
	header := "  ⚡ Zeus initialised successfully!"
	for _, ch := range header {
		fmt.Print(colour(boldYellow, string(ch)))
		time.Sleep(14 * time.Millisecond)
	}
	fmt.Println()
	fmt.Println(colour(boldBlue, `  ════════════════════════════════════`))
	fmt.Println()
	time.Sleep(60 * time.Millisecond)

	fmt.Printf("  %s  %s\n", colour(dim, "config →"), colour(white, configPath))
	fmt.Println()

	// Token box — border draws first, then token types in
	fmt.Println(colour(blue, "  ┌─ Auth Token ─────────────────────────────────────"))
	fmt.Print(colour(blue, "  │  "))
	for _, ch := range token {
		fmt.Print(colour(boldYellow, string(ch)))
		time.Sleep(8 * time.Millisecond)
	}
	fmt.Println()
	fmt.Println(colour(blue, "  │"))
	fmt.Println(colour(blue, "  │  ") + colour(dim, "copy this token to all your clients"))
	fmt.Println(colour(blue, "  └──────────────────────────────────────────────────"))
	fmt.Println()

	// Next steps
	steps := []struct{ num, cmd, hint string }{
		{"1", "edit zeus.yaml", "enable persistence, TLS, webhooks, chat"},
		{"2", "zeus          ", "start the server"},
		{"3", "connect       ", "use the token above in your SDK"},
	}
	fmt.Println(colour(cyan, "  Next steps:"))
	for _, s := range steps {
		fmt.Printf("  %s  %s  %s\n",
			colour(boldWhite, s.num+"."),
			colour(boldWhite, s.cmd),
			colour(dim, "— "+s.hint),
		)
		time.Sleep(40 * time.Millisecond)
	}
	fmt.Println()
}

// ── ConfigPath ────────────────────────────────────────────────────────────────

// ConfigPath is the default location Zeus looks for its config file.
// Can be overridden with the ZEUS_CONFIG env var.
var ConfigPath = "zeus.yaml"

// ── Run ───────────────────────────────────────────────────────────────────────

// Run is the CLI entry point. Call this from main().
// Returns the process exit code (0 = success).
func Run(args []string) int {
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
		fmt.Println(colour(dim, "  The server starts automatically when no subcommand is given."))
		fmt.Println(colour(dim, "  Run: zeus"))
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
		fmt.Fprintf(os.Stderr, colour(red, "  Unknown command: %s\n\n"), args[0])
		printUsage()
		return 1
	}
}

// ── PrintStartupBanner ────────────────────────────────────────────────────────
//
// PrintStartupBanner is called by main() just before the server starts listening.
// It shows the full animated logo then the listen address + token.

func PrintStartupBanner(addr, token string, secEnabled, tlsEnabled bool) {
	printBanner()

	// ── Connection info panel ─────────────────────────────────────────────────
	fmt.Println(colour(boldBlue, "  ╔══════════════════════════════════════╗"))
	fmt.Printf(colour(boldBlue, "  ║")+"  %s  %s  %s\n",
		colour(boldGreen, "●"),
		colour(boldWhite, "ZEUS IS RUNNING"),
		colour(boldBlue, "                       ║"),
	)
	fmt.Println(colour(boldBlue, "  ╠══════════════════════════════════════╣"))
	fmt.Printf(colour(boldBlue, "  ║")+"  %s  %s%s\n",
		colour(dim, "addr  "),
		colour(boldGreen, addr),
		colour(boldBlue, padRight("", 40-6-len(addr))+"║"),
	)
	if secEnabled {
		short := token
		if len(short) > 24 {
			short = short[:12] + "…" + short[len(short)-8:]
		}
		fmt.Printf(colour(boldBlue, "  ║")+"  %s  %s%s\n",
			colour(dim, "token "),
			colour(boldYellow, short),
			colour(boldBlue, padRight("", 40-6-len(short))+"║"),
		)
	} else {
		fmt.Printf(colour(boldBlue, "  ║")+"  %s\n",
			colour(yellow, "⚠  auth disabled                        ║"),
		)
	}
	if tlsEnabled {
		fmt.Printf(colour(boldBlue, "  ║")+"  %s\n",
			colour(cyan, "🔒 tls                                  ║"),
		)
	}
	fmt.Println(colour(boldBlue, "  ╚══════════════════════════════════════╝"))
	fmt.Println()
	fmt.Printf("  %s %s%s\n",
		colour(dim, "logs below   ·   stop with"),
		colour(white, " Ctrl+C"),
		colour(dim, ""),
	)
	fmt.Println()
}

// padRight pads s with spaces to reach length n (used for box alignment).
func padRight(s string, n int) string {
	if n <= 0 {
		return s
	}
	for len(s) < n {
		s += " "
	}
	return s
}

// ── zeus init ─────────────────────────────────────────────────────────────────

func cmdInit() int {
	if _, err := os.Stat(ConfigPath); err == nil {
		fmt.Println()
		fmt.Printf("  %s already exists at %s\n",
			colour(boldCyan, "zeus.yaml"),
			colour(white, ConfigPath),
		)
		fmt.Printf("  Run %s to see your auth token.\n", colour(cyan, "'zeus token'"))
		fmt.Printf("  Run %s to generate a new one.\n", colour(cyan, "'zeus token rotate'"))
		fmt.Println()
		return 0
	}

	cfg, _, err := config.Load(ConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, colour(red, "  Error: %v\n"), err)
		return 1
	}

	printInitBanner(cfg.Security.Token, ConfigPath)
	return 0
}

// ── zeus token ────────────────────────────────────────────────────────────────

func cmdToken() int {
	cfg, err := loadConfig()
	if err != nil {
		return 1
	}
	if !cfg.Security.Enabled {
		fmt.Println()
		fmt.Println(colour(yellow, "  Security is disabled in zeus.yaml (security.enabled: false)"))
		fmt.Println(colour(dim, "  No token required — any client can connect."))
		fmt.Println()
		return 0
	}
	fmt.Println()
	fmt.Println(colour(blue, "  ┌─ Current auth token ──────────────────────────────"))
	fmt.Printf(colour(blue, "  │ ")+"  %s\n", colour(boldYellow, cfg.Security.Token))
	fmt.Println(colour(blue, "  └──────────────────────────────────────────────────"))
	fmt.Println()
	return 0
}

func cmdTokenRotate() int {
	cfg, err := loadConfig()
	if err != nil {
		return 1
	}

	newToken, genErr := generateSecret()
	if genErr != nil {
		fmt.Fprintf(os.Stderr, colour(red, "  Error generating token: %v\n"), genErr)
		return 1
	}

	oldToken := cfg.Security.Token
	cfg.Security.Token = newToken

	if err = config.Save(cfg, ConfigPath); err != nil {
		fmt.Fprintf(os.Stderr, colour(red, "  Error saving config: %v\n"), err)
		return 1
	}

	fmt.Println()
	fmt.Println(colour(blue, "  ┌─ Token rotated ──────────────────────────────────"))
	fmt.Printf(colour(blue, "  │ ")+"  %s %s\n", colour(dim, "old:"), colour(gray, oldToken))
	fmt.Printf(colour(blue, "  │ ")+"  %s %s\n", colour(dim, "new:"), colour(boldYellow, newToken))
	fmt.Println(colour(blue, "  └──────────────────────────────────────────────────"))
	fmt.Println()
	fmt.Println(colour(yellow, "  ⚠  Update all your clients — old token is invalid"))
	fmt.Println(colour(yellow, "  ⚠  Restart Zeus for the new token to take effect"))
	fmt.Println()
	return 0
}

// ── zeus status ───────────────────────────────────────────────────────────────

func cmdStatus() int {
	cfg, err := loadConfig()
	if err != nil {
		return 1
	}

	fmt.Println()
	fmt.Println(colour(boldCyan, "  ┌─ Zeus Server Configuration ──────────────────────"))
	row := func(label, value string) {
		fmt.Printf(colour(boldCyan, "  │ ")+"  %-18s %s\n",
			colour(dim, label), value)
	}
	row("Listen address", colour(boldWhite, cfg.Addr()))
	row("Security", statusVal(cfg.Security.Enabled))
	row("TLS", statusVal(cfg.Security.TLS.Enabled))
	if cfg.Persistence.Enabled {
		row("Persistence", colour(boldGreen, "enabled")+" "+colour(dim, "→ "+cfg.Persistence.DBPath))
	} else {
		row("Persistence", colour(gray, "disabled"))
	}
	row("Channels", fmt.Sprintf("%s  %s",
		statusVal(cfg.Channels.Enabled),
		colour(dim, fmt.Sprintf("(max %d, history %d)", cfg.Channels.MaxChannels, cfg.Channels.HistorySize)),
	))
	row("Queues", fmt.Sprintf("%s  %s",
		statusVal(cfg.Queues.Enabled),
		colour(dim, fmt.Sprintf("(max %d, depth %d)", cfg.Queues.MaxQueues, cfg.Queues.MaxQueueDepth)),
	))
	row("Chat", fmt.Sprintf("%s  %s",
		statusVal(cfg.Chat.Enabled),
		colour(dim, fmt.Sprintf("(max %d rooms, history %d)", cfg.Chat.MaxRooms, cfg.Chat.HistorySize)),
	))
	if cfg.Webhook.Enabled {
		row("Webhooks", colour(boldGreen, "enabled")+" "+colour(dim, "→ "+cfg.Webhook.URL))
	} else {
		row("Webhooks", colour(gray, "disabled"))
	}
	row("RPC", colour(boldGreen, "enabled")+" "+colour(dim, "(always on)"))
	fmt.Println(colour(boldCyan, "  └──────────────────────────────────────────────────"))
	fmt.Println()
	return 0
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func loadConfig() (*config.Config, error) {
	cfg, _, err := config.Load(ConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, colour(red, "  Error loading %s: %v\n"), ConfigPath, err)
		fmt.Fprint(os.Stderr, colour(dim, "  Run 'zeus init' to create a default config.\n"))
	}
	return cfg, err
}

func statusVal(b bool) string {
	if b {
		return colour(boldGreen, "enabled ")
	}
	return colour(gray, "disabled")
}

// generateSecret creates a 32-byte cryptographically random hex token.
func generateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return fmt.Sprintf("%x", b), nil
}

// printUsage renders a coloured help screen.
func printUsage() {
	fmt.Println()
	fmt.Printf("  %s  %s\n",
		colour(boldCyan, "⚡ Zeus"),
		colour(dim, "v1.0.0  •  binary-protocol realtime server"),
	)
	fmt.Println()
	fmt.Println(colour(blue, "  ┌─ Usage ───────────────────────────────────────────"))
	fmt.Println(colour(blue, "  │"))
	fmt.Printf(colour(blue, "  │ ")+"  %s %s\n",
		colour(boldWhite, "zeus"),
		colour(dim, "                 Start the server (reads zeus.yaml)"),
	)
	fmt.Printf(colour(blue, "  │ ")+"  %s %s\n",
		colour(boldWhite, "zeus init"),
		colour(dim, "             Create zeus.yaml with a fresh token"),
	)
	fmt.Printf(colour(blue, "  │ ")+"  %s %s\n",
		colour(boldWhite, "zeus token"),
		colour(dim, "            Print your auth token"),
	)
	fmt.Printf(colour(blue, "  │ ")+"  %s %s\n",
		colour(boldWhite, "zeus token rotate"),
		colour(dim, "     Generate a new auth token"),
	)
	fmt.Printf(colour(blue, "  │ ")+"  %s %s\n",
		colour(boldWhite, "zeus status"),
		colour(dim, "           Show server configuration"),
	)
	fmt.Printf(colour(blue, "  │ ")+"  %s %s\n",
		colour(boldWhite, "zeus help"),
		colour(dim, "             Show this help"),
	)
	fmt.Println(colour(blue, "  │"))
	fmt.Println(colour(blue, "  ├─ Environment ─────────────────────────────────────"))
	fmt.Println(colour(blue, "  │"))
	fmt.Printf(colour(blue, "  │ ")+"  %s   %s\n",
		colour(white, "ZEUS_CONFIG"),
		colour(dim, "Path to config file (default: zeus.yaml)"),
	)
	fmt.Println(colour(blue, "  │"))
	fmt.Println(colour(blue, "  └──────────────────────────────────────────────────"))
	fmt.Println()
}
