// Command redis-glass is a RESP-protocol-compatible key-value store with
// built-in observability. It wires the store, command handler, TCP/TLS
// server, AOF persistence, and monitoring dashboard together and is
// configured entirely through environment variables (see README).
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"redis-glass/commands"
	"redis-glass/events"
	"redis-glass/monitor"
	"redis-glass/server"
	"redis-glass/store"
)

func main() {
	s := store.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.StartExpiry(ctx)

	maxMemoryBytes := parseMBEnv("MAXMEMORY_MB", 0)
	s.SetMaxMemory(maxMemoryBytes)

	password := os.Getenv("REDIS_PASSWORD")

	aofPath := os.Getenv("AOF_PATH")
	var aof *store.AOF
	if aofPath != "" {
		fsync := store.ParseFsyncPolicy(os.Getenv("AOF_FSYNC"))
		var err error
		aof, err = store.NewAOF(aofPath, fsync)
		if err != nil {
			log.Fatalf("failed to open AOF file: %v", err)
		}
		if err := aof.Replay(s); err != nil {
			log.Printf("aof: replay error: %v", err)
		}

		aof.SetRewriteThreshold(parseMBEnv("AOF_REWRITE_SIZE_MB", 64))
		go aof.StartFsyncLoop(ctx)
		go aof.StartAutoRewrite(ctx, s)
	}

	h := commands.New(s, password, aof)
	h.SetMaxMemory(maxMemoryBytes)

	slowlogThreshold := parseMsEnv("SLOWLOG_THRESHOLD_MS", 10)
	rec := events.NewRecorder(slowlogThreshold, 128, sensitiveKeyPatterns())
	h.SetEvents(rec)

	cfg := server.Config{
		Addr:        "0.0.0.0:6379",
		Handler:     h,
		RequireAuth: password != "",
		TLSCertFile: os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:  os.Getenv("TLS_KEY_FILE"),
	}
	srv, err := server.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	h.SetMetrics(srv)

	if dashboardPort := dashboardPortOrDefault(); dashboardPort != "" {
		getStats := func() monitor.Stats {
			aofDisplay := aofPath
			if aofDisplay == "" {
				aofDisplay = "(disabled)"
			}
			return monitor.Stats{
				Uptime:          srv.Uptime().Round(time.Second).String(),
				Conns:           srv.Connections(),
				Commands:        srv.Commands(),
				Keys:            s.KeyCount(),
				Expires:         s.ExpiryCount(),
				AOFPath:         aofDisplay,
				GoVersion:       runtime.Version(),
				MemoryUsedBytes: s.ApproxMemoryBytes(),
				MemoryMaxBytes:  maxMemoryBytes,
			}
		}
		go monitor.StartDashboard(dashboardPort, monitor.Sources{GetStats: getStats, Events: rec})
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		log.Fatal(err)
	case <-sigCtx.Done():
		log.Println("Shutting down...")
		srv.Close()
		if aof != nil {
			if err := aof.Sync(); err != nil {
				log.Printf("aof: sync error on shutdown: %v", err)
			}
		}
		log.Println("Shutdown complete")
		os.Exit(0)
	}
}

func dashboardPortOrDefault() string {
	if port, ok := os.LookupEnv("DASHBOARD_PORT"); ok {
		return port
	}
	return "8080"
}

// sensitiveKeyPatterns parses SENSITIVE_KEYS as a comma-separated list of
// glob patterns (e.g. "session:*,token:*"). Empty/unset means no redaction.
func sensitiveKeyPatterns() []string {
	v := os.Getenv("SENSITIVE_KEYS")
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseMBEnv reads an env var as a megabyte count and returns it in bytes.
// Returns defaultMB (also in bytes) if unset or invalid.
func parseMBEnv(name string, defaultMB int64) int64 {
	v := os.Getenv(name)
	if v == "" {
		return defaultMB * 1024 * 1024
	}
	mb, err := strconv.ParseInt(v, 10, 64)
	if err != nil || mb <= 0 {
		log.Printf("ignoring invalid %s=%q", name, v)
		return defaultMB * 1024 * 1024
	}
	return mb * 1024 * 1024
}

// parseMsEnv reads an env var as a millisecond count and returns it as a
// time.Duration. Returns defaultMs if unset or invalid.
func parseMsEnv(name string, defaultMs int64) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return time.Duration(defaultMs) * time.Millisecond
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms < 0 {
		log.Printf("ignoring invalid %s=%q", name, v)
		return time.Duration(defaultMs) * time.Millisecond
	}
	return time.Duration(ms) * time.Millisecond
}
