package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"example.com/gsb/dnscache/internal/admin"
	"example.com/gsb/dnscache/internal/errs"
	"example.com/gsb/dnscache/internal/resolver"
	"example.com/gsb/dnscache/internal/scheduler"
	"example.com/gsb/dnscache/internal/store"
)

func main() {
	var (
		udpAddr       = flag.String("udp", "127.0.0.1:15353", "UDP DNS listen address (empty disables)")
		tcpAddr       = flag.String("tcp", "127.0.0.1:15353", "TCP DNS listen address (empty disables)")
		adminAddr     = flag.String("admin", "127.0.0.1:18080", "management HTTP address (empty disables)")
		dbPath        = flag.String("db", "./data/dnscache.db", "SQLite database path")
		upstreams     = flag.String("upstream", "127.0.0.1:15354", "comma-separated local authoritative upstream addresses")
		upstreamNet   = flag.String("upstream-network", "udp", "upstream transport: udp or tcp")
		globalLimit   = flag.Int("global-upstream-limit", 8, "maximum concurrent upstream exchanges")
		perAuthLimit  = flag.Int("per-auth-limit", 2, "maximum concurrent exchanges per authoritative server")
		queueLimit    = flag.Int("upstream-queue-limit", 64, "bounded waiting queue length")
		roundBudget   = flag.Duration("round-budget", 2*time.Second, "per-query scheduling round budget")
		upstreamTO    = flag.Duration("upstream-timeout", 500*time.Millisecond, "per-hop upstream timeout")
		detachAfter   = flag.Duration("detach-after", 250*time.Millisecond, "long-tail duplicate detach threshold")
		queryTimeout  = flag.Duration("query-timeout", 3*time.Second, "listener query timeout")
		tcpIdle       = flag.Duration("tcp-idle-timeout", 30*time.Second, "idle TCP connection timeout")
		snapshotFile  = flag.String("snapshot", "", "optional zone snapshot JSON submitted at startup")
		cleanupPeriod = flag.Duration("cleanup-interval", 30*time.Second, "expired cache cleanup interval")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "dnscache ", log.LstdFlags|log.Lmicroseconds)
	up := splitNonEmpty(*upstreams)
	if len(up) == 0 {
		logger.Fatalf("at least one upstream is required")
	}
	st, err := store.Open(*dbPath, store.Options{})
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	defer st.Close()
	report := st.RecoveryReport()
	if report.PendingIntents > 0 || report.ZoneBatchesAborted > 0 || report.ZoneBatchesReplayed > 0 {
		b, _ := json.Marshal(report)
		logger.Printf("recovery report: %s", b)
	}

	if *snapshotFile != "" {
		snap, err := loadSnapshot(*snapshotFile)
		if err != nil {
			logger.Fatalf("load snapshot: %v", err)
		}
		batchID := fmt.Sprintf("startup-snapshot-%s-%d", snap.Zone, snap.Version)
		result, err := st.SubmitSnapshot(context.Background(), snap, batchID)
		if err != nil {
			if errs.IsCode(err, errs.EStaleBase) {
				logger.Printf("startup snapshot ignored because a newer/equal zone version is committed: %v", err)
			} else {
				logger.Fatalf("submit startup snapshot: %v", err)
			}
		} else {
			logger.Printf("startup snapshot committed: zone=%s version=%d caches_invalidated=%d", result.Zone, result.Version, result.InvalidatedCacheEntries)
		}
	}

	sched := scheduler.New(scheduler.Config{
		GlobalLimit: *globalLimit, PerServerLimit: *perAuthLimit, QueueLimit: *queueLimit,
		RoundBudget: *roundBudget, DetachAfter: *detachAfter,
	})
	metrics := resolver.NewMetrics()
	res := resolver.New(st, sched, resolver.Config{
		Upstreams: up, Network: *upstreamNet, UpstreamTimeout: *upstreamTO, MaxCNAMEHops: 8,
	}, metrics, logger)
	dnsServer := resolver.NewServer(res, resolver.ListenConfig{
		UDPAddr: *udpAddr, TCPAddr: *tcpAddr, QueryTimeout: *queryTimeout, TCPIdleTimeout: *tcpIdle,
	}, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := dnsServer.Start(ctx); err != nil {
		logger.Fatalf("start DNS listeners: %v", err)
	}
	var adminServer *admin.Server
	if *adminAddr != "" {
		adminServer = admin.New(st, sched, metrics, logger)
		if err := adminServer.Start(*adminAddr); err != nil {
			_ = dnsServer.Close()
			logger.Fatalf("start admin API: %v", err)
		}
	}

	cleanupCtx, cleanupCancel := context.WithCancel(ctx)
	defer cleanupCancel()
	if *cleanupPeriod > 0 {
		go func() {
			ticker := time.NewTicker(*cleanupPeriod)
			defer ticker.Stop()
			for {
				select {
				case <-cleanupCtx.Done():
					return
				case <-ticker.C:
					if n, err := st.CleanupExpired(cleanupCtx); err != nil {
						logger.Printf("expired cache cleanup failed: %v", err)
					} else if n > 0 {
						logger.Printf("expired cache cleanup removed %d entries", n)
					}
				}
			}
		}()
	}

	logger.Printf("ready: dns_udp=%s dns_tcp=%s admin=%s db=%s upstream=%s/%s", dnsServer.UDPAddr(), dnsServer.TCPAddr(), *adminAddr, *dbPath, strings.Join(up, ","), *upstreamNet)
	<-ctx.Done()
	logger.Printf("shutting down")
	_ = dnsServer.Close()
	if adminServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = adminServer.Shutdown(shutdownCtx)
		cancel()
	}
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func loadSnapshot(path string) (store.ZoneSnapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return store.ZoneSnapshot{}, err
	}
	var snap store.ZoneSnapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return store.ZoneSnapshot{}, err
	}
	return snap, nil
}
