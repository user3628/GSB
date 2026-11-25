// Command alarmengine is the local, dependency-free driver for the alert
// noise-reduction and root-cause-correlation engine. All sample data is
// produced in-process by the fixed-seed generator under a local temp dir.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"alarmengine/internal/engine"
	"alarmengine/internal/errcode"
	"alarmengine/internal/httpapi"
	"alarmengine/internal/model"
	"alarmengine/internal/sample"
	"alarmengine/internal/storage"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "demo":
		err = runDemo(args)
	case "serve":
		err = runServe(args)
	case "send":
		err = runSend(args)
	case "version":
		fmt.Println("alarmengine demo driver")
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] %v\n", errcode.CodeOf(err), err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: alarmengine <command> [flags]

commands:
  demo   run the fixed-seed end-to-end acceptance scenario (offline)
  serve  start the loopback HTTP API on 127.0.0.1:<port>
  send   loopback sender simulator: POST a generated ingest batch
  version
`)
}

type commonFlags struct {
	dbPath     string
	crashPoint string
	crashID    string
	queueCap   int
}

func bindCommon(fs *flag.FlagSet) *commonFlags {
	c := &commonFlags{}
	fs.StringVar(&c.dbPath, "db", "", "sqlite path (default temp dir)")
	fs.StringVar(&c.crashPoint, "crash-point", "", "external crash injection point")
	fs.StringVar(&c.crashID, "crash-id", "", "limit crash to this intent id")
	fs.IntVar(&c.queueCap, "queue-cap", 64, "bounded patrol queue capacity")
	return c
}

func (c *commonFlags) openEngine() (*engine.Engine, func(), error) {
	path := c.dbPath
	if path == "" {
		dir, err := os.MkdirTemp("", "alarmengine-")
		if err != nil {
			return nil, nil, err
		}
		path = filepath.Join(dir, "engine.db")
	}
	store, err := storage.Open(path)
	if err != nil {
		return nil, nil, err
	}
	cfg := engine.Config{
		CrashPoint:          engine.CrashPoint(c.crashPoint),
		CrashIntentID:       c.crashID,
		PatrolQueueCapacity: c.queueCap,
		Now:                 deterministicClock,
	}
	eng, err := engine.New(store, cfg)
	if err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	return eng, func() { eng.Close(); _ = store.Close() }, nil
}

// deterministicClock keeps every run byte-identical.
var demoStart = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func deterministicClock() int64 {
	clockTick++
	return demoStart.Add(time.Duration(clockTick) * time.Millisecond).UnixMilli()
}

var clockTick int64

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	c := bindCommon(fs)
	limit := fs.Int("sample-limit", 20, "sampling path limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, cleanup, err := c.openEngine()
	if err != nil {
		return err
	}
	defer cleanup()

	data := sample.Generate()
	if err := eng.BootstrapServices(data.Services); err != nil {
		return err
	}
	if err := eng.BootstrapPolicy(data.Policy); err != nil {
		return err
	}
	res, err := eng.IngestObservations(engine.IngestRequest{
		RequestID: "req-seed-batch-1", BaseVersion: 1, Observations: data.Observations,
	})
	if err != nil {
		return err
	}
	fmt.Println("== ingest ==")
	printJSON(res)

	alerts, _ := eng.QueryAlerts(res.CommitVersion)
	fmt.Printf("== alerts (%d) ==\n", len(alerts))
	printJSON(summarizeAlerts(alerts))

	hits, _ := eng.QuerySuppressionHits(res.CommitVersion)
	fmt.Printf("== suppression hits (%d) ==\n", len(hits))
	printJSON(hits)

	rcs, _ := eng.QueryRootCauses(res.CommitVersion)
	fmt.Printf("== root causes (%d) ==\n", len(rcs))
	printJSON(rcs)

	full, err := eng.FullNoiseReduction(res.CommitVersion)
	if err != nil {
		return err
	}
	sampled, err := eng.SampledNoiseReduction(res.CommitVersion, *limit)
	if err != nil {
		return err
	}
	fmt.Println("== sampling report ==")
	printJSON(sampled.Sampling)
	diff := engine.ComparePaths(sampled, full)
	fmt.Println("== path diff (line-by-line cross-check) ==")
	printJSON(diff)

	trace, err := eng.PatrolRound(context.Background(), "demo-round-1")
	if err != nil {
		return err
	}
	// Demonstrate a monotonic, checksum-protected snapshot replacement with an
	// atomic cascade rebuild. This also makes CRASH_AFTER_INTENT_SNAPSHOT
	// reachable purely through the external --crash-point parameter.
	next := data.Policy
	next.Version = res.CommitVersion + 1
	next.RoundBudgetMS = 40
	next.Checksum = sample.ComputeChecksum(next)
	repl, err := eng.ReplaceSnapshot(engine.ReplaceSnapshotRequest{
		RequestID: "req-snapshot-2", BaseVersion: res.CommitVersion, Snapshot: next,
	})
	if err != nil {
		return err
	}
	fmt.Println("== snapshot replacement ==")
	printJSON(repl)

	fmt.Println("== patrol trace ==")
	printJSON(trace)
	fmt.Println("== recovery report ==")
	printJSON(eng.RecoveryReportOf())
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	c := bindCommon(fs)
	port := fs.Int("port", 18080, "loopback high port")
	bootstrap := fs.Bool("bootstrap", true, "load fixed-seed services and policy")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, cleanup, err := c.openEngine()
	if err != nil {
		return err
	}
	defer cleanup()
	if *bootstrap {
		data := sample.Generate()
		if err := eng.BootstrapServices(data.Services); err != nil {
			return err
		}
		if err := eng.BootstrapPolicy(data.Policy); err != nil {
			return err
		}
	}
	srv := httpapi.NewServer(eng, *port)
	if err := srv.Start(); err != nil {
		return err
	}
	fmt.Printf("listening on %s (db=%s)\n", srv.Addr(), eng.Store().Path())
	select {}
}

func runSend(args []string) error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	port := fs.Int("port", 18080, "target loopback port")
	reqID := fs.String("request-id", "req-sender-1", "idempotency key")
	base := fs.Int64("base-version", 1, "expected base version")
	count := fs.Int("count", 12, "observations to send")
	if err := fs.Parse(args); err != nil {
		return err
	}
	data := sample.Generate()
	if *count > len(data.Observations) {
		*count = len(data.Observations)
	}
	body := engine.IngestRequest{
		RequestID: *reqID, BaseVersion: *base,
		Observations: data.Observations[:*count],
	}
	raw, _ := json.Marshal(body)
	url := fmt.Sprintf("http://127.0.0.1:%d/ingest", *port)
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		return errcode.New(errcode.EBindFailed, "sender cannot reach %s: %v", url, err)
	}
	defer resp.Body.Close()
	buf, _ := io.ReadAll(resp.Body)
	fmt.Println(string(buf))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("sender got HTTP %d", resp.StatusCode)
	}
	return nil
}

func summarizeAlerts(alerts []model.Alert) []map[string]any {
	out := make([]map[string]any, 0, len(alerts))
	for _, a := range alerts {
		out = append(out, map[string]any{
			"id": a.ID, "state": a.State, "service": a.ServiceID,
			"metric": a.Metric, "fingerprint": a.Fingerprint,
			"first_seen_ms": a.FirstSeenMS, "window_end_ms": a.WindowEndMS,
			"merged_count": a.MergedCount, "suppressed_by": a.SuppressedBy,
			"primary_reason": a.PrimaryReason, "release_at_ms": a.ReleaseAtMS,
		})
	}
	return out
}

func printJSON(v any) {
	raw, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(raw))
}
