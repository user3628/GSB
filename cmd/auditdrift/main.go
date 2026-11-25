package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"auditdrift/internal/app"
	"auditdrift/internal/model"
	"auditdrift/internal/probe"
	"auditdrift/internal/sample"
	"auditdrift/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "seed":
		err = seedCmd(os.Args[2:])
	case "init":
		err = initCmd(os.Args[2:])
	case "audit":
		err = auditCmd(os.Args[2:])
	case "report":
		err = reportCmd(os.Args[2:])
	case "increment-demo":
		err = incrementDemoCmd(os.Args[2:])
	case "stale-base-demo":
		err = staleBaseDemoCmd(os.Args[2:])
	case "crash-demo":
		err = crashDemoCmd(os.Args[2:])
	case "recover":
		err = recoverCmd(os.Args[2:])
	case "endpoint":
		err = endpointCmd(os.Args[2:])
	case "version":
		fmt.Println("auditdrift dev")
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		printError(err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: auditdrift <seed|init|audit|report|increment-demo|stale-base-demo|crash-demo|recover|endpoint> [flags]`)
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printError(err error) {
	code := model.CodeOf(err)
	payload := map[string]any{"ok": false, "error": err.Error()}
	if code != "" {
		payload["code"] = code
	}
	var coded *model.CodedError
	if errors.As(err, &coded) && coded.Details != nil {
		payload["details"] = coded.Details
	}
	data, _ := json.MarshalIndent(payload, "", "  ")
	fmt.Fprintln(os.Stderr, string(data))
}

func seedCmd(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	dir := fs.String("dir", "tmp/seed", "seed output directory")
	seed := fs.Int64("seed", sample.DefaultSeed, "fixed generator seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	set := sample.Generate(*seed)
	if err := app.WriteSeedDirectory(*dir, set); err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "dir": *dir, "baselines": len(set.Baselines), "observations": len(set.Observations)})
}

func initCmd(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	db := fs.String("db", "tmp/audit.db", "sqlite database path")
	session := fs.String("session", app.DefaultSession, "session id")
	quota := fs.Int("quota", 100, "weighted repair action quota")
	if err := fs.Parse(args); err != nil {
		return err
	}
	result, err := app.InitializeDatabase(*db, *session, *quota)
	if err != nil {
		return err
	}
	return printJSON(result)
}

func auditCmd(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	db := fs.String("db", "tmp/audit.db", "sqlite database path")
	session := fs.String("session", app.DefaultSession, "session id")
	concurrency := fs.Int("concurrency", 4, "global probe/audit concurrency")
	queueSize := fs.Int("queue-size", 64, "bounded waiting queue size")
	budget := fs.Duration("budget", 2*time.Second, "per-round time budget")
	probeTimeout := fs.Duration("probe-timeout", 250*time.Millisecond, "independent probe timeout")
	probeBase := fs.String("probe-base", "", "external loopback probe base URL; empty starts deterministic endpoint")
	delayID := fs.String("delay-id", "", "fixture baseline id to delay")
	delay := fs.Duration("delay", 0, "fixture delay duration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := app.RunAudit(context.Background(), *db, app.AuditOptions{
		SessionID: *session, Concurrency: *concurrency, QueueSize: *queueSize, Budget: *budget,
		ProbeTimeout: *probeTimeout, ProbeBaseURL: *probeBase, UseInProcessEndpoint: *probeBase == "",
		EndpointDelayID: *delayID, EndpointDelay: *delay,
	})
	if err != nil {
		return err
	}
	return printJSON(report)
}

func reportCmd(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	db := fs.String("db", "tmp/audit.db", "sqlite database path")
	session := fs.String("session", app.DefaultSession, "session id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*db)
	if err != nil {
		return err
	}
	defer st.Close()
	sess, err := st.GetSession(*session)
	if err != nil {
		return err
	}
	summaries, err := st.ListHostSummaries(*session)
	if err != nil {
		return err
	}
	conclusions, err := st.ListConclusions(*session, "")
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"session": sess, "host_summaries": summaries, "conclusions": conclusions})
}

func incrementDemoCmd(args []string) error {
	fs := flag.NewFlagSet("increment-demo", flag.ContinueOnError)
	db := fs.String("db", "tmp/audit.db", "sqlite database path")
	session := fs.String("session", app.DefaultSession, "session id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*db)
	if err != nil {
		return err
	}
	defer st.Close()
	sess, err := st.GetSession(*session)
	if err != nil {
		return err
	}
	if sess == nil {
		return fmt.Errorf("session %s not found", *session)
	}
	set := sample.FixedSet()
	var changed model.Baseline
	for _, b := range set.Baselines {
		if b.ID == "ACCT_LOCKOUT_THRESHOLD" {
			changed = b
		}
	}
	changed.SeverityWeight = 10
	result, err := st.CommitIncrement(store.Increment{
		SessionID: *session, BaseVersion: sess.CurrentVersion, Reason: "acceptance: increase lockout baseline weight to 10",
		BaselineChanges: []store.BaselineChange{{Op: "upsert", Baseline: &changed}},
	})
	if err != nil {
		return err
	}
	return printJSON(result)
}

func staleBaseDemoCmd(args []string) error {
	fs := flag.NewFlagSet("stale-base-demo", flag.ContinueOnError)
	db := fs.String("db", "tmp/audit.db", "sqlite database path")
	session := fs.String("session", app.DefaultSession, "session id")
	base := fs.Int64("base", 1, "stale base version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*db)
	if err != nil {
		return err
	}
	defer st.Close()
	set := sample.FixedSet()
	b := set.Baselines[0]
	b.SeverityWeight = 99
	_, err = st.CommitIncrement(store.Increment{
		SessionID: *session, BaseVersion: *base, Reason: "intentionally stale",
		BaselineChanges: []store.BaselineChange{{Op: "upsert", Baseline: &b}},
	})
	return err
}

func crashDemoCmd(args []string) error {
	fs := flag.NewFlagSet("crash-demo", flag.ContinueOnError)
	db := fs.String("db", "tmp/audit.db", "sqlite database path")
	session := fs.String("session", app.DefaultSession, "session id")
	wait := fs.Duration("wait", 60*time.Second, "maximum time to remain in crash window")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*db)
	if err != nil {
		return err
	}
	window, err := st.BeginCrashWindow(*session)
	if err != nil {
		return err
	}
	fmt.Printf(`{"event":"crash_window_ready","intent_id":%q,"pid":%d}`+"\n", window.IntentID, os.Getpid())
	_ = os.Stdout.Sync()
	time.Sleep(*wait)
	_ = window.Rollback()
	_ = st.Close()
	return nil
}

func recoverCmd(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	db := fs.String("db", "tmp/audit.db", "sqlite database path")
	session := fs.String("session", "", "optional session id; empty recovers all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*db)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := st.Recover(*session)
	if err != nil {
		return err
	}
	return printJSON(report)
}

func endpointCmd(args []string) error {
	fs := flag.NewFlagSet("endpoint", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:18080", "loopback listen address")
	delayID := fs.String("delay-id", "", "baseline id to make unresponsive")
	delay := fs.Duration("delay", 5*time.Second, "delay for injected baseline")
	if err := fs.Parse(args); err != nil {
		return err
	}
	endpoint, err := probe.NewListeningEndpoint(*addr)
	if err != nil {
		return err
	}
	defer endpoint.Close()
	set := sample.FixedSet()
	for _, obs := range set.Primary {
		endpoint.SetObservation(obs)
	}
	if *delayID != "" {
		endpoint.SetDelay(*delayID, *delay)
	}
	fmt.Printf(`{"event":"endpoint_ready","url":%q}`+"\n", endpoint.URL())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
	return nil
}
