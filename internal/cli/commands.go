package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"casengine/internal/codes"
	"casengine/internal/engine"
	"casengine/internal/sample"
	"casengine/internal/server"
)

// Run is the CLI entry point.
func Run(args []string) int {
	if len(args) == 0 {
		usage()
		return 2
	}
	cmd, rest := args[0], args[1:]
	workDir := os.Getenv("CASENGINE_WORKDIR")
	positional := make([]string, 0, len(rest))
	for i := 0; i < len(rest); i++ {
		switch {
		case rest[i] == "-workdir" || rest[i] == "--workdir":
			if i+1 >= len(rest) {
				fmt.Fprintln(os.Stderr, "-workdir requires a value")
				return 2
			}
			workDir = rest[i+1]
			i++
		case strings.HasPrefix(rest[i], "-workdir="):
			workDir = strings.TrimPrefix(rest[i], "-workdir=")
		default:
			positional = append(positional, rest[i])
		}
	}
	app, err := NewApp(workDir)
	must2(err)

	switch cmd {
	case "init":
		return cmdInit(app)
	case "demo":
		return cmdDemo(app)
	case "replay":
		return cmdReplay(app, positional)
	case "inject":
		return cmdInject(app)
	case "snapshot-gap":
		return cmdSnapshotGap(app)
	case "status":
		return cmdStatus(app, positional)
	case "stats":
		return cmdStats(app)
	case "serve":
		return cmdServe(app, positional)
	case "crash-recovery":
		return cmdCrashRecovery(app)
	case "help", "-h", "--help":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", cmd)
		usage()
		return 2
	}
}

func must2(err error) { must(err) }

func usage() {
	fmt.Fprint(os.Stderr, `casengine - CAS one-time ticket validation engine

Usage:
  casengine <subcommand> [-workdir DIR]

Subcommands:
  init             generate deterministic sample parameters (tenants/SPs/users/keys/scripts)
  demo             full deterministic pipeline; emits repeatable JSON report
  replay <name>    replay a built-in scenario (happy-path|concurrent-one-time|failure-mix)
  inject           run the five fault-injection cases with assertions
  snapshot-gap     inject a snapshot version gap, detect E_SNAPSHOT_VERSION, roll back
  status [tenant]  show tickets, redemptions and snapshot chain of a tenant
  stats            show latency aggregates for the latest demo report
  serve            start the HTTP validation service (real monotonic clock)
  crash-recovery   child/parent forced-kill recovery demonstration
  help             show this help
`)
}

func cmdInit(app *App) int {
	fmt.Printf("workdir : %s\n", app.WorkDir)
	fmt.Printf("params  : %s\n", app.paramsPath())
	fmt.Printf("db      : %s\n", app.DBPath)
	fmt.Printf("tenants : %d\n", len(app.Params.Tenants))
	for _, t := range app.Params.Tenants {
		fmt.Printf("  - %s (sps=%d users=%d key=%s)\n", t.ID, len(t.SPs), len(t.Users), t.Secret)
	}
	fmt.Printf("scenarios: %d\n", len(app.Params.Scenarios))
	for _, s := range app.Params.Scenarios {
		fmt.Printf("  - %-22s %s\n", s.Name, s.Description)
	}
	return 0
}

func cmdDemo(app *App) int {
	resetWorkdir(app)
	rt, sc, err := app.newRuntime(true, deterministicConfig())
	must2(err)
	defer rt.Close()

	var all []engine.Result
	baseIdx := 0
	var seqCur int64 = 1
	runner := engine.NewRunner(rt.Engine, sc, app.Params)
	for i := range app.Params.Scenarios {
		var results []engine.Result
		results, seqCur = runScenario(rt, runner, sc, app.Params.Scenarios[i], &baseIdx, seqCur)
		all = append(all, results...)
	}
	faultResults, checks := runFaultSuite(rt, runner, baseIdx, seqCur)
	all = append(all, faultResults...)

	// Deterministic expiry cleanup demonstration (virtual clock, no sleeping).
	sc.Advance(10 * time.Minute)
	rep, err := rt.Engine.CleanupOnce()
	must2(err)
	if len(rep.Expired) > 0 {
		fmt.Printf("cleanup expired %d ticket(s)\n", len(rep.Expired))
	}

	chain := chainReportsFor(rt)
	doc := buildReport(rt, all, chain)
	path, err := writeReport(rt, "demo-report.json", doc)
	must2(err)

	faultOnlyPass := true
	for _, c := range checks {
		mark := "PASS"
		if !c.Pass {
			mark = "FAIL"
			faultOnlyPass = false
		}
		fmt.Printf("[%s] injection %-20s want=%-18s got=%s %s\n",
			mark, c.Name, c.WantCode, c.GotCode, c.Detail)
	}
	fmt.Printf("report written: %s\n", path)
	if !faultOnlyPass {
		return 1
	}
	return 0
}

func cmdReplay(app *App, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "replay requires a scenario name")
		return 2
	}
	resetWorkdir(app)
	rt, sc, err := app.newRuntime(true, deterministicConfig())
	must2(err)
	defer rt.Close()

	var scn *sample.Scenario
	for i := range app.Params.Scenarios {
		if app.Params.Scenarios[i].Name == args[0] {
			scn = &app.Params.Scenarios[i]
		}
	}
	if scn == nil {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", args[0])
		return 2
	}
	baseIdx := 0
	runner := engine.NewRunner(rt.Engine, sc, app.Params)
	results, _ := runScenario(rt, runner, sc, *scn, &baseIdx, 1)
	doc := buildReport(rt, results, chainReportsFor(rt))
	path, err := writeReport(rt, "replay-"+args[0]+".json", doc)
	must2(err)
	for _, r := range results {
		fmt.Printf("seq=%-2d ticket=%s code=%s latencyNs=%d\n", r.Seq, r.TicketID, r.Code, r.LatencyNS)
	}
	fmt.Printf("report written: %s\n", path)
	return 0
}

func cmdInject(app *App) int {
	resetWorkdir(app)
	rt, sc, err := app.newRuntime(true, deterministicConfig())
	must2(err)
	defer rt.Close()
	injRunner := engine.NewRunner(rt.Engine, sc, app.Params)
	results, checks := runFaultSuite(rt, injRunner, 0, 1)
	doc := buildReport(rt, results, chainReportsFor(rt))
	path, err := writeReport(rt, "inject-report.json", doc)
	must2(err)
	failed := 0
	for _, c := range checks {
		mark := "PASS"
		if !c.Pass {
			mark = "FAIL"
			failed++
		}
		fmt.Printf("[%s] %-20s want=%-10s got=%s %s\n", mark, c.Name, c.WantCode, c.GotCode, c.Detail)
	}
	fmt.Printf("report written: %s\n", path)
	if failed > 0 {
		return 1
	}
	return 0
}

func cmdSnapshotGap(app *App) int {
	resetWorkdir(app)
	rt, sc, err := app.newRuntime(true, deterministicConfig())
	must2(err)
	defer rt.Close()

	// Build some history: issue + redeem two tickets -> several versions.
	scn := app.Params.Scenarios[1]
	baseIdx := 0
	gapRunner := engine.NewRunner(rt.Engine, sc, app.Params)
	runScenario(rt, gapRunner, sc, scn, &baseIdx, 1)
	tenantID := app.Params.Tenants[0].ID

	before, err := rt.Engine.VerifyTenantChain(tenantID)
	must2(err)
	fmt.Printf("before gap: contiguous=%v headVersion=%d\n", before.Contiguous, before.HeadVersion)

	// Input-driven corruption: remove the highest snapshot to create a gap.
	gapped, err := rt.Store.InjectGap(tenantID)
	must2(err)
	fmt.Printf("injected gap by deleting version %d\n", gapped)

	after, err := rt.Engine.VerifyTenantChain(tenantID)
	must2(err)
	fmt.Printf("after gap : contiguous=%v gapAt=%d fault=%v\n",
		after.Contiguous, after.GapAt, rt.Engine.TenantFault(tenantID) != nil)

	// While faulted, validation of that tenant must be refused with the code.
	probe := mustIssueProbe(rt, tenantID)
	res := rt.Engine.Validate(context.Background(), engine.ValidateRequest{
		Seq: 999, Ticket: probe.Ticket, PresentSP: probe.Record.SPID,
	})
	fmt.Printf("validation while faulted -> %s (want %s)\n", res.Code, codes.SnapshotVersion)

	// Roll back to the last contiguous version and confirm service resumes.
	rec, err := rt.Engine.RollbackTenant(tenantID)
	must2(err)
	fmt.Printf("rollback  : recovered=%v contiguous=%v headVersion=%d\n",
		rec.Recovered, rec.Contiguous, rec.HeadVersion)
	res2 := rt.Engine.Validate(context.Background(), engine.ValidateRequest{
		Seq: 1000, Ticket: probe.Ticket, PresentSP: probe.Record.SPID,
	})
	fmt.Printf("validation after rollback -> %s (want %s)\n", res2.Code, codes.OK)

	doc := buildReport(rt, nil, chainReportsFor(rt))
	path, err := writeReport(rt, "snapshot-gap-report.json", doc)
	must2(err)
	fmt.Printf("report written: %s\n", path)
	if res.Code != codes.SnapshotVersion || res2.Code != codes.OK || !rec.Contiguous {
		return 1
	}
	return 0
}

func cmdStatus(app *App, args []string) int {
	rt, _, err := app.newRuntime(true, deterministicConfig())
	must2(err)
	defer rt.Close()
	tenantID := ""
	if len(args) > 0 {
		tenantID = args[0]
	}
	if tenantID == "" {
		for _, t := range app.Params.Tenants {
			printTenantStatus(rt, t.ID)
		}
		return 0
	}
	printTenantStatus(rt, tenantID)
	return 0
}

func printTenantStatus(rt *Runtime, tenantID string) {
	tickets, err := rt.Store.ListTickets(tenantID)
	must2(err)
	reds, err := rt.Store.ListRedemptions(tenantID)
	must2(err)
	snaps, err := rt.Store.ListSnapshots(tenantID)
	must2(err)
	chain, err := rt.Engine.VerifyTenantChain(tenantID)
	must2(err)
	fmt.Printf("tenant=%s chainContiguous=%v headVersion=%d headMatchesLive=%v\n",
		tenantID, chain.Contiguous, chain.HeadVersion, chain.HeadMatches)
	fmt.Printf("tickets(%d):\n", len(tickets))
	for _, t := range tickets {
		fmt.Printf("  %s sp=%s status=%-9s exp=%d\n", t.ID, t.SPID, t.Status, t.ExpiresAt)
	}
	fmt.Printf("redemptions(%d):\n", len(reds))
	for _, r := range reds {
		fmt.Printf("  ticket=%s seq=%d user=%s version=%d\n", r.TicketID, r.Seq, r.UserID, r.Version)
	}
	fmt.Printf("snapshots(%d):\n", len(snaps))
	for _, s := range snaps {
		fmt.Printf("  v=%-3d changed=%-5v reason=%-8s digest=%s\n", s.Version, s.Changed, s.Reason, s.Digest[:12])
	}
}

func cmdStats(app *App) int {
	path := filepath.Join(app.WorkDir, "reports", "demo-report.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "no demo report at %s (run 'demo' first): %v\n", path, err)
		return 1
	}
	var doc map[string]any
	must2(json.Unmarshal(raw, &doc))
	stats, _ := json.MarshalIndent(doc["latencyStats"], "", "  ")
	fmt.Println(string(stats))
	dist, _ := json.MarshalIndent(doc["resultDistribution"], "", "  ")
	fmt.Printf("resultDistribution: %s\n", string(dist))
	return 0
}

func cmdServe(app *App, args []string) int {
	addr := ":8080"
	if len(args) > 0 {
		addr = args[0]
	}
	cfg := engine.DefaultConfig()
	rt, _, err := app.newRuntime(false, cfg)
	must2(err)
	defer rt.Close()
	handler := server.New(rt.Engine)
	fmt.Printf("casengine listening on %s (POST /validate, GET /healthz)\n", addr)
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return 1
	}
	return 0
}

func mustIssueProbe(rt *Runtime, tenantID string) *engine.IssuedTicket {
	var tenant sample.Tenant
	for _, t := range rt.App.Params.Tenants {
		if t.ID == tenantID {
			tenant = t
		}
	}
	issued, err := rt.Engine.Issue(engine.IssueRequest{
		ID:         "ST-PROBE-0001-" + tenantID,
		TenantID:   tenant.ID,
		SPID:       tenant.SPs[0].ID,
		UserID:     tenant.Users[0].ID,
		TTLSeconds: 300,
	})
	must2(err)
	return issued
}
