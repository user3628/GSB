package cli

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"runtime"

	"casengine/internal/codes"
	"casengine/internal/engine"
	"casengine/internal/store"
)

// CrashChildPrepare is the hidden phase-1 subcommand executed in a child process.
const CrashChildPrepare = "__crash_prepare"

// CrashChildKill is the hidden phase-2 subcommand executed in a child process.
const CrashChildKill = "__crash_kill"

func cmdCrashRecovery(app *App) int {
	resetWorkdir(app)
	workdir := app.WorkDir

	// Phase 1: child sets up committed state, then is killed before exit.
	if code := runChild(app, CrashChildPrepare); code != 0 {
		return code
	}

	// Phase 2: child opens an uncommitted transaction and is force-killed,
	// proving rollback-only durability for non-committed work.
	if code := runChild(app, CrashChildKill); code != 0 {
		return code
	}

	// Phase 3: parent reopens the database and asserts recovery invariants.
	rt, _, err := app.newRuntime(true, deterministicConfig())
	must2(err)
	defer rt.Close()

	// Verify snapshot integrity immediately on reopen, before this process
	// performs any new validation that would append further versions. The
	// runtime's startup chain check has already rolled back stale snapshots
	// left by the discarded, non-committed transaction.
	chainAtStart := map[string]engine.ChainReport{}
	for _, t := range app.Params.Tenants {
		chain, err := rt.Engine.VerifyTenantChain(t.ID)
		must2(err)
		chainAtStart[t.ID] = chain
	}

	committed, err := rt.Store.GetTicket("ST-CRASH-COMMITTED")
	must2(err)
	uncommitted, err := rt.Store.GetTicket("ST-CRASH-UNCOMMITTED")
	must2(err)

	fail := 0
	check := func(name string, ok bool, detail string) {
		mark := "PASS"
		if !ok {
			mark = "FAIL"
			fail++
		}
		fmt.Printf("[%s] %-38s %s\n", mark, name, detail)
	}
	check("committed ticket stays REDEEMED",
		committed != nil && committed.Status == store.StatusRedeemed,
		fmt.Sprintf("status=%s", safeStatus(committed)))
	check("uncommitted ticket stays VALID",
		uncommitted != nil && uncommitted.Status == store.StatusValid,
		fmt.Sprintf("status=%s", safeStatus(uncommitted)))

	// The uncommitted ticket must still be redeemable after recovery.
	res := rt.Engine.Validate(nilBg(), engine.ValidateRequest{
		Seq: 5001, Ticket: uncommitted.Raw, PresentSP: uncommitted.SPID,
	})
	check("uncommitted ticket redeems after restart", res.Code == codes.OK,
		fmt.Sprintf("code=%s", res.Code))

	// Re-redeeming the committed ticket must now report consumed.
	res2 := rt.Engine.Validate(nilBg(), engine.ValidateRequest{
		Seq: 5002, Ticket: committed.Raw, PresentSP: committed.SPID,
	})
	check("committed ticket cannot be redeemed twice", res2.Code == codes.Consumed,
		fmt.Sprintf("code=%s", res2.Code))

	// Snapshot chain must be contiguous after recovery.
	for _, t := range app.Params.Tenants {
		chain := chainAtStart[t.ID]
		check("snapshot chain contiguous for "+t.ID, chain.Contiguous && chain.HeadMatches,
			fmt.Sprintf("contiguous=%v headMatches=%v", chain.Contiguous, chain.HeadMatches))
	}
	if fail > 0 {
		return 1
	}
	fmt.Printf("crash-recovery workdir: %s\n", workdir)
	return 0
}

func safeStatus(t *store.Ticket) string {
	if t == nil {
		return "<missing>"
	}
	return t.Status
}

// runChild launches a child process running a hidden phase and force-kills it.
func runChild(app *App, phase string) int {
	exe, err := os.Executable()
	if err != nil {
		// Fall back to `go run` style invocation when not built (development).
		exe = os.Args[0]
	}
	cmd := exec.Command(exe, phase, "-workdir", app.WorkDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "CASENGINE_CHILD=1")
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start child:", err)
		return 1
	}
	// Wait until the child signals that its durable precondition is in place.
	waitForMarker(app, phase)
	if err := cmd.Process.Kill(); err != nil {
		fmt.Fprintln(os.Stderr, "kill child:", err)
		return 1
	}
	_ = cmd.Wait() // reap the forcibly terminated child
	fmt.Printf("force-killed child phase %s (pid=%d, %s)\n", phase, cmd.Process.Pid, runtime.GOOS)
	return 0
}

func markerPath(app *App, phase string) string {
	name := "marker-" + phase
	return app.WorkDir + string(os.PathSeparator) + name
}

func waitForMarker(app *App, phase string) {
	path := markerPath(app, phase)
	for i := 0; i < 200; i++ {
		if _, err := os.Stat(path); err == nil {
			return
		}
		sleep10ms()
	}
}

// RunCrashChild is invoked from main when a hidden crash phase is requested.
func RunCrashChild(phase string, args []string) int {
	workDir := os.Getenv("CASENGINE_WORKDIR")
	fs := newFlagSet(phase, &workDir)
	_ = fs.Parse(args)
	app, err := NewApp(workDir)
	must2(err)
	rt, _, err := app.newRuntime(true, deterministicConfig())
	must2(err)
	defer rt.Close()

	tenant := app.Params.Tenants[0]
	switch phase {
	case CrashChildPrepare:
		issued, err := rt.Engine.Issue(engine.IssueRequest{
			ID: "ST-CRASH-COMMITTED", TenantID: tenant.ID, SPID: tenant.SPs[0].ID,
			UserID: tenant.Users[0].ID, TTLSeconds: 300,
		})
		must2(err)
		res := rt.Engine.Validate(nilBg(), engine.ValidateRequest{
			Seq: 9001, Ticket: issued.Ticket, PresentSP: tenant.SPs[0].ID,
		})
		if res.Code != codes.OK {
			return 1
		}
		// Committed; now create the second ticket (valid, unconsumed).
		_, err = rt.Engine.Issue(engine.IssueRequest{
			ID: "ST-CRASH-UNCOMMITTED", TenantID: tenant.ID, SPID: tenant.SPs[1].ID,
			UserID: tenant.Users[1].ID, TTLSeconds: 300,
		})
		must2(err)
		touch(markerPath(app, phase))
		blockForever()

	case CrashChildKill:
		// Open a raw, deliberately uncommitted transaction on the second
		// ticket's table, then be killed before Commit.
		db, err := sql.Open("sqlite", app.DBPath)
		must2(err)
		defer db.Close()
		tx, err := db.Begin()
		must2(err)
		_, err = tx.Exec(`UPDATE tickets SET status='REDEEMED' WHERE ticket_id='ST-CRASH-UNCOMMITTED'`)
		must2(err)
		// Intentionally no tx.Commit(): the kill must discard this change.
		touch(markerPath(app, phase))
		blockForever()
	}
	return 0
}

func touch(path string) {
	f, err := os.Create(path)
	must2(err)
	_ = f.Close()
}

func blockForever() { select {} }
