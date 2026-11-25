// Command crashdemo proves hard-kill recovery and stale-read refusal:
//  1. a child process commits snapshot v1 successfully;
//  2. the same child arms a hard-kill hook and commits delta v2, dying after
//     facts are durable but before derived views are rebuilt;
//  3. a pre-recovery inspection shows E_STALE_READ for observers of v2;
//  4. the parent re-opens the engine, recovery rolls the unfinished delta
//     back to the last complete version and later reads are served.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"

	"configtree/internal/engine"
	"configtree/internal/seed"
)

func main() {
	dbPath := flag.String("db", "data/crashdemo.db", "SQLite database path")
	child := flag.Bool("child", false, "internal: armed child process")
	flag.Parse()

	if !*child {
		runParent(*dbPath)
		return
	}
	runChild(*dbPath)
}

func runParent(dbPath string) {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(dbPath + suffix)
	}

	cmd := exec.Command(os.Args[0], "-child", "-db", dbPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("child exited (expected hard kill): %v\n", err)
	}

	inspector, err := engine.InspectWithoutRecovery(dbPath)
	if err != nil {
		log.Fatalf("inspect before recovery: %v", err)
	}
	_, err = inspector.ReadView(context.Background(), "gateway", 2)
	fmt.Printf("pre-recovery read at observed=2: %v\n", err)
	older, err := inspector.ReadView(context.Background(), "gateway", 1)
	if err != nil {
		fmt.Printf("pre-recovery read at observed=1 refused: %v\n", err)
	} else {
		fmt.Printf("pre-recovery read at observed=1: used_version=%d (last good)\n",
			older.UsedVersion)
	}
	_ = inspector.Close()

	eng, err := engine.Open(dbPath)
	if err != nil {
		log.Fatalf("reopen after crash: %v", err)
	}
	defer eng.Close()

	report := eng.LastRecoveryReport()
	fmt.Printf("recovery: last_good_version=%d aborted_intents=%v rolled_back=%v\n",
		report.LastGoodVersion, report.AbortedIntents, report.RolledBackCommits)
	for _, c := range report.Components {
		fmt.Printf("  component=%s status=%s view_version=%d pending=%v\n",
			c.ComponentID, c.Status, c.ViewVersion, c.PendingKeys)
	}

	_, err = eng.ReadView(context.Background(), "gateway", 2)
	if err != nil {
		fmt.Printf("post-recovery read at observed=2 refused: %v\n", err)
	} else {
		fmt.Println("post-recovery read at observed=2: served last-good v1 view (intent aborted)")
	}
	res, err := eng.ReadView(context.Background(), "gateway", 1)
	if err != nil {
		log.Fatalf("read after recovery: %v", err)
	}
	fmt.Printf("post-recovery read at observed=1 served: used_version=%d entries=%d converged=%t\n",
		res.UsedVersion, len(res.View.Entries), res.Converged)
}

func runChild(dbPath string) {
	eng, err := engine.Open(dbPath)
	if err != nil {
		log.Fatalf("child open: %v", err)
	}
	if _, err := eng.Commit(context.Background(), &engine.CommitRequest{
		CommitID: "snapshot-v1",
		Kind:     "snapshot",
		Snapshot: seed.V1(),
	}); err != nil {
		log.Fatalf("child snapshot: %v", err)
	}
	fmt.Println("child: snapshot v1 committed")

	eng.SetCrashHook(func(phase string) bool {
		return phase == "after_facts_before_derived"
	})
	_, err = eng.Commit(context.Background(), &engine.CommitRequest{
		CommitID: "delta-v2-crash",
		Kind:     "delta",
		Delta:    seed.DeltaV2(),
	})
	if err != nil {
		log.Fatalf("child commit returned (hook should have killed first): %v", err)
	}
}
