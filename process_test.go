package prefixcache

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestConcurrentCommitsAcrossProcesses(t *testing.T) {
	path := testDBPath(t)
	engine := openTestEngine(t, path)
	initializeDataset(t, engine, DefaultSeed)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	type processOutcome struct {
		code   int
		output string
		err    error
	}
	run := func(threshold, resultFile string) processOutcome {
		command := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
		command.Env = append(os.Environ(),
			"PREFIXCACHE_HELPER=1",
			"PREFIXCACHE_HELPER_DB="+path,
			"PREFIXCACHE_HELPER_THRESHOLD="+threshold,
			"PREFIXCACHE_HELPER_RESULT="+resultFile,
		)
		var output bytes.Buffer
		command.Stdout = &output
		command.Stderr = &output
		err := command.Run()
		code := 0
		if err != nil {
			var exit *exec.ExitError
			if !asExitError(err, &exit) {
				return processOutcome{code: -1, output: output.String(), err: err}
			}
			code = exit.ExitCode()
		}
		return processOutcome{code: code, output: output.String()}
	}

	first := make(chan processOutcome, 1)
	second := make(chan processOutcome, 1)
	dir := t.TempDir()
	go func() { first <- run("0.80", filepath.Join(dir, "first.out")) }()
	go func() { second <- run("0.81", filepath.Join(dir, "second.out")) }()

	outcomes := []processOutcome{<-first, <-second}
	successes := 0
	stales := 0
	for _, outcome := range outcomes {
		switch outcome.code {
		case 0:
			successes++
		case 3:
			stales++
		default:
			t.Fatalf("helper exit=%d err=%v output=%s", outcome.code, outcome.err, outcome.output)
		}
	}
	if successes != 1 || stales != 1 {
		t.Fatalf("cross-process successes=%d stales=%d outcomes=%+v", successes, stales, outcomes)
	}

	reopened := openTestEngine(t, path)
	status, err := reopened.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentVersion != 2 || status.AllocationSequence != 2 {
		t.Fatalf("cross-process head version=%d sequence=%d", status.CurrentVersion, status.AllocationSequence)
	}
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("PREFIXCACHE_HELPER") != "1" {
		t.Skip("helper process")
	}
	dbPath := os.Getenv("PREFIXCACHE_HELPER_DB")
	thresholdText := os.Getenv("PREFIXCACHE_HELPER_THRESHOLD")
	resultPath := os.Getenv("PREFIXCACHE_HELPER_RESULT")
	threshold, err := strconv.ParseFloat(thresholdText, 64)
	if err != nil || dbPath == "" {
		fmt.Fprintln(os.Stderr, "invalid helper configuration")
		os.Exit(4)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	engine, err := Open(ctx, dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(4)
	}
	defer engine.Close()
	result, err := engine.ApplyPatch(ctx, 1, PolicyPatch{MergeThreshold: &threshold})
	if err == nil {
		if resultPath != "" {
			_ = os.WriteFile(resultPath, []byte(strconv.FormatInt(result.Version, 10)), 0o600)
		}
		fmt.Println("SUCCESS")
		os.Exit(0)
	}
	var typed *Error
	if asCacheError(err, &typed) && typed.Code == CodeStaleVersion {
		if resultPath != "" {
			_ = os.WriteFile(resultPath, []byte(typed.Code), 0o600)
		}
		fmt.Println("STALE")
		os.Exit(3)
	}
	fmt.Fprintln(os.Stderr, "unexpected helper error:", err)
	os.Exit(4)
}

func asExitError(err error, target **exec.ExitError) bool {
	exit, ok := err.(*exec.ExitError)
	if ok {
		*target = exit
	}
	return ok
}

func asCacheError(err error, target **Error) bool {
	typed, ok := err.(*Error)
	if ok {
		*target = typed
	}
	return ok
}
