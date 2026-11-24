package service

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"keylife/internal/model"
	"keylife/internal/store"
)

const (
	crashHelperEnv = "KEYLIFE_CRASH_HELPER"
	crashExitCode  = 91
)

func TestCrashHelperProcess(t *testing.T) {
	if os.Getenv(crashHelperEnv) != "1" {
		return
	}
	ctx := context.Background()
	st, err := store.Open(os.Getenv("KEYLIFE_CRASH_DB"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(92)
	}
	clock := model.NewVirtualClock(200)
	point := os.Getenv("KEYLIFE_CRASH_POINT")
	hooks := Hooks{}
	if point == "before_commit" {
		hooks.BeforeCommit = func(operation, keyID string) { os.Exit(crashExitCode) }
	} else {
		hooks.AfterCommit = func(operation, keyID string) { os.Exit(crashExitCode) }
	}
	svc := New(st, clock, Config{MaxConcurrency: 2, DefaultSubjectLimit: 8}, hooks)
	if _, err := svc.Rotate(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(92)
	}
	os.Exit(93)
}

func TestCrashRecoveryAtProcessBoundary(t *testing.T) {
	for _, point := range []string{"before_commit", "after_commit"} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), "crash.db")
			st, err := store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.UpsertSubject(ctx, model.SubjectPolicy{Subject: "alpha", MinStrengthBits: 256, RotationInterval: model.D(100), DefaultLifetime: model.D(1000), MaxPendingRequests: 8}); err != nil {
				t.Fatal(err)
			}
			clock := model.NewVirtualClock(100)
			svc := New(st, clock, Config{MaxConcurrency: 2, DefaultSubjectLimit: 8}, Hooks{})
			if _, err := svc.Generate(ctx, GenerateRequest{KeyID: "k1", Actor: "alpha", Subject: "alpha", Algorithm: "aes-256-gcm"}); err != nil {
				t.Fatal(err)
			}
			clock.Set(110)
			if _, err := svc.Activate(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"}); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelperProcess$")
			cmd.Env = append(os.Environ(), crashHelperEnv+"=1", "KEYLIFE_CRASH_DB="+dbPath, "KEYLIFE_CRASH_POINT="+point)
			output, err := cmd.CombinedOutput()
			exitErr, ok := err.(*exec.ExitError)
			if !ok || exitErr.ExitCode() != crashExitCode {
				t.Fatalf("child exit=%v output=%s", err, output)
			}

			reopened, err := store.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			chain, err := reopened.LoadChain(ctx)
			if err != nil {
				t.Fatalf("chain after crash: %v", err)
			}
			key, err := reopened.GetKey(ctx, "k1")
			if err != nil {
				t.Fatal(err)
			}
			if point == "before_commit" {
				if chain.RecoveredThrough != 2 || key.Phase != model.PhaseActive {
					t.Fatalf("uncommitted rotation was not rolled back: version=%d phase=%s", chain.RecoveredThrough, key.Phase)
				}
				retryClock := model.NewVirtualClock(200)
				retrySvc := New(reopened, retryClock, Config{MaxConcurrency: 2, DefaultSubjectLimit: 8}, Hooks{})
				if _, err := retrySvc.Rotate(ctx, TransitionRequest{Actor: "alpha", KeyID: "k1"}); err != nil {
					t.Fatalf("retry rotation failed: %v", err)
				}
				after, err := reopened.LoadChain(ctx)
				if err != nil || after.RecoveredThrough != 3 {
					t.Fatalf("retry chain: version=%d err=%v", after.RecoveredThrough, err)
				}
				rotated, _ := reopened.GetKey(ctx, "k1")
				if rotated.Phase != model.PhaseRotated {
					t.Fatalf("retry phase=%s", rotated.Phase)
				}
			} else {
				if chain.RecoveredThrough != 3 || key.Phase != model.PhaseRotated {
					t.Fatalf("committed rotation regressed: version=%d phase=%s", chain.RecoveredThrough, key.Phase)
				}
			}
		})
	}
}
