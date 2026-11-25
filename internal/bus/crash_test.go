package bus_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"local/eventbus/internal/agg"
	"local/eventbus/internal/bus"
	"local/eventbus/internal/sample"
)

func TestForcedProcessTerminationHelper(t *testing.T) {
	if os.Getenv("EVENTBUS_CRASH_CHILD") != "1" {
		t.Skip("helper process only")
	}
	stage := os.Getenv("EVENTBUS_CRASH_STAGE")
	path := os.Getenv("EVENTBUS_CRASH_DB")
	leaseToken := os.Getenv("EVENTBUS_CRASH_LEASE")
	service, err := bus.Open(context.Background(), bus.ServiceOptions{
		DBPath:        path,
		LeaseDuration: time.Minute,
		Now:           sample.FixedClock,
		SkipRecovery:  true,
	})
	if err != nil {
		os.Exit(2)
	}
	service.SetFaultInjector(func(current string) error {
		if current == stage {
			os.Exit(42)
		}
		return nil
	})
	_, err = service.Ack(context.Background(), bus.AckRequest{
		RequestID: "forced-child-ack", Topic: "forced", Group: "g", Partition: 0,
		ConsumerID: "child", LeaseToken: leaseToken, Seqs: []int64{1}, AckBatchID: "forced-batch",
	})
	if err != nil {
		os.Exit(3)
	}
	os.Exit(4)
}

func TestForcedProcessTerminationRecovery(t *testing.T) {
	for _, stage := range []string{"ack_after_intent", "ack_before_commit"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "forced.sqlite")
			service, err := bus.Open(context.Background(), bus.ServiceOptions{
				DBPath: path, LeaseDuration: time.Minute, Now: sample.FixedClock,
			})
			if err != nil {
				t.Fatal(err)
			}
			createTopic(t, service, "forced", 1, bus.GroupSpec{Name: "g", Algorithm: string(agg.Kahan)})
			publishSpecs(t, service, "forced", sample.Generate(811, 2, 1))
			pulled, err := service.Pull(context.Background(), bus.PullRequest{
				RequestID: requestID("forced-pull"), Topic: "forced", Group: "g", Partition: 0,
				ConsumerID: "parent", MaxEvents: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := service.Close(); err != nil {
				t.Fatal(err)
			}

			command := exec.Command(os.Args[0], "-test.run=^TestForcedProcessTerminationHelper$")
			command.Env = append(os.Environ(),
				"EVENTBUS_CRASH_CHILD=1",
				"EVENTBUS_CRASH_STAGE="+stage,
				"EVENTBUS_CRASH_DB="+path,
				"EVENTBUS_CRASH_LEASE="+pulled.LeaseToken,
			)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 42 {
				t.Fatalf("child did not terminate at %s: err=%v output=%s", stage, err, output)
			}

			restarted, err := bus.Open(context.Background(), bus.ServiceOptions{
				DBPath: path, LeaseDuration: time.Minute, Now: sample.FixedClock,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			report, err := restarted.LatestRecoveryReport(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if report.PendingScanned != 1 || report.RolledBack != 1 {
				t.Fatalf("unexpected recovery report: %+v", report)
			}
			offset, err := restarted.GetOffset(context.Background(), "forced", "g", 0)
			if err != nil {
				t.Fatal(err)
			}
			if offset.AckedOffset != 0 || offset.LeaseHeld {
				t.Fatalf("uncommitted transaction survived forced termination: %+v", offset)
			}
			var ackCount, batchCount, stateCount int
			if err := restarted.DB().QueryRow(`SELECT COUNT(*) FROM acks WHERE topic='forced'`).Scan(&ackCount); err != nil {
				t.Fatal(err)
			}
			if err := restarted.DB().QueryRow(`SELECT COUNT(*) FROM agg_batches WHERE topic='forced'`).Scan(&batchCount); err != nil {
				t.Fatal(err)
			}
			if err := restarted.DB().QueryRow(`SELECT COUNT(*) FROM agg_states WHERE topic='forced'`).Scan(&stateCount); err != nil {
				t.Fatal(err)
			}
			if ackCount != 0 || batchCount != 0 || stateCount != 0 {
				t.Fatalf("partial uncommitted state remained: acks=%d batches=%d states=%d", ackCount, batchCount, stateCount)
			}
			redelivery, err := restarted.Pull(context.Background(), bus.PullRequest{
				RequestID: requestID("forced-redelivery"), Topic: "forced", Group: "g", Partition: 0,
				ConsumerID: "parent-restarted", MaxEvents: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			if redelivery.Events[0].Seq != 1 || redelivery.Events[0].DeliveryCount != 2 {
				t.Fatalf("unexpected post-crash delivery: %+v", redelivery)
			}
		})
	}
}
