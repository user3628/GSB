package joinshuffle_test

import (
	"context"
	"path/filepath"
	"testing"

	"example.com/joinshuffle/pkg/joinshuffle"
)

func TestPublicFacadeEndToEnd(t *testing.T) {
	dir := t.TempDir()
	left, right, err := joinshuffle.GenerateSamplePair(123, 96, 72, 1, 1, joinshuffle.DefaultSampleGeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	leftPath := filepath.Join(dir, "left.snapshot")
	rightPath := filepath.Join(dir, "right.snapshot")
	if _, err := joinshuffle.WriteSnapshotFile(leftPath, left); err != nil {
		t.Fatal(err)
	}
	if _, err := joinshuffle.WriteSnapshotFile(rightPath, right); err != nil {
		t.Fatal(err)
	}
	engine, err := joinshuffle.Open(filepath.Join(dir, "engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	ctx := context.Background()
	if _, err := engine.LoadSnapshots(ctx, joinshuffle.LoadSnapshotsRequest{
		Left:  joinshuffle.SnapshotLoadInput{Path: leftPath},
		Right: joinshuffle.SnapshotLoadInput{Path: rightPath},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := engine.Submit(ctx, joinshuffle.AggregateRequest{
		RequestID:         "facade-test",
		JoinType:          string(joinshuffle.JoinLeft),
		PartitionCount:    3,
		SkewThreshold:     0.35,
		Algorithm:         string(joinshuffle.SumDoubleDoubleFix),
		FixedScaleBits:    80,
		RelativeTolerance: 1e-12,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Route.Path != string(joinshuffle.PathHigh) || len(response.Groups) == 0 {
		t.Fatalf("unexpected facade response: %+v", response)
	}
	partials, err := engine.GetPartialSums(ctx, "facade-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(partials) != len(response.Groups) {
		t.Fatalf("public facade partial count mismatch: %d vs %d", len(partials), len(response.Groups))
	}
	if engine.Handler() == nil {
		t.Fatal("public facade did not expose an HTTP handler")
	}
}
