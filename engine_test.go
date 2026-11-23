package dnsdetect

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
)

func TestMalformedInputsDoNotAdvanceSnapshot(t *testing.T) {
	dataset := testDataset(t)
	config := testConfig(dataset)
	store := testStore(t, "malformed-no-advance")
	engine := NewEngine(config, store)
	baseline, err := engine.Process(context.Background(), dataset.Sources)
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range dataset.Malformed {
		if sample.ExpectedCode == CodeSnapshotVersion {
			continue
		}
		_, err := engine.Process(context.Background(), []SourceSpec{{Path: sample.Path, Kind: sample.Kind}})
		requireCode(t, err, sample.ExpectedCode)
		latest, latestErr := store.Latest(context.Background())
		if latestErr != nil {
			t.Fatalf("Latest after %s: %v", sample.Name, latestErr)
		}
		if latest == nil || latest.Version != baseline.Snapshot.Version {
			t.Fatalf("snapshot advanced after %s: %+v", sample.Name, latest)
		}
	}
}

func TestReportIsByteStableAndSorted(t *testing.T) {
	dataset := testDataset(t)
	config := testConfig(dataset)
	firstStore := testStore(t, "stable-a")
	secondStore := testStore(t, "stable-b")
	first := processGood(t, dataset, config, firstStore)
	second := processGood(t, dataset, config, secondStore)
	firstBytes, err := first.Report.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := second.Report.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("reports differ across fresh stores")
	}
	repeat := processGood(t, dataset, config, firstStore)
	repeatBytes, _ := repeat.Report.CanonicalBytes()
	if !bytes.Equal(firstBytes, repeatBytes) {
		t.Fatalf("reports differ on idempotent rerun")
	}
	if repeat.Snapshot.Version != first.Snapshot.Version {
		t.Fatalf("idempotent snapshot version %d != %d", repeat.Snapshot.Version, first.Snapshot.Version)
	}
	replayed, err := firstStore.Replay(context.Background(), first.Snapshot.Version)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	replayedBytes, err := replayed.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, replayedBytes) {
		t.Fatalf("replayed report differs from live report")
	}
	if !bytes.Contains(firstBytes, []byte(`"time_scale": "100ns-unix"`)) || !bytes.Contains(firstBytes, []byte(`"sampling": "fixed-100ns"`)) {
		t.Fatal("report lacks fixed time markers")
	}
	if bytes.Contains(firstBytes, []byte("2024-01-01T")) {
		t.Fatal("report contains an RFC3339 wall-clock timestamp")
	}
	if !sort.SliceIsSorted(first.Report.Sources, func(i, j int) bool { return first.Report.Sources[i].Path < first.Report.Sources[j].Path }) {
		t.Fatal("sources not sorted")
	}
	if !sort.SliceIsSorted(first.Report.Ledger, func(i, j int) bool {
		a, b := first.Report.Ledger[i], first.Report.Ledger[j]
		if a.SourcePath != b.SourcePath {
			return a.SourcePath < b.SourcePath
		}
		return a.WindowIndex < b.WindowIndex
	}) {
		t.Fatal("ledger not sorted")
	}
}

func TestSchedulerQueuesBeyondConcurrency(t *testing.T) {
	dataset := testDataset(t)
	config := testConfig(dataset)
	specs := make([]SourceSpec, 0, 10)
	for index := 0; index < 10; index++ {
		specs = append(specs, dataset.Sources[index%len(dataset.Sources)])
	}
	scheduler := NewScheduler(2, LoadOptions{
		MaxEvents:         config.MaxEventsPerSession,
		MaxSpanTicks:      config.MaxSpanTicksPerSession,
		WindowSpanTicks:   config.WindowSpanTicks,
		WindowAnchorTicks: config.WindowAnchorTicks,
		SourcePriorities:  config.SourcePriorities,
	})
	batch := scheduler.Load(context.Background(), specs)
	if err := batch.FirstError(); err != nil {
		t.Fatal(err)
	}
	if len(batch.Sessions) != len(specs) {
		t.Fatalf("sessions = %d, want %d", len(batch.Sessions), len(specs))
	}
	stats := scheduler.Stats()
	if stats.MaxConcurrency != 2 || stats.Completed != int64(len(specs)) || stats.Failed != 0 || stats.Queued != 0 || stats.InFlight != 0 {
		t.Fatalf("scheduler stats = %+v", stats)
	}
	if stats.TotalInputBytes <= 0 || stats.InFlightBytes != 0 {
		t.Fatalf("scheduler byte accounting = %+v", stats)
	}
}

func TestEngineAbortsOnSourceErrorButKeepsTraceability(t *testing.T) {
	dataset := testDataset(t)
	config := testConfig(dataset)
	store := testStore(t, "source-error")
	engine := NewEngine(config, store)
	bad := dataset.Malformed[0]
	specs := append([]SourceSpec(nil), dataset.Sources...)
	specs = append(specs, SourceSpec{Path: bad.Path, Kind: bad.Kind})
	_, err := engine.Process(context.Background(), specs)
	requireCode(t, err, bad.ExpectedCode)
	if !strings.Contains(err.Error(), bad.Path) {
		t.Fatalf("error lacks source path: %v", err)
	}
	latest, latestErr := store.Latest(context.Background())
	if latestErr != nil || latest != nil {
		t.Fatalf("snapshot advanced despite source error: %+v, %v", latest, latestErr)
	}
	if stats := engine.SchedulerStats(); stats.Failed != 1 || stats.Completed != int64(len(dataset.Sources)) {
		t.Fatalf("scheduler stats after error = %+v", stats)
	}
}
