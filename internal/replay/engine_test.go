package replay_test

import (
	"os"
	"path/filepath"
	"testing"

	"maskrevision/internal/generator"
	"maskrevision/internal/imageio"
	"maskrevision/internal/model"
	"maskrevision/internal/replay"
	"maskrevision/internal/revlog"
)

func TestReplayUndoDiffAndDeterminism(t *testing.T) {
	dir := t.TempDir()
	manifest, err := generator.Generate(dir, generator.DefaultSeed)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	var firstDigest string
	var secondDigest string
	for run := 0; run < 2; run++ {
		engine, states, records := replayShard(t, dir, manifest.Shards[0])
		if engine == nil {
			t.Fatal("nil engine")
		}
		if len(records) != 8 {
			t.Fatalf("expected 8 revisions, got %d", len(records))
		}
		if !states[4].Undone[3] || !states[7].Undone[2] || !states[7].Undone[5] {
			t.Fatalf("expected undone revisions 2,3,5 at head: %+v", states[7].Undone)
		}
		if states[7].Undone[1] {
			t.Fatal("revision 1 should remain active")
		}
		diff, be := replay.Diff(1, 4, states[1], states[4], records)
		if be != nil {
			t.Fatalf("diff: %v", be)
		}
		if diff.DifferencePixelCount == 0 {
			t.Fatal("expected pixels changed between revisions 1 and 4")
		}
		if diff.FirstCause == nil || diff.FirstCause.Revision == 0 {
			t.Fatalf("expected a first-cause revision: %+v", diff.FirstCause)
		}
		digest := states[7].RevisionRecords[len(states[7].RevisionRecords)-1].SnapshotDigest
		if run == 0 {
			firstDigest = digest
		} else {
			secondDigest = digest
		}
	}
	if firstDigest == "" || firstDigest != secondDigest {
		t.Fatalf("replay not deterministic: %s != %s", firstDigest, secondDigest)
	}
}

func TestV1UpgradeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manifest, err := generator.Generate(dir, generator.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	sh := manifest.Shards[1]
	w, h, pixels, _, err := imageio.ReadIndexed(filepath.Join(dir, filepath.FromSlash(sh.Image)))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(sh.Log)))
	if err != nil {
		t.Fatal(err)
	}
	up, err := revlog.UpgradeV1(data, revlog.UpgradeOptions{Width: w, Height: h, Pixels: pixels})
	if err != nil {
		t.Fatal(err)
	}
	if up.Report.UnrecoverableCount != 0 {
		t.Fatalf("unexpected unrecoverable actions: %+v", up.Report.Inferences)
	}
	for _, inf := range up.Report.Inferences {
		if inf.InferredAction == model.ActionReassign && inf.TargetSegmentID == 0 {
			t.Fatalf("revision %d missing inferred target", inf.Revision)
		}
		if inf.ConfidencePercent > 1 && inf.Confidence > 1 {
			t.Fatalf("confidence was not converted: %+v", inf)
		}
	}
	v2, err := revlog.EncodeV2(up.Log)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _, err := revlog.Parse(v2)
	if err != nil {
		t.Fatal(err)
	}
	initial := initialFor(t, dir, sh)
	e1, err := replay.NewEngine(initial, up.Log, replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	e2, err := replay.NewEngine(initial, parsed, replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(up.Log.Revisions); i++ {
		r1, be := e1.ApplyNext()
		if be != nil {
			t.Fatalf("v1 replay %d: %v", i, be)
		}
		r2, be := e2.ApplyNext()
		if be != nil {
			t.Fatalf("v2 replay %d: %v", i, be)
		}
		if r1.SnapshotDigest != r2.SnapshotDigest {
			t.Fatalf("roundtrip digest mismatch at %d: %s != %s", i, r1.SnapshotDigest, r2.SnapshotDigest)
		}
	}
}

func TestForkAfterUndoRejected(t *testing.T) {
	dir := t.TempDir()
	manifest, err := generator.Generate(dir, generator.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	sh := manifest.Shards[0]
	initial := initialFor(t, dir, sh)
	mainLog, _, err := revlog.ParseFile(filepath.Join(dir, filepath.FromSlash(sh.Log)))
	if err != nil {
		t.Fatal(err)
	}
	forkLog, _, err := revlog.ParseFile(filepath.Join(dir, filepath.FromSlash(sh.ForkLog)))
	if err != nil {
		t.Fatal(err)
	}
	e, err := replay.NewEngine(initial, mainLog, replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, be := e.ApplyNext(); be != nil {
			t.Fatal(be)
		}
	}
	e, err = replay.NewEngine(initial, forkLog, replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, be := e.ApplyNext(); be != nil {
			t.Fatal(be)
		}
	}
	if _, be := e.ApplyNext(); be == nil || be.Code != "E_FORK_CONFLICT" {
		t.Fatalf("expected fork conflict, got %v", be)
	}
	state := e.State()
	if len(state.ForkDecisions) == 0 || state.ForkDecisions[0].Decision != "rejected" {
		t.Fatalf("missing fork adjudication: %+v", state.ForkDecisions)
	}
}

func replayShard(t *testing.T, dir string, sh model.ManifestShard) (*replay.Engine, map[int]*model.ReplayState, []model.RevisionRecord) {
	t.Helper()
	initial := initialFor(t, dir, sh)
	logPath := filepath.Join(dir, filepath.FromSlash(sh.Log))
	log, upgrade, err := parseLog(t, logPath, dir, sh)
	if err != nil {
		t.Fatal(err)
	}
	if upgrade != nil && upgrade.UnrecoverableCount != 0 {
		t.Fatalf("unexpected unrecoverable v1 action")
	}
	e, err := replay.NewEngine(initial, log, replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	states := map[int]*model.ReplayState{0: model.CloneState(e.State())}
	var records []model.RevisionRecord
	for i := 0; i < len(log.Revisions); i++ {
		rec, be := e.ApplyNext()
		if be != nil {
			t.Fatalf("replay rev %d: %v details=%+v", log.Revisions[i].Revision, be, be.Details)
		}
		records = append(records, *rec)
		states[rec.Revision] = model.CloneState(e.State())
	}
	return e, states, records
}

func initialFor(t *testing.T, dir string, sh model.ManifestShard) replay.Initial {
	t.Helper()
	w, h, pixels, pngData, err := imageio.ReadIndexed(filepath.Join(dir, filepath.FromSlash(sh.Image)))
	if err != nil {
		t.Fatal(err)
	}
	fp := model.InputFingerprint(w, h, pngData, sh.Segments)
	return replay.Initial{ShardID: sh.ShardID, Width: w, Height: h, Pixels: pixels, Segments: sh.Segments, InputFingerprint: fp}
}

func parseLog(t *testing.T, path, dir string, sh model.ManifestShard) (model.Log, *revlog.UpgradeReport, error) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return model.Log{}, nil, err
	}
	log, _, err := revlog.Parse(data)
	if err == nil {
		return log, nil, nil
	}
	w, h, pixels, _, err := imageio.ReadIndexed(filepath.Join(dir, filepath.FromSlash(sh.Image)))
	if err != nil {
		return model.Log{}, nil, err
	}
	res, err := revlog.UpgradeV1(data, revlog.UpgradeOptions{Width: w, Height: h, Pixels: pixels})
	if err != nil {
		return model.Log{}, nil, err
	}
	return res.Log, &res.Report, nil
}
