package replay_test

import (
	"encoding/json"
	"testing"

	"maskrevision/internal/model"
	"maskrevision/internal/replay"
	"maskrevision/internal/revlog"
)

func TestBusinessCodesAndUndoSegmentPolicy(t *testing.T) {
	initial := smallInitial()
	cases := []struct {
		name string
		log  model.Log
		code string
	}{
		{
			name: "out of bounds",
			log:  smallLog(model.Revision{Revision: 1, Action: model.ActionRepaint, Region: model.Region{X: 3, Y: 0, W: 2, H: 2}, CategoryID: 2}),
			code: "E_REGION_OUT_OF_BOUNDS",
		},
		{
			name: "missing segment",
			log:  smallLog(model.Revision{Revision: 1, Action: model.ActionReassign, Region: model.Region{X: 0, Y: 0, W: 2, H: 2}, TargetSegmentID: 99}),
			code: "E_SEGMENT_NOT_FOUND",
		},
		{
			name: "undo initial",
			log:  smallLog(model.Revision{Revision: 1, Action: model.ActionUndo}),
			code: "E_UNDO_INITIAL",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := replay.NewEngine(initial, tc.log, replay.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if _, be := e.ApplyNext(); be == nil || be.Code != tc.code {
				t.Fatalf("expected %s, got %v", tc.code, be)
			}
		})
	}

	undoLog := smallLog(
		model.Revision{Revision: 1, ParentRevision: 0, Action: model.ActionRepaint, Region: model.Region{X: 0, Y: 0, W: 2, H: 2}, CategoryID: 3},
		model.Revision{Revision: 2, ParentRevision: 1, Action: model.ActionUndo},
		model.Revision{Revision: 3, ParentRevision: 2, Action: model.ActionUndo},
	)
	e, err := replay.NewEngine(initial, undoLog, replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	first, be := e.ApplyNext()
	if be != nil || len(first.DerivedSegments) != 1 {
		t.Fatalf("create derived segment: %+v %v", first, be)
	}
	derived := first.DerivedSegments[0]
	if _, be := e.ApplyNext(); be != nil {
		t.Fatal(be)
	}
	if got := e.State().Segments[derived].Status; got != model.StatusRecycled {
		t.Fatalf("expected recycled segment, got %s", got)
	}
	if len(e.State().FreeSegmentIDs) != 1 || e.State().FreeSegmentIDs[0] != derived {
		t.Fatalf("expected recycled id in free pool: %+v", e.State().FreeSegmentIDs)
	}
	if _, be := e.ApplyNext(); be == nil || be.Code != "E_UNDO_INITIAL" {
		t.Fatalf("consecutive undo past initial should fail, got %v", be)
	}

	e2, err := replay.NewEngine(initial, undoLog, replay.Options{UndoSegmentPolicy: model.UndoSegmentVoid})
	if err != nil {
		t.Fatal(err)
	}
	first, _ = e2.ApplyNext()
	derived = first.DerivedSegments[0]
	_, _ = e2.ApplyNext()
	if got := e2.State().Segments[derived].Status; got != model.StatusVoided {
		t.Fatalf("expected voided segment, got %s", got)
	}
	if len(e2.State().FreeSegmentIDs) != 0 {
		t.Fatalf("void policy must not recycle ids: %+v", e2.State().FreeSegmentIDs)
	}
}

func TestOverlapPoliciesAndValidation(t *testing.T) {
	initial := smallInitial()
	log := smallLog(
		model.Revision{Revision: 1, ParentRevision: 0, Action: model.ActionIgnore, Region: model.Region{X: 0, Y: 0, W: 2, H: 4}},
		model.Revision{Revision: 2, ParentRevision: 1, Action: model.ActionRepaint, Region: model.Region{X: 0, Y: 0, W: 2, H: 4}, TargetSegmentID: 2, CategoryID: 2},
	)
	for _, tc := range []struct {
		policy string
		want   int
	}{{model.OverlapLastWrite, 2}, {model.OverlapFirstWrite, 0}} {
		e, err := replay.NewEngine(initial, log, replay.Options{OverlapPolicy: tc.policy})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if _, be := e.ApplyNext(); be != nil {
				t.Fatalf("%s: %v details=%+v", tc.policy, be, be.Details)
			}
		}
		if got := e.State().Pixels[0]; got != tc.want {
			t.Fatalf("%s: got pixel %d want %d", tc.policy, got, tc.want)
		}
	}

	e, err := replay.NewEngine(initial, smallLog(model.Revision{Revision: 1, ParentRevision: 0, Action: model.ActionRepaint, Region: model.Region{X: 0, Y: 0, W: 2, H: 2}, CategoryID: 3}), replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, be := e.ApplyNext(); be != nil {
		t.Fatal(be)
	}
	bad := model.CloneState(e.State())
	bad.Segments[1].PixelCount = 999
	be := model.ValidateState(bad, 1)
	if be == nil || be.Code != "E_PIXEL_COUNT_MISMATCH" {
		t.Fatalf("expected pixel-count mismatch, got %v", be)
	}
	if be.Details["first_x"] != 0 || be.Details["first_y"] != 2 {
		t.Fatalf("expected first differing coordinate, got %+v", be.Details)
	}
}

func TestUnrecoverableV1NoopRoundTrip(t *testing.T) {
	initial := smallInitial()
	raw := []byte(`{
  "schema_version": 1,
  "log_id": "unrecoverable",
  "shard_id": "small",
  "width": 4,
  "height": 4,
  "confidence_percent": 62,
  "revisions": [{"revision":1,"action_phrase":"将该区域改判为相邻实例","region":{"x":0,"y":0,"w":4,"h":4},"confidence":62}]
}`)
	res, err := revlog.UpgradeV1(raw, revlog.UpgradeOptions{Width: 4, Height: 4, Pixels: initial.Pixels})
	if err != nil {
		t.Fatal(err)
	}
	if res.Report.UnrecoverableCount != 1 || res.Log.Revisions[0].Action != model.ActionNoop {
		t.Fatalf("expected explicit noop inference: %+v / %+v", res.Report.Inferences, res.Log.Revisions[0])
	}
	encoded, err := revlog.EncodeV2(res.Log)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _, err := revlog.Parse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	e1, err := replay.NewEngine(initial, res.Log, replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	e2, err := replay.NewEngine(initial, parsed, replay.Options{})
	if err != nil {
		t.Fatal(err)
	}
	r1, be := e1.ApplyNext()
	if be != nil {
		t.Fatal(be)
	}
	r2, be := e2.ApplyNext()
	if be != nil {
		t.Fatal(be)
	}
	if r1.SnapshotDigest != r2.SnapshotDigest || r1.Unrecoverable == "" {
		t.Fatalf("noop roundtrip mismatch: %+v %+v", r1, r2)
	}
}

func smallInitial() replay.Initial {
	segments := []model.Segment{
		{ID: 0, Status: model.StatusIgnored, Source: "reserved-ignore"},
		{ID: 1, CategoryID: 1, InstanceNo: 1, Status: model.StatusActive, PixelCount: 8, Source: "initial"},
		{ID: 2, CategoryID: 2, InstanceNo: 1, Status: model.StatusActive, PixelCount: 8, Source: "initial"},
	}
	pixels := make([]int, 16)
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			if x < 2 {
				pixels[y*4+x] = 1
			} else {
				pixels[y*4+x] = 2
			}
		}
	}
	return replay.Initial{ShardID: "small", Width: 4, Height: 4, Pixels: pixels, Segments: segments, InputFingerprint: "small-input"}
}

func smallLog(revisions ...model.Revision) model.Log {
	return model.Log{SchemaVersion: model.SchemaV2, LogID: "small-log", ShardID: "small", Width: 4, Height: 4, Revisions: revisions}
}

func TestSmallLogCanMarshal(t *testing.T) {
	if _, err := json.Marshal(smallLog()); err != nil {
		t.Fatal(err)
	}
}
