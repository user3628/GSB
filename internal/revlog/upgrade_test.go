package revlog_test

import (
	"testing"

	"maskrevision/internal/model"
	"maskrevision/internal/revlog"
)

func TestConfigurableV1PhraseMappingAndNeighborhood(t *testing.T) {
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
	raw := []byte(`{
  "schema_version":1,"log_id":"custom","shard_id":"small","width":4,"height":4,"confidence_percent":75,
  "revisions":[
    {"revision":1,"action_phrase":"please paint category 4","region":{"x":0,"y":0,"w":2,"h":2},"category_id":4,"confidence":75},
    {"revision":2,"action_phrase":"move to neighbour instance","region":{"x":0,"y":0,"w":2,"h":2},"confidence":75}
  ]
}`)
	rules := []revlog.PhraseRule{
		{Contains: "paint", Action: model.ActionRepaint},
		{Contains: "neighbour", Action: model.ActionReassign},
	}
	res, err := revlog.UpgradeV1(raw, revlog.UpgradeOptions{Width: 4, Height: 4, Pixels: pixels, PhraseRules: rules, NeighborRadius: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Log.Revisions[0].Action != model.ActionRepaint || res.Log.Revisions[0].Confidence != 0.75 {
		t.Fatalf("custom repaint mapping failed: %+v", res.Log.Revisions[0])
	}
	if res.Log.Revisions[1].Action != model.ActionReassign || res.Log.Revisions[1].TargetSegmentID != 2 {
		t.Fatalf("custom reassign inference failed: %+v", res.Log.Revisions[1])
	}
	if res.Report.Inferences[1].Basis == "" || res.Report.Inferences[1].TargetSegmentID != 2 {
		t.Fatalf("missing inference basis: %+v", res.Report.Inferences[1])
	}
}

func TestUnmappedPhraseAndStrictUnrecoverable(t *testing.T) {
	pixels := []int{1, 1, 1, 1}
	raw := []byte(`{"schema_version":1,"log_id":"x","shard_id":"s","width":2,"height":2,"confidence_percent":50,"revisions":[{"revision":1,"action_phrase":"unknown operation","region":{"x":0,"y":0,"w":1,"h":1},"confidence":50}]}`)
	if _, err := revlog.UpgradeV1(raw, revlog.UpgradeOptions{Width: 2, Height: 2, Pixels: pixels}); err == nil {
		t.Fatal("unmapped phrase should fail")
	}
	unrec := []byte(`{"schema_version":1,"log_id":"x","shard_id":"s","width":2,"height":2,"confidence_percent":50,"revisions":[{"revision":1,"action_phrase":"改判为相邻实例","region":{"x":0,"y":0,"w":2,"h":2},"confidence":50}]}`)
	if _, err := revlog.UpgradeV1(unrec, revlog.UpgradeOptions{Width: 2, Height: 2, Pixels: pixels, StrictUnrecoverable: true}); err == nil {
		t.Fatal("strict unrecoverable should fail")
	}
}
