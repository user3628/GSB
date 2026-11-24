package domain

import (
	"bytes"
	"testing"
)

func TestPipelineDeterministicAndShardsComplete(t *testing.T) {
	policy := DefaultPolicy()
	spec := DefaultGenerateSpec()
	spec.Count = 80
	spec.Namespace = "determinism"
	raw := GenerateRawInput(spec)

	first, err := RunAllStages(raw, policy, -1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := RunAllStages(raw, policy, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Stages) != len(second.Stages) {
		t.Fatalf("stage count mismatch: %d vs %d", len(first.Stages), len(second.Stages))
	}
	for i := range first.Stages {
		if first.Stages[i].OutputDigest != second.Stages[i].OutputDigest {
			t.Fatalf("stage %s output digest differs", first.Stages[i].Stage)
		}
		if first.Stages[i].InputDigest != second.Stages[i].InputDigest {
			t.Fatalf("stage %s input digest differs", first.Stages[i].Stage)
		}
	}
	if len(first.Artifacts) != policy.Sharding.Count {
		t.Fatalf("expected %d artifacts, got %d", policy.Sharding.Count, len(first.Artifacts))
	}
	for i := range first.Artifacts {
		if first.Artifacts[i].Digest != second.Artifacts[i].Digest {
			t.Fatalf("shard %d digest differs", i)
		}
		if !bytes.Equal(first.Artifacts[i].Bytes, second.Artifacts[i].Bytes) {
			t.Fatalf("shard %d bytes differ", i)
		}
	}

	assigned := len(first.Assignments)
	fromStats := 0
	for _, stat := range first.ShardStats {
		fromStats += stat.RecordCount
	}
	if assigned != fromStats {
		t.Fatalf("assignment count %d != shard stat count %d", assigned, fromStats)
	}
	seen := make(map[string]struct{}, assigned)
	for _, assignment := range first.Assignments {
		key := assignment.Record.ID
		if _, exists := seen[key]; exists {
			t.Fatalf("record %s assigned more than once", key)
		}
		seen[key] = struct{}{}
		if assignment.ShardIndex < 0 || assignment.ShardIndex >= policy.Sharding.Count {
			t.Fatalf("invalid shard index %d", assignment.ShardIndex)
		}
	}
	if len(seen) != assigned {
		t.Fatalf("shard assignment lost records: seen=%d assigned=%d", len(seen), assigned)
	}
}

func TestSampleRatioBoundaries(t *testing.T) {
	policy := DefaultPolicy()
	raw := GenerateRawInput(GenerateSpec{Count: 32, Namespace: "ratio"})
	parse, err := RunParseClean(raw, policy)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := RunFilter(parse, policy)
	if err != nil {
		t.Fatal(err)
	}
	shards, err := RunShard(filter, policy)
	if err != nil {
		t.Fatal(err)
	}

	zeroPolicy := policy
	zeroPolicy.SampleRatio = 0
	zero, err := RunSample(shards, zeroPolicy)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range zero.Records {
		if record.Selected {
			t.Fatal("ratio=0 selected a record")
		}
	}

	onePolicy := policy
	onePolicy.SampleRatio = 1
	one, err := RunSample(shards, onePolicy)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range one.Records {
		if !record.Selected {
			t.Fatal("ratio=1 did not select a record")
		}
	}
}
