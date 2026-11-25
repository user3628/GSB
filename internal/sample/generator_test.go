package sample_test

import (
	"reflect"
	"testing"

	"leasearbiter/internal/sample"
)

func TestGeneratorIsDeterministic(t *testing.T) {
	first := sample.Generate(sample.DefaultSeed)
	second := sample.Generate(sample.DefaultSeed)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed produced different datasets")
	}
	firstSnapshot := sample.Snapshot(7, sample.DefaultSeed)
	secondSnapshot := sample.Snapshot(7, sample.DefaultSeed)
	if firstSnapshot.Digest != secondSnapshot.Digest {
		t.Fatalf("snapshot digest not deterministic: %s vs %s", firstSnapshot.Digest, secondSnapshot.Digest)
	}
}

func TestGeneratorIncludesDuplicateAndOutOfOrderArrival(t *testing.T) {
	dataset := sample.Generate(sample.DefaultSeed)
	seen := make(map[int64]bool)
	duplicates := 0
	outOfOrder := false
	lastArrival := int64(0)
	for _, record := range dataset.Observations {
		if record.ServerArrivalMS < record.ClientTimestampMS {
			t.Fatalf("server arrival %d precedes client timestamp %d", record.ServerArrivalMS, record.ClientTimestampMS)
		}
		if record.HolderID != "holder-alpha" {
			continue
		}
		if seen[record.ReportSeq] {
			duplicates++
		}
		seen[record.ReportSeq] = true
		if lastArrival > record.ServerArrivalMS {
			outOfOrder = true
		}
		lastArrival = record.ServerArrivalMS
	}
	if duplicates == 0 {
		t.Fatal("sample has no duplicate delivery")
	}
	if !outOfOrder {
		t.Fatal("sample has no out-of-order server arrival")
	}
}
