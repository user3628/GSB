package spec_test

import (
	"testing"

	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
	"recoveryengine/samples"
)

func loadValidSnapshot(t *testing.T) model.Snapshot {
	t.Helper()
	data, err := samples.Read("full-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, business := spec.DecodeSnapshot(data)
	if business != nil {
		t.Fatal(business)
	}
	normalized, business := spec.NormalizeSnapshot(raw, true)
	if business != nil {
		t.Fatal(business)
	}
	return normalized.Snapshot
}

func TestNormalizeSampleAndChecksum(t *testing.T) {
	snapshot := loadValidSnapshot(t)
	if snapshot.Version != 1 || len(snapshot.Components) != 9 || len(snapshot.Edges) != 8 {
		t.Fatalf("unexpected canonical sample: version=%d components=%d edges=%d", snapshot.Version, len(snapshot.Components), len(snapshot.Edges))
	}
	if snapshot.ChecksumSHA256 != spec.ComputeChecksum(snapshot) {
		t.Fatal("canonical checksum is not reproducible")
	}
}

func TestDuplicateComponentKeepsFirstAndRecordsDetail(t *testing.T) {
	snapshot := loadValidSnapshot(t)
	raw := snapshot.Clone()
	raw.ChecksumSHA256 = ""
	duplicate := raw.Components[2]
	duplicate.ID = "cache"
	duplicate.Resources = model.Resource{CPUMillicores: 1, MemoryBytes: 1}
	raw.Components = append(raw.Components, duplicate)
	withoutChecksum, business := spec.NormalizeSnapshot(raw, false)
	if business != nil {
		t.Fatal(business)
	}
	raw.ChecksumSHA256 = withoutChecksum.Snapshot.ChecksumSHA256
	normalized, business := spec.NormalizeSnapshot(raw, true)
	if business != nil {
		t.Fatal(business)
	}
	if len(normalized.Duplicates) != 1 || normalized.Duplicates[0].ID != "cache" {
		t.Fatalf("expected one cache duplicate, got %#v", normalized.Duplicates)
	}
	var cache model.Component
	for _, component := range normalized.Snapshot.Components {
		if component.ID == "cache" {
			cache = component
		}
	}
	if cache.Resources.CPUMillicores != 2000 {
		t.Fatalf("first component was not retained: %#v", cache.Resources)
	}
}

func TestInvalidResourcesAndMissingDependency(t *testing.T) {
	cases := []string{"invalid-negative-resource.json", "invalid-missing-dependency.json", "invalid-weight.json"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			data, err := samples.Read(name)
			if err != nil {
				t.Fatal(err)
			}
			raw, business := spec.DecodeSnapshot(data)
			if business != nil {
				if business.Code != model.CodeSpecInvalid {
					t.Fatalf("unexpected code: %s", business.Code)
				}
				return
			}
			_, business = spec.NormalizeSnapshot(raw, true)
			if business == nil || business.Code != model.CodeSpecInvalid {
				t.Fatalf("expected E_SPEC_INVALID, got %#v", business)
			}
		})
	}
}

func TestBoundaryWeightSnapshotIsAccepted(t *testing.T) {
	data, err := samples.Read("boundary-weight-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	raw, business := spec.DecodeSnapshot(data)
	if business != nil {
		t.Fatal(business)
	}
	normalized, business := spec.NormalizeSnapshot(raw, true)
	if business != nil {
		t.Fatal(business)
	}
	component := normalized.Snapshot.Components[0]
	if component.HealthWeightPermille != 0 || component.ResourceWeightPermille != 1000 || component.ConsistencyWeightPermille != 1000 {
		t.Fatalf("boundary weights were not preserved: %#v", component)
	}
	if component.Resources.CPUMillicores != 0 || component.Resources.MemoryBytes != 0 {
		t.Fatalf("zero resource boundary was not preserved: %#v", component.Resources)
	}
}

func TestOverflowResourceIsRejectedDuringDecode(t *testing.T) {
	data, err := samples.Read("invalid-overflow-resource.json")
	if err != nil {
		t.Fatal(err)
	}
	_, business := spec.DecodeSnapshot(data)
	if business == nil || business.Code != model.CodeSpecInvalid {
		t.Fatalf("expected E_SPEC_INVALID from overflow decode, got %#v", business)
	}
}

func TestApplyIncrementProducesChangedRefs(t *testing.T) {
	snapshot := loadValidSnapshot(t)
	data, err := samples.Read("increments/resource-adjust.json")
	if err != nil {
		t.Fatal(err)
	}
	request, business := spec.DecodeIncrement(data)
	if business != nil {
		t.Fatal(business)
	}
	candidate, refs, business := spec.ApplyIncrement(snapshot, request, 2)
	if business != nil {
		t.Fatal(business)
	}
	if candidate.Version != 2 || len(refs) != 2 {
		t.Fatalf("unexpected candidate/refs: version=%d refs=%#v", candidate.Version, refs)
	}
	var api model.Component
	for _, component := range candidate.Components {
		if component.ID == "api" {
			api = component
		}
	}
	if api.Resources.CPUMillicores != 3000 {
		t.Fatalf("api resource patch was not applied: %#v", api.Resources)
	}
}
