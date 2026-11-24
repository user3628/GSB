package model

type SampleSet struct {
	SnapshotV1  *Policy
	IncrementV2 *Increment
	IncrementV3 *Increment
	IncrementV4 *Increment
}

const (
	sampleTime1 = "2026-11-25T00:00:01Z"
	sampleTime2 = "2026-11-25T00:00:02Z"
	sampleTime3 = "2026-11-25T00:00:03Z"
	sampleTime4 = "2026-11-25T00:00:04Z"
)

func DeterministicSample() SampleSet {
	snapshot := &Policy{
		Version:     1,
		GeneratedAt: sampleTime1,
		Subjects: []Subject{
			{ID: "u-api", Type: SubjectUser, Priority: 100},
			{ID: "svc-indexer", Type: SubjectService, Priority: 80},
			{ID: "u-blocked", Type: SubjectUser, Priority: 10},
			{ID: "g-readers", Type: SubjectGroup, Priority: 50},
			{ID: "g-ops", Type: SubjectGroup, Priority: 60, Members: []string{"g-readers"}},
			{ID: "g-platform", Type: SubjectGroup, Priority: 40, Members: []string{"g-ops"}},
		},
		Paths: []ManagedPath{
			{Path: "apps", Kind: "directory", ACL: []ACE{
				{SubjectID: "g-platform", Effect: "allow", Rights: []string{"read"}},
				{SubjectID: "u-api", Effect: "allow", Rights: []string{"read", "write"}},
			}},
			{Path: "apps/api", Kind: "directory", Inherit: true, ACL: []ACE{
				{SubjectID: "u-api", Effect: "allow", Rights: []string{"write"}},
				{SubjectID: "svc-indexer", Effect: "allow", Rights: []string{"read", "write"}},
			}},
			{Path: "apps/api/config.json", Kind: "file", Inherit: true, ACL: []ACE{
				{SubjectID: "g-readers", Effect: "allow", Rights: []string{"read"}},
				{SubjectID: "svc-indexer", Effect: "allow", Rights: []string{"write"}},
				{SubjectID: "u-blocked", Effect: "deny", Rights: []string{"read", "write"}},
			}},
			{Path: "apps/api/deep/state.json", Kind: "file", Inherit: true, ACL: []ACE{
				{SubjectID: "svc-indexer", Effect: "allow", Rights: []string{"read"}},
			}},
			{Path: "data", Kind: "directory", ACL: []ACE{
				{SubjectID: "u-api", Effect: "allow", Rights: []string{"write"}},
				{SubjectID: "g-platform", Effect: "allow", Rights: []string{"read"}},
			}},
			{Path: "data/private.json", Kind: "file", ACL: []ACE{
				{SubjectID: "u-blocked", Effect: "allow", Rights: []string{"read"}},
				{SubjectID: "u-blocked", Effect: "deny", Rights: []string{"write"}},
				{SubjectID: "g-ops", Effect: "allow", Rights: []string{"read"}},
				{SubjectID: "svc-indexer", Effect: "allow", Rights: []string{"read", "write"}},
			}},
		},
	}
	snapshot.Digest = SnapshotDigest(snapshot)

	incrementV2 := &Increment{
		BaseVersion: 1,
		Version:     2,
		GeneratedAt: sampleTime2,
		SubjectChanges: []SubjectChange{
			{Op: "upsert", Subject: &Subject{ID: "g-readers", Type: SubjectGroup, Priority: 50, Members: []string{"u-api"}}},
		},
	}
	incrementV2.Digest = IncrementDigest(incrementV2)

	incrementV3 := &Increment{
		BaseVersion: 2,
		Version:     3,
		GeneratedAt: sampleTime3,
		PathChanges: []PathChange{
			{Op: "upsert", Path: &ManagedPath{Path: "apps/api", Kind: "directory", Inherit: false, ACL: []ACE{
				{SubjectID: "u-api", Effect: "allow", Rights: []string{"read", "write"}},
				{SubjectID: "svc-indexer", Effect: "allow", Rights: []string{"read", "write"}},
			}}},
		},
	}
	incrementV3.Digest = IncrementDigest(incrementV3)

	incrementV4 := &Increment{
		BaseVersion: 3,
		Version:     4,
		GeneratedAt: sampleTime4,
		PathChanges: []PathChange{
			{Op: "upsert", Path: &ManagedPath{Path: "data", Kind: "directory", ACL: []ACE{
				{SubjectID: "u-api", Effect: "allow", Rights: []string{"read", "write"}},
				{SubjectID: "g-platform", Effect: "allow", Rights: []string{"read"}},
			}}},
		},
	}
	incrementV4.Digest = IncrementDigest(incrementV4)

	return SampleSet{SnapshotV1: snapshot, IncrementV2: incrementV2, IncrementV3: incrementV3, IncrementV4: incrementV4}
}
