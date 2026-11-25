package sample

import (
	"bytes"
	"encoding/json"
	"math/rand"

	"auditdrift/internal/model"
	"auditdrift/internal/normalize"
)

const DefaultSeed int64 = 20261125

type SampleSet struct {
	Seed          int64                    `json:"seed"`
	Hosts         []string                 `json:"hosts"`
	Baselines     []model.Baseline         `json:"baselines"`
	Primary       []model.Observation      `json:"primary_observations"`
	Observations  []model.Observation      `json:"observations"`
	JSONL         []byte                   `json:"-"`
	LegacyBlock   []byte                   `json:"-"`
	LegacyRecords []normalize.LegacyRecord `json:"-"`
}

func FixedSet() *SampleSet {
	return Generate(DefaultSeed)
}

func Generate(seed int64) *SampleSet {
	rng := rand.New(rand.NewSource(seed))
	hosts := []string{"host-a", "host-b", "host-c"}
	baselines := fixedBaselines()
	primary := make([]model.Observation, 0, len(baselines)*len(hosts))
	all := make([]model.Observation, 0, len(baselines)*len(hosts)+1)
	legacy := make([]normalize.LegacyRecord, 0, cap(all))

	baseMS := int64(1_764_000_000_000)
	recordIndex := 0
	for _, baseline := range baselines {
		for hostIndex, host := range hosts {
			value := actualFor(baseline.ID, hostIndex)
			ms := baseMS + int64(recordIndex)*60_000
			obs := model.Observation{Host: host, BaselineID: baseline.ID, Actual: value, SampledAtMS: ms}
			primary = append(primary, obs)
			all = append(all, obs)
			remainder := int64(rng.Intn(1000))
			legacy = append(legacy, normalize.LegacyRecord{Observation: obs, TimestampMicros: ms*1000 + remainder})
			recordIndex++
		}
	}

	conflict := model.Observation{
		Host: "host-c", BaselineID: "SVC_AUDITD_START",
		Actual:      model.Value{Kind: model.KindString, Text: "Disabled"},
		SampledAtMS: baseMS + int64(recordIndex)*60_000,
	}
	all = append(all, conflict)
	legacy = append(legacy, normalize.LegacyRecord{Observation: conflict, TimestampMicros: conflict.SampledAtMS*1000 + int64(rng.Intn(1000)), Collector: "legacy-agent"})

	var jsonl bytes.Buffer
	enc := json.NewEncoder(&jsonl)
	for _, obs := range all {
		_ = enc.Encode(obs)
	}
	block, _ := normalize.EncodeLegacyBlock(legacy)

	return &SampleSet{
		Seed: seed, Hosts: hosts, Baselines: baselines, Primary: primary,
		Observations: all, JSONL: jsonl.Bytes(), LegacyBlock: block, LegacyRecords: legacy,
	}
}

func fixedBaselines() []model.Baseline {
	return []model.Baseline{
		{
			ID: "ACCT_PWD_MINLEN", Category: model.CategoryAccountPolicy, SeverityWeight: 5,
			Expected: model.Expectation{Interval: &model.Interval{Min: 12, Max: 24, MinInclusive: true, MaxInclusive: true}},
		},
		{
			ID: "ACCT_LOCKOUT_THRESHOLD", Category: model.CategoryAccountPolicy, SeverityWeight: 4,
			Expected: model.Expectation{Interval: &model.Interval{Min: 3, Max: 5, MinInclusive: true, MaxInclusive: true}},
		},
		{
			ID: "AUDIT_LOGON_LEVEL", Category: model.CategoryAuditPolicy, SeverityWeight: 7,
			Expected:  model.Expectation{Interval: &model.Interval{Min: 1, Max: 3, MinInclusive: true, MaxInclusive: true}},
			DependsOn: []string{"ACCT_LOCKOUT_THRESHOLD"},
		},
		{
			ID: "AUDIT_OBJECT_ACCESS", Category: model.CategoryAuditPolicy, SeverityWeight: 6,
			Expected:  model.Expectation{Interval: &model.Interval{Min: 2, Max: 4, MinInclusive: true, MaxInclusive: false}},
			DependsOn: []string{"AUDIT_LOGON_LEVEL"},
		},
		{
			ID: "SVC_SSH_START", Category: model.CategoryServiceStartup, SeverityWeight: 8,
			Expected: model.Expectation{ExpectedText: "Automatic", AllowedEnum: []string{"Automatic", "Manual", "Disabled"}},
		},
		{
			ID: "SVC_AUDITD_START", Category: model.CategoryServiceStartup, SeverityWeight: 5,
			Expected:  model.Expectation{ExpectedText: "Automatic", AllowedEnum: []string{"Automatic", "Manual", "Disabled"}},
			DependsOn: []string{"AUDIT_OBJECT_ACCESS"},
		},
		{
			ID: "REG_SAM_ACL", Category: model.CategoryRegistryACL, SeverityWeight: 9,
			Expected: model.Expectation{RequiredSet: []string{"BUILTIN\\Administrators:Full", "NT AUTHORITY\\SYSTEM:Full"}},
		},
		{
			ID: "REG_AUDIT_ACL", Category: model.CategoryRegistryACL, SeverityWeight: 6,
			Expected:  model.Expectation{RequiredSet: []string{"NT AUTHORITY\\SYSTEM:Full", "BUILTIN\\Administrators:Read"}},
			DependsOn: []string{"SVC_AUDITD_START"},
		},
	}
}

func actualFor(baselineID string, hostIndex int) model.Value {
	switch baselineID {
	case "ACCT_PWD_MINLEN":
		return numberValue([]int64{14, 8, 26}[hostIndex])
	case "ACCT_LOCKOUT_THRESHOLD":
		return numberValue([]int64{4, 2, 6}[hostIndex])
	case "AUDIT_LOGON_LEVEL":
		return numberValue([]int64{2, 0, 4}[hostIndex])
	case "AUDIT_OBJECT_ACCESS":
		return numberValue([]int64{2, 3, 4}[hostIndex])
	case "SVC_SSH_START":
		return stringValue([]string{"Automatic", "Manual", "Disabled"}[hostIndex])
	case "SVC_AUDITD_START":
		return stringValue([]string{"Automatic", "Manual", "Manual"}[hostIndex])
	case "REG_SAM_ACL":
		sets := [][]string{
			{"BUILTIN\\Administrators:Full", "NT AUTHORITY\\SYSTEM:Full"},
			{"BUILTIN\\Administrators:Full"},
			{"BUILTIN\\Administrators:Full", "NT AUTHORITY\\SYSTEM:Full", "Everyone:Read"},
		}
		return setValue(sets[hostIndex])
	case "REG_AUDIT_ACL":
		sets := [][]string{
			{"NT AUTHORITY\\SYSTEM:Full", "BUILTIN\\Administrators:Read"},
			{"NT AUTHORITY\\SYSTEM:Full", "Everyone:Read"},
			{"BUILTIN\\Administrators:Read"},
		}
		return setValue(sets[hostIndex])
	default:
		return model.Value{Kind: model.KindString, Text: "unknown"}
	}
}

func numberValue(v int64) model.Value {
	return model.Value{Kind: model.KindNumber, Number: v}
}

func stringValue(v string) model.Value {
	return model.Value{Kind: model.KindString, Text: v}
}

func setValue(items []string) model.Value {
	return model.CanonicalValue(model.Value{Kind: model.KindSet, Set: items})
}
