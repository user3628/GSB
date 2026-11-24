package sample

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	ledger "ledgerworkbench"
)

const FixedSeed int64 = 20261125

type Manifest struct {
	Seed       int64             `json:"seed"`
	BaseTimeMS int64             `json:"base_time_ms"`
	Directory  string            `json:"-"`
	Files      map[string]string `json:"files"`
}

type Bundle struct {
	Manifest Manifest
}

func FixedBaseTimeMS() int64 {
	return time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC).UnixMilli()
}

func Generate(dir string, seed int64) (*Manifest, error) {
	if seed == 0 {
		seed = FixedSeed
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	base := FixedBaseTimeMS()
	rng := rand.New(rand.NewSource(seed))
	files := map[string]any{}

	snapshot := FixedSnapshot(base)
	files["snapshot.json"] = snapshot

	entries := FixedEntryBatch(base, rng)
	files["entries.json"] = entries

	migrations := FixedMigrations(base)
	files["migrations.json"] = migrations

	injections := FixedInjectionParameters(base)
	files["injection_parameters.json"] = injections

	probeRequests := FixedProbeRequests()
	files["probe_requests.json"] = probeRequests

	manifest := Manifest{Seed: seed, BaseTimeMS: base, Directory: dir, Files: map[string]string{}}
	for name, value := range files {
		path := filepath.Join(dir, name)
		buf, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}
		buf = append(buf, '\n')
		if err := os.WriteFile(path, buf, 0o644); err != nil {
			return nil, err
		}
		sum := sha256.Sum256(buf)
		manifest.Files[name] = hex.EncodeToString(sum[:])
	}
	manifestBuf, _ := json.MarshalIndent(manifest, "", "  ")
	manifestBuf = append(manifestBuf, '\n')
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifestBuf, 0o644); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func FixedSnapshot(base int64) ledger.WeightSnapshot {
	snapshot := ledger.WeightSnapshot{
		CommitID:        "commit-snapshot-v1",
		SnapshotVersion: 1,
		GeneratedAtMS:   base,
		Weights: map[string]map[string]int{
			"route-balanced": {"stable": 700, "canary": 300},
			"route-migrate":  {"v1": 800, "v2": 200},
			"route-probe":    {"owner": 1000},
			"route-race":     {"v1": 1000, "v2": 0},
		},
	}
	snapshot.Digest = ledger.SnapshotDigest(snapshot)
	return snapshot
}

func FixedEntryBatch(base int64, rng *rand.Rand) ledger.EntryBatch {
	if rng == nil {
		rng = rand.New(rand.NewSource(FixedSeed))
	}
	entries := []ledger.LedgerEntry{}
	addBalanced := func(version string, count int) {
		for i := 0; i < count; i++ {
			entries = append(entries, ledger.LedgerEntry{
				TimestampMS:      base + 10_000 + int64(len(entries))*19,
				RouteKey:         "route-balanced",
				ClientID:         deterministicClient(rng),
				HitVersion:       version,
				Candidates:       []string{"canary", "stable"},
				CandidateWeights: map[string]int{"stable": 700, "canary": 300},
				Weights:          map[string]int{"stable": 700, "canary": 300},
				RequestWeight:    100,
			})
		}
	}
	for i := 0; i < 70; i++ {
		addBalanced("stable", 1)
	}
	for i := 0; i < 30; i++ {
		addBalanced("canary", 1)
	}

	migrationStart := base + 60_000
	for i := 0; i < 50; i++ {
		entries = append(entries, migrationEntry(migrationStart, i, "v1", 500, 500, rng))
	}
	for i := 0; i < 50; i++ {
		entries = append(entries, migrationEntry(migrationStart, 50+i, "v2", 500, 500, rng))
	}

	for i := 0; i < 12; i++ {
		entries = append(entries, ledger.LedgerEntry{
			TimestampMS:      base + 20_000 + int64(i)*23,
			RouteKey:         "route-probe",
			ClientID:         deterministicClient(rng),
			HitVersion:       "owner",
			Candidates:       []string{"owner"},
			CandidateWeights: map[string]int{"owner": 1000},
			Weights:          map[string]int{"owner": 1000},
			RequestWeight:    100,
		})
	}

	return ledger.EntryBatch{CommitID: "commit-entries-fixed", Entries: entries}
}

func migrationEntry(start int64, index int, hit string, v1Weight, v2Weight int, rng *rand.Rand) ledger.LedgerEntry {
	return ledger.LedgerEntry{
		TimestampMS:      start + 5_000 + int64(index)*17,
		RouteKey:         "route-migrate",
		ClientID:         deterministicClient(rng),
		HitVersion:       hit,
		Candidates:       []string{"v1", "v2"},
		CandidateWeights: map[string]int{"v1": v1Weight, "v2": v2Weight},
		Weights:          map[string]int{"v1": v1Weight, "v2": v2Weight},
		RequestWeight:    100,
	}
}

func deterministicClient(rng *rand.Rand) string {
	return "client-" + pad3(rng.Intn(24))
}

func pad3(value int) string {
	if value < 10 {
		return "00" + itoa(value)
	}
	if value < 100 {
		return "0" + itoa(value)
	}
	return itoa(value)
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[i:])
}

type MigrationSet struct {
	Valid      ledger.MigrationFragment `json:"valid"`
	RaceFirst  ledger.MigrationFragment `json:"race_first"`
	RaceSecond ledger.MigrationFragment `json:"race_second"`
	StaleBase  ledger.MigrationFragment `json:"stale_base"`
	DuringMS   int64                    `json:"during_ms"`
	AfterMS    int64                    `json:"after_ms"`
}

func FixedMigrations(base int64) MigrationSet {
	start := base + 60_000
	return MigrationSet{
		Valid: ledger.MigrationFragment{
			CommitID: "commit-migration-valid", MigrationID: "mig-route-migrate-v1-v2",
			BaseVersion: 1, RouteKey: "route-migrate", FromVersion: "v1", ToVersion: "v2",
			RatioPermille: 300, StartAtMS: start, TransitionMS: 60_000,
		},
		RaceFirst: ledger.MigrationFragment{
			CommitID: "commit-race-first", MigrationID: "mig-race-a",
			BaseVersion: 1, RouteKey: "route-race", FromVersion: "v1", ToVersion: "v2",
			RatioPermille: 600, StartAtMS: start, TransitionMS: 100_000,
		},
		RaceSecond: ledger.MigrationFragment{
			CommitID: "commit-race-second", MigrationID: "mig-race-b",
			BaseVersion: 2, RouteKey: "route-race", FromVersion: "v1", ToVersion: "v2",
			RatioPermille: 300, StartAtMS: start + 10_000, TransitionMS: 100_000,
		},
		StaleBase: ledger.MigrationFragment{
			CommitID: "commit-stale-base", MigrationID: "mig-stale",
			BaseVersion: 1, RouteKey: "route-balanced", FromVersion: "stable", ToVersion: "canary",
			RatioPermille: 100, StartAtMS: start, TransitionMS: 10_000,
		},
		DuringMS: start + 20_000,
		AfterMS:  start + 120_000,
	}
}

type InjectionParameters struct {
	LedgerInvalidSum      ledger.EntryBatch        `json:"ledger_invalid_sum"`
	MigrationRatioInvalid ledger.MigrationFragment `json:"migration_ratio_out_of_range"`
	VersionGapSnapshot    ledger.WeightSnapshot    `json:"version_gap_snapshot"`
	ProbeRefused          ledger.ProbeRequest      `json:"probe_refused"`
	ProbeTimeout          ledger.ProbeRequest      `json:"probe_timeout"`
	ProbeMismatch         ledger.ProbeRequest      `json:"probe_mismatch"`
	ProbeLateResponse     ledger.ProbeRequest      `json:"probe_late_response"`
	ExpectedBusinessCodes map[string]string        `json:"expected_business_codes"`
}

func FixedInjectionParameters(base int64) InjectionParameters {
	validEntries := FixedEntryBatch(base, rand.New(rand.NewSource(FixedSeed))).Entries
	invalidEntries := make([]ledger.LedgerEntry, 0, len(validEntries)+1)
	invalidEntries = append(invalidEntries, validEntries...)
	bad := validEntries[0]
	bad.ClientID = "injection-bad-sum"
	bad.Weights = map[string]int{"stable": 701, "canary": 300}
	bad.CandidateWeights = bad.Weights
	invalidEntries = append(invalidEntries, bad)

	gapSnapshot := FixedSnapshot(base)
	gapSnapshot.CommitID = "commit-version-gap"
	gapSnapshot.SnapshotVersion = 99
	gapSnapshot.Digest = ledger.SnapshotDigest(gapSnapshot)

	return InjectionParameters{
		LedgerInvalidSum: ledger.EntryBatch{CommitID: "commit-invalid-sum", Entries: invalidEntries},
		MigrationRatioInvalid: ledger.MigrationFragment{
			CommitID: "commit-invalid-ratio", MigrationID: "mig-invalid-ratio", BaseVersion: 1,
			RouteKey: "route-balanced", FromVersion: "stable", ToVersion: "canary",
			RatioPermille: 1500, StartAtMS: base + 60_000, TransitionMS: 10_000,
		},
		VersionGapSnapshot: gapSnapshot,
		ProbeRefused: ledger.ProbeRequest{
			BatchID: "probe-inject-refused", RouteKey: "route-probe", ExpectedOwner: "owner",
			TimeoutMS: 200, Injection: &ledger.ProbeInjection{Mode: ledger.ProbeModeRefused},
		},
		ProbeTimeout: ledger.ProbeRequest{
			BatchID: "probe-inject-timeout", RouteKey: "route-probe", BaseURL: "__PROBE_BASE_URL__",
			ExpectedOwner: "owner", TimeoutMS: 50,
			Injection: &ledger.ProbeInjection{Mode: ledger.ProbeModeTimeout},
		},
		ProbeMismatch: ledger.ProbeRequest{
			BatchID: "probe-inject-mismatch", RouteKey: "route-probe", BaseURL: "__PROBE_BASE_URL__",
			ExpectedOwner: "owner", TimeoutMS: 1000,
			Injection: &ledger.ProbeInjection{Mode: ledger.ProbeModeMismatch},
		},
		ProbeLateResponse: ledger.ProbeRequest{
			BatchID: "probe-inject-late", RouteKey: "route-probe", BaseURL: "__PROBE_BASE_URL__",
			ExpectedOwner: "owner", TimeoutMS: 1000, LateThresholdMS: 40,
			Injection: &ledger.ProbeInjection{Mode: ledger.ProbeModeLate, DelayMS: 120},
		},
		ExpectedBusinessCodes: map[string]string{
			"ledger_invalid_sum":           ledger.CodeLedgerInvalid,
			"migration_ratio_out_of_range": ledger.CodeMigrationRatioInvalid,
			"version_gap":                  ledger.CodeVersionGap,
			"probe_refused":                ledger.CodeProbeRefused,
			"probe_timeout":                ledger.CodeProbeTimeout,
			"probe_mismatch":               ledger.CodeProbeMismatch,
			"probe_late_response":          ledger.CodeProbeLate,
		},
	}
}

func FixedProbeRequests() map[string]ledger.ProbeRequest {
	return map[string]ledger.ProbeRequest{
		"ok": {
			BatchID: "probe-fixed-ok", RouteKey: "route-probe", BaseURL: "__PROBE_BASE_URL__",
			ExpectedOwner: "owner", TimeoutMS: 1000,
		},
		"timeout_slot_check": {
			BatchID: "probe-fixed-timeout-slot", RouteKey: "route-probe", BaseURL: "__PROBE_BASE_URL__",
			ExpectedOwner: "owner", TimeoutMS: 40,
			Injection: &ledger.ProbeInjection{Mode: ledger.ProbeModeTimeout},
		},
	}
}
