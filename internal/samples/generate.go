package samples

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"meshmigrate/internal/codec"
	"meshmigrate/internal/model"
)

const (
	LegacyBinary      = "v1-legacy.bin"
	CurrentJSONL      = "v2-current.jsonl"
	UnsupportedJSONL  = "v3-unknown-required.jsonl"
	DuplicateJSONL    = "v4-duplicate-reject.jsonl"
	CorruptCRCPath    = "corrupt/v1-crc.bin"
	CorruptLengthPath = "corrupt/v1-length.bin"
	CorruptMagicPath  = "corrupt/v1-record-magic.bin"
	ConflictManifest  = "conflicts.json"
	ChecksumsManifest = "checksums.json"
)

type Manifest struct {
	ManifestVersion int              `json:"manifestVersion"`
	Description     string           `json:"description"`
	Files           ManifestFiles    `json:"files"`
	Expectations    ManifestExpected `json:"expectations"`
}

type ManifestFiles struct {
	LegacyBinary     string `json:"legacyBinary"`
	CurrentJSONL     string `json:"currentJSONL"`
	UnsupportedJSONL string `json:"unsupportedJSONL"`
	DuplicateJSONL   string `json:"duplicateJSONL"`
	CorruptCRC       string `json:"corruptCRC"`
	CorruptLength    string `json:"corruptLength"`
	CorruptMagic     string `json:"corruptRecordMagic"`
}

type ManifestExpected struct {
	Legacy struct {
		SnapshotVersion uint64 `json:"snapshotVersion"`
		PolicyCount     int    `json:"policyCount"`
		Decision        string `json:"decision"`
	} `json:"legacy"`
	Current struct {
		SnapshotVersion   uint64            `json:"snapshotVersion"`
		Decision          string            `json:"decision"`
		Items             map[string]string `json:"items"`
		RetainedPolicyIDs []string          `json:"retainedPolicyIds"`
		RemovedPolicyIDs  []string          `json:"removedPolicyIds"`
	} `json:"current"`
	Unsupported struct {
		SnapshotVersion uint64   `json:"snapshotVersion"`
		Decision        string   `json:"decision"`
		UnknownFields   []string `json:"unknownFields"`
	} `json:"unsupported"`
	DuplicateBatch struct {
		SnapshotVersion uint64 `json:"snapshotVersion"`
		ErrorCode       string `json:"errorCode"`
	} `json:"duplicateBatch"`
	FinalActiveVersion uint64 `json:"finalActiveVersion"`
}

type checksumFile struct {
	Files map[string]string `json:"files"`
}

// Generate writes a deterministic sample set. It overwrites only the declared
// fixed files, never any database or endpoint state.
func Generate(outDir string) (*Manifest, error) {
	if err := os.MkdirAll(filepath.Join(outDir, "corrupt"), 0o755); err != nil {
		return nil, err
	}
	legacy := legacySnapshot()
	legacyBytes, err := codec.EncodeBinary(legacy, false)
	if err != nil {
		return nil, err
	}
	current := currentSnapshot()
	currentBytes, err := codec.EncodeJSONL(current)
	if err != nil {
		return nil, err
	}
	unsupported := unsupportedSnapshot()
	unsupportedBytes, err := codec.EncodeJSONL(unsupported)
	if err != nil {
		return nil, err
	}
	duplicate := duplicateSnapshot()
	duplicateBytes, err := codec.EncodeJSONL(duplicate)
	if err != nil {
		return nil, err
	}
	corruptCRC := append([]byte(nil), legacyBytes...)
	recordStart := binaryFirstRecordOffset(corruptCRC)
	corruptCRC[recordStart+13] ^= 0x01
	corruptLength := append([]byte(nil), legacyBytes...)
	for i := 0; i < 4; i++ {
		corruptLength[recordStart+8+i] = 0xff
	}
	corruptMagic := append([]byte(nil), legacyBytes...)
	corruptMagic[recordStart] ^= 0x20

	files := []struct {
		name string
		data []byte
	}{
		{LegacyBinary, legacyBytes},
		{CurrentJSONL, currentBytes},
		{UnsupportedJSONL, unsupportedBytes},
		{DuplicateJSONL, duplicateBytes},
		{CorruptCRCPath, corruptCRC},
		{CorruptLengthPath, corruptLength},
		{CorruptMagicPath, corruptMagic},
	}
	checksums := checksumFile{Files: make(map[string]string)}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(outDir, f.name), f.data, 0o644); err != nil {
			return nil, err
		}
		sum := sha256.Sum256(f.data)
		checksums.Files[filepath.ToSlash(f.name)] = "sha256:" + hex.EncodeToString(sum[:])
	}
	m := defaultManifest()
	if err := writeJSON(filepath.Join(outDir, ConflictManifest), m); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(outDir, ChecksumsManifest), checksums); err != nil {
		return nil, err
	}
	return m, nil
}

func defaultManifest() *Manifest {
	m := &Manifest{
		ManifestVersion: 1,
		Description:     "Deterministic loopback fixtures: legacy binary defaults, current JSONL conflict resolution, unsupported v3 and duplicate-reject batch.",
		Files: ManifestFiles{
			LegacyBinary: LegacyBinary, CurrentJSONL: CurrentJSONL, UnsupportedJSONL: UnsupportedJSONL,
			DuplicateJSONL: DuplicateJSONL, CorruptCRC: CorruptCRCPath, CorruptLength: CorruptLengthPath,
			CorruptMagic: CorruptMagicPath,
		},
	}
	m.Expectations.Legacy.SnapshotVersion = 1
	m.Expectations.Legacy.PolicyCount = 4
	m.Expectations.Legacy.Decision = "downgrade_accept"
	m.Expectations.Current.SnapshotVersion = 2
	m.Expectations.Current.Decision = "accept"
	m.Expectations.Current.Items = map[string]string{
		"checkout-rate":   "overwrite",
		"catalog-timeout": "overwrite",
		"legacy-report":   "skip",
		"cart-circuit":    "overwrite",
	}
	m.Expectations.Current.RetainedPolicyIDs = []string{"legacy-report"}
	m.Expectations.Current.RemovedPolicyIDs = []string{"remove-me"}
	m.Expectations.Unsupported.SnapshotVersion = 3
	m.Expectations.Unsupported.Decision = "reject"
	m.Expectations.Unsupported.UnknownFields = []string{"geoAffinity"}
	m.Expectations.DuplicateBatch.SnapshotVersion = 4
	m.Expectations.DuplicateBatch.ErrorCode = "E_BATCH_REJECTED"
	m.Expectations.FinalActiveVersion = 2
	return m
}

func legacySnapshot() model.ParsedSnapshot {
	return model.ParsedSnapshot{
		SnapshotVersion: 1,
		SnapshotID:      "legacy-binary-2026-02-01",
		GeneratedAtMs:   1760000000000,
		SourceFormat:    model.FormatBinary,
		FormatVersion:   1,
		Policies: []model.RawPolicy{
			{
				PolicyID: "checkout-rate", VirtualServiceID: "checkout",
				PolicyRevision: 1, LastWriteMs: 1000,
				Match:        model.RouteMatch{Method: "POST", PathPrefix: "/checkout", Headers: map[string]string{"x-tenant": "retail"}},
				TimeoutValue: 1500, TimeoutUnit: "ms",
				CapturedAtValue: 1760000000123, CapturedAtUnit: "ms",
				Presence: model.Presence{Timeout: true, CapturedAt: true},
			},
			{
				PolicyID: "catalog-timeout", VirtualServiceID: "catalog",
				PolicyRevision: 5, LastWriteMs: 3000,
				Match:          model.RouteMatch{Method: "GET", PathPrefix: "/catalog"},
				RateLimitValue: 120000, RateLimitUnit: "bps",
				TimeoutValue: 2000, TimeoutUnit: "ms",
				CircuitWindowVal: 25000, CircuitWindowUnit: "ms",
				RateWindowValue: 60000, RateWindowUnit: "ms",
				CapturedAtValue: 1760000000456, CapturedAtUnit: "ms",
				WindowAlignment: model.AlignmentDown,
				Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
			},
			{
				PolicyID: "legacy-report", VirtualServiceID: "reporting",
				PolicyRevision: 3, LastWriteMs: 4000,
				Match:          model.RouteMatch{Method: "POST", PathPrefix: "/reports"},
				RateLimitValue: 80000, RateLimitUnit: "bps",
				TimeoutValue: 1200, TimeoutUnit: "ms",
				CircuitWindowVal: 30000, CircuitWindowUnit: "ms",
				RateWindowValue: 60000, RateWindowUnit: "ms",
				CapturedAtValue: 1760000000789, CapturedAtUnit: "ms",
				WindowAlignment: model.AlignmentUp,
				Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
			},
			{
				PolicyID: "remove-me", VirtualServiceID: "retired",
				PolicyRevision: 1, LastWriteMs: 5000,
				Match:          model.RouteMatch{Method: "GET", PathPrefix: "/retired"},
				RateLimitValue: 10000, RateLimitUnit: "bps",
				TimeoutValue: 1000, TimeoutUnit: "ms",
				CircuitWindowVal: 15000, CircuitWindowUnit: "ms",
				RateWindowValue: 60000, RateWindowUnit: "ms",
				CapturedAtValue: 1760000000999, CapturedAtUnit: "ms",
				WindowAlignment: model.AlignmentDown,
				Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
			},
		},
	}
}

func currentSnapshot() model.ParsedSnapshot {
	return model.ParsedSnapshot{
		SnapshotVersion: 2,
		SnapshotID:      "current-jsonl-2026-02-02",
		GeneratedAtMs:   1760000100000,
		SourceFormat:    model.FormatJSONL,
		FormatVersion:   2,
		Policies: []model.RawPolicy{
			{
				PolicyID: "checkout-rate", VirtualServiceID: "checkout",
				PolicyRevision: 2, LastWriteMs: 6000,
				Match:          model.RouteMatch{Method: "POST", PathPrefix: "/checkout", Headers: map[string]string{"x-tenant": "retail"}},
				RateLimitValue: 250, RateLimitUnit: "kbps",
				TimeoutValue: 1750000, TimeoutUnit: "us",
				CircuitWindowVal: 45000000, CircuitWindowUnit: "us",
				RateWindowValue: 60000000, RateWindowUnit: "us",
				CapturedAtValue: 1760000100123456, CapturedAtUnit: "us",
				WindowAlignment: model.AlignmentUp,
				Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
			},
			{
				PolicyID: "catalog-timeout", VirtualServiceID: "catalog",
				PolicyRevision: 5, LastWriteMs: 7000,
				Match:          model.RouteMatch{Method: "GET", PathPrefix: "/catalog"},
				RateLimitValue: 90, RateLimitUnit: "kbps",
				TimeoutValue: 2500000, TimeoutUnit: "us",
				CircuitWindowVal: 20000000, CircuitWindowUnit: "us",
				RateWindowValue: 30000000, RateWindowUnit: "us",
				CapturedAtValue: 1760000100654321, CapturedAtUnit: "us",
				WindowAlignment: model.AlignmentDown,
				Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
			},
			{
				PolicyID: "legacy-report", VirtualServiceID: "reporting",
				PolicyRevision: 2, LastWriteMs: 8000,
				Match:          model.RouteMatch{Method: "POST", PathPrefix: "/reports/v2"},
				RateLimitValue: 42, RateLimitUnit: "kbps",
				TimeoutValue: 900000, TimeoutUnit: "us",
				CircuitWindowVal: 10000000, CircuitWindowUnit: "us",
				RateWindowValue: 15000000, RateWindowUnit: "us",
				CapturedAtValue: 1760000100888001, CapturedAtUnit: "us",
				WindowAlignment: model.AlignmentUp,
				Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
			},
			{
				PolicyID: "cart-circuit", VirtualServiceID: "cart",
				PolicyRevision: 1, LastWriteMs: 100,
				Match:          model.RouteMatch{Method: "POST", PathPrefix: "/cart"},
				RateLimitValue: 150, RateLimitUnit: "kbps",
				TimeoutValue: 900000, TimeoutUnit: "us",
				CircuitWindowVal: 15000000, CircuitWindowUnit: "us",
				RateWindowValue: 60000000, RateWindowUnit: "us",
				CapturedAtValue: 1760000100999999, CapturedAtUnit: "us",
				WindowAlignment: model.AlignmentUp,
				Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
			},
		},
	}
}

func unsupportedSnapshot() model.ParsedSnapshot {
	return model.ParsedSnapshot{
		SnapshotVersion:  3,
		SnapshotID:       "future-jsonl-2026-02-03",
		GeneratedAtMs:    1760000200000,
		SourceFormat:     model.FormatJSONL,
		FormatVersion:    3,
		RequiredFeatures: []string{"geoAffinity"},
		Policies: []model.RawPolicy{
			{
				PolicyID: "geo-route", VirtualServiceID: "edge",
				PolicyRevision: 1, LastWriteMs: 9000,
				Match:          model.RouteMatch{Method: "GET", PathPrefix: "/edge"},
				RateLimitValue: 100, RateLimitUnit: "kbps",
				TimeoutValue: 1000000, TimeoutUnit: "us",
				CircuitWindowVal: 15000000, CircuitWindowUnit: "us",
				RateWindowValue: 60000000, RateWindowUnit: "us",
				CapturedAtValue: 1760000200123456, CapturedAtUnit: "us",
				WindowAlignment: model.AlignmentUp,
				Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
				ExtraFields:     map[string]string{"geoAffinity": "region=cn-east"},
			},
		},
	}
}

func duplicateSnapshot() model.ParsedSnapshot {
	p := func(rev uint64, path string) model.RawPolicy {
		return model.RawPolicy{
			PolicyID: "cart-circuit", VirtualServiceID: "cart",
			PolicyRevision: rev, LastWriteMs: 10000 + int64(rev),
			Match:          model.RouteMatch{Method: "POST", PathPrefix: path},
			RateLimitValue: 160, RateLimitUnit: "kbps",
			TimeoutValue: 950000, TimeoutUnit: "us",
			CircuitWindowVal: 16000000, CircuitWindowUnit: "us",
			RateWindowValue: 60000000, RateWindowUnit: "us",
			CapturedAtValue: 1760000300123456, CapturedAtUnit: "us",
			WindowAlignment: model.AlignmentUp,
			Presence:        model.Presence{RateLimit: true, Timeout: true, CircuitWindow: true, RateWindow: true, CapturedAt: true, WindowAlignment: true},
		}
	}
	return model.ParsedSnapshot{
		SnapshotVersion: 4,
		SnapshotID:      "duplicate-reject-2026-02-04",
		GeneratedAtMs:   1760000300000,
		SourceFormat:    model.FormatJSONL,
		FormatVersion:   2,
		Policies:        []model.RawPolicy{p(2, "/cart/v2"), p(3, "/cart/v3")},
	}
}

func binaryFirstRecordOffset(data []byte) int {
	if len(data) < codec.BinaryHeaderLen {
		return 0
	}
	order := byteOrder(data[5])
	n := order.Uint32(data[28:32])
	return codec.BinaryHeaderLen + int(n)
}

func byteOrder(marker byte) interface {
	Uint32([]byte) uint32
} {
	if marker == codec.BinaryEndianBig {
		return bigOrder{}
	}
	return littleOrder{}
}

type littleOrder struct{}

func (littleOrder) Uint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

type bigOrder struct{}

func (bigOrder) Uint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func LoadManifest(dir string) (*Manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, ConflictManifest))
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode conflict manifest: %w", err)
	}
	return &m, nil
}

func SortedChecksums(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
