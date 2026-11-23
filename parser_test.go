package dnsdetect

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestThreeSourceParsers(t *testing.T) {
	dataset := testDataset(t)
	config := testConfig(dataset)
	for _, spec := range dataset.Sources {
		session, err := LoadSource(context.Background(), spec, LoadOptions{SourcePriorities: config.SourcePriorities})
		if err != nil {
			t.Fatalf("load %s: %v", spec.Path, err)
		}
		if len(session.Events) == 0 {
			t.Fatalf("%s produced no events", spec.Path)
		}
		if len(session.Ledger.Entries) != len(session.Events) {
			t.Fatalf("%s ledger entries=%d events=%d", spec.Path, len(session.Ledger.Entries), len(session.Events))
		}
		for _, event := range session.Events {
			if event.Offset < 0 {
				t.Fatalf("%s has negative offset", spec.Path)
			}
			if event.Question != strings.ToLower(strings.TrimSuffix(event.Question, ".")) {
				t.Fatalf("%s question not normalized: %q", spec.Path, event.Question)
			}
		}
		switch session.Kind {
		case SourceBinary:
			if session.FormatVersion != 1 && session.FormatVersion != 2 {
				t.Fatalf("binary format version = %d", session.FormatVersion)
			}
			if session.FormatVersion == 2 {
				if session.Events[0].SourceAddress != "192.0.2.53" || session.Events[0].ResolverID != "resolver-lab-1" {
					t.Fatalf("v2 metadata missing: %+v", session.Events[0])
				}
				for _, event := range session.Events {
					if event.TXID == 0x1001 && event.Role == RoleResponse && len(event.Records) == 1 && event.Records[0].TTL != 120 {
						t.Fatalf("v2 little-endian TTL decoded as %d", event.Records[0].TTL)
					}
				}
			} else if session.FormatVersion == 1 {
				for _, event := range session.Events {
					if event.TXID == 0x1002 && event.Role == RoleResponse && len(event.Records) == 1 && event.Records[0].TTL != 60 {
						t.Fatalf("v1 little-endian TTL decoded as %d", event.Records[0].TTL)
					}
				}
			}
		case SourceJSONL:
			if session.FormatVersion != 0 {
				t.Fatalf("JSON format version = %d", session.FormatVersion)
			}
		case SourceText:
			if session.Events[0].TXID != 0x1001 || session.Events[0].TimeErrorNS != 0 {
				t.Fatalf("text event = %+v", session.Events[0])
			}
		}
	}

	jsonSource := filepath.Join(dataset.Dir, "probe.jsonl")
	autoSession, err := LoadSource(context.Background(), SourceSpec{Path: jsonSource, Kind: SourceAuto}, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if autoSession.Kind != SourceJSONL {
		t.Fatalf("auto-detected kind = %s", autoSession.Kind)
	}
}

func TestMalformedSamplesBusinessCodes(t *testing.T) {
	dataset := testDataset(t)
	config := testConfig(dataset)
	for _, sample := range dataset.Malformed {
		if sample.ExpectedCode == CodeSnapshotVersion {
			continue
		}
		session, err := LoadSource(context.Background(), SourceSpec{Path: sample.Path, Kind: sample.Kind}, LoadOptions{})
		if sample.ExpectedCode == CodeTXIDQNameConflict {
			if err != nil {
				t.Fatalf("conflict sample should parse: %v", err)
			}
			_, err = Adjudicate(session.Events, config)
		}
		requireCode(t, err, sample.ExpectedCode)
		if OffsetOf(err) < 0 {
			t.Fatalf("%s error lacks offset: %v", sample.Name, err)
		}
	}
}

func TestSessionBudgets(t *testing.T) {
	dataset := testDataset(t)
	var binaryPath string
	for _, source := range dataset.Sources {
		if strings.HasSuffix(source.Path, "binary-v2.dnb") {
			binaryPath = source.Path
			break
		}
	}
	_, err := LoadSource(context.Background(), SourceSpec{Path: binaryPath, Kind: SourceBinary, ObservationStart: dataset.ObservationStartTicks}, LoadOptions{MaxEvents: 1})
	requireCode(t, err, CodeEventBudget)
	_, err = LoadSource(context.Background(), SourceSpec{Path: binaryPath, Kind: SourceBinary, ObservationStart: dataset.ObservationStartTicks}, LoadOptions{MaxSpanTicks: 100})
	requireCode(t, err, CodeWindowBudget)
}

func TestSnapshotGapSample(t *testing.T) {
	dataset := testDataset(t)
	store, err := OpenSnapshotStore(dataset.SnapshotGapDB)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	result, err := store.Load(context.Background())
	requireCode(t, err, CodeSnapshotVersion)
	if result.RejectedVersion != 2 || result.Latest == nil || result.Latest.Version != 1 {
		t.Fatalf("gap fallback = rejected %d latest %+v", result.RejectedVersion, result.Latest)
	}
}

func TestGeneratorSourceFilesAreDeterministic(t *testing.T) {
	first, err := GenerateDataset(t.TempDir() + "/first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateDataset(t.TempDir() + "/second")
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"binary-v1.dnb", "binary-v2.dnb", "probe.jsonl", "legacy.txt"}
	for _, name := range names {
		firstBytes, err := os.ReadFile(filepath.Join(first.Dir, name))
		if err != nil {
			t.Fatal(err)
		}
		secondBytes, err := os.ReadFile(filepath.Join(second.Dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(firstBytes, secondBytes) {
			t.Fatalf("generated source %s is not deterministic", name)
		}
	}
	for _, malformed := range first.Malformed {
		if strings.HasSuffix(malformed.Name, ".db") {
			continue
		}
		matchingPath := filepath.Join(second.Dir, "malformed", malformed.Name)
		firstBytes, _ := os.ReadFile(malformed.Path)
		secondBytes, err := os.ReadFile(matchingPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(firstBytes, secondBytes) {
			t.Fatalf("malformed source %s is not deterministic", malformed.Name)
		}
	}
}
