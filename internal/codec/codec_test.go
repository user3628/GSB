package codec_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/codec"
	"example.com/logicplan/editor/internal/fixtures"
	"example.com/logicplan/editor/internal/model"
)

func TestBinaryRoundTripLittleAndBigEndian(t *testing.T) {
	for _, order := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
		t.Run(fmt.Sprintf("%T", order), func(t *testing.T) {
			snapshot := fixtures.Current(2, 201)
			var buf bytes.Buffer
			if err := codec.EncodeBinary(&buf, snapshot, codec.BinaryOptions{ByteOrder: order}); err != nil {
				t.Fatal(err)
			}
			header, err := codec.Inspect(buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if header.Format != codec.FormatBinary || header.FormatVersion != 2 || header.SnapshotSeq != 201 {
				t.Fatalf("unexpected header: %+v", header)
			}
			decoded, err := codec.DecodeBinary(buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := model.Analyze(decoded.Nodes); err != nil {
				t.Fatal(err)
			}
			if len(decoded.Nodes) != len(snapshot.Nodes) {
				t.Fatalf("node count = %d, want %d", len(decoded.Nodes), len(snapshot.Nodes))
			}
		})
	}
}

func TestBinaryCRCErrorCarriesRecordAndChecksum(t *testing.T) {
	var buf bytes.Buffer
	if err := codec.EncodeBinary(&buf, fixtures.Current(2, 201), codec.BinaryOptions{}); err != nil {
		t.Fatal(err)
	}
	data := append([]byte(nil), buf.Bytes()...)
	recordLength := int(binary.LittleEndian.Uint32(data[26:30]))
	payloadStart := 30
	if recordLength < 10 {
		t.Fatalf("unexpected record length %d", recordLength)
	}
	data[payloadStart+5] ^= 0xff
	_, err := codec.DecodeBinary(data)
	if err == nil {
		t.Fatal("expected CRC failure")
	}
	be, ok := bizerr.As(err)
	if !ok || be.Code != bizerr.CodeFormatCorrupt {
		t.Fatalf("error = %v, want %s", err, bizerr.CodeFormatCorrupt)
	}
	if be.Context["record_seq"] != 1 {
		t.Fatalf("record_seq = %#v, want 1", be.Context["record_seq"])
	}
	if be.Context["crc32"] == "" || be.Context["crc32"] == nil {
		t.Fatalf("missing crc32 context: %#v", be.Context)
	}
}

func TestJSONLRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := codec.EncodeJSONL(&buf, fixtures.Current(2, 200), codec.JSONLOptions{StatsSeparate: true}); err != nil {
		t.Fatal(err)
	}
	header, err := codec.Inspect(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if header.Format != codec.FormatJSONL || header.FormatVersion != 2 || header.SnapshotSeq != 200 {
		t.Fatalf("unexpected header: %+v", header)
	}
	decoded, err := codec.DecodeJSONL(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := model.Analyze(decoded.Nodes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Root != "sort_amount" {
		t.Fatalf("root = %q", plan.Root)
	}
}

func TestLegacySamplesDegradeDeterministically(t *testing.T) {
	for _, name := range []string{"plan_v1.bin", "plan_v1.jsonl"} {
		t.Run(name, func(t *testing.T) {
			data := mustReadSample(t, name)
			header, err := codec.Inspect(data)
			if err != nil {
				t.Fatal(err)
			}
			compat, err := codec.Negotiate(header.Format, header.FormatVersion)
			if err != nil {
				t.Fatal(err)
			}
			if compat.Decision != codec.DecisionDegrade {
				t.Fatalf("decision = %s, want degrade", compat.Decision)
			}
			var snapshot *model.Snapshot
			if header.Format == codec.FormatBinary {
				snapshot, err = codec.DecodeBinary(data)
			} else {
				snapshot, err = codec.DecodeJSONL(data)
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Degradations) != 8 {
				t.Fatalf("degradations = %d, want 8", len(snapshot.Degradations))
			}
			for _, node := range snapshot.Nodes {
				if node.Stats == nil || node.Stats.RowCount != 0 {
					t.Fatalf("node %s has no default stats: %#v", node.ID, node.Stats)
				}
				if node.Columns == nil || len(node.Columns) != 0 {
					t.Fatalf("node %s columns = %#v, want empty non-nil", node.ID, node.Columns)
				}
			}
		})
	}
}

func TestLegacyBinaryMatchesExpectedDegradationDetails(t *testing.T) {
	var expected struct {
		Count        int                 `json:"count"`
		Decision     string              `json:"decision"`
		Degradations []model.Degradation `json:"degradations"`
	}
	if err := json.Unmarshal(mustReadSample(t, "plan_v1_degradations.expected.json"), &expected); err != nil {
		t.Fatal(err)
	}
	snapshot, err := codec.DecodeBinary(mustReadSample(t, "plan_v1.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if expected.Count != len(snapshot.Degradations) || expected.Decision != "degrade" {
		t.Fatalf("expected summary count=%d, got=%d", expected.Count, len(snapshot.Degradations))
	}
	wantJSON, _ := json.Marshal(expected.Degradations)
	gotJSON, _ := json.Marshal(snapshot.Degradations)
	if !bytes.Equal(wantJSON, gotJSON) {
		t.Fatalf("degradation details differ\nwant: %s\ngot:  %s", wantJSON, gotJSON)
	}
}

func TestHighVersionIsRejected(t *testing.T) {
	for _, name := range []string{"plan_v3_required.jsonl", "plan_v3_required.bin"} {
		t.Run(name, func(t *testing.T) {
			data := mustReadSample(t, name)
			header, err := codec.Inspect(data)
			if err != nil {
				t.Fatal(err)
			}
			compat, err := codec.Negotiate(header.Format, header.FormatVersion)
			if err != nil {
				t.Fatal(err)
			}
			if compat.Decision != codec.DecisionReject {
				t.Fatalf("matrix decision = %s, want reject", compat.Decision)
			}
			if len(header.RequiredFeatures) > 0 {
				if err := codec.RejectUnknownRequired(header.Format, header.FormatVersion, header.RequiredFeatures); err == nil {
					t.Fatal("expected unsupported error")
				} else if be, _ := bizerr.As(err); be == nil || be.Code != bizerr.CodeFormatUnsupported {
					t.Fatalf("error = %v, want %s", err, bizerr.CodeFormatUnsupported)
				}
			}
		})
	}
}

func TestJSONLCorruptionReportsPhysicalLine(t *testing.T) {
	data := []byte("{\"record\":\"snapshot\",\"format\":\"jsonl\",\"format_version\":2,\"snapshot_seq\":1,\"generated_at\":\"2026-11-20T08:00:00Z\"}\n{\"record\":\"operator\",bad}\n")
	_, err := codec.DecodeJSONL(data)
	if err == nil {
		t.Fatal("expected corrupt JSONL error")
	}
	be, _ := bizerr.As(err)
	if be == nil || be.Code != bizerr.CodeFormatCorrupt || be.Context["record_seq"] != 2 {
		t.Fatalf("unexpected error: %#v", be)
	}
}

func mustReadSample(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "samples", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
