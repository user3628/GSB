package format

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mpckpt/internal/model"
)

func testManifest(version int) Manifest {
	masterDigest := strings.Repeat("a", 64)
	momentum := strings.Repeat("b", 64)
	pool := model.PoolPersistent
	if version == 1 {
		pool = ""
	}
	return Manifest{
		Format:             FormatBinary,
		Version:            version,
		LossScaleFixed:     8_000_000,
		LossScalePrecision: model.LossScalePrecisionFixed64,
		Shards: []model.Shard{
			{ID: "master-a", Round: 1, Precision: model.PrecisionFP32, SizeBytes: 128, Digest: masterDigest, Pool: pool, LastAccess: 10, MomentumDigest: momentum, Priority: 10},
			{ID: "copy-a", Round: 1, Precision: model.PrecisionFP16, SizeBytes: 64, Digest: strings.Repeat("c", 64), Pool: pool, LastAccess: 11, SourceDigest: masterDigest, MomentumDigest: momentum, Priority: 20},
		},
	}
}

func TestRoundTripBinaryAndJSONL(t *testing.T) {
	t.Run("binary-v2", func(t *testing.T) {
		manifest := testManifest(2)
		manifest.Format = FormatBinary
		raw, err := Encode(manifest)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := Decode(raw, FormatBinary)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if decoded.Version != 2 || decoded.Format != FormatBinary || len(decoded.Shards) != len(manifest.Shards) {
			t.Fatalf("unexpected decoded manifest: %+v", decoded)
		}
		for _, shard := range decoded.Shards {
			if shard.Pool == "" || shard.Round < 1 || shard.Digest == "" {
				t.Fatalf("decoded shard lost fields: %+v", shard)
			}
		}
	})
	t.Run("jsonl-v1-degraded-shape", func(t *testing.T) {
		manifest := testManifest(1)
		manifest.Format = FormatJSONL
		manifest.LossScalePrecision = model.LossScalePrecisionFloat32
		raw, err := Encode(manifest)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := Decode(raw, FormatJSONL)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if decoded.Version != 1 || decoded.LossScalePrecision != model.LossScalePrecisionFloat32 {
			t.Fatalf("unexpected metadata: %+v", decoded)
		}
		for _, shard := range decoded.Shards {
			if shard.Pool != "" {
				t.Fatalf("legacy shard unexpectedly has pool: %+v", shard)
			}
			if shard.Precision == model.PrecisionFP16 && shard.SourceDigest == "" {
				t.Fatalf("legacy FP16 shard lost source digest: %+v", shard)
			}
		}
		firstLine := bytes.Split(bytes.TrimSpace(raw), []byte{'\n'})[0]
		if bytes.Contains(firstLine, []byte(`"kind":"meta"`)) || !bytes.Contains(firstLine, []byte(`"format_version":1`)) {
			t.Fatalf("JSONL output is not self-describing record-only: %s", firstLine)
		}
	})
}

func TestLegacyJSONLMetadataLineRemainsDecodable(t *testing.T) {
	manifest := testManifest(2)
	manifest.Format = FormatJSONL
	recordOnly, err := Encode(manifest)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	metaPayload := jsonlMetaPayload{
		Format: FormatJSONL, Version: 2, RequiredMask: manifest.RequiredMask,
		RecordCount: len(manifest.Shards), LossScaleFixed: manifest.LossScaleFixed,
		LossScalePrecision: manifest.LossScalePrecision,
	}
	rawMeta, jerr := json.Marshal(metaPayload)
	if jerr != nil {
		t.Fatal(jerr)
	}
	metaLine, jerr := json.Marshal(jsonlMeta{
		Kind: "meta", Format: FormatJSONL, Version: 2, RequiredMask: manifest.RequiredMask,
		RecordCount: len(manifest.Shards), LossScaleFixed: manifest.LossScaleFixed,
		LossScalePrecision: manifest.LossScalePrecision, CRC32: fmt.Sprintf("%08x", crc32IEEE(rawMeta)),
	})
	if jerr != nil {
		t.Fatal(jerr)
	}
	raw := append(append(metaLine, '\n'), recordOnly...)
	decoded, berr := Decode(raw, FormatJSONL)
	if berr != nil {
		t.Fatalf("decode legacy JSONL: %v", berr)
	}
	if len(decoded.Shards) != len(manifest.Shards) {
		t.Fatalf("legacy meta decode lost records: %+v", decoded)
	}
}

func TestBinaryCorruptionReportsRecordIndex(t *testing.T) {
	manifest := testManifest(2)
	manifest.Format = FormatBinary
	raw, err := Encode(manifest)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	order := binary.LittleEndian
	secondOffset := binaryHeaderSize
	firstLen := int(order.Uint32(raw[secondOffset : secondOffset+4]))
	secondOffset += 4 + firstLen
	secondLen := int(order.Uint32(raw[secondOffset : secondOffset+4]))
	if secondLen < 8 {
		t.Fatalf("unexpected second record length %d", secondLen)
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[secondOffset+4+secondLen-1] ^= 0xff
	_, berr := Decode(corrupt, FormatBinary)
	if berr == nil || berr.Code != model.CodeRecordCorrupt {
		t.Fatalf("expected E_RECORD_CORRUPT, got %v", berr)
	}
	if got := fmt.Sprint(berr.Context["record_index"]); got != "2" {
		t.Fatalf("record_index = %s, want 2; context=%v", got, berr.Context)
	}
}

func TestBinaryDecodesBigEndianMarker(t *testing.T) {
	shard := testManifest(2).Shards[0]
	order := binary.BigEndian
	header := make([]byte, binaryHeaderSize)
	copy(header[:4], binaryMagic[:])
	header[4] = 2
	header[5] = 2
	order.PutUint16(header[6:8], 0)
	order.PutUint32(header[8:12], 1)
	order.PutUint64(header[12:20], uint64(8_000_000))
	header[20] = 2
	order.PutUint32(header[24:28], crc32IEEE(header[:24]))
	record, berr := encodeBinaryRecord(order, shard)
	if berr != nil {
		t.Fatalf("encode big-endian record: %v", berr)
	}
	raw := append(header, record...)
	decoded, berr := Decode(raw, FormatBinary)
	if berr != nil {
		t.Fatalf("decode big-endian: %v", berr)
	}
	if len(decoded.Shards) != 1 || decoded.Shards[0].ID != shard.ID {
		t.Fatalf("unexpected decoded shard: %+v", decoded.Shards)
	}
}

func TestUnknownRequiredMaskRejected(t *testing.T) {
	manifest := testManifest(2)
	manifest.Format = FormatBinary
	raw, err := Encode(manifest)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	order := binary.LittleEndian
	order.PutUint16(raw[6:8], 0x8000)
	order.PutUint32(raw[24:28], crc32IEEE(raw[:24]))
	_, berr := Decode(raw, FormatBinary)
	if berr == nil || berr.Code != model.CodeFormatUnsupported {
		t.Fatalf("expected E_FORMAT_UNSUPPORTED, got %v", berr)
	}
}

func TestJSONLCorruptionAndPresenceValidation(t *testing.T) {
	manifest := testManifest(2)
	manifest.Format = FormatJSONL
	raw, err := Encode(manifest)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	lines := bytes.Split(bytes.TrimSpace(raw), []byte{'\n'})
	if len(lines) < 2 {
		t.Fatalf("not enough JSONL lines: %d", len(lines))
	}
	t.Run("record-crc", func(t *testing.T) {
		corruptLines := make([][]byte, len(lines))
		copy(corruptLines, lines)
		corruptLines[1] = append([]byte(nil), lines[1]...)
		corruptLines[1][len(corruptLines[1])-2] ^= 0x01
		_, berr := Decode(bytes.Join(corruptLines, []byte{'\n'}), FormatJSONL)
		if berr == nil || berr.Code != model.CodeRecordCorrupt {
			t.Fatalf("expected E_RECORD_CORRUPT, got %v", berr)
		}
		if got := fmt.Sprint(berr.Context["record_index"]); got != "2" {
			t.Fatalf("record_index=%s context=%v", got, berr.Context)
		}
	})
	t.Run("unknown-presence", func(t *testing.T) {
		corruptLines := make([][]byte, len(lines))
		copy(corruptLines, lines)
		corruptLines[0] = []byte(strings.Replace(string(lines[0]), `"presence":63`, `"presence":255`, 1))
		_, berr := Decode(bytes.Join(corruptLines, []byte{'\n'}), FormatJSONL)
		if berr == nil || berr.Code != model.CodeRecordCorrupt {
			t.Fatalf("expected E_RECORD_CORRUPT, got %v", berr)
		}
		if !strings.Contains(fmt.Sprint(berr.Context), "unknown presence") {
			t.Fatalf("context did not identify bitmap failure: %v", berr.Context)
		}
	})
}

func TestCompatibilityMatrixOverrideAndReject(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "compat.json")
	config := map[string]any{"rules": map[string]any{
		"binary": map[string]any{"1": "ACCEPT", "4": "REJECT"},
	}}
	raw, _ := json.Marshal(config)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	matrix, err := LoadCompatibilityMatrix(path)
	if err != nil {
		t.Fatalf("load matrix: %v", err)
	}
	verdict, berr := matrix.Decide(FormatBinary, 1)
	if berr != nil || verdict != VerdictAccept {
		t.Fatalf("override verdict=%v err=%v", verdict, berr)
	}
	verdict, berr = matrix.Decide(FormatBinary, 4)
	if berr == nil || berr.Code != model.CodeFormatUnsupported || verdict != VerdictReject {
		t.Fatalf("expected reject, verdict=%v err=%v", verdict, berr)
	}
}
