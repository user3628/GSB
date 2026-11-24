package policy

import (
	"encoding/binary"
	"os"
	"reflect"
	"testing"

	"example.com/replicabudget/internal/apperr"
)

func readSample(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("../../samples/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDualFormatSemanticEquivalence(t *testing.T) {
	jsonlData := readSample(t, "new.jsonl")
	jsonl, jsonlInfo, err := DecodeJSONL(jsonlData)
	if err != nil {
		t.Fatal(err)
	}
	if jsonlInfo.Format != FormatJSONL || jsonlInfo.Version != 1 {
		t.Fatalf("unexpected jsonl info: %+v", jsonlInfo)
	}
	binaryData := readSample(t, "new.bin")
	binaryStrategies, binaryInfo, err := DecodeBinary(binaryData)
	if err != nil {
		t.Fatal(err)
	}
	if binaryInfo.Format != FormatBinary || binaryInfo.Version != 1 || binaryInfo.Endian != "little" {
		t.Fatalf("unexpected binary info: %+v", binaryInfo)
	}
	if !reflect.DeepEqual(jsonl, binaryStrategies) {
		t.Fatalf("formats decoded to different models:\njsonl=%+v\nbinary=%+v", jsonl, binaryStrategies)
	}
}

func TestJSONLRoundTrip(t *testing.T) {
	strategies, _, err := DecodeJSONL(readSample(t, "new.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeJSONL(strategies)
	if err != nil {
		t.Fatal(err)
	}
	decoded, info, err := DecodeJSONL(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if info.Version != 1 || !reflect.DeepEqual(strategies, decoded) {
		t.Fatalf("JSONL round trip failed: %+v", info)
	}
}

func TestBinaryBigEndianRoundTrip(t *testing.T) {
	strategies, _, err := DecodeJSONL(readSample(t, "new.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeBinary(strategies, binary.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	decoded, info, err := DecodeBinary(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if info.Endian != "big" || !reflect.DeepEqual(strategies, decoded) {
		t.Fatalf("big endian round trip failed: %+v", info)
	}
}

func TestBinaryCorruptionReportsRecordIndex(t *testing.T) {
	original := readSample(t, "new.bin")
	t.Run("crc", func(t *testing.T) {
		corrupt := append([]byte(nil), original...)
		corrupt[len(corrupt)-1] ^= 0xff
		_, _, err := DecodeBinary(corrupt)
		assertBizError(t, err, apperr.RecordCorrupt, 3)
	})
	t.Run("length", func(t *testing.T) {
		corrupt := append([]byte(nil), original...)
		binary.LittleEndian.PutUint32(corrupt[13:17], 1<<30)
		_, _, err := DecodeBinary(corrupt)
		assertBizError(t, err, apperr.RecordCorrupt, 1)
	})
	t.Run("record_type", func(t *testing.T) {
		corrupt := append([]byte(nil), original...)
		corrupt[10] = 0x7f
		_, _, err := DecodeBinary(corrupt)
		assertBizError(t, err, apperr.RecordCorrupt, 1)
	})
	t.Run("trailing_bytes", func(t *testing.T) {
		corrupt := append(append([]byte(nil), original...), 0xde, 0xad)
		_, _, err := DecodeBinary(corrupt)
		assertBizError(t, err, apperr.RecordCorrupt, 4)
	})
}

func assertBizError(t *testing.T, err error, code string, record int) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s", code)
	}
	be := apperr.As(err)
	if be.Code != code {
		t.Fatalf("code=%s want=%s err=%v", be.Code, code, err)
	}
	if be.RecordIndex != record {
		t.Fatalf("record_index=%d want=%d", be.RecordIndex, record)
	}
}

func TestJSONLUnknownRecordTypeIsCorrupt(t *testing.T) {
	data := []byte(`{"format":"strategy-jsonl","format_version":1,"record_type":"mystery","service_id":"svc-a"}` + "\n")
	_, _, err := DecodeJSONL(data)
	assertBizError(t, err, apperr.RecordCorrupt, 1)
}

func TestHighVersionRejectedByMatrix(t *testing.T) {
	info, err := DetectFormat(readSample(t, "new_v2_unknown.jsonl"), FormatAuto)
	if err != nil {
		t.Fatal(err)
	}
	if info.Version != 2 {
		t.Fatalf("version=%d", info.Version)
	}
	_, err = Negotiate(info)
	if err == nil || apperr.As(err).Code != apperr.FormatUnsupported {
		t.Fatalf("expected E_FORMAT_UNSUPPORTED, got %v", err)
	}
}

func TestValidationRejectsDuplicateAndInvalidBounds(t *testing.T) {
	quotas := []QuotaGroup{{ID: "q", VRAMBytes: 1 << 40, ComputeMilli: 10000}}
	base := NewStrategy{ServiceID: "svc", MinReplicas: 2, MaxReplicas: 4, TargetConcurrency: 256, LatencyBudgetMS: 100, QuotaGroupRef: "q", WeightPermille: 1000, VRAMBytesPerReplica: 1 << 30, ComputeMilliPerReplica: 100}
	if err := ValidateStrategyBatch([]NewStrategy{base}, quotas); err != nil {
		t.Fatal(err)
	}
	duplicate := append([]NewStrategy(nil), base, base)
	err := ValidateStrategyBatch(duplicate, quotas)
	assertBizError(t, err, apperr.SpecInvalid, 2)
	base.MinReplicas = 5
	err = ValidateStrategyBatch([]NewStrategy{base}, quotas)
	assertBizError(t, err, apperr.SpecInvalid, 1)
}
