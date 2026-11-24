package format

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"replicationlag/internal/apperr"
	"replicationlag/internal/compat"
	"replicationlag/internal/domain"
)

const (
	formatSourceOne = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	formatSourceTwo = "6f9d1a7e-8f5b-4cf4-a4a7-3bdfbc3f7d21"
)

func TestJSONLAndBinaryRoundTrip(t *testing.T) {
	observations := formatObservations(t)
	matrix := compat.Default()

	jsonl, err := EncodeJSONL(observations)
	if err != nil {
		t.Fatalf("encode JSONL: %v", err)
	}
	jsonResult, err := Import(jsonl, matrix, 999)
	if err != nil {
		t.Fatalf("import JSONL: %v", err)
	}
	assertObservationsEqual(t, observations, jsonResult.Observations)

	binaryData, err := EncodeBinary(observations)
	if err != nil {
		t.Fatalf("encode binary: %v", err)
	}
	binaryResult, err := Import(binaryData, matrix, 999)
	if err != nil {
		t.Fatalf("import binary: %v", err)
	}
	assertObservationsEqual(t, observations, binaryResult.Observations)

	detectedFormat, detectedVersion, detectErr := Detect(binaryData)
	if detectErr != nil || detectedFormat != BinaryFormat || detectedVersion != CurrentBinaryVersion {
		t.Fatalf("binary detect = %s/%d err=%v", detectedFormat, detectedVersion, detectErr)
	}
}

func TestBinaryCorruptionReportsCRCAndOffset(t *testing.T) {
	data, err := EncodeBinary(formatObservations(t))
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-5] ^= 0xff // last payload byte, before CRC32
	_, err = Import(corrupt, compat.Default(), 1)
	appErr := apperr.As(err)
	if appErr.Code != apperr.CodeFormatCorrupt {
		t.Fatalf("code = %s, %v", appErr.Code, err)
	}
	if _, ok := appErr.Context["offset"]; !ok {
		t.Fatalf("missing offset context: %#v", appErr.Context)
	}
	if appErr.Context["record"] != 3 {
		t.Fatalf("record context = %#v", appErr.Context)
	}
}

func TestJSONLMalformedLineReportsLineAndOffset(t *testing.T) {
	data := append([]byte("{\"format\":\"replication-observation-jsonl\",\"format_version\":1}\n"), []byte("{\"replica_id\":\"r1\"\n")...)
	_, err := Import(data, compat.Default(), 1)
	appErr := apperr.As(err)
	if appErr.Code != apperr.CodeFormatCorrupt {
		t.Fatalf("code = %s", appErr.Code)
	}
	if appErr.Context["line"] != 2 {
		t.Fatalf("line context = %#v", appErr.Context)
	}
	if _, ok := appErr.Context["offset"]; !ok {
		t.Fatalf("offset context = %#v", appErr.Context)
	}
}

func TestLegacyJSONLDegradesAndMatchesExplicitIntervals(t *testing.T) {
	matrix := compat.Default()
	rule := matrix.Rules["jsonl/0"]
	rule.DefaultGTIDSource = formatSourceOne
	matrix.Rules["jsonl/0"] = rule
	legacy := []byte(`{"format":"replication-observation-jsonl","format_version":0}
{"replica_id":"legacy-a","sampled_at":1700100000,"gtid_segments":["1-5"],"position_name":"` + formatSourceOne + `:1~5","lag_ms":100}
{"replica_id":"legacy-a","sampled_at":1700100010,"gtid_segments":["1-8"],"position_name":"` + formatSourceOne + `:1~8","lag_ms":120}
`)
	result, err := Import(legacy, matrix, 55)
	if err != nil {
		t.Fatalf("legacy import: %v", err)
	}
	if result.Decision != compat.DecisionDegrade || len(result.Degradations) < 5 {
		t.Fatalf("decision=%s degradations=%#v", result.Decision, result.Degradations)
	}
	if len(result.Observations) != 2 {
		t.Fatalf("observations = %d", len(result.Observations))
	}
	first := result.Observations[0]
	if first.SampledAtMS != 1700100000000 || first.GTID.String() != formatSourceOne+":1-5" || first.ReportedVersion.String() != "5.7.0" {
		t.Fatalf("normalized legacy observation = %+v", first)
	}
	if first.PositionName != formatSourceOne+":1-5" {
		t.Fatalf("legacy position = %q", first.PositionName)
	}

	explicit := []byte(`{"format":"replication-observation-jsonl","format_version":1}
{"replica_id":"legacy-a","sampled_at_ms":1700100000000,"gtid_segments":["` + formatSourceOne + `:1-5"],"position_name":"` + formatSourceOne + `:1-5","lag_ms":100,"reported_version":"5.7.0","last_write_ms":55}
{"replica_id":"legacy-a","sampled_at_ms":1700100010000,"gtid_segments":["` + formatSourceOne + `:1-8"],"position_name":"` + formatSourceOne + `:1-8","lag_ms":120,"reported_version":"5.7.0","last_write_ms":55}
`)
	explicitResult, err := Import(explicit, compat.Default(), 55)
	if err != nil {
		t.Fatalf("explicit import: %v", err)
	}
	assertGTIDAndPositionEqual(t, explicitResult.Observations, result.Observations)
}

func TestLegacyBinaryV0Degrades(t *testing.T) {
	observations := formatObservations(t)
	for i := range observations {
		observations[i].SampledAtMS = 1700100000000 + int64(i)*1000
		observations[i].FormatVersion = 0
	}
	matrix := compat.Default()
	rule := matrix.Rules["binary/0"]
	rule.DefaultGTIDSource = formatSourceOne
	matrix.Rules["binary/0"] = rule
	data, err := EncodeBinaryWithOptions(observations, BinaryEncodeOptions{
		Version: 0, TimestampInSeconds: true, OmitReportedVersion: true,
	})
	if err != nil {
		t.Fatalf("encode legacy binary: %v", err)
	}
	result, err := Import(data, matrix, 77)
	if err != nil {
		t.Fatalf("import legacy binary: %v", err)
	}
	if result.Decision != compat.DecisionDegrade || len(result.Observations) != len(observations) {
		t.Fatalf("decision=%s observations=%d", result.Decision, len(result.Observations))
	}
	if result.Observations[0].SampledAtMS != 1700100000000 || result.Observations[0].ReportedVersion.String() != "5.7.0" {
		t.Fatalf("legacy binary normalization = %+v", result.Observations[0])
	}
}

func TestUnknownMandatoryFieldIsUnsupported(t *testing.T) {
	data := []byte(`{"format":"replication-observation-jsonl","format_version":9,"required_fields":["future_mandatory_field"]}
{"replica_id":"r1","sampled_at_ms":1000,"gtid_segments":["` + formatSourceOne + `:1"],"position_name":"` + formatSourceOne + `:1","lag_ms":1,"reported_version":"8.0.0"}
`)
	_, err := Import(data, compat.Default(), 1)
	appErr := apperr.As(err)
	if appErr.Code != apperr.CodeFormatUnsupported {
		t.Fatalf("code = %s, err=%v", appErr.Code, err)
	}
	fields, ok := appErr.Context["required_fields"].([]string)
	if !ok || len(fields) != 1 || fields[0] != "future_mandatory_field" {
		t.Fatalf("context = %#v", appErr.Context)
	}
}

func TestInvalidObservationFieldUsesBusinessError(t *testing.T) {
	data := []byte(`{"format":"replication-observation-jsonl","format_version":1}
{"replica_id":"r1","sampled_at_ms":1000,"gtid_segments":["` + formatSourceOne + `:1"],"position_name":"` + formatSourceOne + `:1","lag_ms":-1,"reported_version":"8.0.0"}
`)
	_, err := Import(data, compat.Default(), 1)
	appErr := apperr.As(err)
	if appErr.Code != apperr.CodeObservationInvalid {
		t.Fatalf("code = %s", appErr.Code)
	}
	if appErr.Context["field"] != "lag_ms" || appErr.Context["replica_id"] != "r1" || appErr.Context["sampled_at_ms"] != int64(1000) {
		t.Fatalf("context = %#v", appErr.Context)
	}
}

func TestBinaryUnknownBitmapIsUnsupported(t *testing.T) {
	observation := formatObservations(t)[0]
	data, err := EncodeBinary([]domain.Observation{observation})
	if err != nil {
		t.Fatal(err)
	}
	// payload starts after magic(4)+version(1)+count(4)+length(4).
	data[binaryHeaderSize+4] |= 0x80
	// Recompute CRC so the parser reaches the bitmap decision.
	payloadLength := int(binary.BigEndian.Uint32(data[binaryHeaderSize : binaryHeaderSize+4]))
	payloadStart := binaryHeaderSize + 4
	crcStart := payloadStart + payloadLength
	binary.BigEndian.PutUint32(data[crcStart:crcStart+4], crc32.ChecksumIEEE(data[payloadStart:crcStart]))
	_, err = Import(data, compat.Default(), 1)
	appErr := apperr.As(err)
	if appErr.Code != apperr.CodeFormatUnsupported {
		t.Fatalf("code = %s, err=%v", appErr.Code, err)
	}
}

func formatObservations(t *testing.T) []domain.Observation {
	t.Helper()
	version, _ := domain.ParseVersion("8.0.36")
	specs := []struct {
		replica  string
		at       int64
		gtid     string
		position string
		lag      int64
	}{
		{"replica-a", 1700000000000, formatSourceOne + ":1-5", formatSourceOne + ":1-5", 120},
		{"replica-a", 1700000001000, formatSourceOne + ":1-8," + formatSourceTwo + ":1-2", formatSourceOne + ":1-8," + formatSourceTwo + ":1-2", 1500},
		{"replica-b", 1700000000500, formatSourceOne + ":1-10", formatSourceOne + ":1-10", 6000},
	}
	result := make([]domain.Observation, 0, len(specs))
	for i, spec := range specs {
		set, err := domain.ParseGTID(spec.gtid)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, domain.Observation{
			ReplicaID: spec.replica, SampledAtMS: spec.at, GTID: set, PositionName: spec.position,
			LagMS: spec.lag, ReportedVersion: version, FormatVersion: 1, LastWriteMS: 1700200000000 + int64(i),
		})
	}
	return result
}

func assertObservationsEqual(t *testing.T, want, got []domain.Observation) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("observation count = %d, want %d", len(got), len(want))
	}
	orderedWant := domain.SortObservations(want)
	orderedGot := domain.SortObservations(got)
	for i := range orderedWant {
		a, b := orderedWant[i], orderedGot[i]
		if a.ReplicaID != b.ReplicaID || a.SampledAtMS != b.SampledAtMS || !a.GTID.Equal(b.GTID) ||
			a.PositionName != b.PositionName || a.LagMS != b.LagMS || a.ReportedVersion.String() != b.ReportedVersion.String() ||
			a.FormatVersion != b.FormatVersion || a.LastWriteMS != b.LastWriteMS {
			t.Fatalf("observation %d differs:\nwant=%+v\ngot=%+v", i, a, b)
		}
	}
}

func assertGTIDAndPositionEqual(t *testing.T, a, b []domain.Observation) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("length mismatch: %d != %d", len(a), len(b))
	}
	for i := range a {
		if !a[i].GTID.Equal(b[i].GTID) || a[i].PositionName != b[i].PositionName || a[i].SampledAtMS != b[i].SampledAtMS {
			t.Fatalf("normalized record %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}
