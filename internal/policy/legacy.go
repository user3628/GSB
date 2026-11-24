package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"example.com/replicabudget/internal/apperr"
)

func ParseLegacyJSON(data []byte) ([]LegacyStrategy, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raws []json.RawMessage
	if err := dec.Decode(&raws); err != nil {
		return nil, apperr.New(apperr.SpecInvalid, "legacy policy must be a JSON array: "+err.Error())
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, apperr.New(apperr.SpecInvalid, "legacy policy has trailing JSON values")
	}
	if len(raws) == 0 {
		return nil, apperr.New(apperr.SpecInvalid, "legacy policy batch is empty")
	}
	out := make([]LegacyStrategy, 0, len(raws))
	seen := make(map[string]int, len(raws))
	for i, raw := range raws {
		rec := LegacyStrategy{RecordIndex: i + 1, SourceJSON: append([]byte(nil), raw...)}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&rec); err != nil {
			return nil, apperr.Record(apperr.SpecInvalid, err.Error(), i+1, nil)
		}
		if err := validateLegacyRecord(rec, seen); err != nil {
			if be := apperr.As(err); be.RecordIndex == 0 {
				be.RecordIndex = i + 1
			}
			return nil, err
		}
		seen[rec.ServiceID] = i + 1
		out = append(out, rec)
	}
	return out, nil
}

func validateLegacyRecord(rec LegacyStrategy, seen map[string]int) error {
	if err := validateServiceID(rec.ServiceID); err != nil {
		return err
	}
	if first, exists := seen[rec.ServiceID]; exists {
		return apperr.WithDetails(apperr.SpecInvalid, "duplicate service_id", map[string]any{
			"service_id": rec.ServiceID, "first_record_index": first,
		})
	}
	if rec.FixedReplicas <= 0 || rec.FixedReplicas > 1_000_000 {
		return apperr.WithDetails(apperr.SpecInvalid, "fixed_replicas must be in [1,1000000]", map[string]any{
			"service_id": rec.ServiceID, "fixed_replicas": rec.FixedReplicas,
		})
	}
	if rec.VRAMBytesPerReplica == 0 || rec.VRAMBytesPerReplica > uint64(^uint64(0)>>1) {
		return apperr.WithDetails(apperr.SpecInvalid, "vram_bytes_per_replica out of range", map[string]any{
			"service_id": rec.ServiceID,
		})
	}
	if rec.ComputeMilliPerReplica == 0 || rec.ComputeMilliPerReplica > uint64(^uint64(0)>>1) {
		return apperr.WithDetails(apperr.SpecInvalid, "compute_milli_per_replica out of range", map[string]any{
			"service_id": rec.ServiceID,
		})
	}
	if int64(rec.FixedReplicas)*ConcurrencyPerReplica > 10_000_000_000 {
		return apperr.WithDetails(apperr.SpecInvalid, "derived target concurrency overflows policy range", map[string]any{
			"service_id": rec.ServiceID,
		})
	}
	return nil
}

func DecodeDeprecatedString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("deprecated field is not a string")
	}
	return s, nil
}
