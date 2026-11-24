package codec

import (
	"bytes"
	"encoding/json"

	"mitmpipeline/internal/domain"
)

const CurrentVersion = 2

const DefaultProtocolVersion = "TLS 1.2"

type MatrixEntry struct {
	Format             string `json:"format"`
	WriterVersion      int    `json:"writer_version"`
	ReaderVersion      int    `json:"reader_version"`
	Readable           bool   `json:"readable"`
	CanonicalOverwrite bool   `json:"canonical_overwrite"`
	Action             string `json:"action"`
	Notes              string `json:"notes"`
}

func CompatibilityMatrix() []MatrixEntry {
	return []MatrixEntry{
		{Format: domain.FormatBinary, WriterVersion: 1, ReaderVersion: 2, Readable: true, CanonicalOverwrite: true, Action: domain.ActionDegraded, Notes: "protocol absent; event offsets are millisecond precision"},
		{Format: domain.FormatBinary, WriterVersion: 2, ReaderVersion: 2, Readable: true, CanonicalOverwrite: true, Action: domain.ActionAccept, Notes: "nanosecond offsets, bitmap and timeline supported"},
		{Format: domain.FormatBinary, WriterVersion: 3, ReaderVersion: 2, Readable: false, CanonicalOverwrite: false, Action: domain.ActionReject, Notes: "unknown required bitmap/layout fields"},
		{Format: domain.FormatJSONL, WriterVersion: 1, ReaderVersion: 2, Readable: true, CanonicalOverwrite: true, Action: domain.ActionDegraded, Notes: "protocol absent; RFC3339 timestamps use second precision"},
		{Format: domain.FormatJSONL, WriterVersion: 2, ReaderVersion: 2, Readable: true, CanonicalOverwrite: true, Action: domain.ActionAccept, Notes: "RFC3339Nano and explicit protocol"},
		{Format: domain.FormatJSONL, WriterVersion: 3, ReaderVersion: 2, Readable: false, CanonicalOverwrite: false, Action: domain.ActionReject, Notes: "unknown required JSON field"},
	}
}

func Detect(data []byte) (string, int, *domain.Error) {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return "", 0, domain.NewError(domain.CodeRecordCorrupt, "empty input")
	}
	if bytes.HasPrefix(trimmed, BinaryMagic[:]) {
		if len(trimmed) < 5 {
			return domain.FormatBinary, 0, domain.NewError(domain.CodeRecordCorrupt, "truncated binary header").WithContext("record_index", 0)
		}
		return domain.FormatBinary, int(trimmed[4]), nil
	}
	if trimmed[0] == '{' {
		var header struct {
			Format        string `json:"format"`
			FormatVersion int    `json:"format_version"`
		}
		line := trimmed
		if idx := bytes.IndexByte(trimmed, '\n'); idx >= 0 {
			line = trimmed[:idx]
		}
		if err := json.Unmarshal(line, &header); err != nil {
			return domain.FormatJSONL, 0, domain.WrapError(domain.CodeRecordCorrupt, err, "invalid JSON header").WithContext("record_index", 1)
		}
		if header.FormatVersion == 0 {
			header.FormatVersion = 1
		}
		return domain.FormatJSONL, header.FormatVersion, nil
	}
	return "", 0, domain.NewError(domain.CodeRecordCorrupt, "unrecognized format magic or JSON object")
}

func Negotiate(data []byte) (domain.Negotiation, error) {
	format, version, derr := Detect(data)
	if derr != nil {
		return domain.Negotiation{Action: domain.ActionReject}, derr
	}
	decision := NegotiateVersion(format, version)
	if decision.Action == domain.ActionReject {
		apiErr := domain.NewError(domain.CodeFormatUnsupported, decision.Message).
			WithContextMap(map[string]any{
				"format":             format,
				"requested_version":  version,
				"supported_versions": []int{1, 2},
			})
		if format == domain.FormatBinary {
			if bitmap, bitmapErr := inspectFirstBitmap(data); bitmapErr == nil {
				apiErr = apiErr.WithContext("bitmap", bitmap).
					WithContext("unknown_required_bits", bitmap&^(bitmapProtocol|bitmapTimeline))
			}
		}
		return decision, apiErr
	}
	return decision, nil
}

func NegotiateVersion(format string, version int) domain.Negotiation {
	n := domain.Negotiation{Format: format, RequestedVersion: version, EffectiveVersion: CurrentVersion}
	switch {
	case version == CurrentVersion:
		n.Action = domain.ActionAccept
	case version == 1:
		n.Action = domain.ActionDegraded
		n.Degradations = []domain.Degradation{{
			Field:    "format_version",
			Rule:     "read_v1_and_write_canonical_v2",
			Supplied: "1",
			Filled:   "2",
			Reason:   CompatibilityMatrix()[0].Notes,
		}}
	default:
		n.Action = domain.ActionReject
		n.Code = domain.CodeFormatUnsupported
		n.Message = "format version is newer than reader and contains unsupported required layout"
		n.RequiredFields = []string{"unknown_required_extension"}
	}
	return n
}

func DecodeAny(data []byte) ([]domain.Record, domain.Negotiation, []domain.Degradation, error) {
	decision, err := Negotiate(data)
	if err != nil {
		return nil, decision, nil, err
	}
	var records []domain.Record
	var degradations []domain.Degradation
	switch decision.Format {
	case domain.FormatBinary:
		records, degradations, err = DecodeBinary(data)
	case domain.FormatJSONL:
		records, degradations, err = DecodeJSONL(data)
	default:
		err = domain.NewError(domain.CodeRecordCorrupt, "unknown format")
	}
	if err != nil {
		return nil, decision, degradations, err
	}
	for i := range degradations {
		if degradations[i].RecordIndex == 0 {
			degradations[i].RecordIndex = i + 1
		}
	}
	decision.Degradations = append(decision.Degradations, degradations...)
	return records, decision, degradations, nil
}
