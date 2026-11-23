package codec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"meshmigrate/internal/model"
)

const JSONLFormatName = "mesh-policy-jsonl"

type jsonHeaderLine struct {
	Type            string `json:"type"`
	Format          string `json:"format"`
	FormatVersion   int    `json:"formatVersion"`
	SnapshotVersion uint64 `json:"snapshotVersion"`
	SnapshotID      string `json:"snapshotId"`
	GeneratedAtMs   int64  `json:"generatedAtMs"`
	PolicyCount     int    `json:"policyCount"`
}

type jsonPolicyLine struct {
	Type             string            `json:"type"`
	PolicyID         string            `json:"policyId"`
	VirtualServiceID string            `json:"virtualServiceId"`
	PolicyRevision   *uint64           `json:"policyRevision"`
	LastWriteMs      *int64            `json:"lastWriteMs"`
	Match            model.RouteMatch  `json:"match"`
	Optional         []string          `json:"optional,omitempty"`
	RateLimitKbps    *uint64           `json:"rateLimitKbps,omitempty"`
	TimeoutUs        *uint64           `json:"timeoutUs,omitempty"`
	CircuitWindowUs  *uint64           `json:"circuitWindowUs,omitempty"`
	RateWindowUs     *uint64           `json:"rateWindowUs,omitempty"`
	CapturedAtUs     *int64            `json:"capturedAtUs,omitempty"`
	WindowAlignment  *string           `json:"windowAlignment,omitempty"`
	RequiredFeatures []string          `json:"requiredFeatures,omitempty"`
	ExtraFields      map[string]string `json:"-"`
}

type jsonTrailerLine struct {
	Type        string `json:"type"`
	PolicyCount int    `json:"policyCount"`
	Digest      string `json:"digest"`
}

var allowedPolicyFields = map[string]struct{}{
	"type": {}, "policyId": {}, "virtualServiceId": {}, "policyRevision": {},
	"lastWriteMs": {}, "match": {}, "optional": {}, "rateLimitKbps": {},
	"timeoutUs": {}, "circuitWindowUs": {}, "rateWindowUs": {}, "capturedAtUs": {},
	"windowAlignment": {}, "requiredFeatures": {},
}

// EncodeJSONL emits a deterministic line-delimited snapshot. The trailer digest
// binds the header and all trimmed policy-line bytes.
func EncodeJSONL(s model.ParsedSnapshot) ([]byte, error) {
	header := jsonHeaderLine{
		Type:            "header",
		Format:          JSONLFormatName,
		FormatVersion:   s.FormatVersion,
		SnapshotVersion: s.SnapshotVersion,
		SnapshotID:      s.SnapshotID,
		GeneratedAtMs:   s.GeneratedAtMs,
		PolicyCount:     len(s.Policies),
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.Write(headerBytes)
	out.WriteByte('\n')
	var digestInput bytes.Buffer
	digestInput.Write(headerBytes)
	for i, p := range s.Policies {
		line, err := jsonMarshalPolicy(p, s.RequiredFeatures)
		if err != nil {
			return nil, fmt.Errorf("policy %d: %w", i, err)
		}
		digestInput.Write(line)
		out.Write(line)
		out.WriteByte('\n')
	}
	sum := sha256.Sum256(digestInput.Bytes())
	trailer := jsonTrailerLine{
		Type:        "trailer",
		PolicyCount: len(s.Policies),
		Digest:      "sha256:" + hex.EncodeToString(sum[:]),
	}
	trailerBytes, err := json.Marshal(trailer)
	if err != nil {
		return nil, err
	}
	out.Write(trailerBytes)
	out.WriteByte('\n')
	return out.Bytes(), nil
}

func jsonMarshalPolicy(p model.RawPolicy, requiredFeatures []string) ([]byte, error) {
	w := jsonPolicyLine{
		Type:             "policy",
		PolicyID:         p.PolicyID,
		VirtualServiceID: p.VirtualServiceID,
		Match:            p.Match,
		RequiredFeatures: append([]string(nil), requiredFeatures...),
	}
	rev := p.PolicyRevision
	last := p.LastWriteMs
	w.PolicyRevision = &rev
	w.LastWriteMs = &last
	if p.Presence.RateLimit {
		v := p.RateLimitValue
		w.RateLimitKbps = &v
		w.Optional = append(w.Optional, "rateLimitKbps")
	}
	if p.Presence.Timeout {
		v := p.TimeoutValue
		w.TimeoutUs = &v
		w.Optional = append(w.Optional, "timeoutUs")
	}
	if p.Presence.CircuitWindow {
		v := p.CircuitWindowVal
		w.CircuitWindowUs = &v
		w.Optional = append(w.Optional, "circuitWindowUs")
	}
	if p.Presence.RateWindow {
		v := p.RateWindowValue
		w.RateWindowUs = &v
		w.Optional = append(w.Optional, "rateWindowUs")
	}
	if p.Presence.CapturedAt {
		v := p.CapturedAtValue
		w.CapturedAtUs = &v
		w.Optional = append(w.Optional, "capturedAtUs")
	}
	if p.Presence.WindowAlignment {
		v := p.WindowAlignment
		w.WindowAlignment = &v
		w.Optional = append(w.Optional, "windowAlignment")
	}
	sort.Strings(w.Optional)
	base, err := json.Marshal(w)
	if err != nil {
		return nil, err
	}
	if len(p.ExtraFields) == 0 {
		return base, nil
	}
	var m map[string]any
	if err := json.Unmarshal(base, &m); err != nil {
		return nil, err
	}
	for k, v := range p.ExtraFields {
		m[k] = v
	}
	return json.Marshal(m)
}

type jsonLine struct {
	start int
	line  int
	text  []byte
}

func splitJSONLines(data []byte) ([]jsonLine, *Error) {
	lines := make([]jsonLine, 0)
	start := 0
	lineNo := 1
	for start <= len(data) {
		var end int
		if idx := bytes.IndexByte(data[start:], '\n'); idx >= 0 {
			end = start + idx
		} else {
			end = len(data)
		}
		raw := data[start:end]
		text := bytes.TrimSuffix(raw, []byte("\r"))
		if len(bytes.TrimSpace(text)) == 0 {
			if start == len(data) || (end == len(data) && len(bytes.TrimSpace(data[start:])) == 0) {
				break
			}
			return nil, corrupt(int64(start), "blank JSONL line %d", lineNo)
		}
		lines = append(lines, jsonLine{start: start, line: lineNo, text: append([]byte(nil), text...)})
		if end == len(data) {
			break
		}
		start = end + 1
		lineNo++
	}
	return lines, nil
}

// DecodeJSONL validates every line, counts, and the trailer digest before
// returning any policy. It never returns partially decoded input.
func DecodeJSONL(data []byte) (*model.ParsedSnapshot, *Error) {
	lines, er := splitJSONLines(data)
	if er != nil {
		return nil, er
	}
	if len(lines) < 2 {
		return nil, corrupt(0, "JSONL requires a header, at least one policy, and a trailer")
	}
	var header jsonHeaderLine
	if err := json.Unmarshal(lines[0].text, &header); err != nil {
		return nil, jsonError(lines[0], err, "header")
	}
	if header.Type != "header" {
		return nil, corrupt(int64(lines[0].start), "first JSONL line must be type=header, got %q", header.Type)
	}
	if header.Format != JSONLFormatName {
		return nil, corrupt(int64(lines[0].start), "unexpected JSONL format %q", header.Format)
	}
	var trailer jsonTrailerLine
	last := lines[len(lines)-1]
	if err := json.Unmarshal(last.text, &trailer); err != nil {
		return nil, jsonError(last, err, "trailer")
	}
	if trailer.Type != "trailer" {
		return nil, corrupt(int64(last.start), "last JSONL line must be type=trailer, got %q", trailer.Type)
	}
	policyLines := lines[1 : len(lines)-1]
	if header.PolicyCount != len(policyLines) || trailer.PolicyCount != len(policyLines) {
		return nil, corrupt(int64(lines[0].start), "policy count mismatch: header=%d trailer=%d actual=%d", header.PolicyCount, trailer.PolicyCount, len(policyLines))
	}
	var digestInput bytes.Buffer
	digestInput.Write(lines[0].text)
	unknown := make([]string, 0)
	required := make([]string, 0)
	policies := make([]model.RawPolicy, 0, len(policyLines))
	for _, ln := range policyLines {
		var rawMap map[string]json.RawMessage
		if err := json.Unmarshal(ln.text, &rawMap); err != nil {
			return nil, jsonError(ln, err, "policy")
		}
		for k := range rawMap {
			if _, ok := allowedPolicyFields[k]; !ok {
				unknown = append(unknown, k)
			}
		}
		var w jsonPolicyLine
		if err := json.Unmarshal(ln.text, &w); err != nil {
			return nil, jsonError(ln, err, "policy")
		}
		if w.Type != "policy" {
			return nil, corrupt(int64(ln.start), "line %d must be type=policy, got %q", ln.line, w.Type)
		}
		if er := validateOptionalMarkers(w, ln); er != nil {
			return nil, er
		}
		required = append(required, w.RequiredFeatures...)
		raw := model.RawPolicy{
			PolicyID:          w.PolicyID,
			VirtualServiceID:  w.VirtualServiceID,
			Match:             w.Match,
			SourceOffset:      int64(ln.start),
			SourceLine:        ln.line,
			ExtraFields:       make(map[string]string),
			RateLimitUnit:     "kbps",
			TimeoutUnit:       "us",
			CircuitWindowUnit: "us",
			RateWindowUnit:    "us",
			CapturedAtUnit:    "us",
		}
		if w.PolicyRevision != nil {
			raw.PolicyRevision = *w.PolicyRevision
		}
		if w.LastWriteMs != nil {
			raw.LastWriteMs = *w.LastWriteMs
		}
		if w.RateLimitKbps != nil {
			raw.RateLimitValue = *w.RateLimitKbps
			raw.Presence.RateLimit = true
		}
		if w.TimeoutUs != nil {
			raw.TimeoutValue = *w.TimeoutUs
			raw.Presence.Timeout = true
		}
		if w.CircuitWindowUs != nil {
			raw.CircuitWindowVal = *w.CircuitWindowUs
			raw.Presence.CircuitWindow = true
		}
		if w.RateWindowUs != nil {
			raw.RateWindowValue = *w.RateWindowUs
			raw.Presence.RateWindow = true
		}
		if w.CapturedAtUs != nil {
			raw.CapturedAtValue = *w.CapturedAtUs
			raw.Presence.CapturedAt = true
		}
		if w.WindowAlignment != nil {
			raw.WindowAlignment = *w.WindowAlignment
			raw.Presence.WindowAlignment = true
		}
		for k := range rawMap {
			if _, ok := allowedPolicyFields[k]; !ok {
				var str string
				if err := json.Unmarshal(rawMap[k], &str); err == nil {
					raw.ExtraFields[k] = str
				} else {
					raw.ExtraFields[k] = string(rawMap[k])
				}
			}
		}
		policies = append(policies, raw)
		digestInput.Write(ln.text)
	}
	wantDigest := trailer.Digest
	actual := sha256.Sum256(digestInput.Bytes())
	actualDigest := "sha256:" + hex.EncodeToString(actual[:])
	if wantDigest != actualDigest {
		return nil, corrupt(int64(last.start), "snapshot digest mismatch: got %s want %s", wantDigest, actualDigest)
	}
	if header.FormatVersion <= 0 || header.FormatVersion > math.MaxUint8 {
		return nil, corrupt(int64(lines[0].start), "format version out of range: %d", header.FormatVersion)
	}
	return &model.ParsedSnapshot{
		SnapshotVersion:  header.SnapshotVersion,
		SnapshotID:       header.SnapshotID,
		GeneratedAtMs:    header.GeneratedAtMs,
		SourceFormat:     model.FormatJSONL,
		FormatVersion:    header.FormatVersion,
		PolicyCount:      len(policies),
		SourceDigest:     wantDigest,
		Policies:         policies,
		UnknownFields:    uniqueStrings(unknown),
		RequiredFeatures: uniqueStrings(required),
	}, nil
}

func jsonError(ln jsonLine, err error, context string) *Error {
	offset := int64(ln.start)
	if se, ok := err.(*json.SyntaxError); ok {
		offset += se.Offset - 1
	}
	return &Error{Code: CodeFormatCorrupt, Message: fmt.Sprintf("invalid %s JSON on line %d: %v", context, ln.line, err), Offset: offset, Line: ln.line}
}

// Decode dispatches on a detected source format.
func Decode(data []byte, hint string) (*model.ParsedSnapshot, *Error) {
	format, er := Detect(data, hint)
	if er != nil {
		return nil, er
	}
	switch format {
	case model.FormatBinary:
		return DecodeBinary(data)
	case model.FormatJSONL:
		return DecodeJSONL(data)
	default:
		return nil, &Error{Code: CodeFormatUnsupported, Message: "unsupported format " + format, Offset: 0}
	}
}

var knownOptionalFields = map[string]struct{}{
	"rateLimitKbps": {}, "timeoutUs": {}, "circuitWindowUs": {},
	"rateWindowUs": {}, "capturedAtUs": {}, "windowAlignment": {},
}

func validateOptionalMarkers(w jsonPolicyLine, ln jsonLine) *Error {
	actual := map[string]bool{
		"rateLimitKbps":   w.RateLimitKbps != nil,
		"timeoutUs":       w.TimeoutUs != nil,
		"circuitWindowUs": w.CircuitWindowUs != nil,
		"rateWindowUs":    w.RateWindowUs != nil,
		"capturedAtUs":    w.CapturedAtUs != nil,
		"windowAlignment": w.WindowAlignment != nil,
	}
	marked := make(map[string]bool, len(w.Optional))
	for _, name := range w.Optional {
		if _, known := knownOptionalFields[name]; !known {
			continue
		}
		if marked[name] {
			return corrupt(int64(ln.start), "line %d: duplicate optional marker %q", ln.line, name)
		}
		marked[name] = true
		if !actual[name] {
			return corrupt(int64(ln.start), "line %d: optional marker %q is present but the field is absent", ln.line, name)
		}
	}
	for name, present := range actual {
		if present && !marked[name] {
			return corrupt(int64(ln.start), "line %d: optional field %q is present but missing from optional markers", ln.line, name)
		}
	}
	return nil
}
