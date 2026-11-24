// Package sample parses and deterministically generates the two local sample
// formats: JSON-lines process/service manifests and length-prefixed event
// streams. All parse failures carry a stable business code and byte offset.
package sample

import (
	"bytes"
	"encoding/json"
	"fmt"

	"privescworkbench/internal/domain"
)

// MaskMax is the inclusive upper bound of a legal permission bitmask.
const MaskMax uint64 = 0xFFFF

// LegalAccounts maps an account name to its privilege tier (0..4).
var LegalAccounts = map[string]int{
	"LocalSystem": 4,
	"Service":     3,
	"Admin":       2,
	"User":        1,
	"Anonymous":   0,
}

// AccountTier returns the privilege tier of a declared account.
func AccountTier(account string) (int, bool) {
	tier, ok := LegalAccounts[account]
	return tier, ok
}

// ParseManifest parses JSON-lines process/service declarations.
func ParseManifest(data []byte) ([]domain.ProcessDecl, error) {
	procs := make([]domain.ProcessDecl, 0)
	seen := map[int64]int64{}
	offset := int64(0)
	for len(data) > 0 {
		// find end of line, tracking absolute byte offset
		nl := bytes.IndexByte(data, '\n')
		var line []byte
		var lineLen int64
		if nl < 0 {
			line = data
			lineLen = int64(len(data))
		} else {
			line = data[:nl]
			lineLen = int64(nl) + 1
		}
		trimmed := bytes.TrimSpace(line)
		lineStart := offset
		if len(trimmed) > 0 {
			var p domain.ProcessDecl
			if err := json.Unmarshal(trimmed, &p); err != nil {
				return nil, domain.NewOffsetError(domain.CodeSampleParse,
					"manifest line is not valid JSON: "+err.Error(), lineStart)
			}
			p.SourceOffset = lineStart
			if err := validateProcess(p, seen); err != nil {
				return nil, err
			}
			seen[p.PID] = lineStart
			procs = append(procs, p)
		}
		if nl < 0 {
			break
		}
		data = data[nl+1:]
		offset += lineLen
	}
	return procs, nil
}

func validateProcess(p domain.ProcessDecl, seen map[int64]int64) error {
	if p.PID <= 0 {
		return domain.NewOffsetError(domain.CodeSampleParse,
			fmt.Sprintf("pid must be positive, got %d", p.PID), p.SourceOffset)
	}
	if first, dup := seen[p.PID]; dup {
		return domain.NewOffsetError(domain.CodeSampleParse,
			fmt.Sprintf("duplicate pid %d, first seen at offset %d", p.PID, first), p.SourceOffset)
	}
	if _, ok := LegalAccounts[p.Account]; !ok {
		return domain.NewOffsetError(domain.CodeSampleParse,
			"illegal account tier: "+p.Account, p.SourceOffset)
	}
	if p.ServiceWrite < 0 || p.ServiceWrite > 3 {
		return domain.NewOffsetError(domain.CodeSampleParse,
			fmt.Sprintf("service write grade out of range 0..3: %d", p.ServiceWrite), p.SourceOffset)
	}
	if p.InheritedMask > MaskMax {
		return domain.NewOffsetError(domain.CodeSampleParse,
			fmt.Sprintf("inherited mask 0x%x exceeds 0x%x", p.InheritedMask, MaskMax), p.SourceOffset)
	}
	if p.ExePath == "" {
		return domain.NewOffsetError(domain.CodeSampleParse, "empty exe path", p.SourceOffset)
	}
	return nil
}

// ParseEventStream extracts length-prefixed event blocks.
// Wire format per block: "LEN <decimal>\n" + exactly LEN JSON bytes + "\n".
func ParseEventStream(data []byte) ([]domain.Event, error) {
	events := make([]domain.Event, 0)
	seenSeq := map[int64]bool{}
	pos := int64(0)
	for len(data) > 0 {
		// skip blank separators
		if data[0] == '\n' {
			data = data[1:]
			pos++
			continue
		}
		headerEnd := bytes.IndexByte(data, '\n')
		if headerEnd < 0 {
			return nil, domain.NewOffsetError(domain.CodeSampleParse,
				"dangling LEN header", pos)
		}
		header := string(bytes.TrimSpace(data[:headerEnd]))
		var length int64
		if _, err := fmt.Sscanf(header, "LEN %d", &length); err != nil || length < 0 {
			return nil, domain.NewOffsetError(domain.CodeSampleParse,
				"illegal LEN header: "+header, pos)
		}
		blockStart := pos
		payloadStart := pos + int64(headerEnd) + 1
		rest := data[headerEnd+1:]
		if int64(len(rest)) < length+1 {
			return nil, domain.NewOffsetError(domain.CodeSampleParse,
				"event block truncated before declared length", blockStart)
		}
		payload := rest[:length]
		if rest[length] != '\n' {
			return nil, domain.NewOffsetError(domain.CodeSampleParse,
				"event block missing trailing newline", payloadStart+length)
		}
		var ev domain.Event
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, domain.NewOffsetError(domain.CodeSampleParse,
				"event payload is not valid JSON: "+err.Error(), payloadStart)
		}
		ev.SourceOffset = payloadStart
		if err := validateEvent(ev, seenSeq); err != nil {
			return nil, err
		}
		seenSeq[ev.Seq] = true
		events = append(events, ev)
		consumed := headerEnd + 1 + int(length) + 1
		data = rest[length+1:]
		pos += int64(consumed)
	}
	return events, nil
}

func validateEvent(ev domain.Event, seenSeq map[int64]bool) error {
	if ev.Seq <= 0 {
		return domain.NewOffsetError(domain.CodeSampleParse,
			fmt.Sprintf("event seq must be positive, got %d", ev.Seq), ev.SourceOffset)
	}
	if seenSeq[ev.Seq] {
		return domain.NewOffsetError(domain.CodeSampleParse,
			fmt.Sprintf("duplicate event seq %d", ev.Seq), ev.SourceOffset)
	}
	if ev.Mask > MaskMax {
		return domain.NewOffsetError(domain.CodeSampleParse,
			fmt.Sprintf("event mask 0x%x exceeds 0x%x", ev.Mask, MaskMax), ev.SourceOffset)
	}
	if ev.OffsetNS < 0 {
		return domain.NewOffsetError(domain.CodeSampleParse,
			"negative event time offset", ev.SourceOffset)
	}
	if ev.SubjectPID <= 0 {
		return domain.NewOffsetError(domain.CodeSampleParse,
			"event subject pid must be positive", ev.SourceOffset)
	}
	return nil
}

// ParseBundle normalizes both sample files into one in-memory bundle.
func ParseBundle(version string, manifest, stream []byte) (*domain.Bundle, error) {
	procs, err := ParseManifest(manifest)
	if err != nil {
		return nil, err
	}
	events, err := ParseEventStream(stream)
	if err != nil {
		return nil, err
	}
	return &domain.Bundle{Version: version, Processes: procs, Events: events}, nil
}
