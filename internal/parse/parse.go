// Package parse reads rule snapshots from JSON files and from
// length-prefixed text blocks, validating and normalizing each rule.
// Invalid rules produce E_RULE_INVALID issues carrying the source
// offset and never pollute the remaining valid rules.
package parse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/model"
)

// RawRule is the wire form of one rule in both supported sources.
type RawRule struct {
	Index     int    `json:"index"`
	Direction string `json:"direction"`
	Protocol  string `json:"protocol"`
	Src       string `json:"src"`
	Dst       string `json:"dst"`
	PortLo    int    `json:"port_lo"`
	PortHi    int    `json:"port_hi"`
	Action    string `json:"action"`
	Priority  int    `json:"priority"`
}

// File is the JSON snapshot document.
type File struct {
	Snapshot string    `json:"snapshot"`
	Rules    []RawRule `json:"rules"`
}

// Result carries the valid rules and the per-entry issues.
type Result struct {
	SnapshotID string
	Rules      []model.Rule
	Issues     []*codes.Error
}

type rawFile struct {
	Snapshot string            `json:"snapshot"`
	Rules    []json.RawMessage `json:"rules"`
}

// ParseJSON parses a JSON snapshot document.
func ParseJSON(data []byte) (*Result, error) {
	var head rawFile
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("decode json snapshot: %w", err)
	}
	res := &Result{SnapshotID: head.Snapshot}
	searchFrom := 0
	for _, raw := range head.Rules {
		off := int64(-1)
		if i := bytes.Index(data[searchFrom:], raw); i >= 0 {
			off = int64(searchFrom + i)
			searchFrom += i + len(raw)
		}
		var rr RawRule
		if err := json.Unmarshal(raw, &rr); err != nil {
			res.Issues = append(res.Issues, codes.At(codes.ERuleInvalid,
				"malformed rule entry: "+err.Error(), head.Snapshot, -1, off))
			continue
		}
		res.addRule(rr, head.Snapshot, off)
	}
	res.checkDup()
	return res, nil
}

// ParseBlocks parses a length-prefixed text block stream. Each block is
// an 8-digit zero-padded decimal length followed by exactly that many
// payload bytes; the payload is one JSON-encoded RawRule.
func ParseBlocks(data []byte, snapshotID string) *Result {
	res := &Result{SnapshotID: snapshotID}
	pos := 0
	for pos < len(data) {
		blkStart := pos
		if len(data)-pos < 8 {
			res.Issues = append(res.Issues, codes.At(codes.ERuleInvalid,
				"truncated block header", snapshotID, -1, int64(blkStart)))
			break
		}
		n, err := strconv.Atoi(string(data[pos : pos+8]))
		if err != nil || n < 0 {
			res.Issues = append(res.Issues, codes.At(codes.ERuleInvalid,
				"bad block length prefix", snapshotID, -1, int64(blkStart)))
			break
		}
		pos += 8
		if len(data)-pos < n {
			res.Issues = append(res.Issues, codes.At(codes.ERuleInvalid,
				"truncated block payload", snapshotID, -1, int64(blkStart)))
			break
		}
		payload := data[pos : pos+n]
		pos += n
		var rr RawRule
		if err := json.Unmarshal(payload, &rr); err != nil {
			res.Issues = append(res.Issues, codes.At(codes.ERuleInvalid,
				"malformed block rule: "+err.Error(), snapshotID, -1, int64(blkStart)))
			continue
		}
		res.addRule(rr, snapshotID, int64(blkStart))
	}
	res.checkDup()
	return res
}

func (r *Result) addRule(rr RawRule, snapshotID string, off int64) {
	rule, ierr := validate(rr, snapshotID, off)
	if ierr != nil {
		r.Issues = append(r.Issues, ierr)
		return
	}
	r.Rules = append(r.Rules, *rule)
}

func (r *Result) checkDup() {
	seen := map[int]bool{}
	kept := r.Rules[:0]
	for _, rule := range r.Rules {
		if seen[rule.Index] {
			r.Issues = append(r.Issues, codes.At(codes.ERuleInvalid,
				fmt.Sprintf("duplicate rule index %d", rule.Index),
				r.SnapshotID, rule.Index, -1))
			continue
		}
		seen[rule.Index] = true
		kept = append(kept, rule)
	}
	r.Rules = kept
}

func validate(rr RawRule, snapshotID string, off int64) (*model.Rule, *codes.Error) {
	bad := func(msg string) *codes.Error {
		return codes.At(codes.ERuleInvalid, msg, snapshotID, rr.Index, off)
	}
	if rr.Index <= 0 {
		return nil, bad(fmt.Sprintf("rule index must be positive, got %d", rr.Index))
	}
	if !model.ValidDirection(rr.Direction) {
		return nil, bad("unsupported direction: " + rr.Direction)
	}
	if !model.ValidProtocol(rr.Protocol) {
		return nil, bad("unsupported protocol: " + rr.Protocol)
	}
	if !model.ValidAction(rr.Action) {
		return nil, bad("unsupported action: " + rr.Action)
	}
	if rr.PortLo < 0 || rr.PortHi > 65535 || rr.PortLo > rr.PortHi {
		return nil, bad(fmt.Sprintf("invalid port range [%d,%d]", rr.PortLo, rr.PortHi))
	}
	rule := &model.Rule{
		SnapshotID: snapshotID,
		Index:      rr.Index,
		Direction:  model.Direction(rr.Direction),
		Protocol:   model.Protocol(rr.Protocol),
		SrcCIDR:    rr.Src,
		DstCIDR:    rr.Dst,
		PortLo:     rr.PortLo,
		PortHi:     rr.PortHi,
		Action:     model.Action(rr.Action),
		Priority:   rr.Priority,
	}
	if err := rule.Normalize(); err != nil {
		return nil, bad(err.Error())
	}
	return rule, nil
}
