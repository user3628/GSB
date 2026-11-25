package parser

import (
	"bufio"
	"encoding/json"
	"io"
	"math/big"
	"os"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/model"
)

// Dataset is the fully parsed and validated sample input.
type Dataset struct {
	Specs      []model.Spec
	SpecByKey  map[string]model.Spec
	Nodes      []model.Node
	NodeByID   map[string]model.Node
	Sources    []model.SourceLine
	References map[string]model.Reference // node|key|channel -> reference
}

// Load reads every shard described by the given paths.
func Load(specPath, nodePath, referencePath string, sourcePaths []string) (*Dataset, error) {
	ds := &Dataset{
		SpecByKey:  map[string]model.Spec{},
		NodeByID:   map[string]model.Node{},
		References: map[string]model.Reference{},
	}
	if err := readJSONL(specPath, func(line int, b []byte) error {
		var s model.Spec
		if err := json.Unmarshal(b, &s); err != nil {
			return fixedpoint.NewError(fixedpoint.ESpecInvalid, "malformed json: %v", err).WithField("spec").WithLine(line)
		}
		if err := model.ValidateSpec(s, line); err != nil {
			return err
		}
		if _, dup := ds.SpecByKey[s.KeyPath]; dup {
			return fixedpoint.NewError(fixedpoint.ESpecInvalid, "duplicate key_path %q", s.KeyPath).
				WithField("key_path").WithLine(line)
		}
		if s.Checksum != "" {
			if err := verifyChecksum(s, line); err != nil {
				return err
			}
		}
		ds.Specs = append(ds.Specs, s)
		ds.SpecByKey[s.KeyPath] = s
		return nil
	}); err != nil {
		return nil, err
	}

	if err := readJSONL(nodePath, func(line int, b []byte) error {
		var n model.Node
		if err := json.Unmarshal(b, &n); err != nil {
			return fixedpoint.NewError(fixedpoint.ESpecInvalid, "malformed json: %v", err).WithField("node").WithLine(line)
		}
		if err := model.ValidateNode(n, line); err != nil {
			return err
		}
		if _, dup := ds.NodeByID[n.ID]; dup {
			return fixedpoint.NewError(fixedpoint.ESpecInvalid, "duplicate node id %q", n.ID).
				WithField("id").WithLine(line)
		}
		ds.Nodes = append(ds.Nodes, n)
		ds.NodeByID[n.ID] = n
		return nil
	}); err != nil {
		return nil, err
	}

	for _, p := range sourcePaths {
		if err := readJSONL(p, func(line int, b []byte) error {
			var sl model.SourceLine
			if err := json.Unmarshal(b, &sl); err != nil {
				return fixedpoint.NewError(fixedpoint.ESpecInvalid, "malformed json: %v", err).
					WithField("source").WithLine(line)
			}
			if err := validateSource(sl, ds, line); err != nil {
				return err
			}
			ds.Sources = append(ds.Sources, sl)
			return nil
		}); err != nil {
			return nil, err
		}
	}

	if err := readJSONL(referencePath, func(line int, b []byte) error {
		var r model.Reference
		if err := json.Unmarshal(b, &r); err != nil {
			return fixedpoint.NewError(fixedpoint.ESpecInvalid, "malformed json: %v", err).
				WithField("reference").WithLine(line)
		}
		if _, ok := ds.NodeByID[r.NodeID]; !ok {
			return fixedpoint.NewError(fixedpoint.ESpecInvalid, "unknown node %q", r.NodeID).
				WithField("node_id").WithLine(line)
		}
		if _, ok := ds.SpecByKey[r.KeyPath]; !ok {
			return fixedpoint.NewError(fixedpoint.ESpecInvalid, "unknown key_path %q", r.KeyPath).
				WithField("key_path").WithLine(line)
		}
		if _, ok := new(big.Rat).SetString(r.Exact); !ok {
			return fixedpoint.NewError(fixedpoint.ESpecInvalid, "reference exact value %q is not rational", r.Exact).
				WithField("exact").WithLine(line)
		}
		ds.References[refKey(r.NodeID, r.KeyPath, r.Channel)] = r
		return nil
	}); err != nil {
		return nil, err
	}
	return ds, nil
}

func validateSource(sl model.SourceLine, ds *Dataset, line int) error {
	field := func(name string) *fixedpoint.Error {
		return fixedpoint.NewError(fixedpoint.ESpecInvalid, "invalid source line").
			WithField(name).WithLine(line)
	}
	if _, ok := ds.NodeByID[sl.NodeID]; !ok {
		return field("node_id")
	}
	spec, ok := ds.SpecByKey[sl.KeyPath]
	if !ok {
		return field("key_path")
	}
	node := ds.NodeByID[sl.NodeID]
	if !channelExists(node, sl.Channel) {
		return field("channel")
	}
	if sl.Format != "v1" && sl.Format != "v2" {
		return field("format")
	}
	if sl.Format == "v2" {
		if sl.Value == "" || sl.Unit == "" {
			return field("value/unit")
		}
	}
	if !model.ValidKeyPath(spec.KeyPath) {
		return field("key_path")
	}
	return nil
}

func channelExists(n model.Node, name string) bool {
	for _, c := range n.Channels {
		if c.Name == name {
			return true
		}
	}
	return false
}

func refKey(node, key, channel string) string { return node + "|" + key + "|" + channel }

func readJSONL(path string, handle func(line int, raw []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	line := 0
	for {
		raw, err := br.ReadBytes('\n')
		if len(raw) > 0 {
			line++
			trimmed := trimSpace(raw)
			if len(trimmed) > 0 {
				if hErr := handle(line, trimmed); hErr != nil {
					return hErr
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func trimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && (b[start] == ' ' || b[start] == '\t' || b[start] == '\r' || b[start] == '\n') {
		start++
	}
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t' || b[end-1] == '\r' || b[end-1] == '\n') {
		end--
	}
	return b[start:end]
}
