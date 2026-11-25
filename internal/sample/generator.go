package sample

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/model"
	"configaudit/internal/units"
)

// DefaultSeed is the fixed deterministic seed for offline sample generation.
const DefaultSeed int64 = 20261126

// Layout describes where generated artifacts live.
type Layout struct {
	Dir       string
	SpecFile  string
	NodeFile  string
	Sources   []string
	Reference string
}

// Spec definitions cover integer and decimal items, multiply/divide chains and
// every allowed scale.
var specDefs = []model.Spec{
	{
		KeyPath: "storage.buffer_bytes", Type: "integer", Scale: 0,
		Chain:         []fixedpoint.ChainStep{{Op: "mul", Factor: 1024}, {Op: "mul", Factor: 1024}},
		Priority:      10,
		DeclaredBound: "1",
	},
	{
		KeyPath: "network.rate_kbps", Type: "decimal", Scale: 3,
		Chain:         []fixedpoint.ChainStep{{Op: "mul", Factor: 1000}, {Op: "div", Factor: 8}},
		Priority:      20,
		DeclaredBound: "0.5",
	},
	{
		KeyPath: "runtime.timeout_ms", Type: "decimal", Scale: 6,
		Chain:         []fixedpoint.ChainStep{{Op: "div", Factor: 3}, {Op: "mul", Factor: 7}},
		Priority:      30,
		DeclaredBound: "0.001",
	},
	{
		KeyPath: "quota.cpu_millis", Type: "decimal", Scale: 9,
		Chain:         []fixedpoint.ChainStep{{Op: "mul", Factor: 1000}, {Op: "div", Factor: 1024}},
		Priority:      40,
		DeclaredBound: "0.0001",
	},
}

var nodeDefs = []model.Node{
	{ID: "node-a", Channels: []model.Channel{
		{Name: "file", Priority: 10, Order: 0},
		{Name: "env", Priority: 30, Order: 1},
		{Name: "template", Priority: 20, Order: 2},
		{Name: "script", Priority: 40, Order: 3, Exclusive: true},
	}},
	{ID: "node-b", Channels: []model.Channel{
		{Name: "file", Priority: 20, Order: 0},
		{Name: "env", Priority: 30, Order: 1},
		{Name: "template", Priority: 10, Order: 2},
	}},
}

// value templates per spec: numeric literal + unit enum, valid for the registry.
var valueTemplates = map[string][]struct {
	num  string
	unit string
}{
	"storage.buffer_bytes": {{"512", "MiB"}, {"256", "MiB"}, {"1", "GiB"}, {"768", "KiB"}},
	"network.rate_kbps":    {{"1.5", "GB"}, {"500", "MB"}, {"2.25", "GB"}, {"100", "MB"}},
	"runtime.timeout_ms":   {{"250", "m"}, {"1.2", "B"}, {"750", "m"}, {"3", "B"}},
	"quota.cpu_millis":     {{"1.25", "KiB"}, {"800", "B"}, {"2", "MiB"}, {"350", "B"}},
}

var channelNames = []string{"file", "env", "template", "script"}

// Generate writes deterministic JSONL shards and reference files under dir.
func Generate(dir string, seed int64) (*Layout, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewSource(seed))
	layout := &Layout{
		Dir:       dir,
		SpecFile:  filepath.Join(dir, "specs.jsonl"),
		NodeFile:  filepath.Join(dir, "nodes.jsonl"),
		Reference: filepath.Join(dir, "references.jsonl"),
	}

	// Specs with a deterministic checksum.
	if err := writeJSONL(layout.SpecFile, len(specDefs), func(i int) (any, error) {
		s := specDefs[i]
		s.Checksum = checksumSpec(s)
		return s, nil
	}); err != nil {
		return nil, err
	}
	if err := writeJSONL(layout.NodeFile, len(nodeDefs), func(i int) (any, error) {
		return nodeDefs[i], nil
	}); err != nil {
		return nil, err
	}

	// One source shard per node, alternating description generations.
	var refs []model.Reference
	for ni, node := range nodeDefs {
		shard := filepath.Join(dir, fmt.Sprintf("sources-%s.jsonl", node.ID))
		layout.Sources = append(layout.Sources, shard)
		type lineBundle struct {
			line model.SourceLine
			ref  model.Reference
		}
		var bundles []lineBundle
		for si, spec := range specDefs {
			tpls := valueTemplates[spec.KeyPath]
			for ci, chName := range channelNames {
				if !hasChannel(node, chName) {
					continue
				}
				tpl := tpls[(rng.Intn(len(tpls))+si+ci+ni)%len(tpls)]
				ch := channel(node, chName)
				useV1 := (si+ci+ni)%2 == 0
				sl := model.SourceLine{
					NodeID: node.ID, KeyPath: spec.KeyPath, Channel: chName,
					Format: map[bool]string{true: "v1", false: "v2"}[useV1],
					Text:   legacyText(tpl.num, tpl.unit),
				}
				if !useV1 {
					sl.Value = tpl.num
					sl.Unit = tpl.unit
					sl.Priority = ch.Priority
					sl.Order = ch.Order
					sl.Text = fmt.Sprintf(`{"value":%q,"unit":%q}`, tpl.num, tpl.unit)
				}
				// High precision reference: exact unit normalization followed by
				// the exact rational evaluation of the spec conversion chain.
				exact, err := exactUnitValue(tpl.num, tpl.unit)
				if err != nil {
					return nil, err
				}
				for _, st := range spec.Chain {
					if st.Op == "mul" {
						exact.Mul(exact, big.NewRat(st.Factor, 1))
					} else {
						exact.Quo(exact, big.NewRat(st.Factor, 1))
					}
				}
				bundles = append(bundles, lineBundle{
					line: sl,
					ref: model.Reference{
						NodeID: node.ID, KeyPath: spec.KeyPath, Channel: chName,
						Exact: exact.RatString(),
					},
				})
			}
		}
		if err := writeJSONL(shard, len(bundles), func(i int) (any, error) {
			return bundles[i].line, nil
		}); err != nil {
			return nil, err
		}
		for _, b := range bundles {
			refs = append(refs, b.ref)
		}
	}
	if err := writeJSONL(layout.Reference, len(refs), func(i int) (any, error) {
		return refs[i], nil
	}); err != nil {
		return nil, err
	}
	return layout, nil
}

// exactUnitValue returns the exact rational value in base units (num * unit).
func exactUnitValue(num, enum string) (*big.Rat, error) {
	u, ok := units.ByEnum(enum)
	if !ok {
		return nil, fixedpoint.NewError(fixedpoint.EUnitInvalid, "unknown enum %q", enum)
	}
	r, ok := new(big.Rat).SetString(num)
	if !ok {
		return nil, fixedpoint.NewError(fixedpoint.EInvalidInput, "bad number %q", num)
	}
	return new(big.Rat).Mul(r, u.Rat()), nil
}

func legacyText(num, enum string) string {
	switch enum {
	case "m":
		return num + "m"
	case "B":
		return num + "B"
	case "KiB":
		return num + "Ki" // exercise IEC alias suffixes
	case "MiB":
		return num + "MiB"
	case "GiB":
		return num + "GiB"
	case "TiB":
		return num + "TiB"
	case "kB":
		return num + "k"
	case "MB":
		return num + "M"
	case "GB":
		return num + "G"
	default:
		return num + enum
	}
}

func hasChannel(n model.Node, name string) bool {
	for _, c := range n.Channels {
		if c.Name == name {
			return true
		}
	}
	return false
}

func channel(n model.Node, name string) model.Channel {
	for _, c := range n.Channels {
		if c.Name == name {
			return c
		}
	}
	return model.Channel{}
}

func writeJSONL(path string, n int, item func(i int) (any, error)) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for i := 0; i < n; i++ {
		v, err := item(i)
		if err != nil {
			return err
		}
		if err := enc.Encode(v); err != nil {
			return err
		}
	}
	return w.Flush()
}
