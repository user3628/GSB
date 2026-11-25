package dataset

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"

	"local/quantops/pkg/canon"
	"local/quantops/pkg/quant"
)

type shardPlan struct {
	id            string
	ordinal       int
	description   string
	rows          int
	inputCols     int
	weightCols    []int
	configs       []quant.Config
	actOutliers   map[int]float64
	weightOutlier map[string]map[int]float64
	zeroActRows   []int
	zeroWeightCh  map[int][]int
	nanAct        []int
	nanWeight     map[int][]int
	absTol        float64
	relTol        float64
}

// Generate creates the complete local sample workspace from a fixed seed. It
// is deterministic for a given seed and directory content layout.
func Generate(dir string, seed int64) (*Loaded, error) {
	if seed == 0 {
		seed = DefaultSeed
	}
	if err := os.MkdirAll(filepath.Join(dir, "tensors"), 0o755); err != nil {
		return nil, err
	}
	plans := samplePlans()
	m := Manifest{
		SchemaVersion: SchemaVersion,
		Name:          "low-precision-quantization-demo",
		Seed:          seed,
	}
	for _, p := range plans {
		sh := Shard{
			ID:          p.id,
			Ordinal:     p.ordinal,
			Description: p.description,
			Verification: VerificationSpec{
				AbsoluteTolerance: p.absTol,
				RelativeTolerance: p.relTol,
			},
		}
		actName := p.id + ".activation"
		actRef, actFile := makeTensor(p.id+" activation", actName, p.rows, p.inputCols, seed, p.ordinal, "activation")
		applyMarkers(&actFile, p.actOutliers, p.zeroActRows, p.nanAct)
		actFile.ChannelScales = channelScales(actFile)
		if err := writeTensor(dir, actName, actFile); err != nil {
			return nil, err
		}
		actRef.Path = tensorRelPath(actName)
		sh.Activation = actRef

		prevCols := actFile.Shape[1]
		for layer := 0; layer < len(p.weightCols); layer++ {
			cols := p.weightCols[layer]
			wName := fmt.Sprintf("%s.weight.%d", p.id, layer)
			wRef, wFile := makeTensor(fmt.Sprintf("%s weight %d", p.id, layer), wName, cols, prevCols, seed, p.ordinal*10+layer+1, "weight")
			var outliers map[int]float64
			if p.weightOutlier != nil {
				outliers = p.weightOutlier[fmt.Sprintf("layer%d", layer)]
			}
			var zeroRows []int
			if p.zeroWeightCh != nil {
				zeroRows = p.zeroWeightCh[layer]
			}
			var nans []int
			if p.nanWeight != nil {
				nans = p.nanWeight[layer]
			}
			applyMarkers(&wFile, outliers, zeroRows, nans)
			wFile.ChannelScales = channelScales(wFile)
			if err := writeTensor(dir, wName, wFile); err != nil {
				return nil, err
			}
			wRef.Path = tensorRelPath(wName)
			sh.Weights = append(sh.Weights, wRef)
			cfg := p.configs[layer]
			sh.Chain = append(sh.Chain, Layer{
				Name:       fmt.Sprintf("linear_%d", layer),
				Activation: cfg,
				Weight:     cfg,
			})
			prevCols = cols
		}
		sh.ExpectedOutputShape = []int{p.rows, prevCols}
		sh.Verification.SamplePoints = samplePoints(p.rows, p.weightCols)
		m.Shards = append(m.Shards, sh)
	}
	sort.Slice(m.Shards, func(i, j int) bool { return m.Shards[i].Ordinal < m.Shards[j].Ordinal })
	mb, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	mb = append(mb, '\n')
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := canon.WriteFileAtomic(manifestPath, mb, 0o644); err != nil {
		return nil, err
	}
	return Load(manifestPath)
}

func samplePlans() []shardPlan {
	sym8pc := quant.Config{Bits: 8, Mode: quant.Symmetric, Granularity: quant.PerChannel}
	asym8pt := quant.Config{Bits: 8, Mode: quant.Asymmetric, Granularity: quant.PerTensor}
	sym4pc := quant.Config{Bits: 4, Mode: quant.Symmetric, Granularity: quant.PerChannel}
	asym4pt := quant.Config{Bits: 4, Mode: quant.Asymmetric, Granularity: quant.PerTensor}
	asym4pc := quant.Config{Bits: 4, Mode: quant.Asymmetric, Granularity: quant.PerChannel}
	return []shardPlan{
		{
			id: "s01-sym8-perchannel-outlier", ordinal: 1,
			description: "8-bit symmetric per-channel with activation and weight outliers",
			rows:        4, inputCols: 8, weightCols: []int{6}, configs: []quant.Config{sym8pc},
			actOutliers:   map[int]float64{3: 18.75},
			weightOutlier: map[string]map[int]float64{"layer0": {5: -11.5, 29: 13.25}},
			absTol:        1.25, relTol: 0.50,
		},
		{
			id: "s02-asym8-pertensor-zerochannel", ordinal: 2,
			description: "8-bit asymmetric per-tensor with all-zero activation and weight channels",
			rows:        3, inputCols: 10, weightCols: []int{5}, configs: []quant.Config{asym8pt},
			zeroActRows: []int{1}, zeroWeightCh: map[int][]int{0: {3}},
			absTol: 0.15, relTol: 0.15,
		},
		{
			id: "s03-sym4-perchannel-outlier", ordinal: 3,
			description: "4-bit symmetric per-channel with strong outliers and explicit zeros",
			rows:        4, inputCols: 8, weightCols: []int{7}, configs: []quant.Config{sym4pc},
			actOutliers:   map[int]float64{2: 16.0, 18: -12.5},
			weightOutlier: map[string]map[int]float64{"layer0": {7: 9.25}},
			absTol:        3.25, relTol: 0.75,
		},
		{
			id: "s04-asym4-pertensor-nan", ordinal: 4,
			description: "4-bit asymmetric per-tensor with explicit NaN anomaly metadata",
			rows:        3, inputCols: 9, weightCols: []int{6}, configs: []quant.Config{asym4pt},
			nanAct: []int{2, 13}, nanWeight: map[int][]int{0: {4, 21}},
			absTol: 2.5, relTol: 0.85,
		},
		{
			id: "s05-chain-mixed-8-4", ordinal: 5,
			description: "two-layer mixed chain: 8-bit symmetric then 4-bit asymmetric, both per-channel",
			rows:        4, inputCols: 8, weightCols: []int{6, 5}, configs: []quant.Config{sym8pc, asym4pc},
			actOutliers:   map[int]float64{1: 9.5},
			weightOutlier: map[string]map[int]float64{"layer0": {2: -7.75}, "layer1": {9: 7.5}},
			absTol:        3.5, relTol: 0.80,
		},
		{
			id: "s06-chain-asym4-perchannel", ordinal: 6,
			description: "two-layer 4-bit asymmetric per-channel chain with a zero weight channel",
			rows:        3, inputCols: 7, weightCols: []int{5, 4}, configs: []quant.Config{asym4pc, asym4pc},
			zeroWeightCh: map[int][]int{0: {2}},
			actOutliers:  map[int]float64{5: 8.25},
			absTol:       4.0, relTol: 0.95,
		},
	}
}

func makeTensor(desc, name string, rows, cols int, seed int64, salt int, kind string) (TensorRef, TensorFile) {
	shape := []int{rows, cols}
	values := make([]float64, rows*cols)
	r := rand.New(rand.NewSource(mixSeed(seed, name, salt)))
	limit := 1.1
	if kind == "weight" {
		limit = 0.9
	}
	for i := range values {
		v := (r.Float64()*2 - 1) * limit
		values[i] = round6(v)
		if i%11 == 0 {
			values[i] = 0
		}
	}
	tf := TensorFile{
		SchemaVersion: SchemaVersion, Name: name, Shape: shape,
		Layout: "row_major", Values: values, ChannelScales: make([]float64, rows),
	}
	return TensorRef{Name: name, Shape: shape}, tf
}

func mixSeed(seed int64, name string, salt int) int64 {
	// FNV-1a on the tensor name keeps each tensor stable even if unrelated
	// shards are added or reordered.
	h := uint64(1469598103934665603)
	for i := 0; i < len(name); i++ {
		h ^= uint64(name[i])
		h *= 1099511628211
	}
	return seed ^ int64(h) ^ int64(salt)*0x9e3779b9
}

func applyMarkers(tf *TensorFile, outliers map[int]float64, zeroRows []int, nans []int) {
	rows, cols := tf.Shape[0], tf.Shape[1]
	for pos, v := range outliers {
		if pos >= 0 && pos < len(tf.Values) {
			tf.Values[pos] = v
		}
	}
	for _, row := range zeroRows {
		if row < 0 || row >= rows {
			continue
		}
		for col := 0; col < cols; col++ {
			tf.Values[row*cols+col] = 0
		}
	}
	seen := make(map[int]bool)
	for _, pos := range nans {
		if pos < 0 || pos >= len(tf.Values) || seen[pos] {
			continue
		}
		seen[pos] = true
		tf.Values[pos] = 0
		tf.NaNPositions = append(tf.NaNPositions, pos)
	}
	sort.Ints(tf.NaNPositions)
}

func channelScales(tf TensorFile) []float64 {
	rows, cols := tf.Shape[0], tf.Shape[1]
	nans := make(map[int]bool, len(tf.NaNPositions))
	for _, p := range tf.NaNPositions {
		nans[p] = true
	}
	out := make([]float64, rows)
	for row := 0; row < rows; row++ {
		maxAbs := 0.0
		for col := 0; col < cols; col++ {
			i := row*cols + col
			if nans[i] {
				continue
			}
			if a := math.Abs(tf.Values[i]); a > maxAbs {
				maxAbs = a
			}
		}
		if maxAbs == 0 {
			out[row] = 1
		} else {
			out[row] = maxAbs / 127.0
		}
	}
	return out
}

func writeTensor(dir, name string, tf TensorFile) error {
	b, err := json.MarshalIndent(tf, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return canon.WriteFileAtomic(filepath.Join(dir, filepath.FromSlash(tensorRelPath(name))), b, 0o644)
}

func tensorRelPath(name string) string { return "tensors/" + name + ".json" }

func samplePoints(rows int, weightCols []int) []SamplePoint {
	var points []SamplePoint
	for layer, cols := range weightCols {
		candidates := [][2]int{
			{0, 0}, {rows - 1, cols - 1}, {rows / 2, cols / 2},
			{0, cols - 1}, {rows - 1, 0},
		}
		seen := make(map[[2]int]bool)
		for _, c := range candidates {
			if c[0] < 0 || c[0] >= rows || c[1] < 0 || c[1] >= cols || seen[c] {
				continue
			}
			seen[c] = true
			points = append(points, SamplePoint{Layer: layer, Row: c[0], Col: c[1]})
			if len(seen) == 3 {
				break
			}
		}
	}
	return points
}

func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }
