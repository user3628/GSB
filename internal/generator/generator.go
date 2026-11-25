// Package generator produces the fully deterministic local demo dataset:
// network weights, grayscale PNG shards, quantization policy and high-precision
// reference outputs. No network access or external data is involved.
package generator

import (
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"

	"qaudit/internal/infer"
	"qaudit/internal/model"
)

const (
	ImgWidth   = 8
	ImgHeight  = 8
	NumShards  = 32
	NumClasses = 4
	SeedGen    = int64(20261125)
)

// Layer shape plan for the 4-layer demo network.
var layerPlan = []struct {
	Name  string
	In    int
	Out   int
	Relu  bool
	Bits  int
	Sym   bool
	PerCh bool
}{
	{"fc1", ImgWidth * ImgHeight, 24, true, 8, true, false},
	{"fc2", 24, 16, true, 4, false, true},
	{"fc3", 16, 12, true, 8, false, false},
	{"fc4", 12, NumClasses, false, 4, true, true},
}

// Generate writes all demo artifacts under dataDir and returns the in-memory dataset.
func Generate(dataDir string) (*model.Dataset, error) {
	if err := os.MkdirAll(filepath.Join(dataDir, "shards"), 0o755); err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewPCG(uint64(SeedGen), uint64(SeedGen^0x5DEECE66D)))

	// 1. Network weights and biases with bounded, deterministic magnitudes.
	net := model.Network{Name: "demo-mlp"}
	for _, lp := range layerPlan {
		layer := model.Layer{Name: lp.Name, InDim: lp.In, OutDim: lp.Out, Relu: lp.Relu}
		scale := 1.0 / math.Sqrt(float64(lp.In))
		layer.Weights = make([][]float64, lp.Out)
		for o := 0; o < lp.Out; o++ {
			layer.Weights[o] = make([]float64, lp.In)
			for i := 0; i < lp.In; i++ {
				layer.Weights[o][i] = gaussian(rng) * scale
			}
		}
		layer.Bias = make([]float64, lp.Out)
		for o := range layer.Bias {
			layer.Bias[o] = gaussian(rng) * 0.05
		}
		net.Layers = append(net.Layers, layer)
	}
	// Inject an outlier channel into fc2 so clamping is exercised during calibration.
	for i := range net.Layers[1].Weights[3] {
		net.Layers[1].Weights[3][i] *= 1.8
	}
	net.Layers[1].Weights[3][0] = 0.9

	// 2. Grayscale shards: class-conditioned block patterns plus deterministic noise.
	shards := make([]model.Shard, 0, NumShards)
	prototype := classPrototypes(rng)
	for n := 0; n < NumShards; n++ {
		label := n % NumClasses
		img := image.NewGray(image.Rect(0, 0, ImgWidth, ImgHeight))
		for y := 0; y < ImgHeight; y++ {
			for x := 0; x < ImgWidth; x++ {
				base := int(prototype[label][y*ImgWidth+x])
				v := int(float64(base) + rng.Float64()*40)
				if v < 0 {
					v = 0
				}
				if v > 255 {
					v = 255
				}
				img.SetGray(x, y, color.Gray{Y: uint8(v)})
			}
		}
		id := shardID(n)
		relPath := filepath.Join("shards", id+".png")
		full := filepath.Join(dataDir, relPath)
		f, err := os.Create(full)
		if err != nil {
			return nil, err
		}
		if err := png.Encode(f, img); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
		shards = append(shards, model.Shard{
			ID: id, Width: ImgWidth, Height: ImgHeight, Label: label, Path: relPath,
		})
	}

	// 3. Quantization policy covering both bit widths and both modes.
	policy := model.Policy{
		Name:            "demo-policy",
		BitWidths:       map[string]int{},
		Symmetric:       map[string]bool{},
		PerChannel:      map[string]bool{},
		CalibRatio:      0.5,
		Seed:            7331,
		Tolerance:       1.25,
		SamplePositions: []int{0, 5, 10, 17, 23, 31},
	}
	for _, lp := range layerPlan {
		policy.BitWidths[lp.Name] = lp.Bits
		policy.Symmetric[lp.Name] = lp.Sym
		policy.PerChannel[lp.Name] = lp.PerCh
	}

	ds := &model.Dataset{
		Network:    net,
		Shards:     shards,
		Policy:     policy,
		InputDim:   ImgWidth * ImgHeight,
		NumClasses: NumClasses,
	}

	// 4. High-precision reference outputs at every sample position.
	ds.References = make([]model.ReferenceOutput, 0, len(policy.SamplePositions))
	for _, pos := range policy.SamplePositions {
		feat := normalizedFeature(shards[pos], mustReadImage(dataDir, shards[pos]))
		logits, boundaries := infer.ReferenceForward(net, feat)
		ds.References = append(ds.References, model.ReferenceOutput{
			Position:     pos,
			Logits:       logits,
			Label:        shards[pos].Label,
			LayerOutputs: boundaries,
		})
	}

	if err := writeJSON(filepath.Join(dataDir, "network.json"), net); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(dataDir, "shards.json"), shards); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(dataDir, "policy.json"), policy); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(dataDir, "references.json"), ds.References); err != nil {
		return nil, err
	}
	return ds, nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func shardID(n int) string {
	return "shard_" + pad(n)
}

func pad(n int) string {
	if n < 10 {
		return "0" + itoa(n)
	}
	return itoa(n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func gaussian(rng *rand.Rand) float64 {
	u, v := rng.Float64(), rng.Float64()
	if u == 0 {
		u = 1e-12
	}
	return math.Sqrt(-2*math.Log(u)) * math.Cos(2*math.Pi*v)
}

func classPrototypes(rng *rand.Rand) [NumClasses][]byte {
	var proto [NumClasses][]byte
	for c := 0; c < NumClasses; c++ {
		buf := make([]byte, ImgWidth*ImgHeight)
		// Each class lights up a distinct quadrant/block so labels are separable.
		for y := 0; y < ImgHeight; y++ {
			for x := 0; x < ImgWidth; x++ {
				base := 30
				qx, qy := c%2, c/2
				if x/4 == qx && y/4 == qy {
					base = 200
				}
				buf[y*ImgWidth+x] = uint8(base + int(rng.Float64()*20))
			}
		}
		proto[c] = buf
	}
	return proto
}

func mustReadImage(dataDir string, s model.Shard) image.Image {
	f, err := os.Open(filepath.Join(dataDir, s.Path))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	img, err := png.Decode(f)
	if err != nil {
		panic(err)
	}
	return img
}

// normalizedFeature mirrors the calib-stage fixed normalization rule.
func normalizedFeature(s model.Shard, img image.Image) []float64 {
	b := img.Bounds()
	feat := make([]float64, s.Width*s.Height)
	k := 0
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			r, g, bl, _ := img.At(x, y).RGBA()
			gray := uint8((0.299*float64(r) + 0.587*float64(g) + 0.114*float64(bl)) / 257.0)
			feat[k] = (float64(gray)/255.0 - 0.5) / 0.5
			k++
		}
	}
	return feat
}
