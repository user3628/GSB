package recipe

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// DemoSeed is the fixed seed of the built-in deterministic sample generator.
const DemoSeed int64 = 20261125

// GenerateBook produces the built-in deterministic recipe book. No network,
// GPU or external dataset is involved.
func GenerateBook() *Book {
	rng := NewRNG(DemoSeed)
	cards := make([]CardSpec, 0, 4)
	for _, dims := range [][2]int{{8, 8}, {12, 12}, {10, 8}, {16, 12}} {
		name := cardName(len(cards))
		cards = append(cards, CardSpec{
			Name:           name,
			Width:          dims[0],
			Height:         dims[1],
			DeclaredWidth:  dims[0],
			DeclaredHeight: dims[1],
		})
		// Advance the stream in a fixed pattern even though the card pixels
		// are rendered by the palette package with its own seeded stream.
		rng.Uint64()
	}
	recipes := []Recipe{
		{
			MaterialID:       "MAT-0001",
			Description:      "Brushed  Ａｌｕｍｉｎｕｍ　拉丝铝 metal, fine grain／细条纹",
			BaseColor:        [3]int{176, 182, 190},
			Roughness:        0.38,
			Metallic:         0.92,
			Noise:            makeNoise(rng, 4, 2.0),
			CardName:         cards[0].Name,
			TargetResolution: 32,
		},
		{
			MaterialID:       "MAT-0002",
			Description:      "weathered Ｒｕｓｔ铁锈 rusted iron／锈蚀金属板",
			BaseColor:        [3]int{140, 78, 47},
			Roughness:        0.81,
			Metallic:         0.35,
			Noise:            makeNoise(rng, 6, 1.5),
			CardName:         cards[1].Name,
			TargetResolution: 48,
		},
		{
			MaterialID:       "MAT-0003",
			Description:      "matte ceramic 陶瓷 glaze／雾面　Glaze, soft spots",
			BaseColor:        [3]int{224, 220, 208},
			Roughness:        0.64,
			Metallic:         0.02,
			Noise:            makeNoise(rng, 3, 3.0),
			CardName:         cards[2].Name,
			TargetResolution: 24,
		},
		{
			MaterialID:       "MAT-0004",
			Description:      "wet mossy Ｓｔｏｎｅ石材 stone／青苔石 coarse noise",
			BaseColor:        [3]int{84, 103, 74},
			Roughness:        0.73,
			Metallic:         0.0,
			Noise:            makeNoise(rng, 5, 2.5),
			CardName:         cards[3].Name,
			TargetResolution: 40,
		},
	}
	return &Book{
		SchemaVersion: 1,
		GeneratorSeed: DemoSeed,
		Cards:         cards,
		Recipes:       recipes,
	}
}

func makeNoise(rng *RNG, octaves int, frequency float64) NoiseParams {
	amps := make([]float64, octaves)
	total := 0.0
	for i := range amps {
		a := 1.0 / float64(i+1)
		amps[i] = a
		total += a
	}
	for i := range amps {
		amps[i] /= total
	}
	return NoiseParams{
		Octaves:    octaves,
		Frequency:  frequency,
		Amplitudes: amps,
		Seed:       int64(rng.Int63()),
	}
}

// Int63 returns a deterministic non-negative int64.
func (r *RNG) Int63() int64 {
	return int64(r.Uint64() >> 1)
}

func cardName(i int) string {
	return "card-" + string(rune('A'+i)) + ".png"
}

// WriteBook serializes the recipe book as canonical-ish indented JSON.
func WriteBook(book *Book, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(book, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

// ReadBook loads a recipe book from JSON.
func ReadBook(path string) (*Book, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var book Book
	if err := json.Unmarshal(data, &book); err != nil {
		return nil, err
	}
	return &book, nil
}
