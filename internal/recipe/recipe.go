// Package recipe defines the material recipe book, reference card metadata and
// incremental patch structures consumed by the pipeline.
package recipe

// NoiseParams configures deterministic value-noise octave synthesis.
type NoiseParams struct {
	Octaves    int       `json:"octaves"`
	Frequency  float64   `json:"frequency"`
	Amplitudes []float64 `json:"amplitudes"`
	Seed       int64     `json:"seed"`
}

// Recipe is a single deterministic material specification.
type Recipe struct {
	MaterialID       string      `json:"material_id"`
	Description      string      `json:"description"`
	BaseColor        [3]int      `json:"base_color"`
	Roughness        float64     `json:"roughness"`
	Metallic         float64     `json:"metallic"`
	Noise            NoiseParams `json:"noise"`
	CardName         string      `json:"card_name,omitempty"`
	TargetResolution int         `json:"target_resolution"`
}

// CardSpec pairs a card name with its actual and declared pixel dimensions.
type CardSpec struct {
	Name           string `json:"name"`
	Width          int    `json:"width"`
	Height         int    `json:"height"`
	DeclaredWidth  int    `json:"declared_width"`
	DeclaredHeight int    `json:"declared_height"`
}

// Book is the full material recipe manifest plus card declarations.
type Book struct {
	SchemaVersion int        `json:"schema_version"`
	GeneratorSeed int64      `json:"generator_seed"`
	Cards         []CardSpec `json:"cards"`
	Recipes       []Recipe   `json:"recipes"`
}

// Find returns a pointer to the recipe with the given id, or nil.
func (b *Book) Find(materialID string) *Recipe {
	for i := range b.Recipes {
		if b.Recipes[i].MaterialID == materialID {
			return &b.Recipes[i]
		}
	}
	return nil
}

// FindCard returns the card spec for a name, or nil.
func (b *Book) FindCard(name string) *CardSpec {
	for i := range b.Cards {
		if b.Cards[i].Name == name {
			return &b.Cards[i]
		}
	}
	return nil
}
