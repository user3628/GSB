package recipe

import "fmt"

// Patch describes an incremental edit of one material recipe. Pointer fields
// are nil when unchanged; non-nil fields are the affected field intervals.
type Patch struct {
	MaterialID string `json:"material_id"`

	Description *string `json:"description,omitempty"`

	NoiseOctaves    *int       `json:"noise_octaves,omitempty"`
	NoiseFrequency  *float64   `json:"noise_frequency,omitempty"`
	NoiseAmplitudes *[]float64 `json:"noise_amplitudes,omitempty"`

	CardName         *string  `json:"card_name,omitempty"`
	TargetResolution *int     `json:"target_resolution,omitempty"`
	BaseColor        *[3]int  `json:"base_color,omitempty"`
	Roughness        *float64 `json:"roughness,omitempty"`
	Metallic         *float64 `json:"metallic,omitempty"`
}

// Clone returns a deep copy of the recipe book.
func (b *Book) Clone() *Book {
	cp := *b
	cp.Cards = append([]CardSpec(nil), b.Cards...)
	cp.Recipes = make([]Recipe, len(b.Recipes))
	for i, r := range b.Recipes {
		r.Noise.Amplitudes = append([]float64(nil), r.Noise.Amplitudes...)
		cp.Recipes[i] = r
	}
	return &cp
}

// ApplyPatch mutates the book in place and returns the patched recipe.
func (b *Book) ApplyPatch(p Patch) (*Recipe, error) {
	r := b.Find(p.MaterialID)
	if r == nil {
		return nil, fmt.Errorf("recipe: unknown material_id %q", p.MaterialID)
	}
	if p.Description != nil {
		r.Description = *p.Description
	}
	if p.NoiseOctaves != nil {
		if *p.NoiseOctaves < 0 {
			return nil, fmt.Errorf("recipe: octaves must be >= 0")
		}
		r.Noise.Octaves = *p.NoiseOctaves
	}
	if p.NoiseFrequency != nil {
		r.Noise.Frequency = *p.NoiseFrequency
	}
	if p.NoiseAmplitudes != nil {
		r.Noise.Amplitudes = append([]float64(nil), (*p.NoiseAmplitudes)...)
	}
	if p.CardName != nil {
		r.CardName = *p.CardName
	}
	if p.TargetResolution != nil {
		if *p.TargetResolution <= 0 {
			return nil, fmt.Errorf("recipe: target resolution must be positive")
		}
		r.TargetResolution = *p.TargetResolution
	}
	if p.BaseColor != nil {
		r.BaseColor = *p.BaseColor
	}
	if p.Roughness != nil {
		r.Roughness = *p.Roughness
	}
	if p.Metallic != nil {
		r.Metallic = *p.Metallic
	}
	return r, nil
}
