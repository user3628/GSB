// Package palette implements stage 3: reference card loading/validation,
// palette matching, deterministic resampling and 8-bit quantization.
package palette

import (
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"

	"matbench/internal/recipe"
)

// RenderCard writes a deterministic seeded reference color card PNG. Card
// pixels are generated locally from splitmix64 mixing - no external assets.
func RenderCard(dir string, spec recipe.CardSpec, seed int64) error {
	img := image.NewRGBA(image.Rect(0, 0, spec.Width, spec.Height))
	for y := 0; y < spec.Height; y++ {
		for x := 0; x < spec.Width; x++ {
			h := mix64(uint64(seed), uint64(x), uint64(y), uint64(spec.Width), uint64(spec.Height))
			img.SetRGBA(x, y, color.RGBA{
				R: uint8(h & 0xFF),
				G: uint8((h >> 8) & 0xFF),
				B: uint8((h >> 16) & 0xFF),
				A: 255,
			})
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, spec.Name))
	if err != nil {
		return err
	}
	defer f.Close()
	return png.Encode(f, img)
}

func mix64(parts ...uint64) uint64 {
	h := uint64(0xCBF29CE484222325)
	for _, p := range parts {
		h ^= p
		h *= 0x100000001B3
		h = (h ^ (h >> 33)) * 0xFF51AFD7ED558CCD
		h ^= h >> 33
	}
	return h
}

// LoadCard reads and validates a reference card against its declared size.
func LoadCard(dir string, spec recipe.CardSpec) (*image.RGBA, error) {
	f, err := os.Open(filepath.Join(dir, spec.Name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	img, err := png.Decode(f)
	if err != nil {
		return nil, err
	}
	bounds := img.Bounds()
	if bounds.Dx() != spec.DeclaredWidth || bounds.Dy() != spec.DeclaredHeight {
		return nil, nil
	}
	rgba, ok := img.(*image.RGBA)
	if !ok {
		rgba = image.NewRGBA(bounds)
		for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
			for x := bounds.Min.X; x < bounds.Max.X; x++ {
				rgba.Set(x, y, img.At(x, y))
			}
		}
	}
	if rgba.Rect.Dx() != spec.Width || rgba.Rect.Dy() != spec.Height {
		return nil, nil
	}
	return rgba, nil
}

// RGB is a palette color.
type RGB struct {
	R, G, B uint8
}

// ExtractPalette returns de-duplicated card colors in deterministic raster order.
func ExtractPalette(img *image.RGBA) []RGB {
	seen := map[RGB]bool{}
	var out []RGB
	b := img.Bounds()
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			c := img.RGBAAt(x, y)
			col := RGB{R: c.R, G: c.G, B: c.B}
			if !seen[col] {
				seen[col] = true
				out = append(out, col)
			}
		}
	}
	return out
}
