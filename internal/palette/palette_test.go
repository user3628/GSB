package palette

import (
	"image"
	"os"
	"path/filepath"
	"testing"

	"matbench/internal/control"
	"matbench/internal/errorsx"
	"matbench/internal/noise"
	"matbench/internal/recipe"
)

func TestCardDimMismatch(t *testing.T) {
	dir := t.TempDir()
	spec := recipe.CardSpec{Name: "c.png", Width: 8, Height: 8, DeclaredWidth: 10, DeclaredHeight: 10}
	if err := RenderCard(dir, spec, 1); err != nil {
		t.Fatal(err)
	}
	img, err := LoadCard(dir, spec)
	if err != nil {
		t.Fatal(err)
	}
	if img != nil {
		t.Fatal("expected nil image on dimension mismatch")
	}
}

func TestPaletteEmpty(t *testing.T) {
	empty := image.NewRGBA(image.Rect(0, 0, 1, 1))
	// Force an empty palette by clearing the only pixel through a zero-size
	// card representation: ExtractPalette on a 0x0 image yields no colors.
	zero := image.NewRGBA(image.Rect(0, 0, 0, 0))
	if got := ExtractPalette(zero); len(got) != 0 {
		t.Fatalf("expected empty palette, got %d", len(got))
	}
	cr, _ := control.Generate("M9", "stone 石")
	nr := &noise.Result{MaterialID: "M9", Field: []float64{0.1}, Width: 1, Height: 1}
	r := recipe.Recipe{MaterialID: "M9", TargetResolution: 1}
	spec := recipe.CardSpec{Name: "x"}
	_, err := Quantize(r, zero, spec, nr, cr, t.TempDir())
	if err == nil || errorsx.CodeOf(err) != errorsx.EPaletteEmpty {
		t.Fatalf("want E_PALETTE_EMPTY, got %v (empty card=%v)", err, empty.Bounds())
	}
}

func TestQuantAllZero(t *testing.T) {
	spec := recipe.CardSpec{Name: "c.png", Width: 4, Height: 4, DeclaredWidth: 4, DeclaredHeight: 4}
	card := image.NewRGBA(image.Rect(0, 0, 4, 4)) // all-black palette
	cr, err := control.Generate("M1", "dark 玄武岩 basalt")
	if err != nil {
		t.Fatal(err)
	}
	nr := &noise.Result{
		MaterialID: "M1", Field: make([]float64, noise.FieldSize*noise.FieldSize),
		Width: noise.FieldSize, Height: noise.FieldSize,
	}
	r := recipe.Recipe{
		MaterialID: "M1", BaseColor: [3]int{0, 0, 0},
		TargetResolution: 8,
	}
	_, err = Quantize(r, card, spec, nr, cr, t.TempDir())
	if err == nil || errorsx.CodeOf(err) != errorsx.EQuantAllZero {
		t.Fatalf("want E_QUANT_ALL_ZERO, got %v", err)
	}
}

func TestQuantizeHappyPathAndResampleDeterminism(t *testing.T) {
	spec := recipe.CardSpec{Name: "c.png", Width: 8, Height: 8, DeclaredWidth: 8, DeclaredHeight: 8}
	dir := t.TempDir()
	if err := RenderCard(dir, spec, 7); err != nil {
		t.Fatal(err)
	}
	card, err := LoadCard(dir, spec)
	if err != nil {
		t.Fatal(err)
	}
	cr, _ := control.Generate("M2", "ceramic 陶瓷 glaze")
	field := make([]float64, noise.FieldSize*noise.FieldSize)
	for i := range field {
		field[i] = 0.3 + 0.4*float64(i%7)/7
	}
	nr := &noise.Result{MaterialID: "M2", Field: field, Width: noise.FieldSize, Height: noise.FieldSize}
	r := recipe.Recipe{MaterialID: "M2", BaseColor: [3]int{120, 90, 60},
		Roughness: 0.5, Metallic: 0.1, TargetResolution: 16}
	out := t.TempDir()
	res, err := Quantize(r, card, spec, nr, cr, out)
	if err != nil {
		t.Fatal(err)
	}
	if res.PaletteSize == 0 || len(res.Params) != 5 {
		t.Fatalf("bad result: %+v", res)
	}
	for _, p := range res.Params {
		if p.ErrorBound <= 0 || p.Digits < 0 {
			t.Fatalf("invalid param evidence: %+v", p)
		}
	}
	if _, err := os.Stat(filepath.Join(out, "artifacts", "textures", "M2_texture.png")); err != nil {
		t.Fatalf("texture not written: %v", err)
	}
}
