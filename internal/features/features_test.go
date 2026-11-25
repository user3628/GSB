package features

import (
	"image"
	"image/color"
	"testing"

	"imretrace/internal/model"
)

func TestNormalizeNFKCFullWidthAndWhitespace(t *testing.T) {
	got := NormalizeText("  ＡＢＣ　hello\t\u3000世界\n")
	want := "ABC hello 世界"
	if got != want {
		t.Fatalf("NormalizeText() = %q, want %q", got, want)
	}
	_, terms, vec, err := ExtractText("item-test", got)
	if err != nil {
		t.Fatal(err)
	}
	if len(terms) == 0 || allZero(vec) {
		t.Fatal("expected non-empty terms and vector")
	}
}

func TestFeatureInvalidCases(t *testing.T) {
	if _, _, _, err := ExtractText("item-empty", " \t\n "); err == nil {
		t.Fatal("expected empty caption error")
	} else if biz, ok := model.AsBiz(err); !ok || biz.Code != model.CodeFeatureInvalid || biz.ItemID != "item-empty" || biz.Stage != model.StageFeature {
		t.Fatalf("unexpected error: %#v", err)
	}

	uniform := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			uniform.SetRGBA(x, y, color.RGBA{R: 200, G: 10, B: 10, A: 255})
		}
	}
	if _, err := ExtractVisual(uniform, "item-blank", 2); err == nil {
		t.Fatal("expected blank image error")
	} else if biz, ok := model.AsBiz(err); !ok || biz.Code != model.CodeFeatureInvalid || biz.ItemID != "item-blank" {
		t.Fatalf("unexpected blank error: %#v", err)
	}

	checker := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for y := 0; y < 8; y++ {
		for x := 0; x < 8; x++ {
			if (x+y)%2 == 0 {
				checker.SetRGBA(x, y, color.RGBA{R: 255, A: 255})
			} else {
				checker.SetRGBA(x, y, color.RGBA{B: 255, A: 255})
			}
		}
	}
	_, err := ExtractVisual(checker, "item-grid", 0)
	if err == nil {
		t.Fatal("expected zero grid error")
	} else if biz, ok := model.AsBiz(err); !ok || biz.Code != model.CodeFeatureInvalid || biz.ItemID != "item-grid" || biz.Stage != model.StageFeature {
		t.Fatalf("expected structured grid error with item/stage, got %#v", err)
	}
}

func TestVisualDeterministicBytes(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 12, 12))
	for y := 0; y < 12; y++ {
		for x := 0; x < 12; x++ {
			img.SetRGBA(x, y, color.RGBA{R: uint8(x * 20), G: uint8(y * 20), B: 120, A: 255})
		}
	}
	a, err := ExtractVisual(img, "item-det", 3)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ExtractVisual(img, "item-det", 3)
	if err != nil {
		t.Fatal(err)
	}
	ca := model.MustCanonical(a)
	cb := model.MustCanonical(b)
	if string(ca) != string(cb) {
		t.Fatal("visual features are not byte deterministic")
	}
	if len(a) != model.VisualDim(3) {
		t.Fatalf("dim=%d want %d", len(a), model.VisualDim(3))
	}
}
