package generator

import (
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"

	"ocrlab/internal/canonical"
	"ocrlab/internal/model"
	"ocrlab/internal/workspace"
)

const Seed int64 = 20261125

var textAlphabet = []rune("ABCDEFGHJKLMNPQRSTUVWXYZ23456789")

type groupPlan struct {
	name     string
	shards   int
	width    int
	height   int
	lines    int
	language []string
}

var plans = []groupPlan{
	{name: "small", shards: 4, width: 320, height: 200, lines: 3, language: []string{"en-Latn", "zh-Latn"}},
	{name: "medium", shards: 16, width: 360, height: 230, lines: 4, language: []string{"en-Latn", "zh-Latn", "und-Latn"}},
	{name: "large", shards: 40, width: 400, height: 260, lines: 5, language: []string{"en-Latn", "zh-Latn", "und-Latn"}},
}

var textPalette = []color.RGBA{
	{R: 28, G: 45, B: 78, A: 255},
	{R: 81, G: 35, B: 39, A: 255},
	{R: 24, G: 67, B: 52, A: 255},
	{R: 66, G: 42, B: 86, A: 255},
	{R: 92, G: 55, B: 24, A: 255},
}

type Result struct {
	Manifest     model.Manifest `json:"manifest"`
	ManifestPath string         `json:"manifest_path"`
	ManifestSHA  string         `json:"manifest_sha256"`
	Images       int            `json:"images"`
}

// Ensure creates the deterministic corpus if it is absent. force rewrites the
// same bytes and is primarily useful for reproducibility checks.
func Ensure(paths workspace.Paths, force bool) (Result, error) {
	manifestExists := fileExists(paths.ManifestPath)
	if manifestExists && !force {
		b, err := os.ReadFile(paths.ManifestPath)
		if err != nil {
			return Result{}, err
		}
		var manifest model.Manifest
		if err := json.Unmarshal(b, &manifest); err != nil {
			return Result{}, err
		}
		if manifest.SchemaVersion != model.SchemaVersion || manifest.GeneratorVersion != model.GeneratorVersion || manifest.Seed != Seed {
			return Generate(paths)
		}
		allPresent := true
		for _, imageSpec := range manifest.Images {
			path, err := paths.ImagePath(imageSpec.File)
			if err != nil || !fileExists(path) {
				allPresent = false
				break
			}
		}
		if allPresent {
			return Result{Manifest: manifest, ManifestPath: paths.ManifestPath, ManifestSHA: canonical.DigestBytes(b), Images: len(manifest.Images)}, nil
		}
	}
	return Generate(paths)
}

func Generate(paths workspace.Paths) (Result, error) {
	rng := rand.New(rand.NewSource(Seed))
	manifest := model.Manifest{
		SchemaVersion:    model.SchemaVersion,
		Seed:             Seed,
		GeneratorVersion: model.GeneratorVersion,
		Groups:           make([]model.GroupSpec, 0, len(plans)),
		Images:           make([]model.ImageSpec, 0, 60),
	}
	for _, plan := range plans {
		groupImages := make([]model.ImageSpec, 0, plan.shards)
		totalChars := 0
		for i := 0; i < plan.shards; i++ {
			imageID := fmt.Sprintf("%s-%03d", plan.name, i+1)
			spec, img, err := renderSample(rng, plan, imageID)
			if err != nil {
				return Result{}, err
			}
			path, err := paths.ImagePath(spec.File)
			if err != nil {
				return Result{}, err
			}
			if err := writePNG(path, img); err != nil {
				return Result{}, err
			}
			for _, box := range spec.Truth {
				totalChars += len([]rune(box.Text))
			}
			groupImages = append(groupImages, spec)
		}
		manifest.Groups = append(manifest.Groups, model.GroupSpec{
			Name:         plan.name,
			TargetShards: plan.shards,
			Observed: model.GroupObserved{
				ShardCount:      len(groupImages),
				TotalCharacters: totalChars,
			},
		})
		manifest.Images = append(manifest.Images, groupImages...)
	}
	if err := os.MkdirAll(filepath.Dir(paths.ManifestPath), 0o755); err != nil {
		return Result{}, err
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Result{}, err
	}
	b = append(b, '\n')
	tmp := paths.ManifestPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return Result{}, err
	}
	if err := os.Rename(tmp, paths.ManifestPath); err != nil {
		return Result{}, err
	}
	return Result{Manifest: manifest, ManifestPath: paths.ManifestPath, ManifestSHA: canonical.DigestBytes(b), Images: len(manifest.Images)}, nil
}

// renderSample chooses all style and truth parameters. renderImage later uses
// those same parameters, making the PNG and the manifest inseparable.
func renderSample(rng *rand.Rand, plan groupPlan, imageID string) (model.ImageSpec, image.Image, error) {
	slotHeight := float64(plan.height-28) / float64(plan.lines)
	truth := make([]model.TruthBox, 0, plan.lines)
	styles := make([]lineStyle, 0, plan.lines)
	for line := 0; line < plan.lines; line++ {
		scale := 2 + rng.Intn(3)
		length := 3 + rng.Intn(3)
		runes := make([]rune, length)
		for i := range runes {
			runes[i] = textAlphabet[rng.Intn(len(textAlphabet))]
		}
		angle := (rng.Float64()*16 - 8) * math.Pi / 180
		estimatedWidth := float64(length * 8 * scale)
		estimatedWidth += math.Abs(math.Tan(angle)) * float64(7*scale)
		maxX := float64(plan.width-24) - estimatedWidth
		if maxX < 16 {
			maxX = 16
		}
		x := 12 + rng.Float64()*maxX
		y := 14 + float64(line)*slotHeight + (slotHeight-float64(7*scale))/2
		style := lineStyle{
			text:  string(runes),
			x:     x,
			y:     y,
			scale: scale,
			angle: angle,
			color: textPalette[(line+rng.Intn(len(textPalette)))%len(textPalette)],
		}
		minX, minY, maxX2, maxY2 := paintBounds(style)
		pad := 2.0
		rect := model.Rect{
			X: normalize(clamp(minX-pad, 0, float64(plan.width-1)), plan.width),
			Y: normalize(clamp(minY-pad, 0, float64(plan.height-1)), plan.height),
			W: normalize(clamp(maxX2-minX+2*pad, 1, float64(plan.width)), plan.width),
			H: normalize(clamp(maxY2-minY+2*pad, 1, float64(plan.height)), plan.height),
		}
		if rect.X+rect.W > 1 {
			rect.W = 1 - rect.X
		}
		if rect.Y+rect.H > 1 {
			rect.H = 1 - rect.Y
		}
		truth = append(truth, model.TruthBox{Index: line + 1, Box: rect, Text: style.text})
		styles = append(styles, style)
	}
	spec := model.ImageSpec{
		ImageID:  imageID,
		Group:    plan.name,
		File:     fmt.Sprintf("images/%s/%s.png", plan.name, imageID),
		Width:    plan.width,
		Height:   plan.height,
		Language: plan.language[rng.Intn(len(plan.language))],
		Truth:    truth,
	}
	img, err := renderImage(spec, styles)
	if err != nil {
		return model.ImageSpec{}, nil, err
	}
	return spec, img, nil
}

type lineStyle struct {
	text  string
	x     float64
	y     float64
	scale int
	angle float64
	color color.RGBA
}

func renderImage(spec model.ImageSpec, styles []lineStyle) (image.Image, error) {
	img := image.NewRGBA(image.Rect(0, 0, spec.Width, spec.Height))
	bg := color.RGBA{R: 247, G: 245, B: 240, A: 255}
	for y := 0; y < spec.Height; y++ {
		for x := 0; x < spec.Width; x++ {
			img.SetRGBA(x, y, bg)
		}
	}
	total := 0
	for _, style := range styles {
		total += drawText(img, style)
	}
	if total == 0 {
		return nil, fmt.Errorf("rendered image %s has no ink", spec.ImageID)
	}
	return img, nil
}

func drawText(img *image.RGBA, style lineStyle) int {
	ink := 0
	height := 7 * float64(style.scale)
	for gi, r := range []rune(style.text) {
		pattern, ok := glyphs[r]
		if !ok {
			continue
		}
		charX := style.x + float64(gi*8*style.scale)
		for gy := 0; gy < 7; gy++ {
			for gx := 0; gx < 5; gx++ {
				if pattern[gy][gx] != '1' {
					continue
				}
				shift := math.Tan(style.angle) * ((float64(gy)+1)*float64(style.scale) - height/2)
				x0 := charX + float64(gx*style.scale) + shift
				y0 := style.y + float64(gy*style.scale)
				for py := int(math.Floor(y0)); py < int(math.Ceil(y0+float64(style.scale))); py++ {
					for px := int(math.Floor(x0)); px < int(math.Ceil(x0+float64(style.scale))); px++ {
						if image.Pt(px, py).In(img.Bounds()) {
							img.SetRGBA(px, py, style.color)
							ink++
						}
					}
				}
			}
		}
	}
	return ink
}

func paintBounds(style lineStyle) (float64, float64, float64, float64) {
	minX, minY := math.Inf(1), math.Inf(1)
	maxX, maxY := math.Inf(-1), math.Inf(-1)
	height := 7 * float64(style.scale)
	for gi, r := range []rune(style.text) {
		pattern, ok := glyphs[r]
		if !ok {
			continue
		}
		charX := style.x + float64(gi*8*style.scale)
		for gy := 0; gy < 7; gy++ {
			for gx := 0; gx < 5; gx++ {
				if pattern[gy][gx] != '1' {
					continue
				}
				shift := math.Tan(style.angle) * ((float64(gy)+1)*float64(style.scale) - height/2)
				x0 := charX + float64(gx*style.scale) + shift
				y0 := style.y + float64(gy*style.scale)
				minX = math.Min(minX, math.Floor(x0))
				minY = math.Min(minY, math.Floor(y0))
				maxX = math.Max(maxX, math.Ceil(x0+float64(style.scale)))
				maxY = math.Max(maxY, math.Ceil(y0+float64(style.scale)))
			}
		}
	}
	return minX, minY, maxX, maxY
}

func writePNG(path string, img image.Image) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if img == nil {
		return fmt.Errorf("nil image")
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".png-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := png.Encode(tmp, img); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func normalize(v float64, dimension int) float64 {
	return math.Round(v/float64(dimension)*1e6) / 1e6
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func rectsOverlap(a, b model.Rect) bool {
	return a.X < b.X+b.W && b.X < a.X+a.W && a.Y < b.Y+b.H && b.Y < a.Y+a.H
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// SortedImageIDs is useful to consumers that need explicit deterministic order.
func SortedImageIDs(manifest model.Manifest) []string {
	ids := make([]string, 0, len(manifest.Images))
	for _, imageSpec := range manifest.Images {
		ids = append(ids, imageSpec.ImageID)
	}
	sort.Strings(ids)
	return ids
}
