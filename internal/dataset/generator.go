package dataset

import (
	"bufio"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"math"
	"os"
	"path/filepath"

	"imretrace/internal/model"
)

type captionSpec struct {
	Caption string
	Lang    string
	Source  string
	Shape   string
	Colors  [3][3]uint8
	Bright  float64
}

func specs() []captionSpec {
	return []captionSpec{
		{"红色实心圆 red circle 浮在蓝色渐变背景上，明亮视角", "zh-en", "builtin:geometry-v1/bright", "circle", [3][3]uint8{{214, 48, 49}, {39, 96, 220}, {255, 214, 90}}, 1.08},
		{"deep blue rectangle 深蓝色矩形 block 与高对比阴影，dark view", "zh-en", "builtin:geometry-v1/dark", "rect", [3][3]uint8{{24, 61, 145}, {8, 18, 48}, {120, 190, 255}}, 0.72},
		{"green triangle 绿色三角形 under yellow gradient light", "zh-en", "builtin:geometry-v1/light", "triangle", [3][3]uint8{{34, 153, 84}, {245, 197, 66}, {220, 255, 220}}, 1.00},
		{"橙色斜矩形 orange bar 穿过青色渐变 cyan gradient", "zh-en", "builtin:geometry-v1/bar", "bar", [3][3]uint8{{240, 140, 30}, {24, 178, 196}, {255, 240, 180}}, 1.12},
		{"purple circle 紫色圆盘 and pale grid, 暗光 diff view", "zh-en", "builtin:brightness-study/dark", "circle", [3][3]uint8{{111, 66, 193}, {30, 27, 75}, {190, 170, 255}}, 0.66},
		{"teal rectangles 青绿色方块 stacked near warm gradient", "zh-en", "builtin:geometry-v1/blocks", "rect", [3][3]uint8{{22, 160, 133}, {255, 120, 80}, {235, 255, 250}}, 1.02},
		{"bright yellow triangle 明黄三角 with violet shadow gradient", "zh-en", "builtin:brightness-study/bright", "triangle", [3][3]uint8{{245, 210, 40}, {91, 63, 190}, {255, 255, 210}}, 1.18},
		{"gray-white gradient 灰白渐变上的 black outline ring 圆环", "zh-en", "builtin:gradient-v1/mono", "ring", [3][3]uint8{{28, 32, 38}, {170, 180, 192}, {245, 247, 250}}, 0.96},
		{"rose red blocks 玫红方块 beside emerald gradient, mixed view", "zh-en", "builtin:geometry-v1/mixed", "rect", [3][3]uint8{{214, 51, 132}, {16, 140, 110}, {255, 214, 235}}, 1.05},
	}
}

type deterministicRng struct{ state uint64 }

func newRng(seed int64, index int) *deterministicRng {
	return &deterministicRng{state: uint64(seed) ^ (uint64(index+1) * 0x9e3779b97f4a7c15)}
}

func (r *deterministicRng) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func clampByte(v float64) uint8 {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return uint8(math.Round(v))
}

func scaled(c [3]uint8, bright float64) color.RGBA {
	return color.RGBA{R: clampByte(float64(c[0]) * bright), G: clampByte(float64(c[1]) * bright), B: clampByte(float64(c[2]) * bright), A: 255}
}

func Generate(paths model.Paths, params model.GeneratorParams) ([]model.ManifestItem, error) {
	if params.ShardCount < 3 {
		params.ShardCount = 3
	}
	if params.ImageWidth <= 0 {
		params.ImageWidth = 96
	}
	if params.ImageHeight <= 0 {
		params.ImageHeight = 72
	}
	if err := os.MkdirAll(paths.ConfigDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(paths.ShardsDir, 0o755); err != nil {
		return nil, err
	}
	paramsBytes, err := model.CanonicalBytes(params)
	if err != nil {
		return nil, err
	}
	paramsBytes = append(paramsBytes, '\n')
	if err := os.WriteFile(paths.ParamsPath, paramsBytes, 0o644); err != nil {
		return nil, err
	}

	allSpecs := specs()
	items := make([]model.ManifestItem, 0, params.ShardCount)
	manifest, err := os.Create(paths.Manifest)
	if err != nil {
		return nil, err
	}
	defer manifest.Close()
	writer := bufio.NewWriter(manifest)

	for i := 0; i < params.ShardCount; i++ {
		spec := allSpecs[i%len(allSpecs)]
		if i >= len(allSpecs) {
			spec.Caption = spec.Caption + " extra deterministic view " + string(rune('a'+(i%26)))
		}
		shardID := fmtShard(i + 1)
		itemID := "item-" + shardID
		shardDir := filepath.Join(paths.ShardsDir, shardID)
		imgRel := filepath.Join("shards", shardID, "image.png")
		imgPath := filepath.Join(paths.DemoDir, imgRel)
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			return nil, err
		}
		img := renderImage(params.ImageWidth, params.ImageHeight, i, spec)
		f, err := os.Create(imgPath)
		if err != nil {
			return nil, err
		}
		enc := png.Encoder{CompressionLevel: png.BestCompression}
		if err := enc.Encode(f, img); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		item := model.ManifestItem{
			ItemID:    itemID,
			ShardID:   shardID,
			ImageFile: imgRel,
			Caption:   spec.Caption,
			Lang:      spec.Lang,
			Source:    spec.Source,
		}
		line, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		if _, err := writer.Write(append(line, '\n')); err != nil {
			return nil, err
		}
		shardItem := item
		shardItem.ImageFile = "image.png"
		shardLine, err := json.Marshal(shardItem)
		if err != nil {
			return nil, err
		}
		shardLinePath := filepath.Join(shardDir, "item.jsonl")
		if err := os.WriteFile(shardLinePath, append(append([]byte{}, shardLine...), '\n'), 0o644); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	return items, nil
}

func fmtShard(n int) string {
	return "shard-" + pad4(n)
}

func pad4(n int) string {
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}

func renderImage(w, h, idx int, spec captionSpec) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	rng := newRng(model.DefaultGeneratorParams().Seed, idx)
	bg1 := scaled(spec.Colors[1], spec.Bright)
	bg2 := scaled(spec.Colors[2], math.Min(1.25, spec.Bright+0.18))
	for y := 0; y < h; y++ {
		t := float64(y) / float64(max(1, h-1))
		if idx%2 == 1 {
			t = 1 - t
		}
		for x := 0; x < w; x++ {
			t2 := t*0.78 + float64(x)/float64(max(1, w-1))*0.22
			c := lerpRGBA(bg1, bg2, t2)
			img.SetRGBA(x, y, c)
		}
	}
	primary := scaled(spec.Colors[0], spec.Bright)
	shadow := color.RGBA{R: 10, G: 12, B: 20, A: 70}
	switch spec.Shape {
	case "circle", "ring":
		cx := w/2 + int(rng.next()%13) - 6
		cy := h/2 + int(rng.next()%9) - 4
		r := h*27/100 + int(rng.next()%7)
		fillCircle(img, cx+3, cy+4, r, shadow)
		if spec.Shape == "ring" {
			fillRing(img, cx, cy, r, r*55/100, primary)
		} else {
			fillCircle(img, cx, cy, r, primary)
			fillCircle(img, cx-r/3, cy-r/3, r/5, scaled(spec.Colors[2], 1.15))
		}
	case "triangle":
		cx := w / 2
		top := h * 16 / 100
		bottom := h * 84 / 100
		half := w * 30 / 100
		fillTriangle(img, cx+3, top+4, cx-half+3, bottom+4, cx+half+3, bottom+4, shadow)
		fillTriangle(img, cx, top, cx-half, bottom, cx+half, bottom, primary)
		fillTriangle(img, cx, top+h*12/100, cx-half*55/100, bottom-h*8/100, cx+half*20/100, bottom-h*8/100, scaled(spec.Colors[2], 1.1))
	case "bar":
		fillRect(img, w*16/100+3, h*34/100+4, w*70/100, h*24/100, shadow)
		fillRect(img, w*16/100, h*34/100, w*70/100, h*24/100, primary)
		fillRect(img, w*22/100, h*40/100, w*28/100, h*4/100, scaled(spec.Colors[2], 1.2))
	default:
		x := w*22/100 + int(rng.next()%7)
		y := h*24/100 + int(rng.next()%5)
		rw := w * 42 / 100
		rh := h * 48 / 100
		fillRect(img, x+4, y+5, rw, rh, shadow)
		fillRect(img, x, y, rw, rh, primary)
		fillRect(img, x+rw*18/100, y+rh*16/100, rw*24/100, rh*18/100, scaled(spec.Colors[2], 1.12))
		fillRect(img, x+rw*56/100, y+rh*55/100, rw*22/100, rh*14/100, scaled(spec.Colors[2], 0.95))
	}
	for k := 0; k < 18; k++ {
		x := int(rng.next() % uint64(w))
		y := int(rng.next() % uint64(h))
		img.SetRGBA(x, y, scaled(spec.Colors[2], 0.85+float64(k%3)*0.08))
	}
	return img
}

func lerpRGBA(a, b color.RGBA, t float64) color.RGBA {
	if t < 0 {
		t = 0
	}
	if t > 1 {
		t = 1
	}
	return color.RGBA{
		R: clampByte(float64(a.R)*(1-t) + float64(b.R)*t),
		G: clampByte(float64(a.G)*(1-t) + float64(b.G)*t),
		B: clampByte(float64(a.B)*(1-t) + float64(b.B)*t),
		A: 255,
	}
}

func fillRect(img *image.RGBA, x0, y0, w, h int, c color.Color) {
	b := img.Bounds()
	rgba := color.RGBAModel.Convert(c).(color.RGBA)
	for y := y0; y < y0+h; y++ {
		for x := x0; x < x0+w; x++ {
			if image.Pt(x, y).In(b) {
				img.SetRGBA(x, y, rgba)
			}
		}
	}
}

func fillCircle(img *image.RGBA, cx, cy, r int, c color.Color) {
	rgba := color.RGBAModel.Convert(c).(color.RGBA)
	r2 := r * r
	for y := cy - r; y <= cy+r; y++ {
		for x := cx - r; x <= cx+r; x++ {
			dx, dy := x-cx, y-cy
			if dx*dx+dy*dy <= r2 && image.Pt(x, y).In(img.Bounds()) {
				img.SetRGBA(x, y, rgba)
			}
		}
	}
}

func fillRing(img *image.RGBA, cx, cy, outer, inner int, c color.Color) {
	ro, ri := outer*outer, inner*inner
	rgba := color.RGBAModel.Convert(c).(color.RGBA)
	for y := cy - outer; y <= cy+outer; y++ {
		for x := cx - outer; x <= cx+outer; x++ {
			dx, dy := x-cx, y-cy
			d2 := dx*dx + dy*dy
			if d2 <= ro && d2 >= ri && image.Pt(x, y).In(img.Bounds()) {
				img.SetRGBA(x, y, rgba)
			}
		}
	}
}

func fillTriangle(img *image.RGBA, x1, y1, x2, y2, x3, y3 int, c color.Color) {
	minX := minInt(x1, minInt(x2, x3))
	maxX := maxInt(x1, maxInt(x2, x3))
	minY := minInt(y1, minInt(y2, y3))
	maxY := maxInt(y1, maxInt(y2, y3))
	rgba := color.RGBAModel.Convert(c).(color.RGBA)
	for y := minY; y <= maxY; y++ {
		for x := minX; x <= maxX; x++ {
			if sign(x1, y1, x2, y2, x, y) >= 0 && sign(x2, y2, x3, y3, x, y) >= 0 && sign(x3, y3, x1, y1, x, y) >= 0 {
				if image.Pt(x, y).In(img.Bounds()) {
					img.SetRGBA(x, y, rgba)
				}
			}
		}
	}
}

func sign(x1, y1, x2, y2, x3, y3 int) int {
	return (x1-x3)*(y2-y3) - (y1-y3)*(x2-x3)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max(a, b int) int { return maxInt(a, b) }
