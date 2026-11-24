package imageio

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"

	"maskrevision/internal/model"
)

func CategoryColor(categoryID int) color.RGBA {
	palette := [...]color.RGBA{
		{0, 0, 0, 255},
		{45, 114, 178, 255},
		{230, 159, 0, 255},
		{0, 158, 115, 255},
		{204, 121, 167, 255},
		{86, 180, 233, 255},
		{213, 94, 0, 255},
		{240, 228, 66, 255},
		{0, 0, 0, 255},
	}
	if categoryID <= 0 {
		return color.RGBA{28, 28, 28, 255}
	}
	c := palette[(categoryID-1)%7+1]
	// Keep category colors distinct while deterministic for larger category IDs.
	offset := uint8((categoryID / 7) * 23)
	c.R = uint8((int(c.R) + int(offset)) % 256)
	c.G = uint8((int(c.G) + int(offset)*2) % 256)
	c.B = uint8((int(c.B) + int(offset)*3) % 256)
	return c
}

func PaletteForSegments(segments map[int]*model.Segment, ignored color.RGBA) color.Palette {
	pal := make(color.Palette, 256)
	pal[0] = ignored
	for i := 1; i < 256; i++ {
		pal[i] = color.RGBA{uint8((i * 53) % 256), uint8((i * 97) % 256), uint8((i * 193) % 256), 255}
	}
	for id, seg := range segments {
		if id > 0 && id < len(pal) {
			pal[id] = CategoryColor(seg.CategoryID)
		}
	}
	return pal
}

func EncodeIndexed(width, height int, pixels []int, segments map[int]*model.Segment) ([]byte, error) {
	if width <= 0 || height <= 0 || len(pixels) != width*height {
		return nil, fmt.Errorf("invalid mask dimensions %dx%d for %d pixels", width, height, len(pixels))
	}
	img := image.NewPaletted(image.Rect(0, 0, width, height), PaletteForSegments(segments, color.RGBA{28, 28, 28, 255}))
	for i, id := range pixels {
		if id < 0 || id > 255 {
			return nil, fmt.Errorf("segment id %d at pixel %d cannot be represented in indexed PNG", id, i)
		}
		img.Pix[i] = uint8(id)
	}
	var buf bytes.Buffer
	enc := png.Encoder{CompressionLevel: png.BestSpeed}
	if err := enc.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ReadIndexed(path string) (int, int, []int, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	img, err := png.Decode(bytes.NewReader(data))
	if err != nil {
		return 0, 0, nil, nil, err
	}
	paletted, ok := img.(*image.Paletted)
	if !ok {
		return 0, 0, nil, nil, fmt.Errorf("image %s is not indexed-color PNG", path)
	}
	b := paletted.Bounds()
	pixels := make([]int, 0, b.Dx()*b.Dy())
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			pixels = append(pixels, int(paletted.ColorIndexAt(x, y)))
		}
	}
	return b.Dx(), b.Dy(), pixels, data, nil
}
