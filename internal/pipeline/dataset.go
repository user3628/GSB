package pipeline

import (
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
)

// ViewImage holds the grayscale samples of one view plus the sentinel flags
// for non-finite pixels. PNG cannot represent NaN, so NaN pixels are declared
// in a sidecar "<view>.nan.json" array of [x,y] pairs (used by the test
// fixtures); in-memory they are marked with NaNSentinel.
type ViewImage struct {
	ShardID string
	ViewID  string
	Width   int
	Height  int
	Gray    []uint8
	NaN     []bool
}

// NaNSentinel is the byte value used in memory for a non-finite pixel.
const NaNSentinel = 0xFE

// NaNSidecarPath is the optional sidecar declaring NaN pixel coordinates.
func NaNSidecarPath(dir, shardID, viewID string) string {
	return filepath.Join(dir, shardID, viewID+".nan.json")
}

// LoadDataset reads the manifest and every referenced grayscale tile.
func LoadDataset(dir string) (*Manifest, map[string]*ViewImage, error) {
	manifest, err := LoadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, nil, err
	}
	images := map[string]*ViewImage{}
	for _, shard := range manifest.Shards {
		for _, cam := range shard.Views {
			img, err := loadViewImage(dir, shard.ShardID, cam)
			if err != nil {
				return nil, nil, err
			}
			images[cam.ViewID] = img
		}
	}
	return manifest, images, nil
}

// LoadManifest parses a manifest file.
func LoadManifest(path string) (*Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, bizError(CodeNotFound, "manifest not found: %v", err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, bizError(CodeInvalidRequest, "invalid manifest json: %v", err)
	}
	return &m, nil
}

func loadViewImage(dir, shardID string, cam Camera) (*ViewImage, error) {
	path := filepath.Join(dir, shardID, cam.ViewID+".png")
	f, err := os.Open(path)
	if err != nil {
		return nil, bizError(CodeNotFound, "tile missing %s: %v", cam.ViewID, err)
	}
	defer f.Close()
	raw, err := png.Decode(f)
	if err != nil {
		return nil, bizError(CodeInvalidRequest, "tile not decodable %s: %v", cam.ViewID, err)
	}
	gray, ok := raw.(*image.Gray)
	if !ok {
		// Convert any PNG color model to gray deterministically.
		bounds := raw.Bounds()
		gray = image.NewGray(bounds)
		for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
			for x := bounds.Min.X; x < bounds.Max.X; x++ {
				g := color.GrayModel.Convert(raw.At(x, y)).(color.Gray)
				gray.SetGray(x, y, g)
			}
		}
	}
	bounds := gray.Bounds()
	w, h := bounds.Dx(), bounds.Dy()
	vi := &ViewImage{
		ShardID: shardID,
		ViewID:  cam.ViewID,
		Width:   w,
		Height:  h,
		Gray:    make([]uint8, w*h),
		NaN:     make([]bool, w*h),
	}
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			vi.Gray[y*w+x] = gray.GrayAt(x, y).Y
		}
	}
	if b, err := os.ReadFile(NaNSidecarPath(dir, shardID, cam.ViewID)); err == nil {
		var pts [][2]int
		if err := json.Unmarshal(b, &pts); err != nil {
			return nil, bizError(CodeInvalidRequest, "bad nan sidecar: %v", err)
		}
		for _, p := range pts {
			if p[0] < 0 || p[0] >= w || p[1] < 0 || p[1] >= h {
				return nil, bizError(CodeOutOfRange, "nan marker outside image in %s field %s", cam.ViewID, "nan")
			}
			idx := p[1]*w + p[0]
			vi.NaN[idx] = true
			vi.Gray[idx] = NaNSentinel
		}
	}
	return vi, nil
}

// Pixel returns the grayscale value, reporting NaN pixels as a business error
// rather than silently sampling them.
func (v *ViewImage) Pixel(x, y int) (uint8, error) {
	if x < 0 || y < 0 || x >= v.Width || y >= v.Height {
		return 0, &BizError{Code: CodeOutOfRange,
			Message: "pixel outside image tile", ViewID: v.ViewID, Field: "pixel"}
	}
	idx := y*v.Width + x
	if v.NaN[idx] {
		return 0, &BizError{Code: CodeNaNInput,
			Message: "non-finite pixel in image tile", ViewID: v.ViewID, Field: "pixel"}
	}
	return v.Gray[idx], nil
}

// ContentDigest fingerprints the raw pixel content and NaN mask so a single
// changed pixel changes the scene content fingerprint.
func (v *ViewImage) ContentDigest() string {
	return MustDigest(struct {
		ViewID string
		Width  int
		Height int
		Gray   []uint8
		NaN    []bool
	}{v.ViewID, v.Width, v.Height, v.Gray, v.NaN})
}
