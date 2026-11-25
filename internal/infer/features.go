package infer

import (
	"image"
	_ "image/png"
	"os"
	"path/filepath"

	"qaudit/internal/model"
)

func loadShardFeature(ds *model.Dataset, dataDir string, pos int) ([]float64, error) {
	s := ds.Shards[pos]
	path := s.Path
	if !filepath.IsAbs(path) {
		path = filepath.Join(dataDir, path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, model.NewBizError(model.CodeIO, "cannot open shard", map[string]string{"path": path})
	}
	defer f.Close()
	img, _, err := image.Decode(f)
	if err != nil {
		return nil, model.NewBizError(model.CodeIO, "cannot decode png", map[string]string{"path": path})
	}
	b := img.Bounds()
	feat := make([]float64, s.Width*s.Height)
	k := 0
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			r, g, bl, _ := img.At(x, y).RGBA()
			gray := uint8((0.299*float64(r) + 0.587*float64(g) + 0.114*float64(bl)) / 257.0)
			feat[k] = (float64(gray)/255.0 - 0.5) / 0.5
			k++
		}
	}
	return feat, nil
}
