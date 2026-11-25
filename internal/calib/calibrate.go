// Package calib implements stage 1: deterministic calibration sample selection
// and fixed-rule feature normalization.
package calib

import (
	"fmt"
	"image"
	_ "image/png"
	"math"
	"math/rand/v2"
	"os"
	"sort"

	"qaudit/internal/model"
)

// Entry is one selected calibration sample with its normalized flat feature.
type Entry struct {
	Index   int       `json:"index"`
	ShardID string    `json:"shard_id"`
	Label   int       `json:"label"`
	Feature []float64 `json:"feature"`
}

// CalibSet is the fully materialized, reproducible calibration selection.
type CalibSet struct {
	Seed           int64   `json:"seed"`
	Ratio          float64 `json:"ratio"`
	DeclaredWidth  int     `json:"declared_width"`
	DeclaredHeight int     `json:"declared_height"`
	Entries        []Entry `json:"entries"`
	// ManifestDigest covers the selection list (indices, ids, labels) only.
	ManifestDigest string `json:"manifest_digest"`
	// FeatureDigest covers the canonical feature byte blob.
	FeatureDigest string `json:"feature_digest"`
}

// Manifest is the byte-stable selection description.
type Manifest struct {
	Seed   int64          `json:"seed"`
	Ratio  float64        `json:"ratio"`
	Picked []ManifestItem `json:"picked"`
}

// ManifestItem identifies one selected shard.
type ManifestItem struct {
	Index   int    `json:"index"`
	ShardID string `json:"shard_id"`
	Label   int    `json:"label"`
}

// Select performs deterministic stratified sampling and feature normalization.
func Select(shards []model.Shard, dataDir string, ratio float64, seed int64, width, height int) (*CalibSet, error) {
	if ratio <= 0 {
		return nil, model.NewBizError(model.CodeCalibSample, "calibration ratio must be positive",
			map[string]string{"ratio": fmt.Sprintf("%g", ratio)})
	}
	n := len(shards)
	if n == 0 {
		return nil, model.NewBizError(model.CodeCalibSample, "no shards available",
			map[string]string{"ratio": fmt.Sprintf("%g", ratio), "available": "0"})
	}
	want := int(math.Round(ratio * float64(n)))
	if want <= 0 || want > n {
		return nil, model.NewBizError(model.CodeCalibSample, "calibration sample count out of range",
			map[string]string{
				"ratio":     fmt.Sprintf("%g", ratio),
				"requested": fmt.Sprintf("%d", want),
				"available": fmt.Sprintf("%d", n),
			})
	}

	// Group shard indices by ground-truth label for stratification.
	strata := map[int][]int{}
	for i, s := range shards {
		strata[s.Label] = append(strata[s.Label], i)
	}
	labels := make([]int, 0, len(strata))
	for l := range strata {
		labels = append(labels, l)
	}
	sort.Ints(labels)

	// Largest-remainder allocation keeps totals exact and deterministic.
	quotas := make(map[int]int, len(labels))
	remainders := make([]struct {
		label int
		rem   float64
	}, 0, len(labels))
	allocated := 0
	for _, l := range labels {
		exact := ratio * float64(len(strata[l]))
		q := int(math.Floor(exact))
		quotas[l] = q
		allocated += q
		remainders = append(remainders, struct {
			label int
			rem   float64
		}{l, exact - float64(q)})
	}
	sort.SliceStable(remainders, func(i, j int) bool {
		if remainders[i].rem != remainders[j].rem {
			return remainders[i].rem > remainders[j].rem
		}
		return remainders[i].label < remainders[j].label
	})
	for k := 0; allocated < want; k++ {
		quotas[remainders[k%len(remainders)].label]++
		allocated++
	}

	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^uint64(0x9E3779B97F4A7C15)))
	picked := make([]int, 0, want)
	for _, l := range labels {
		idxs := append([]int(nil), strata[l]...)
		rng.Shuffle(len(idxs), func(i, j int) { idxs[i], idxs[j] = idxs[j], idxs[i] })
		picked = append(picked, idxs[:quotas[l]]...)
	}
	sort.Ints(picked)

	entries := make([]Entry, 0, len(picked))
	manifest := Manifest{Seed: seed, Ratio: ratio, Picked: make([]ManifestItem, 0, len(picked))}
	featureRows := make([][]float64, 0, len(picked))
	for _, idx := range picked {
		s := shards[idx]
		feat, err := loadFeature(dataDir, s, width, height)
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Index: idx, ShardID: s.ID, Label: s.Label, Feature: feat})
		manifest.Picked = append(manifest.Picked, ManifestItem{Index: idx, ShardID: s.ID, Label: s.Label})
		featureRows = append(featureRows, feat)
	}

	md, err := model.Digest(manifest)
	if err != nil {
		return nil, err
	}
	cs := &CalibSet{
		Seed:           seed,
		Ratio:          ratio,
		DeclaredWidth:  width,
		DeclaredHeight: height,
		Entries:        entries,
		ManifestDigest: md,
		FeatureDigest:  model.DigestBytes(model.FeatureBlob(featureRows)),
	}
	return cs, nil
}

// loadFeature reads a PNG, validates dimensions, converts to grayscale and
// applies the fixed normalization x = (gray/255 - 0.5) / 0.5 in [-1, 1].
func loadFeature(dataDir string, s model.Shard, width, height int) ([]float64, error) {
	path := s.Path
	if !filepathIsAbs(path) {
		path = joinPath(dataDir, path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, model.NewBizError(model.CodeIO, "cannot open shard", map[string]string{"shard": s.ID, "path": path})
	}
	defer f.Close()
	img, _, err := image.Decode(f)
	if err != nil {
		return nil, model.NewBizError(model.CodeIO, "cannot decode shard png", map[string]string{"shard": s.ID})
	}
	bounds := img.Bounds()
	gotW, gotH := bounds.Dx(), bounds.Dy()
	if gotW != s.Width || gotH != s.Height || gotW != width || gotH != height {
		return nil, model.NewBizError(model.CodeCalibSample, "shard dimension mismatch", map[string]string{
			"shard":           s.ID,
			"declared_width":  fmt.Sprintf("%d", width),
			"declared_height": fmt.Sprintf("%d", height),
			"actual_width":    fmt.Sprintf("%d", gotW),
			"actual_height":   fmt.Sprintf("%d", gotH),
		})
	}
	feat := make([]float64, width*height)
	k := 0
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			r, g, b, _ := img.At(x, y).RGBA()
			gray := uint8((0.299*float64(r) + 0.587*float64(g) + 0.114*float64(b)) / 257.0)
			feat[k] = (float64(gray)/255.0 - 0.5) / 0.5
			k++
		}
	}
	return feat, nil
}

func filepathIsAbs(p string) bool { return len(p) > 0 && p[0] == '/' }

func joinPath(dir, p string) string {
	if dir == "" {
		return p
	}
	return dir + "/" + p
}

// CheckFinite returns E_CALIB_SAMPLE context if any feature contains non-finite values.
func CheckFinite(cs *CalibSet) error {
	for _, e := range cs.Entries {
		for _, v := range e.Feature {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return model.NewBizError(model.CodeCalibSample, "non-finite feature",
					map[string]string{"shard": e.ShardID})
			}
		}
	}
	return nil
}
