package pipeline

import (
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"math"
	"os"
	"path/filepath"
)

// lcg is a fixed linear-congruential generator. It deliberately does not use
// math/rand so generated samples remain byte-identical across Go releases and
// platforms. Seed is part of the manifest.
type lcg struct {
	state uint64
}

func newLCG(seed uint64) *lcg { return &lcg{state: seed} }

func (r *lcg) next() uint64 {
	// Numerical Recipes constants (64-bit).
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}

func (r *lcg) float64() float64 {
	return float64(r.next()>>11) / float64(uint64(1)<<53)
}

func (r *lcg) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

// DefaultConfig is the deterministic baseline configuration.
func DefaultConfig() PipelineConfig {
	return PipelineConfig{
		VoxelResolution: 0.25,
		Truncation:      0.45,
		FusionRadius:    1,
		SampleDensity:   1,
		RayStep:         0.1,
		RayNear:         0.2,
		RayFar:          8.0,
		SparsityPrune:   2,
		DistTolerance:   0.6,
		PixelTolerance:  2.5,
		QueryBudgetMs:   200,
	}
}

// GenerateParams controls the size of a generated workspace.
type GenerateParams struct {
	Seed     int64
	Shards   int
	ViewsPer int
	ImageW   int
	ImageH   int
	OutDir   string
	Config   PipelineConfig
}

// GenerateDataset writes a multi-view grayscale dataset and its manifest into
// outDir. Layout:
//
//	manifest.json
//	shard-xx/view-yy.png   (grayscale depth map, 0 = no surface hit)
//	shard-xx/view-yy.meta.json (camera, also embedded in manifest)
func GenerateDataset(p GenerateParams) (*Manifest, error) {
	if p.Shards <= 0 {
		p.Shards = 2
	}
	if p.ViewsPer <= 0 {
		p.ViewsPer = 4
	}
	if p.ImageW <= 0 {
		p.ImageW = 16
	}
	if p.ImageH <= 0 {
		p.ImageH = 16
	}
	if p.Config == (PipelineConfig{}) {
		p.Config = DefaultConfig()
	}
	if err := os.MkdirAll(p.OutDir, 0o755); err != nil {
		return nil, err
	}
	rng := newLCG(uint64(p.Seed)*2862933555777941757 + 3037000493)

	manifest := &Manifest{
		SchemaVersion: 1,
		Seed:          p.Seed,
		Config:        p.Config,
	}
	totalViews := p.Shards * p.ViewsPer

	for si := 0; si < p.Shards; si++ {
		shardID := shardName(si)
		shard := ShardSpec{ShardID: shardID}
		shardDir := filepath.Join(p.OutDir, shardID)
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			return nil, err
		}
		for vi := 0; vi < p.ViewsPer; vi++ {
			globalIdx := si*p.ViewsPer + vi
			viewID := viewName(globalIdx)
			angle := 2 * math.Pi * float64(globalIdx) / float64(totalViews)
			cam := orbitCamera(viewID, angle, 4.0, float64(p.ImageW), float64(p.ImageH))

			depth := make([]float64, p.ImageW*p.ImageH)
			img := image.NewGray(image.Rect(0, 0, p.ImageW, p.ImageH))
			for y := 0; y < p.ImageH; y++ {
				for x := 0; x < p.ImageW; x++ {
					// A smooth radial depth field plus fixed-seed jitter keeps
					// the scene structured yet non-trivial.
					dx := (float64(x) - float64(p.ImageW-1)/2) / float64(p.ImageW)
					dy := (float64(y) - float64(p.ImageH-1)/2) / float64(p.ImageH)
					r := math.Hypot(dx, dy)
					d := 3.0 + 0.8*math.Cos(3*r+angle) + 0.15*rng.float64()
					if d < p.Config.RayNear || d > p.Config.RayFar {
						d = 0
					}
					depth[y*p.ImageW+x] = d
					if d > 0 {
						img.SetGray(x, y, color.Gray{Y: depthToByte(d, p.Config.RayFar)})
					}
				}
			}
			if err := writePNG(filepath.Join(shardDir, viewID+".png"), img); err != nil {
				return nil, err
			}
			metaPath := filepath.Join(shardDir, viewID+".meta.json")
			if err := writeIndentedFile(metaPath, cam); err != nil {
				return nil, err
			}
			shard.Views = append(shard.Views, cam)

			// Ground truth: deterministic sampled surface points reconstructed
			// from the depth map through the same camera model.
			for s := 0; s < 3; s++ {
				gx := rng.intn(p.ImageW)
				gy := rng.intn(p.ImageH)
				d := depth[gy*p.ImageW+gx]
				if d <= 0 {
					continue
				}
				world := pointAtDepth(cam, gx, gy, d)
				manifest.Truth = append(manifest.Truth, GroundTruthPoint{
					ViewID: viewID,
					PixelX: gx,
					PixelY: gy,
					Point:  world,
					Color:  depthToByte(d, p.Config.RayFar),
				})
			}
		}
		manifest.Shards = append(manifest.Shards, shard)
	}

	if err := writeIndentedFile(filepath.Join(p.OutDir, "manifest.json"), manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func orbitCamera(viewID string, angle, radius, w, h float64) Camera {
	eye := Vec3{radius * math.Cos(angle), 0.6 * math.Sin(angle), radius * math.Sin(angle)}
	target := Vec3{0, 0, 0}
	up := Vec3{0, 1, 0}

	fwd := normalize(vecSub(target, eye))
	right := normalize(cross(fwd, up))
	up2 := cross(right, fwd)
	// Camera looks down +Z in its own frame; rows are right, up, backward so
	// that Xc = R Xw + T yields positive depth in front.
	var r Mat3
	r[0], r[1], r[2] = right[0], right[1], right[2]
	r[3], r[4], r[5] = up2[0], up2[1], up2[2]
	back := vecScale(fwd, -1)
	r[6], r[7], r[8] = back[0], back[1], back[2]
	t := vecScale(matVec(r, eye), -1)
	f := 1.4 * w
	return Camera{
		ViewID: viewID,
		R:      r,
		T:      t,
		K:      Intrinsics{FX: f, FY: f, CX: (w - 1) / 2, CY: (h - 1) / 2},
		Width:  int(w),
		Height: int(h),
	}
}

func pointAtDepth(cam Camera, px, py int, depth float64) Vec3 {
	origin, dir := cameraRay(cam, px, py)
	return vecAdd(origin, vecScale(dir, depth))
}

func normalize(v Vec3) Vec3 {
	n := vecNorm(v)
	if n == 0 {
		return v
	}
	return vecScale(v, 1/n)
}

func cross(a, b Vec3) Vec3 {
	return Vec3{
		a[1]*b[2] - a[2]*b[1],
		a[2]*b[0] - a[0]*b[2],
		a[0]*b[1] - a[1]*b[0],
	}
}

func depthToByte(d, far float64) uint8 {
	v := math.Round(255 * d / far)
	if v < 0 {
		v = 0
	}
	if v > 255 {
		v = 255
	}
	return uint8(v)
}

func writePNG(path string, img *image.Gray) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return png.Encode(f, img)
}

func writeIndentedFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func shardName(i int) string { return "shard-" + twoDigits(i) }
func viewName(i int) string  { return "view-" + twoDigits(i) }

func twoDigits(i int) string {
	if i < 10 {
		return "0" + itoa(i)
	}
	return itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
