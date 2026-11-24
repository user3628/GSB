package motion

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"motionlab/internal/model"
)

const (
	ManifestFile = "manifest.json"
	PolicyFile   = "policy.json"
	FramesFile   = "frames.json"
	SequencesDir = "sequences"
)

type FrameRecord struct {
	Path   string `json:"path"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	SHA256 string `json:"sha256"`
}

type SequenceFrames struct {
	SequenceID       string        `json:"sequence_id"`
	InitialSHA256    string        `json:"initial_sha256"`
	BackgroundSHA256 string        `json:"background_sha256"`
	Frames           []FrameRecord `json:"frames"`
}

type FramesIndex struct {
	Version   string           `json:"version"`
	Sequences []SequenceFrames `json:"sequences"`
}

type SequenceData struct {
	Spec      model.SequenceSpec
	Shared    *SharedFrame
	Truth     []*Frame
	TruthFile []string
	Index     SequenceFrames
}

type Catalog struct {
	Dir       string
	Manifest  model.Manifest
	Policy    model.PolicySnapshot
	Sequences map[string]*SequenceData
	Order     []string
}

func DefaultManifest() model.Manifest {
	policy := model.PolicySnapshot{
		Version:        1,
		ParentVersion:  0,
		PolicyID:       "balanced-v1",
		Concurrency:    2,
		BranchQuota:    12,
		RoundBudgetMS:  80,
		PruneThreshold: 2.5,
	}
	specs := []model.SequenceSpec{
		{
			SequenceID: "harbor_slide", Name: "Harbor Slide", Seed: 37017, Width: 64, Height: 64,
			TotalFrames: 17, PredictSteps: 16, Branches: 4, DirectionChangeStep: 8,
			VelocityDecay: 0.965,
			InitialState:  model.MotionState{Step: 0, X: 0, Y: 0, VX: 2.60, VY: 1.35, ScaleX: 1, ScaleY: 1, Rotation: 0},
			Scene: model.SceneSpec{
				BackgroundSeed: 7113,
				SquareRect:     model.Rect{X: 9, Y: 11, W: 16, H: 16},
				BlockRect:      model.Rect{X: 37, Y: 36, W: 22, H: 18},
			},
			Tags: []string{"baseline", "direction-turn", "pruning"},
		},
		{
			SequenceID: "orchid_drift", Name: "Orchid Drift", Seed: 58121, Width: 56, Height: 56,
			TotalFrames: 15, PredictSteps: 14, Branches: 3, DirectionChangeStep: 6,
			VelocityDecay: 0.955,
			InitialState:  model.MotionState{Step: 0, X: -2.0, Y: 1.0, VX: 2.15, VY: -1.55, ScaleX: 1, ScaleY: 1, Rotation: 0},
			Scene: model.SceneSpec{
				BackgroundSeed: 9211,
				SquareRect:     model.Rect{X: 8, Y: 14, W: 14, H: 14},
				BlockRect:      model.Rect{X: 31, Y: 31, W: 19, H: 17},
			},
			Tags: []string{"deformation", "direction-turn", "multi-branch"},
		},
		{
			SequenceID: "signal_turn", Name: "Signal Turn", Seed: 84109, Width: 72, Height: 64,
			TotalFrames: 19, PredictSteps: 18, Branches: 5, DirectionChangeStep: 9,
			VelocityDecay: 0.972,
			InitialState:  model.MotionState{Step: 0, X: 1.5, Y: -1.0, VX: 2.85, VY: 1.70, ScaleX: 1, ScaleY: 1, Rotation: 0},
			Scene: model.SceneSpec{
				BackgroundSeed: 14831,
				SquareRect:     model.Rect{X: 10, Y: 9, W: 18, H: 18},
				BlockRect:      model.Rect{X: 43, Y: 35, W: 23, H: 19},
			},
			Tags: []string{"long-horizon", "quota", "pruning"},
		},
	}
	for i := range specs {
		specs[i].ExpectedStateRange = deriveRange(specs[i])
	}
	return model.Manifest{
		Version:   model.ManifestVersion,
		Seed:      20261125,
		Sequences: specs,
		Policy:    policy,
	}
}

func deriveRange(spec model.SequenceSpec) model.StateRange {
	states := TruthTrajectory(spec)
	minX, maxX := states[0].X, states[0].X
	minY, maxY := states[0].Y, states[0].Y
	minVX, maxVX := states[0].VX, states[0].VX
	minVY, maxVY := states[0].VY, states[0].VY
	minSX, maxSX := states[0].ScaleX, states[0].ScaleX
	minSY, maxSY := states[0].ScaleY, states[0].ScaleY
	for _, s := range states[1:] {
		minX, maxX = math.Min(minX, s.X), math.Max(maxX, s.X)
		minY, maxY = math.Min(minY, s.Y), math.Max(maxY, s.Y)
		minVX, maxVX = math.Min(minVX, s.VX), math.Max(maxVX, s.VX)
		minVY, maxVY = math.Min(minVY, s.VY), math.Max(maxVY, s.VY)
		minSX, maxSX = math.Min(minSX, s.ScaleX), math.Max(maxSX, s.ScaleX)
		minSY, maxSY = math.Min(minSY, s.ScaleY), math.Max(maxSY, s.ScaleY)
	}
	return model.StateRange{
		X:      model.FloatRange{Min: minX - 5, Max: maxX + 5},
		Y:      model.FloatRange{Min: minY - 5, Max: maxY + 5},
		VX:     model.FloatRange{Min: minVX*1.35 - 0.2, Max: maxVX*1.35 + 0.2},
		VY:     model.FloatRange{Min: minVY*1.35 - 0.2, Max: maxVY*1.35 + 0.2},
		ScaleX: model.FloatRange{Min: minSX - 0.08, Max: maxSX + 0.08},
		ScaleY: model.FloatRange{Min: minSY - 0.08, Max: maxSY + 0.08},
	}
}

// Initialize creates the deterministic sample corpus. Existing valid data is
// preserved unless force is true.
func Initialize(dir string, force bool) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	manifestPath := filepath.Join(dir, ManifestFile)
	if !force {
		if _, err := os.Stat(manifestPath); err == nil {
			if _, err := LoadCatalog(dir); err == nil {
				return nil
			}
		}
	}
	manifest := DefaultManifest()
	policy := manifest.Policy
	index := FramesIndex{Version: "motion-frames/v1"}
	for _, spec := range manifest.Sequences {
		seqDir := filepath.Join(dir, SequencesDir, spec.SequenceID)
		truthDir := filepath.Join(seqDir, "truth")
		if err := os.MkdirAll(truthDir, 0o755); err != nil {
			return err
		}
		background, squareTex, blockTex, err := buildSceneAssets(spec)
		if err != nil {
			return err
		}
		initial := composeInitial(spec, background, squareTex, blockTex)
		initialPath := filepath.Join(seqDir, "initial.png")
		if err := writePNG(initialPath, initial); err != nil {
			return err
		}
		backgroundPath := filepath.Join(seqDir, "background.png")
		if err := writePNG(backgroundPath, background); err != nil {
			return err
		}
		shared := NewSharedFrame(spec.SequenceID, initial, background, spec.Scene.SquareRect, spec.Scene.BlockRect)
		states := TruthTrajectory(spec)
		records := make([]FrameRecord, 0, spec.TotalFrames)
		for i := 0; i < spec.TotalFrames; i++ {
			frame := initial
			if i > 0 {
				frame, err = (Renderer{}).Render(shared, spec, states[i])
				if err != nil {
					return err
				}
			}
			rel := filepath.ToSlash(filepath.Join(SequencesDir, spec.SequenceID, "truth", fmt.Sprintf("%04d.png", i)))
			if err := writePNG(filepath.Join(dir, filepath.FromSlash(rel)), frame); err != nil {
				return err
			}
			records = append(records, FrameRecord{Path: rel, Width: frame.Width, Height: frame.Height, SHA256: frame.SHA256})
		}
		index.Sequences = append(index.Sequences, SequenceFrames{
			SequenceID:       spec.SequenceID,
			InitialSHA256:    initial.SHA256,
			BackgroundSHA256: background.SHA256,
			Frames:           records,
		})
	}
	if err := writeJSON(filepath.Join(dir, ManifestFile), manifest); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, PolicyFile), policy); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, FramesFile), index); err != nil {
		return err
	}
	return nil
}

func LoadCatalog(dir string) (*Catalog, error) {
	var manifest model.Manifest
	if err := readJSON(filepath.Join(dir, ManifestFile), &manifest); err != nil {
		return nil, err
	}
	var policy model.PolicySnapshot
	if err := readJSON(filepath.Join(dir, PolicyFile), &policy); err != nil {
		policy = manifest.Policy
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	var index FramesIndex
	if err := readJSON(filepath.Join(dir, FramesFile), &index); err != nil {
		return nil, err
	}
	indexBySeq := make(map[string]SequenceFrames, len(index.Sequences))
	for _, s := range index.Sequences {
		indexBySeq[s.SequenceID] = s
	}
	cat := &Catalog{Dir: dir, Manifest: manifest, Policy: policy, Sequences: make(map[string]*SequenceData)}
	for _, spec := range manifest.Sequences {
		seqIndex, ok := indexBySeq[spec.SequenceID]
		if !ok {
			return nil, fmt.Errorf("frames index missing sequence %s", spec.SequenceID)
		}
		initialPNG, err := os.ReadFile(filepath.Join(dir, SequencesDir, spec.SequenceID, "initial.png"))
		if err != nil {
			return nil, err
		}
		initial, err := DecodePNG(initialPNG)
		if err != nil {
			return nil, err
		}
		backgroundPNG, err := os.ReadFile(filepath.Join(dir, SequencesDir, spec.SequenceID, "background.png"))
		if err != nil {
			return nil, err
		}
		background, err := DecodePNG(backgroundPNG)
		if err != nil {
			return nil, err
		}
		if initial.Width != spec.Width || initial.Height != spec.Height || background.Width != spec.Width || background.Height != spec.Height {
			return nil, fmt.Errorf("sequence %s frame dimensions mismatch", spec.SequenceID)
		}
		shared := NewSharedFrame(spec.SequenceID, initial, background, spec.Scene.SquareRect, spec.Scene.BlockRect)
		seq := &SequenceData{Spec: spec, Shared: shared, Index: seqIndex}
		for i, rec := range seqIndex.Frames {
			pngBytes, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rec.Path)))
			if err != nil {
				return nil, err
			}
			frame, err := DecodePNG(pngBytes)
			if err != nil {
				return nil, err
			}
			if frame.SHA256 != rec.SHA256 {
				return nil, fmt.Errorf("sequence %s truth frame %d digest mismatch", spec.SequenceID, i)
			}
			seq.Truth = append(seq.Truth, frame)
			seq.TruthFile = append(seq.TruthFile, rec.Path)
		}
		if len(seq.Truth) != spec.TotalFrames {
			return nil, fmt.Errorf("sequence %s has %d truth frames, manifest requires %d", spec.SequenceID, len(seq.Truth), spec.TotalFrames)
		}
		cat.Sequences[spec.SequenceID] = seq
		cat.Order = append(cat.Order, spec.SequenceID)
	}
	sort.Strings(cat.Order)
	return cat, nil
}

func (c *Catalog) Sequence(id string) (*SequenceData, *model.AppError) {
	s, ok := c.Sequences[id]
	if !ok {
		err := model.NewError(model.CodeNotFound, "sequence not found")
		err.Details = map[string]any{"sequence_id": id}
		err.SequenceID = id
		return nil, err
	}
	return s, nil
}

func (c *Catalog) Summary() []map[string]any {
	out := make([]map[string]any, 0, len(c.Order))
	for _, id := range c.Order {
		s := c.Sequences[id]
		out = append(out, map[string]any{
			"sequence_id":           id,
			"name":                  s.Spec.Name,
			"width":                 s.Spec.Width,
			"height":                s.Spec.Height,
			"total_frames":          s.Spec.TotalFrames,
			"predict_steps":         s.Spec.PredictSteps,
			"branches":              s.Spec.Branches,
			"direction_change_step": s.Spec.DirectionChangeStep,
			"frame_sha256":          s.Shared.Initial.SHA256,
			"tags":                  s.Spec.Tags,
		})
	}
	return out
}

func buildSceneAssets(spec model.SequenceSpec) (*Frame, *Frame, *Frame, error) {
	if spec.Width < 8 || spec.Height < 8 || spec.Scene.SquareRect.W < 2 || spec.Scene.BlockRect.W < 2 {
		return nil, nil, nil, fmt.Errorf("invalid scene dimensions")
	}
	r := newPRNG(spec.Seed)
	bg := make([]byte, spec.Width*spec.Height*3)
	for y := 0; y < spec.Height; y++ {
		for x := 0; x < spec.Width; x++ {
			i := (y*spec.Width + x) * 3
			noise := r.intn(9) - 4
			grid := 0
			if (x/8+y/8)%2 == 0 {
				grid = 3
			}
			bg[i] = uint8(clampInt(9+(x*19/spec.Width)+grid+noise, 0, 255))
			bg[i+1] = uint8(clampInt(14+(y*23/spec.Height)+grid+noise, 0, 255))
			bg[i+2] = uint8(clampInt(23+(x*10/spec.Width)+grid+noise, 0, 255))
		}
	}
	background := NewFrame(spec.Width, spec.Height, bg)
	sr := spec.Scene.SquareRect
	sq := make([]byte, sr.W*sr.H*3)
	for y := 0; y < sr.H; y++ {
		for x := 0; x < sr.W; x++ {
			i := (y*sr.W + x) * 3
			edge := x == 0 || y == 0 || x == sr.W-1 || y == sr.H-1
			stripe := (x+y)%6 < 2
			if edge {
				sq[i], sq[i+1], sq[i+2] = 255, 211, 92
				continue
			}
			if stripe {
				sq[i], sq[i+1], sq[i+2] = 230, 91, 72
			} else {
				sq[i], sq[i+1], sq[i+2] = 244, 129, 94
			}
		}
	}
	squareTex := NewFrame(sr.W, sr.H, sq)
	br := spec.Scene.BlockRect
	bl := make([]byte, br.W*br.H*3)
	for y := 0; y < br.H; y++ {
		for x := 0; x < br.W; x++ {
			i := (y*br.W + x) * 3
			u, v := float64(x)/float64(br.W-1), float64(y)/float64(br.H-1)
			wave := 0.5 + 0.5*math.Sin(u*8+v*4)
			bl[i] = uint8(math.Round(38 + 62*u + 24*wave))
			bl[i+1] = uint8(math.Round(151 + 68*v + 18*wave))
			bl[i+2] = uint8(math.Round(181 + 54*(1-u) + 14*wave))
			if x == 0 || y == 0 || x == br.W-1 || y == br.H-1 {
				bl[i], bl[i+1], bl[i+2] = 87, 212, 224
			}
		}
	}
	blockTex := NewFrame(br.W, br.H, bl)
	return background, squareTex, blockTex, nil
}

func composeInitial(spec model.SequenceSpec, background, squareTex, blockTex *Frame) *Frame {
	out := append([]byte(nil), background.RGB...)
	drawSquare(out, spec.Width, spec.Height, squareTex, spec.Scene.SquareRect, spec.InitialState, spec.InitialState)
	drawBlock(out, spec.Width, spec.Height, blockTex, spec.Scene.BlockRect, spec.InitialState, spec.InitialState)
	return NewFrame(spec.Width, spec.Height, out)
}

func writePNG(path string, frame *Frame) error {
	data, err := frame.PNG()
	if err != nil {
		return err
	}
	return writeAtomic(path, data, 0o644)
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeAtomic(path, data, 0o644)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp-" + strconv.Itoa(os.Getpid())
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
