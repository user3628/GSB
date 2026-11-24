package generator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"maskrevision/internal/imageio"
	"maskrevision/internal/model"
	"maskrevision/internal/revlog"
)

const (
	DefaultSeed   int64 = 20261125
	ShardWidth          = 40
	ShardHeight         = 30
	GeneratorName       = "panoptic-fixed-grid-v1"
)

type actionSpec struct {
	Action          string
	Region          model.Region
	CategoryID      int
	TargetSegmentID int
	ConfidencePct   int
	Actor           string
	Note            string
}

type generated struct {
	ManifestV2   model.Manifest
	ManifestV1   model.Manifest
	V2Logs       map[string][]byte
	V1Logs       map[string][]byte
	ForkLogs     map[string][]byte
	EditedLogs   map[string][]byte
	RollbackLogs map[string][]byte
	Digest       string
}

func Generate(outDir string, seed int64) (model.Manifest, error) {
	g, err := generate(seed)
	if err != nil {
		return model.Manifest{}, err
	}
	if err := os.RemoveAll(outDir); err != nil {
		return model.Manifest{}, err
	}
	for _, dir := range []string{outDir, filepath.Join(outDir, "shards"), filepath.Join(outDir, "logs"), filepath.Join(outDir, "fixtures"), filepath.Join(outDir, "legacy")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return model.Manifest{}, err
		}
	}
	for i := range g.ManifestV2.Shards {
		sh := &g.ManifestV2.Shards[i]
		imgPath := filepath.Join(outDir, filepath.FromSlash(sh.Image))
		if err := os.WriteFile(imgPath, g.imageData[sh.ShardID], 0o644); err != nil {
			return model.Manifest{}, err
		}
	}
	for name, data := range g.V2Logs {
		if err := writeJSON(filepath.Join(outDir, "logs", name+".v2.json"), data); err != nil {
			return model.Manifest{}, err
		}
	}
	for name, data := range g.V1Logs {
		if err := writeJSON(filepath.Join(outDir, "logs", name+".v1.json"), data); err != nil {
			return model.Manifest{}, err
		}
	}
	for name, data := range g.ForkLogs {
		if err := writeJSON(filepath.Join(outDir, "fixtures", name+".fork.json"), data); err != nil {
			return model.Manifest{}, err
		}
	}
	for name, data := range g.EditedLogs {
		if err := writeJSON(filepath.Join(outDir, "fixtures", name+".edited.json"), data); err != nil {
			return model.Manifest{}, err
		}
	}
	for name, data := range g.RollbackLogs {
		if err := writeJSON(filepath.Join(outDir, "fixtures", name+".rollback.json"), data); err != nil {
			return model.Manifest{}, err
		}
	}
	for name, data := range g.LegacyLogs {
		if err := writeJSON(filepath.Join(outDir, "legacy", name+".json"), data); err != nil {
			return model.Manifest{}, err
		}
	}
	for i := range g.ManifestV1.Shards {
		sh := &g.ManifestV1.Shards[i]
		sh.Log = strings.Replace(sh.Log, "/logs/", "/logs/", 1)
	}
	if err := writeJSON(filepath.Join(outDir, "manifest.json"), mustJSON(g.ManifestV2)); err != nil {
		return model.Manifest{}, err
	}
	if err := writeJSON(filepath.Join(outDir, "manifest.v1.json"), mustJSON(g.ManifestV1)); err != nil {
		return model.Manifest{}, err
	}
	manifest := g.ManifestV2
	return manifest, nil
}

type generationState struct {
	ManifestV2   model.Manifest
	ManifestV1   model.Manifest
	V2Logs       map[string][]byte
	V1Logs       map[string][]byte
	ForkLogs     map[string][]byte
	EditedLogs   map[string][]byte
	RollbackLogs map[string][]byte
	LegacyLogs   map[string][]byte
	imageData    map[string][]byte
	Digest       string
}

func generate(seed int64) (*generationState, error) {
	if seed == 0 {
		seed = DefaultSeed
	}
	g := &generationState{
		V2Logs:       map[string][]byte{},
		V1Logs:       map[string][]byte{},
		ForkLogs:     map[string][]byte{},
		EditedLogs:   map[string][]byte{},
		RollbackLogs: map[string][]byte{},
		LegacyLogs:   map[string][]byte{},
		imageData:    map[string][]byte{},
	}
	baseSegs := initialSegments()
	pixels := initialPixels()
	pngBytes, err := imageio.EncodeIndexed(ShardWidth, ShardHeight, pixels, baseSegs)
	if err != nil {
		return nil, err
	}
	imageFP := model.PixelDigest(ShardWidth, ShardHeight, pixels)
	g.ManifestV2 = model.Manifest{SchemaVersion: model.SchemaV2, Seed: seed, Generator: GeneratorName}
	g.ManifestV1 = model.Manifest{SchemaVersion: model.SchemaV1, Seed: seed, Generator: GeneratorName}
	specs := shardSpecs()
	for i := 0; i < 4; i++ {
		shardID := fmt.Sprintf("shard-%02d", i)
		imgRel := fmt.Sprintf("shards/%s.png", shardID)
		g.imageData[shardID] = pngBytes
		segs := cloneSegmentList(model.SortedSegments(baseSegs))
		v2Log := model.Log{
			SchemaVersion: model.SchemaV2,
			LogID:         shardID + "-main",
			ShardID:       shardID,
			Width:         ShardWidth,
			Height:        ShardHeight,
			Revisions:     []model.Revision{},
		}
		for j, spec := range specs[i] {
			rev := model.Revision{
				Revision:        j + 1,
				ParentRevision:  j,
				Action:          spec.Action,
				Region:          spec.Region,
				CategoryID:      spec.CategoryID,
				TargetSegmentID: spec.TargetSegmentID,
				Confidence:      float64(spec.ConfidencePct) / 100.0,
				Actor:           spec.Actor,
				SourceVersion:   model.SchemaV2,
				Note:            spec.Note,
			}
			v2Log.Revisions = append(v2Log.Revisions, rev)
		}
		v2Data := mustJSON(v2Log)
		g.V2Logs[shardID] = v2Data
		v1Log := downgradeV1(v2Log, seed)
		v1Data := mustJSON(v1Log)
		g.V1Logs[shardID] = v1Data

		// Verify that every generated legacy reassignment can be resolved from
		// the initial indexed mask. A broken fixture should fail generation,
		// rather than silently becoming a no-op during the demo.
		upgrade, err := revlog.UpgradeV1(v1Data, revlog.UpgradeOptions{Width: ShardWidth, Height: ShardHeight, Pixels: pixels})
		if err != nil {
			return nil, fmt.Errorf("validate generated v1 %s: %w", shardID, err)
		}
		if upgrade.Report.UnrecoverableCount != 0 {
			return nil, fmt.Errorf("generated v1 %s contains %d unrecoverable actions", shardID, upgrade.Report.UnrecoverableCount)
		}

		mainLog := "logs/" + shardID
		mainLogPath := mainLog + ".v2.json"
		if shardID == "shard-01" {
			mainLogPath = mainLog + ".v1.json"
		}
		v2ManifestShard := model.ManifestShard{
			ShardID:        shardID,
			Image:          imgRel,
			ImageSHA256:    model.SHA256Hex(pngBytes),
			Width:          ShardWidth,
			Height:         ShardHeight,
			Log:            mainLogPath,
			Segments:       segs,
			InitialPixelFP: imageFP,
		}
		v1ManifestShard := v2ManifestShard
		v1ManifestShard.Log = mainLog + ".v1.json"
		g.ManifestV2.Shards = append(g.ManifestV2.Shards, v2ManifestShard)
		g.ManifestV1.Shards = append(g.ManifestV1.Shards, v1ManifestShard)
	}
	g.ManifestV2.Shards[0].ForkLog = "fixtures/shard-00.fork.json"
	main0 := mustDecodeLog(g.V2Logs["shard-00"])
	fork := model.Log{
		SchemaVersion: model.SchemaV2,
		LogID:         "shard-00-branch-after-undo",
		ShardID:       "shard-00",
		Width:         ShardWidth,
		Height:        ShardHeight,
		ForkOf:        "shard-00-main",
		Branch:        "actor-b-after-undo",
	}
	for _, rev := range main0.Revisions {
		if rev.Revision <= 4 {
			fork.Revisions = append(fork.Revisions, rev)
		}
	}
	fork.Revisions = append(fork.Revisions, model.Revision{
		Revision:       5,
		ParentRevision: 2,
		Action:         model.ActionRepaint,
		Region:         model.Region{X: 30, Y: 2, W: 6, H: 5},
		CategoryID:     8,
		Confidence:     0.77,
		Actor:          "actor-b",
		SourceVersion:  model.SchemaV2,
		Note:           "inserted after undo of revision 3; parent is revision 2",
	})
	g.ForkLogs["shard-00"] = mustJSON(fork)

	edited := main0
	edited.LogID = "shard-00-edited-revision"
	edited.Revisions = append([]model.Revision(nil), main0.Revisions...)
	edited.Revisions[4].Region = model.Region{X: 27, Y: 17, W: 6, H: 5}
	edited.Revisions[4].Note = "revision action content was edited upstream"
	g.EditedLogs["shard-00"] = mustJSON(edited)
	rollback := main0
	rollback.LogID = "shard-00-rollback"
	rollback.Revisions = append([]model.Revision(nil), main0.Revisions[:4]...)
	g.RollbackLogs["shard-00"] = mustJSON(rollback)

	// A small legacy fixture proves the unrecoverable path without breaking the
	// replayable main corpus.
	unrec := rawV1LogFixture(seed)
	g.LegacyLogs["v1-unrecoverable"] = mustJSON(unrec)
	g.Digest = digestGeneration(g)
	return g, nil
}

func initialSegments() map[int]*model.Segment {
	segs := map[int]*model.Segment{
		0: {ID: 0, CategoryID: 0, InstanceNo: 0, Status: model.StatusIgnored, PixelCount: 0, Source: "reserved-ignore", CreatedRevision: 0, UpdatedRevision: 0},
	}
	for id := 1; id <= 12; id++ {
		category := (id-1)%6 + 1
		instance := (id-1)/6 + 1
		segs[id] = &model.Segment{
			ID: id, CategoryID: category, InstanceNo: instance, Status: model.StatusActive,
			PixelCount: 100, Source: "initial", CreatedRevision: 0, UpdatedRevision: 0,
		}
	}
	return segs
}

func initialPixels() []int {
	pixels := make([]int, ShardWidth*ShardHeight)
	for y := 0; y < ShardHeight; y++ {
		for x := 0; x < ShardWidth; x++ {
			col := x / 10
			row := y / 10
			pixels[y*ShardWidth+x] = row*4 + col + 1
		}
	}
	return pixels
}

func shardSpecs() [][]actionSpec {
	return [][]actionSpec{
		{
			{Action: model.ActionRepaint, Region: model.Region{X: 2, Y: 2, W: 6, H: 5}, CategoryID: 4, ConfidencePct: 93, Actor: "actor-a", Note: "repaint to a new instance segment"},
			{Action: model.ActionReassign, Region: model.Region{X: 15, Y: 3, W: 5, H: 5}, TargetSegmentID: 4, ConfidencePct: 88, Actor: "actor-b"},
			{Action: model.ActionIgnore, Region: model.Region{X: 22, Y: 2, W: 6, H: 6}, ConfidencePct: 81, Actor: "actor-c"},
			{Action: model.ActionUndo, ConfidencePct: 99, Actor: "actor-a"},
			{Action: model.ActionRepaint, Region: model.Region{X: 3, Y: 13, W: 7, H: 6}, CategoryID: 5, TargetSegmentID: 2, ConfidencePct: 90, Actor: "actor-b"},
			{Action: model.ActionUndo, ConfidencePct: 99, Actor: "actor-a"},
			{Action: model.ActionUndo, ConfidencePct: 99, Actor: "actor-c"},
			{Action: model.ActionRepaint, Region: model.Region{X: 25, Y: 15, W: 9, H: 7}, CategoryID: 6, ConfidencePct: 86, Actor: "actor-b"},
		},
		{
			{Action: model.ActionReassign, Region: model.Region{X: 5, Y: 4, W: 5, H: 4}, TargetSegmentID: 8, ConfidencePct: 84, Actor: "legacy-a"},
			{Action: model.ActionRepaint, Region: model.Region{X: 13, Y: 3, W: 5, H: 5}, CategoryID: 3, ConfidencePct: 79, Actor: "legacy-b"},
			{Action: model.ActionIgnore, Region: model.Region{X: 24, Y: 4, W: 6, H: 5}, ConfidencePct: 83, Actor: "legacy-a"},
			{Action: model.ActionUndo, ConfidencePct: 98, Actor: "legacy-b"},
			{Action: model.ActionRepaint, Region: model.Region{X: 16, Y: 13, W: 6, H: 5}, TargetSegmentID: 6, CategoryID: 8, ConfidencePct: 87, Actor: "legacy-c"},
			{Action: model.ActionUndo, ConfidencePct: 98, Actor: "legacy-c"},
		},
		{
			{Action: model.ActionRepaint, Region: model.Region{X: 3, Y: 13, W: 6, H: 6}, TargetSegmentID: 9, CategoryID: 7, ConfidencePct: 91, Actor: "actor-a"},
			{Action: model.ActionIgnore, Region: model.Region{X: 23, Y: 3, W: 6, H: 5}, ConfidencePct: 82, Actor: "actor-b"},
			{Action: model.ActionUndo, ConfidencePct: 99, Actor: "actor-c"},
			{Action: model.ActionReassign, Region: model.Region{X: 25, Y: 14, W: 5, H: 4}, TargetSegmentID: 11, ConfidencePct: 85, Actor: "actor-c"},
			{Action: model.ActionUndo, ConfidencePct: 98, Actor: "actor-a"},
			{Action: model.ActionRepaint, Region: model.Region{X: 12, Y: 22, W: 7, H: 6}, CategoryID: 2, ConfidencePct: 89, Actor: "actor-b"},
		},
		{
			{Action: model.ActionIgnore, Region: model.Region{X: 2, Y: 2, W: 6, H: 5}, ConfidencePct: 83, Actor: "actor-c"},
			{Action: model.ActionRepaint, Region: model.Region{X: 13, Y: 2, W: 6, H: 5}, CategoryID: 7, ConfidencePct: 92, Actor: "actor-a"},
			{Action: model.ActionUndo, ConfidencePct: 99, Actor: "actor-b"},
			{Action: model.ActionReassign, Region: model.Region{X: 15, Y: 13, W: 5, H: 6}, TargetSegmentID: 12, ConfidencePct: 80, Actor: "actor-b"},
			{Action: model.ActionUndo, ConfidencePct: 97, Actor: "actor-c"},
			{Action: model.ActionRepaint, Region: model.Region{X: 5, Y: 22, W: 8, H: 6}, TargetSegmentID: 1, CategoryID: 4, ConfidencePct: 88, Actor: "actor-a"},
		},
	}
}

type rawV1Log struct {
	SchemaVersion     int             `json:"schema_version"`
	LogID             string          `json:"log_id"`
	ShardID           string          `json:"shard_id"`
	Width             int             `json:"width"`
	Height            int             `json:"height"`
	ConfidencePercent int             `json:"confidence_percent"`
	Revisions         []rawV1Revision `json:"revisions"`
}

type rawV1Revision struct {
	Revision     int          `json:"revision"`
	ActionPhrase string       `json:"action_phrase"`
	Region       model.Region `json:"region"`
	CategoryID   int          `json:"category_id,omitempty"`
	Confidence   int          `json:"confidence"`
	Actor        string       `json:"actor"`
	Note         string       `json:"note,omitempty"`
}

func downgradeV1(v2 model.Log, seed int64) rawV1Log {
	out := rawV1Log{
		SchemaVersion:     model.SchemaV1,
		LogID:             v2.LogID + "-legacy",
		ShardID:           v2.ShardID,
		Width:             v2.Width,
		Height:            v2.Height,
		ConfidencePercent: 85,
	}
	for _, r := range v2.Revisions {
		phrase := ""
		switch r.Action {
		case model.ActionRepaint:
			phrase = fmt.Sprintf("将区域重涂为类别 %d", r.CategoryID)
		case model.ActionReassign:
			phrase = "将该区域改判为相邻实例"
		case model.ActionIgnore:
			phrase = "将该区域置为忽略"
		case model.ActionUndo:
			phrase = "撤销上一修订"
		}
		rv := rawV1Revision{
			Revision: r.Revision, ActionPhrase: phrase, Region: r.Region,
			CategoryID: r.CategoryID, Confidence: int(r.Confidence*100 + 0.5), Actor: r.Actor, Note: r.Note,
		}
		if r.Action == model.ActionReassign {
			// v1 intentionally omits target_segment_id. The importer must infer it.
			rv.CategoryID = 0
		}
		out.Revisions = append(out.Revisions, rv)
	}
	return out
}

func rawV1LogFixture(seed int64) rawV1Log {
	return rawV1Log{
		SchemaVersion:     model.SchemaV1,
		LogID:             fmt.Sprintf("shard-01-unrecoverable-%d", seed),
		ShardID:           "shard-01",
		Width:             ShardWidth,
		Height:            ShardHeight,
		ConfidencePercent: 61,
		Revisions: []rawV1Revision{
			{Revision: 1, ActionPhrase: "将该区域改判为相邻实例", Region: model.Region{X: 2, Y: 2, W: 2, H: 2}, Confidence: 61, Actor: "legacy-x", Note: "segment 1 is completely enclosed by itself"},
		},
	}
}

func cloneSegmentList(in []model.Segment) []model.Segment {
	out := make([]model.Segment, len(in))
	copy(out, in)
	for i := range out {
		out[i].History = nil
	}
	return out
}

func mustDecodeLog(data []byte) model.Log {
	var log model.Log
	if err := json.Unmarshal(data, &log); err != nil {
		panic(err)
	}
	return log
}

func mustJSON(v any) []byte {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(data, '\n')
}

func writeJSON(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func digestGeneration(g *generationState) string {
	var names []string
	for name := range g.V2Logs {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("generated-v1\x00")
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte(0)
		b.WriteString(model.SHA256Hex(g.V2Logs[name]))
		b.WriteByte(0)
		b.WriteString(model.SHA256Hex(g.V1Logs[name]))
		b.WriteByte(0)
	}
	for name := range g.ForkLogs {
		b.WriteString(name)
		b.WriteString("-fork\x00")
		b.WriteString(model.SHA256Hex(g.ForkLogs[name]))
		b.WriteByte(0)
	}
	return model.SHA256Hex([]byte(b.String()))
}
