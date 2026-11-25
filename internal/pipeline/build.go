package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"image"
	_ "image/png"
	"math"
	"os"

	"imretrace/internal/features"
	"imretrace/internal/model"
	"imretrace/internal/quant"
	"imretrace/internal/reference"
	"imretrace/internal/search"
	"imretrace/internal/store"
)

type Hooks struct {
	AfterPublish func(seq int, shardID, itemID string) error
}

type BuildOptions struct {
	Hooks Hooks
}

var ErrStopAfterCommit = errors.New("simulated forced stop immediately after committed checkpoint")

type StageReport struct {
	Stage     string `json:"stage"`
	Seq       int    `json:"seq"`
	Published int    `json:"published"`
	Skipped   int    `json:"skipped"`
}

type BuildReport struct {
	OK          bool          `json:"ok"`
	Policy      model.Policy  `json:"policy"`
	ItemCount   int           `json:"item_count"`
	Stages      []StageReport `json:"stages"`
	Published   int           `json:"published"`
	Skipped     int           `json:"skipped"`
	Interrupted bool          `json:"interrupted,omitempty"`
}

type Stage2Payload struct {
	ItemID         string        `json:"item_id"`
	ShardID        string        `json:"shard_id"`
	PolicyVersion  int           `json:"policy_version"`
	MatrixDigest   string        `json:"matrix_digest"`
	Matrix         *quant.Matrix `json:"matrix"`
	VisualRef      []float64     `json:"visual_ref"`
	TextRef        []float64     `json:"text_ref"`
	Visual         *quant.Vector `json:"visual"`
	Text           *quant.Vector `json:"text"`
	VisualParams   *quant.Params `json:"visual_params"`
	TextParams     *quant.Params `json:"text_params"`
	QuantParamHash string        `json:"quant_param_hash"`
}

type VerifyPayload struct {
	ItemID        string                 `json:"item_id"`
	ShardID       string                 `json:"shard_id"`
	PolicyVersion int                    `json:"policy_version"`
	RefCosine     float64                `json:"ref_cosine"`
	QuantCosine   float64                `json:"quant_cosine"`
	VisualRoute   *quant.RouteDiagnostic `json:"visual_route"`
	TextRoute     *quant.RouteDiagnostic `json:"text_route"`
	Score         quant.ScoreBound       `json:"score"`
	NumericCheck  search.NumericCheck    `json:"numeric_check"`
	Digest        string                 `json:"digest"`
}

func (s *Service) Build(ctx context.Context, opts BuildOptions) (*BuildReport, error) {
	s.buildMu.Lock()
	defer s.buildMu.Unlock()
	if _, err := s.EnsureDemo(ctx); err != nil {
		return nil, err
	}
	policy, err := s.Store.CurrentPolicy(ctx)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		p := model.DefaultPolicy()
		policy = &p
		if _, _, err := s.Store.ActivatePolicy(ctx, p); err != nil {
			return nil, err
		}
	}
	items, hashes, err := s.Store.ListItems(ctx)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, model.NewBiz(model.CodeFeatureInvalid, "build", "", "manifest contains no items")
	}
	active, err := s.Store.ActiveCheckpoints(ctx, policy.Version)
	if err != nil {
		return nil, err
	}
	existing := map[string]store.Checkpoint{}
	for _, c := range active {
		existing[c.ItemID+"|"+c.Stage] = c
	}
	report := &BuildReport{Policy: *policy, ItemCount: len(items), OK: true}

	featureByID := map[string]*features.FeatureSet{}
	stage1 := StageReport{Stage: model.StageFeature, Seq: 1}
	for _, item := range items {
		key := item.ItemID + "|" + model.StageFeature
		if c, ok := existing[key]; ok {
			var fs features.FeatureSet
			if err := json.Unmarshal([]byte(c.PayloadJSON), &fs); err != nil {
				return nil, err
			}
			featureByID[item.ItemID] = &fs
			stage1.Skipped++
			continue
		}
		imgPath, err := s.ImagePath(item)
		if err != nil {
			return nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, item.ItemID, "locate image: %v", err)
		}
		f, err := os.Open(imgPath)
		if err != nil {
			return nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, item.ItemID, "open image: %v", err)
		}
		img, _, derr := image.Decode(f)
		_ = f.Close()
		if derr != nil {
			return nil, model.NewBiz(model.CodeFeatureInvalid, model.StageFeature, item.ItemID, "decode PNG: %v", derr)
		}
		visual, verr := features.ExtractVisual(img, item.ItemID, policy.Grid)
		if verr != nil {
			return nil, verr
		}
		normalized, terms, textVec, terr := features.ExtractText(item.ItemID, item.Caption)
		if terr != nil {
			return nil, terr
		}
		fs := &features.FeatureSet{
			ItemID: item.ItemID, ShardID: item.ShardID, Stage: model.StageFeature,
			InputFingerprint: hashes[item.ItemID], PolicyVersion: policy.Version, Grid: policy.Grid,
			VisualDim: policy.VisualDim, TextDim: model.TextDim, Visual: visual, Text: textVec,
			Terms: terms, NormalizedCaption: normalized,
		}
		fs.RefreshDigest()
		if err := s.publishPayload(ctx, item, model.StageFeature, 1, *policy, hashes[item.ItemID], "", fs, fs.Digest); err != nil {
			return nil, err
		}
		featureByID[item.ItemID] = fs
		stage1.Published++
		report.Published++
		if herr := runHook(opts.Hooks, 1, item.ShardID, item.ItemID); herr != nil {
			report.Interrupted = true
			report.Stages = append(report.Stages, stage1)
			return report, herr
		}
	}
	stage1.Skipped = len(items) - stage1.Published
	report.Stages = append(report.Stages, stage1)

	captions := make([]string, len(items))
	visualX := make([][]float64, len(items))
	textX := make([][]float64, len(items))
	for i, item := range items {
		fs := featureByID[item.ItemID]
		captions[i] = item.Caption
		visualX[i] = fs.Visual
		textX[i] = fs.Text
	}
	matrix, merr := quant.FitAlignmentMatrix(*policy, captions, visualX, textX)
	if merr != nil {
		return nil, merr
	}
	visualRefs := make([][]float64, len(items))
	textRefs := make([][]float64, len(items))
	stage2Payloads := map[string]*Stage2Payload{}
	for i, item := range items {
		fs := featureByID[item.ItemID]
		vRaw, err := quant.Multiply(item.ItemID, "visual", matrix.Visual, fs.Visual, policy.ProjectionDim)
		if err != nil {
			return nil, err
		}
		vRef, _, err := quant.Normalize(item.ItemID+"#visual", vRaw)
		if err != nil {
			return nil, err
		}
		tRaw, err := quant.Multiply(item.ItemID, "text", matrix.Text, fs.Text, policy.ProjectionDim)
		if err != nil {
			return nil, err
		}
		tRef, _, err := quant.Normalize(item.ItemID+"#text", tRaw)
		if err != nil {
			return nil, err
		}
		visualRefs[i], textRefs[i] = vRef, tRef
	}
	vp, err := quant.Calibrate("visual", visualRefs, *policy)
	if err != nil {
		return nil, err
	}
	tp, err := quant.Calibrate("text", textRefs, *policy)
	if err != nil {
		return nil, err
	}
	quantHash := model.HashBytes("quant-param-pair-v1:", model.MustCanonical(map[string]string{"visual": vp.ParamDigest, "text": tp.ParamDigest}))

	stage2 := StageReport{Stage: model.StageProjectQuant, Seq: 2}
	for i, item := range items {
		key := item.ItemID + "|" + model.StageProjectQuant
		if c, ok := existing[key]; ok {
			var p Stage2Payload
			if err := json.Unmarshal([]byte(c.PayloadJSON), &p); err != nil {
				return nil, err
			}
			stage2Payloads[item.ItemID] = &p
			stage2.Skipped++
			continue
		}
		vqv, qerr := quant.QuantizeVector(item.ItemID, "visual", visualRefs[i], vp)
		if qerr != nil {
			return nil, qerr
		}
		tqv, qerr := quant.QuantizeVector(item.ItemID, "text", textRefs[i], tp)
		if qerr != nil {
			return nil, qerr
		}
		payload := &Stage2Payload{
			ItemID: item.ItemID, ShardID: item.ShardID, PolicyVersion: policy.Version, MatrixDigest: matrix.Digest, Matrix: matrix,
			VisualRef: visualRefs[i], TextRef: textRefs[i], Visual: vqv, Text: tqv,
			VisualParams: vp, TextParams: tp, QuantParamHash: quantHash,
		}
		payloadBytes, err := model.CanonicalBytes(payload)
		if err != nil {
			return nil, err
		}
		digest := model.HashBytes("stage2-artifact-v1:", payloadBytes)
		if err := s.publishPayload(ctx, item, model.StageProjectQuant, 2, *policy, hashes[item.ItemID], quantHash, payload, digest); err != nil {
			return nil, err
		}
		stage2Payloads[item.ItemID] = payload
		stage2.Published++
		report.Published++
		if herr := runHook(opts.Hooks, 2, item.ShardID, item.ItemID); herr != nil {
			report.Interrupted = true
			report.Stages = append(report.Stages, stage2)
			return report, herr
		}
	}
	stage2.Skipped = len(items) - stage2.Published
	report.Stages = append(report.Stages, stage2)

	stage3 := StageReport{Stage: model.StageIndexBuild, Seq: 3}
	for _, item := range items {
		key := item.ItemID + "|" + model.StageIndexBuild
		if _, ok := existing[key]; ok {
			stage3.Skipped++
			continue
		}
		fs := featureByID[item.ItemID]
		p2 := stage2Payloads[item.ItemID]
		entry := search.Entry{
			Item: item, InputHash: hashes[item.ItemID], Terms: fs.Terms,
			VisualRef: p2.VisualRef, TextRef: p2.TextRef, VisualQuant: p2.Visual, TextQuant: p2.Text,
			FeatureDigest: fs.Digest,
		}
		payloadBytes, err := model.CanonicalBytes(entry)
		if err != nil {
			return nil, err
		}
		digest := model.HashBytes("stage3-index-entry-v1:", payloadBytes)
		if err := s.publishPayload(ctx, item, model.StageIndexBuild, 3, *policy, hashes[item.ItemID], quantHash, entry, digest); err != nil {
			return nil, err
		}
		stage3.Published++
		report.Published++
		if herr := runHook(opts.Hooks, 3, item.ShardID, item.ItemID); herr != nil {
			report.Interrupted = true
			report.Stages = append(report.Stages, stage3)
			return report, herr
		}
	}
	stage3.Skipped = len(items) - stage3.Published
	report.Stages = append(report.Stages, stage3)

	idx, err := s.LoadIndex(ctx)
	if err != nil {
		return nil, err
	}
	stage4 := StageReport{Stage: model.StageRecallVerify, Seq: 4}
	for _, item := range items {
		key := item.ItemID + "|" + model.StageRecallVerify
		if _, ok := existing[key]; ok {
			stage4.Skipped++
			continue
		}
		entry, ok := idx.Get(item.ItemID)
		if !ok {
			return nil, model.NewBiz(model.CodeStaleCheckpoint, model.StageRecallVerify, item.ItemID, "index entry missing after stage 3")
		}
		vr, err := quant.DiagnoseRoute(item.ItemID, "pair-visual", "visual", entry.VisualRef, entry.VisualQuant.Dequant, idx.VisualParams)
		if err != nil {
			return nil, err
		}
		tr, err := quant.DiagnoseRoute(item.ItemID, "pair-text", "text", entry.TextRef, entry.TextQuant.Dequant, idx.TextParams)
		if err != nil {
			return nil, err
		}
		refCos := reference.Cosine(entry.TextRef, entry.VisualRef)
		qCos := quant.Cosine(entry.TextQuant.Dequant, entry.VisualQuant.Dequant)
		bound := quant.BoundCosine(refCos, vr, tr)
		sd := quant.ScoreBound{
			CosineBound: bound, CosineActualAbs: abs(refCos - qCos),
			FinalScoreBound: bound, FinalActualAbs: abs(refCos - qCos),
			ReferenceFusedScore: refCos, QuantFusedScore: qCos,
			RelativeErrorBound: bound / maxFloat(abs(refCos), policy.RelativeScoreFloor),
			RelativeActual:     abs(refCos-qCos) / maxFloat(abs(refCos), policy.RelativeScoreFloor),
			Formula:            "recall verification uses cosine-only fusion; query fusion weights are checked separately",
		}
		sd.EffectiveBits = effectiveBits(sd.RelativeErrorBound)
		sd.EffectiveSignificantDigits = digits(sd.RelativeErrorBound)
		sd.EffectiveAbsDecimalDigits = digits(sd.FinalScoreBound)
		sd = quant.WithVerifiedReference(sd, refCos, policy.RelativeScoreFloor)
		check := search.NumericCheck{
			Passed: abs(refCos-qCos) <= policy.NumericAbsTolerance && sd.RelativeActual <= policy.NumericRelTolerance &&
				vr.VectorL2Actual <= policy.NumericAbsTolerance && tr.VectorL2Actual <= policy.NumericAbsTolerance,
			WorstItemID: item.ItemID, MaxAbsDiff: maxFloat(abs(refCos-qCos), maxFloat(vr.VectorL2Actual, tr.VectorL2Actual)),
			MaxRelDiff: sd.RelativeActual, AbsTolerance: policy.NumericAbsTolerance, RelTolerance: policy.NumericRelTolerance,
		}
		if !check.Passed {
			fA, fR := check.MaxAbsDiff, check.MaxRelDiff
			biz := model.NewBiz(model.CodeNumericMismatch, model.StageRecallVerify, item.ItemID,
				"reference/quantized deviation exceeds tolerance: max_abs=%.6g max_rel=%.6g", fA, fR)
			biz.MaxAbsDiff, biz.MaxRelDiff = &fA, &fR
			return report, biz
		}
		verify := VerifyPayload{
			ItemID: item.ItemID, ShardID: item.ShardID, PolicyVersion: policy.Version,
			RefCosine: refCos, QuantCosine: qCos, VisualRoute: vr, TextRoute: tr, Score: sd, NumericCheck: check,
		}
		verify.Digest = model.HashBytes("stage4-verification-v1:", model.MustCanonical(verify))
		if err := s.publishPayload(ctx, item, model.StageRecallVerify, 4, *policy, hashes[item.ItemID], quantHash, verify, verify.Digest); err != nil {
			return nil, err
		}
		stage4.Published++
		report.Published++
		if herr := runHook(opts.Hooks, 4, item.ShardID, item.ItemID); herr != nil {
			report.Interrupted = true
			report.Stages = append(report.Stages, stage4)
			return report, herr
		}
	}
	stage4.Skipped = len(items) - stage4.Published
	report.Stages = append(report.Stages, stage4)
	return report, nil
}

func (s *Service) publishPayload(ctx context.Context, item model.ManifestItem, stage string, seq int, policy model.Policy, inputHash, quantHash string, payload any, artifactDigest string) error {
	payloadBytes, err := model.CanonicalBytes(payload)
	if err != nil {
		return err
	}
	_, err = s.Store.PublishCheckpoint(ctx, store.PublishInput{
		ShardID: item.ShardID, ItemID: item.ItemID, Stage: stage, Seq: seq,
		PolicyVersion: policy.Version, PolicyHash: policy.ContentHash, InputHash: inputHash,
		QuantParamHash: quantHash, ArtifactDigest: artifactDigest, PayloadJSON: string(payloadBytes),
	})
	return err
}

func runHook(h Hooks, seq int, shardID, itemID string) error {
	if h.AfterPublish == nil {
		return nil
	}
	return h.AfterPublish(seq, shardID, itemID)
}

func (s *Service) LoadIndex(ctx context.Context) (*search.Index, error) {
	policy, err := s.Store.CurrentPolicy(ctx)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, model.NewBiz(model.CodePolicyInvalid, "index", "", "no active policy")
	}
	checkpoints, err := s.Store.ActiveCheckpoints(ctx, policy.Version)
	if err != nil {
		return nil, err
	}
	stage2 := map[string]Stage2Payload{}
	entries := []*search.Entry{}
	var vp *quant.Params
	var tp *quant.Params
	matrixDigest := ""
	var matrix *quant.Matrix
	for _, c := range checkpoints {
		switch c.Stage {
		case model.StageProjectQuant:
			var p Stage2Payload
			if err := json.Unmarshal([]byte(c.PayloadJSON), &p); err != nil {
				return nil, err
			}
			stage2[c.ItemID] = p
			vp, tp, matrixDigest, matrix = p.VisualParams, p.TextParams, p.MatrixDigest, p.Matrix
		case model.StageIndexBuild:
			var e search.Entry
			if err := json.Unmarshal([]byte(c.PayloadJSON), &e); err != nil {
				return nil, err
			}
			entries = append(entries, &e)
		}
	}
	if len(entries) == 0 || vp == nil || tp == nil {
		return nil, model.NewBiz(model.CodeStaleCheckpoint, "index", "", "active index is incomplete")
	}
	idx := search.NewIndex(*policy, matrixDigest, vp, tp, entries)
	idx.Matrix = matrix
	return idx, nil
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
func effectiveBits(bound float64) float64 {
	if bound <= 0 {
		return 53
	}
	return float64(int(maxFloat(0, -math.Log2(bound))*1000)) / 1000
}
func digits(bound float64) float64 {
	if bound <= 0 {
		return 15
	}
	return float64(int(maxFloat(0, -math.Log10(bound))))
}
