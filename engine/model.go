package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
)

type TrainingMeta struct {
	Seq           int64
	BatchID       string
	ModelVersion  int64
	Generation    int64
	DatasetDigest string
}

type UnitUpdate struct {
	UnitID        int     `json:"unit_id"`
	UnitVersion   int64   `json:"unit_version"`
	SampleSeq     int64   `json:"sample_seq"`
	ModelVersion  int64   `json:"model_version"`
	BatchID       string  `json:"batch_id"`
	Scales        []int64 `json:"scales"`
	GradientAccum []int64 `json:"gradient_accum"`
	Deltas        []int64 `json:"deltas"`
}

func NewCheckpoint(cfg Config, datasetDigest string, generation int64) (*Checkpoint, error) {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfgDigest, err := cfg.Digest()
	if err != nil {
		return nil, err
	}
	cp := &Checkpoint{
		Format:        FormatVersion,
		Config:        cfg,
		ConfigDigest:  cfgDigest,
		DatasetDigest: datasetDigest,
		Generation:    generation,
		Units:         make([]UnitCheckpoint, 0, cfg.Units),
	}
	for u := 0; u < cfg.Units; u++ {
		dims := UnitDims(u, cfg.Dim, cfg.Units)
		unit := UnitCheckpoint{UnitID: u, UnitVersion: 0, Dims: make([]DimState, 0, len(dims))}
		for _, d := range dims {
			unit.Dims = append(unit.Dims, DimState{
				Dim:       d,
				UnitID:    u,
				Scale:     ScaleForDim(d, cfg),
				Weight:    0,
				RefWeight: "0/1",
			})
		}
		cp.Units = append(cp.Units, unit)
	}
	if err := cp.RefreshFingerprint(); err != nil {
		return nil, err
	}
	return cp, nil
}

func (cp *Checkpoint) unitIndex(unitID int) int {
	for i := range cp.Units {
		if cp.Units[i].UnitID == unitID {
			return i
		}
	}
	return -1
}

func (cp *Checkpoint) DimState(dim int) (*DimState, error) {
	u := UnitOfDim(dim, cp.Config.Units)
	ui := cp.unitIndex(u)
	if ui < 0 {
		d := dim
		return nil, &Error{Code: CodeFeatureDimInvalid, Dim: &d, Message: "specialization unit not found"}
	}
	for i := range cp.Units[ui].Dims {
		if cp.Units[ui].Dims[i].Dim == dim {
			return &cp.Units[ui].Dims[i], nil
		}
	}
	d := dim
	return nil, &Error{Code: CodeFeatureDimInvalid, Dim: &d, Message: "dimension state not found"}
}

func gradientAccumulator(fv *FeatureVector, label, epochs int) []int64 {
	polarity := int64(1)
	if label == 0 {
		polarity = -1
	}
	accum := make([]int64, fv.Dim)
	for epoch := 0; epoch < epochs; epoch++ {
		for _, f := range fv.Features {
			accum[f.Dim] += polarity * int64(f.Value)
		}
	}
	return accum
}

func truncRat(r *big.Rat) int64 {
	q, _ := new(big.Int).QuoRem(r.Num(), r.Denom(), new(big.Int))
	return q.Int64()
}

// TrainSample applies one sample in deterministic arrival order. Coordinates are
// independent, which is what makes shard-local replay byte-equivalent to full replay.
func TrainSample(cp *Checkpoint, in SampleInput, meta TrainingMeta) ([]UnitUpdate, *FeatureVector, error) {
	if err := ValidateSample(in); err != nil {
		return nil, nil, err
	}
	fv, err := Featurize(in, cp.Config)
	if err != nil {
		return nil, nil, err
	}
	if err := ValidateFeatureVector(fv, cp.Config); err != nil {
		return nil, nil, err
	}

	accum := gradientAccumulator(fv, in.Label, cp.Config.Epochs)
	lr := big.NewRat(cp.Config.LRNum, cp.Config.LRDen)
	updates := make([]UnitUpdate, 0)

	for ui := range cp.Units {
		unit := &cp.Units[ui]
		touched := false
		for i := range unit.Dims {
			if accum[unit.Dims[i].Dim] != 0 {
				touched = true
				break
			}
		}
		if !touched {
			continue
		}

		deltas := make([]int64, len(unit.Dims))
		unitAccum := make([]int64, len(unit.Dims))
		scales := make([]int64, len(unit.Dims))
		for i := range unit.Dims {
			ds := &unit.Dims[i]
			a := accum[ds.Dim]
			unitAccum[i] = a
			scales[i] = ds.Scale
			ds.LastDelta = 0
			if a == 0 {
				continue
			}
			touched = true

			ref, ok := new(big.Rat).SetString(ds.RefWeight)
			if !ok {
				return nil, nil, newError(CodeNumeric, "invalid high precision weight at dim %d", ds.Dim)
			}
			step := new(big.Rat).Mul(lr, big.NewRat(a, 1))
			ref.Add(ref, step)
			ds.RefWeight = ref.String()

			num := new(big.Int).Mul(big.NewInt(ds.Scale), big.NewInt(cp.Config.LRNum))
			num.Mul(num, big.NewInt(a))
			quot := new(big.Int).Quo(num, big.NewInt(cp.Config.LRDen))
			if !quot.IsInt64() {
				d := ds.Dim
				return nil, nil, &Error{Code: CodeNumeric, Dim: &d, Message: "fixed-point delta overflow"}
			}
			delta := quot.Int64()
			ds.Weight += delta
			ds.LastDelta = delta
			ds.UpdateCount++
			deltas[i] = delta
		}

		unit.UnitVersion++
		updates = append(updates, UnitUpdate{
			UnitID:        unit.UnitID,
			UnitVersion:   unit.UnitVersion,
			SampleSeq:     meta.Seq,
			ModelVersion:  meta.ModelVersion,
			BatchID:       meta.BatchID,
			Scales:        scales,
			GradientAccum: unitAccum,
			Deltas:        deltas,
		})
	}

	cp.ModelVersion = meta.ModelVersion
	cp.EventSequence = meta.Seq
	if meta.Generation != 0 {
		cp.Generation = meta.Generation
	}
	if meta.DatasetDigest != "" {
		cp.DatasetDigest = meta.DatasetDigest
	}
	if err := cp.RefreshFingerprint(); err != nil {
		return nil, nil, err
	}
	return updates, fv, nil
}

func sortedStoredSamples(samples []StoredSample) []StoredSample {
	out := append([]StoredSample(nil), samples...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// RebuildUnit replays only the supplied dependency samples for one unit.
func RebuildUnit(unitID int, samples []StoredSample, cfg Config) (*UnitCheckpoint, []UnitUpdate, error) {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}
	dims := UnitDims(unitID, cfg.Dim, cfg.Units)
	unit := &UnitCheckpoint{UnitID: unitID, UnitVersion: 0, Dims: make([]DimState, 0, len(dims))}
	for _, d := range dims {
		unit.Dims = append(unit.Dims, DimState{
			Dim:       d,
			UnitID:    unitID,
			Scale:     ScaleForDim(d, cfg),
			Weight:    0,
			RefWeight: "0/1",
		})
	}
	updates := make([]UnitUpdate, 0)
	lr := big.NewRat(cfg.LRNum, cfg.LRDen)

	for _, sample := range sortedStoredSamples(samples) {
		if err := ValidateSample(sample.SampleInput); err != nil {
			return nil, nil, err
		}
		fv, err := Featurize(sample.SampleInput, cfg)
		if err != nil {
			return nil, nil, err
		}
		accum := gradientAccumulator(fv, sample.Label, cfg.Epochs)
		touched := false
		for i := range unit.Dims {
			if accum[unit.Dims[i].Dim] != 0 {
				touched = true
				break
			}
		}
		if !touched {
			continue
		}

		unit.UnitVersion++
		deltas := make([]int64, len(unit.Dims))
		unitAccum := make([]int64, len(unit.Dims))
		scales := make([]int64, len(unit.Dims))
		for i := range unit.Dims {
			ds := &unit.Dims[i]
			a := accum[ds.Dim]
			unitAccum[i] = a
			scales[i] = ds.Scale
			ds.LastDelta = 0
			if a == 0 {
				continue
			}

			ref, ok := new(big.Rat).SetString(ds.RefWeight)
			if !ok {
				d := ds.Dim
				return nil, nil, &Error{Code: CodeNumeric, Dim: &d, Message: "invalid high precision weight"}
			}
			ref.Add(ref, new(big.Rat).Mul(lr, big.NewRat(a, 1)))
			ds.RefWeight = ref.String()

			num := new(big.Int).Mul(big.NewInt(ds.Scale), big.NewInt(cfg.LRNum))
			num.Mul(num, big.NewInt(a))
			quot := new(big.Int).Quo(num, big.NewInt(cfg.LRDen))
			if !quot.IsInt64() {
				d := ds.Dim
				return nil, nil, &Error{Code: CodeNumeric, Dim: &d, Message: "fixed-point delta overflow"}
			}
			delta := quot.Int64()
			ds.Weight += delta
			ds.LastDelta = delta
			ds.UpdateCount++
			deltas[i] = delta
		}

		updates = append(updates, UnitUpdate{
			UnitID:        unitID,
			UnitVersion:   unit.UnitVersion,
			SampleSeq:     sample.Seq,
			ModelVersion:  sample.Seq,
			BatchID:       sample.BatchID,
			Scales:        scales,
			GradientAccum: unitAccum,
			Deltas:        deltas,
		})
	}
	return unit, updates, nil
}

// RebuildAll performs the from-zero reference replay.
func RebuildAll(samples []StoredSample, cfg Config, datasetDigest string, modelVersion, generation int64) (*Checkpoint, error) {
	cp, err := NewCheckpoint(cfg, datasetDigest, generation)
	if err != nil {
		return nil, err
	}
	for _, sample := range sortedStoredSamples(samples) {
		if _, _, err := TrainSample(cp, sample.SampleInput, TrainingMeta{
			Seq:          sample.Seq,
			BatchID:      sample.BatchID,
			ModelVersion: sample.Seq,
			Generation:   generation,
		}); err != nil {
			return nil, err
		}
	}
	cp.ModelVersion = modelVersion
	cp.DatasetDigest = datasetDigest
	cp.Generation = generation
	if err := cp.RefreshFingerprint(); err != nil {
		return nil, err
	}
	return cp, nil
}

func CloneCheckpoint(in *Checkpoint) (*Checkpoint, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var out Checkpoint
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, out.RefreshFingerprint()
}

// AssembleIncremental replaces only rebuilt units and reuses all other unit bytes.
func AssembleIncremental(base *Checkpoint, rebuilt map[int]*UnitCheckpoint, datasetDigest string, modelVersion, generation int64) (*Checkpoint, error) {
	out, err := CloneCheckpoint(base)
	if err != nil {
		return nil, err
	}
	for i := range out.Units {
		if u, ok := rebuilt[out.Units[i].UnitID]; ok {
			clone, err := CloneUnit(u)
			if err != nil {
				return nil, err
			}
			out.Units[i] = *clone
		}
	}
	out.DatasetDigest = datasetDigest
	out.ModelVersion = modelVersion
	out.Generation = generation
	if err := out.RefreshFingerprint(); err != nil {
		return nil, err
	}
	return out, nil
}

func CloneUnit(in *UnitCheckpoint) (*UnitCheckpoint, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var out UnitCheckpoint
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func EnsureCheckpointsEqual(local, full *Checkpoint) error {
	la, err := local.Bytes()
	if err != nil {
		return err
	}
	fb, err := full.Bytes()
	if err != nil {
		return err
	}
	if diff := FirstByteDifference(la, fb); diff >= 0 {
		return &Error{
			Code:          CodeIncrementalMismatch,
			FirstDiffByte: diff,
			Message:       fmt.Sprintf("incremental checkpoint differs from full rebuild at byte %d (local=%s full=%s)", diff, local.Fingerprint[:12], full.Fingerprint[:12]),
		}
	}
	return nil
}

type NumericFault struct {
	Dim  int    `json:"dim"`
	Kind string `json:"kind"` // nan or inf
}

type ScoreRequest struct {
	RequestID    string            `json:"request_id"`
	Text         string            `json:"text"`
	Fields       map[string]string `json:"fields,omitempty"`
	ModelVersion int64             `json:"model_version,omitempty"`
	Fault        *NumericFault     `json:"fault,omitempty"`
}

type DimBound struct {
	Dim                       int     `json:"dim"`
	UnitID                    int     `json:"unit_id"`
	FeatureValue              int     `json:"feature_value"`
	Scale                     int64   `json:"scale"`
	PathAmplification         float64 `json:"path_amplification"`
	UpdateCount               int64   `json:"update_count"`
	FixedContribution         float64 `json:"fixed_contribution"`
	ReferenceContribution     string  `json:"reference_contribution_rational"`
	HighPrecisionContribution string  `json:"reference_contribution_hp"`
	ObservedError             float64 `json:"observed_error"`
	Bound                     float64 `json:"bound"`
}

type ScoreResult struct {
	RequestID            string     `json:"request_id"`
	CacheKey             string     `json:"cache_key"`
	CacheHit             bool       `json:"cache_hit"`
	ModelVersion         int64      `json:"model_version"`
	VersionFingerprint   string     `json:"version_fingerprint"`
	ScoreQ               int64      `json:"score_q"`
	Score                float64    `json:"score"`
	HighPrecisionScore   string     `json:"high_precision_score_rational"`
	HighPrecisionScoreHP string     `json:"high_precision_score_hp"`
	ObservedError        float64    `json:"observed_error"`
	TotalBound           float64    `json:"total_bound"`
	DimBounds            []DimBound `json:"dim_bounds"`
}

func finiteNumericError(requestID string, dim int, value float64) error {
	if math.IsNaN(value) {
		d := dim
		return &Error{Code: CodeNumeric, RequestID: requestID, Dim: &d, Message: fmt.Sprintf("NaN detected for request %s at dimension %d", requestID, dim)}
	}
	if math.IsInf(value, 0) {
		d := dim
		return &Error{Code: CodeNumeric, RequestID: requestID, Dim: &d, Message: fmt.Sprintf("Inf detected for request %s at dimension %d", requestID, dim)}
	}
	return nil
}

// ScoreCheckpoint computes fixed-point score and per-dimension error bounds.
func ScoreCheckpoint(cp *Checkpoint, req ScoreRequest) (*ScoreResult, error) {
	if cp == nil {
		return nil, newError(CodeNotFound, "checkpoint is not available")
	}
	if req.RequestID == "" {
		req.RequestID = "req-" + DigestBytes([]byte(req.Text))[:16]
	}
	if req.Fault != nil {
		dim := req.Fault.Dim
		if dim < 0 || dim >= cp.Config.Dim {
			return nil, &Error{Code: CodeFeatureDimInvalid, RequestID: req.RequestID, Dim: &dim, Message: "numeric fault dimension is outside hash space"}
		}
		bad := math.NaN()
		if req.Fault.Kind == "inf" || req.Fault.Kind == "+inf" {
			bad = math.Inf(1)
		} else if req.Fault.Kind == "-inf" {
			bad = math.Inf(-1)
		}
		if err := finiteNumericError(req.RequestID, dim, bad); err != nil {
			return nil, err
		}
	}

	in := SampleInput{ID: req.RequestID, Text: req.Text, Label: 0, Fields: req.Fields}
	fv, err := Featurize(in, cp.Config)
	if err != nil {
		return nil, err
	}
	if err := ValidateFeatureVector(fv, cp.Config); err != nil {
		return nil, err
	}

	result := &ScoreResult{
		RequestID:          req.RequestID,
		ModelVersion:       cp.ModelVersion,
		VersionFingerprint: cp.Fingerprint,
		DimBounds:          make([]DimBound, 0, len(fv.Features)),
	}
	refScore := new(big.Rat)
	scoreQ := int64(0)
	totalBound := 0.0

	for _, f := range fv.Features {
		ds, err := cp.DimState(f.Dim)
		if err != nil {
			return nil, err
		}
		refW, ok := new(big.Rat).SetString(ds.RefWeight)
		if !ok {
			d := f.Dim
			return nil, &Error{Code: CodeNumeric, RequestID: req.RequestID, Dim: &d, Message: "invalid high precision weight"}
		}
		refContrib := new(big.Rat).Mul(refW, big.NewRat(int64(f.Value), 1))
		refScore.Add(refScore, refContrib)

		scaled := new(big.Rat).Mul(big.NewRat(ds.Weight, 1), big.NewRat(cp.Config.ScoreScale, ds.Scale))
		q := truncRat(scaled)
		termQ := q * int64(f.Value)
		scoreQ += termQ
		fixedRat := big.NewRat(termQ, cp.Config.ScoreScale)

		errRat := new(big.Rat).Abs(new(big.Rat).Sub(fixedRat, refContrib))
		observed, _ := new(big.Float).SetPrec(256).SetRat(errRat).Float64()
		paramBound := math.Abs(float64(f.Value)) * float64(ds.UpdateCount) / float64(ds.Scale)
		rescaleBound := math.Abs(float64(f.Value)) / float64(cp.Config.ScoreScale)
		bound := paramBound + rescaleBound + 1e-12
		amp := float64(cp.Config.BaseScale) / float64(ds.Scale)
		hp := new(big.Float).SetPrec(256).SetRat(refContrib).Text('f', 12)
		fixedContrib, _ := new(big.Float).SetPrec(256).SetRat(fixedRat).Float64()

		db := DimBound{
			Dim:                       f.Dim,
			UnitID:                    ds.UnitID,
			FeatureValue:              f.Value,
			Scale:                     ds.Scale,
			PathAmplification:         amp,
			UpdateCount:               ds.UpdateCount,
			FixedContribution:         fixedContrib,
			ReferenceContribution:     refContrib.String(),
			HighPrecisionContribution: hp,
			ObservedError:             observed,
			Bound:                     bound,
		}
		if err := finiteNumericError(req.RequestID, f.Dim, db.FixedContribution); err != nil {
			return nil, err
		}
		if err := finiteNumericError(req.RequestID, f.Dim, observed); err != nil {
			return nil, err
		}
		if err := finiteNumericError(req.RequestID, f.Dim, bound); err != nil {
			return nil, err
		}
		result.DimBounds = append(result.DimBounds, db)
		totalBound += bound
	}

	result.ScoreQ = scoreQ
	result.Score = float64(scoreQ) / float64(cp.Config.ScoreScale)
	result.HighPrecisionScore = refScore.String()
	result.HighPrecisionScoreHP = new(big.Float).SetPrec(256).SetRat(refScore).Text('f', 12)
	totalErrRat := new(big.Rat).Sub(big.NewRat(scoreQ, cp.Config.ScoreScale), refScore)
	totalErrRat.Abs(totalErrRat)
	result.ObservedError, _ = new(big.Float).SetPrec(256).SetRat(totalErrRat).Float64()
	result.TotalBound = totalBound + 1e-10

	if err := finiteNumericError(req.RequestID, -1, result.Score); err != nil {
		return nil, err
	}
	if result.ObservedError > result.TotalBound {
		// This should be impossible; keep the numeric contract explicit if it happens.
		dim := 0
		return nil, &Error{Code: CodeNumeric, RequestID: req.RequestID, Dim: &dim, Message: fmt.Sprintf("error bound violation observed=%.12g bound=%.12g", result.ObservedError, result.TotalBound)}
	}

	keyPayload, err := CanonicalJSON(struct {
		Text     string            `json:"text"`
		Fields   map[string]string `json:"fields"`
		Config   string            `json:"config_digest"`
		Version  string            `json:"version_fingerprint"`
		Model    int64             `json:"model_version"`
		Features []Feature         `json:"features"`
	}{NormalizeText(req.Text), req.Fields, cp.ConfigDigest, cp.Fingerprint, cp.ModelVersion, fv.Features})
	if err != nil {
		return nil, err
	}
	result.CacheKey = DigestBytes(keyPayload)
	return result, nil
}

type ParamInjection struct {
	Kind      string       `json:"kind"`
	RequestID string       `json:"request_id,omitempty"`
	Dim       int          `json:"dim,omitempty"`
	Sample    *SampleInput `json:"sample,omitempty"`
	Config    *Config      `json:"config,omitempty"`
}

// InjectParam is the stable fault/parameter injection entry point.
func InjectParam(inj ParamInjection) error {
	switch inj.Kind {
	case "hash_dim_zero":
		cfg := DefaultConfig()
		cfg.Dim = 0
		return cfg.Validate()
	case "illegal_feature_dim":
		dim := inj.Dim
		cfg := DefaultConfig()
		if dim >= 0 && dim < cfg.Dim {
			dim = cfg.Dim + max(1, dim)
		}
		if dim == 0 {
			dim = cfg.Dim + 1
		}
		return ValidateFeatureVector(&FeatureVector{Dim: cfg.Dim, Features: []Feature{{Dim: dim, Value: 1}}}, cfg)
	case "label_non_binary":
		s := SampleInput{ID: "inject-label", Text: "注入标签", Label: 7}
		if inj.Sample != nil {
			s = *inj.Sample
			if s.Label == 0 || s.Label == 1 {
				s.Label = 7
			}
		}
		return ValidateSample(s)
	case "learning_rate_non_positive":
		cfg := DefaultConfig()
		cfg.LRNum = 0
		if inj.Config != nil {
			cfg = *inj.Config
			cfg.LRNum = 0
		}
		return cfg.Validate()
	case "numeric_nan", "numeric_inf":
		dim := inj.Dim
		if dim <= 0 {
			dim = 1
		}
		reqID := inj.RequestID
		if reqID == "" {
			reqID = "inject-numeric"
		}
		v := math.NaN()
		if inj.Kind == "numeric_inf" {
			v = math.Inf(1)
		}
		return finiteNumericError(reqID, dim, v)
	default:
		return newError(CodeConfigInvalid, "unknown injection kind %q", inj.Kind)
	}
}
