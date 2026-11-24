package cockpitaudio

import "reflect"

type VersionFingerprint struct {
	InputFingerprint       string `json:"input_fingerprint"`
	VADConfigFingerprint   string `json:"vad_config_fingerprint"`
	AttributionFingerprint string `json:"attribution_config_fingerprint"`
	QueryFingerprint       string `json:"query_fingerprint"`
	EditFingerprint        string `json:"edit_fingerprint"`
}

type RecomputePlan struct {
	EarliestStage string             `json:"earliest_stage"`
	ReusedStages  []string           `json:"reused_stages"`
	Recomputed    []string           `json:"recomputed_stages"`
	Fingerprints  VersionFingerprint `json:"fingerprints"`
}

type PipelineCache struct {
	ds     *Dataset
	cfg    Config
	edit   *EditState
	fp     VersionFingerprint
	result *SessionResult
}

func VADConfigFingerprint(cfg Config) string {
	fp, err := Fingerprint(struct {
		SampleRateHz   int     `json:"sample_rate_hz"`
		FrameLengthMS  int     `json:"frame_length_ms"`
		FrameShiftMS   int     `json:"frame_shift_ms"`
		HighThreshold  float64 `json:"high_threshold"`
		LowThreshold   float64 `json:"low_threshold"`
		MinSpeechMS    int     `json:"min_speech_ms"`
		MinSilenceMS   int     `json:"min_silence_ms"`
		MaskIterations int     `json:"mask_iterations"`
	}{cfg.SampleRateHz, cfg.FrameLengthMS, cfg.FrameShiftMS, cfg.HighThreshold, cfg.LowThreshold,
		cfg.MinSpeechMS, cfg.MinSilenceMS, cfg.MaskIterations})
	if err != nil {
		panic(err)
	}
	return fp
}

func AttributionConfigFingerprint(cfg Config) string {
	fp, err := Fingerprint(struct {
		SpeedMPS       float64            `json:"speed_mps"`
		GapThreshold   float64            `json:"gap_threshold"`
		TDOASigmaMS    float64            `json:"tdoa_sigma_ms"`
		PriorityWeight float64            `json:"priority_weight"`
		MaxSpeakers    int                `json:"max_speakers"`
		Geometry       map[string]Point   `json:"geometry"`
		Priorities     map[string]float64 `json:"priorities"`
	}{cfg.SpeedMPS, cfg.AttributionGapThreshold, cfg.TDOASigmaMS, cfg.PriorityWeight,
		cfg.MaxSpeakers, cfg.Geometry, cfg.Priorities})
	if err != nil {
		panic(err)
	}
	return fp
}

func QueryConfigFingerprint(cfg Config) string {
	fp, err := Fingerprint(struct {
		LatencyBudgetMS float64 `json:"latency_budget_ms"`
	}{cfg.LatencyBudgetMS})
	if err != nil {
		panic(err)
	}
	return fp
}

func (e *EditState) Fingerprint() string {
	if e == nil {
		return ""
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	fp, err := Fingerprint(struct {
		Version    int                                `json:"version"`
		Intervals  []TimelineInterval                 `json:"intervals"`
		Priorities map[string]float64                 `json:"priorities"`
		Hits       map[string]map[string]CandidateHit `json:"hits"`
		Log        []EditRecord                       `json:"log"`
	}{e.Version, e.Intervals, e.Priorities, e.Hits, e.Log})
	if err != nil {
		panic(err)
	}
	return fp
}

func ComputeFingerprints(ds *Dataset, cfg Config, edit *EditState) (VersionFingerprint, *Error) {
	inputFP, err := InputFingerprint(ds)
	if err != nil {
		return VersionFingerprint{}, NewError(CodeInternal, StageIngest, "", "input fingerprint: %v", err)
	}
	return VersionFingerprint{
		InputFingerprint:       inputFP,
		VADConfigFingerprint:   VADConfigFingerprint(cfg),
		AttributionFingerprint: AttributionConfigFingerprint(cfg),
		QueryFingerprint:       QueryConfigFingerprint(cfg),
		EditFingerprint:        edit.Fingerprint(),
	}, nil
}

func EarliestStage(oldFP, newFP VersionFingerprint) string {
	if oldFP.InputFingerprint != newFP.InputFingerprint {
		return StageVAD
	}
	if oldFP.VADConfigFingerprint != newFP.VADConfigFingerprint {
		return StageVAD
	}
	if oldFP.AttributionFingerprint != newFP.AttributionFingerprint || oldFP.EditFingerprint != newFP.EditFingerprint {
		return StageAttribution
	}
	if oldFP.QueryFingerprint != newFP.QueryFingerprint {
		return StageQuery
	}
	return ""
}

func NewPipelineCache(ds *Dataset, cfg Config, edit *EditState) (*PipelineCache, *SessionResult, *Error) {
	result, err := BuildSession(ds, cfg, edit)
	if err != nil {
		return nil, nil, err
	}
	fp, ferr := ComputeFingerprints(ds, cfg, edit)
	if ferr != nil {
		return nil, nil, ferr
	}
	if edit == nil {
		edit = NewEditState(collectIntervals(result), cfg.Priorities)
	}
	return &PipelineCache{ds: ds, cfg: cfg.Clone(), edit: edit, fp: fp, result: result}, result, nil
}

func collectIntervals(result *SessionResult) []Interval {
	out := []Interval{}
	if result == nil {
		return out
	}
	for _, s := range result.Shards {
		if s.VAD != nil {
			for _, z := range s.VAD.Zones {
				out = append(out, z.Intervals...)
			}
		}
	}
	return out
}

func (c *PipelineCache) Result() *SessionResult { return c.result }

func (c *PipelineCache) EditState() *EditState { return c.edit }

func (c *PipelineCache) Refresh(cfg Config, edit *EditState) (*SessionResult, RecomputePlan, *Error) {
	if edit == nil {
		edit = c.edit
	}
	newFP, ferr := ComputeFingerprints(c.ds, cfg, edit)
	if ferr != nil {
		return nil, RecomputePlan{}, ferr
	}
	stage := EarliestStage(c.fp, newFP)
	plan := RecomputePlan{EarliestStage: stage, Fingerprints: newFP}
	if stage == "" {
		c.cfg = cfg.Clone()
		c.edit = edit
		return c.result, plan, nil
	}

	resolved, rerr := cfg.ResolveVAD("")
	if rerr != nil {
		return nil, plan, rerr
	}

	switch stage {
	case StageVAD:
		results, baseIntervals, verr := ComputeVADStages(c.ds, cfg)
		if verr != nil {
			return nil, plan, verr
		}
		plan.ReusedStages = []string{StageIngest}
		plan.Recomputed = []string{StageVAD, StageAttribution, StageTimeline, StageQuery}
		result, aerr := AssembleSessionFromVAD(c.ds, cfg, edit, results, newFP.InputFingerprint, resolved, baseIntervals)
		if aerr != nil {
			return nil, plan, aerr
		}
		c.commit(cfg, edit, newFP, result)
		return result, plan, nil
	case StageAttribution:
		results := append([]ShardResult(nil), c.result.Shards...)
		baseIntervals := collectIntervals(c.result)
		plan.ReusedStages = []string{StageVAD}
		plan.Recomputed = []string{StageAttribution, StageTimeline, StageQuery}
		result, aerr := AssembleSessionFromVAD(c.ds, cfg, edit, results, newFP.InputFingerprint, resolved, baseIntervals)
		if aerr != nil {
			return nil, plan, aerr
		}
		c.commit(cfg, edit, newFP, result)
		return result, plan, nil
	default:
		cp := *c.result
		cp.ConfigVersion = resolved.ConfigVersion
		cp.ConfigFingerprint = resolved.ConfigFingerprint
		cp.Summary.ConfigVersion = resolved.ConfigVersion
		cp.Summary.ConfigFingerprint = resolved.ConfigFingerprint
		plan.ReusedStages = []string{StageVAD, StageAttribution, StageTimeline}
		plan.Recomputed = []string{StageQuery}
		c.commit(cfg, edit, newFP, &cp)
		return &cp, plan, nil
	}
}

func (c *PipelineCache) commit(cfg Config, edit *EditState, fp VersionFingerprint, result *SessionResult) {
	c.cfg = cfg.Clone()
	c.edit = edit
	c.fp = fp
	c.result = result
}

func CompareSessionResults(incremental, full *SessionResult) *Error {
	if incremental == nil || full == nil {
		return NewError(CodeIncrementalMismatch, StageTimeline, "", "incremental or full result is nil")
	}
	if diff := firstVADDifference(incremental, full); diff != "" {
		return mismatchError(StageVAD, diff, incremental, full)
	}
	if b, err := CanonicalJSON(incremental.Shards); err == nil {
		if f, _ := CanonicalJSON(full.Shards); string(b) != string(f) {
			if diff := firstAttributionDifference(incremental, full); diff != "" {
				return mismatchError(StageAttribution, diff, incremental, full)
			}
			return mismatchError(StageAttribution, "shards canonical payload differs", incremental, full)
		}
	}
	if !reflect.DeepEqual(incremental.Timeline, full.Timeline) {
		return mismatchError(StageTimeline, firstTimelineDifference(incremental, full), incremental, full)
	}
	if !reflect.DeepEqual(incremental.Summary, full.Summary) {
		return mismatchError(StageTimeline, "summary differs", incremental, full)
	}
	return nil
}

func firstVADDifference(a, b *SessionResult) string {
	if len(a.Shards) != len(b.Shards) {
		return "shard count"
	}
	for i := range a.Shards {
		as, bs := a.Shards[i].VAD, b.Shards[i].VAD
		if as == nil || bs == nil {
			return "vad missing at " + a.Shards[i].ShardID
		}
		for z := 0; z < len(as.Zones) && z < len(bs.Zones); z++ {
			az, bz := as.Zones[z], bs.Zones[z]
			if az.ZoneID != bz.ZoneID {
				return as.ShardID + " zone order " + az.ZoneID + "/" + bz.ZoneID
			}
			if len(az.Intervals) != len(bz.Intervals) {
				return az.ShardID + "/" + az.ZoneID + " interval count"
			}
			for j := range az.Intervals {
				ai, bi := az.Intervals[j], bz.Intervals[j]
				if ai != bi {
					return formatIntervalDiff(az.ShardID+"/"+az.ZoneID, j, ai, bi)
				}
			}
			ab, _ := CanonicalJSON(az)
			bb, _ := CanonicalJSON(bz)
			if string(ab) != string(bb) {
				return az.ShardID + "/" + az.ZoneID + " frame analysis differs"
			}
		}
	}
	return ""
}

func formatIntervalDiff(path string, idx int, a, b Interval) string {
	return path + " interval#" + itoa(idx) +
		" expected=[" + itoa(a.StartMS) + "," + itoa(a.EndMS) + ")" +
		" actual=[" + itoa(b.StartMS) + "," + itoa(b.EndMS) + ")"
}

func firstAttributionDifference(a, b *SessionResult) string {
	for i := range a.Shards {
		aa, bb := a.Shards[i].Attribution, b.Shards[i].Attribution
		if aa == nil || bb == nil || len(aa.Items) != len(bb.Items) {
			return a.Shards[i].ShardID + " attribution count"
		}
		for j := range aa.Items {
			aj, bj := aa.Items[j], bb.Items[j]
			ab, _ := CanonicalJSON(aj)
			bbj, _ := CanonicalJSON(bj)
			if string(ab) != string(bbj) {
				return aj.ShardID + "/" + aj.CommandID
			}
		}
	}
	return ""
}

func firstTimelineDifference(a, b *SessionResult) string {
	if len(a.Timeline.Intervals) != len(b.Timeline.Intervals) {
		return "timeline interval count"
	}
	for i := range a.Timeline.Intervals {
		if a.Timeline.Intervals[i].ID != b.Timeline.Intervals[i].ID || a.Timeline.Intervals[i].Interval != b.Timeline.Intervals[i].Interval {
			in := a.Timeline.Intervals[i]
			return "timeline " + in.ID + " [" + itoa(in.StartMS) + "," + itoa(in.EndMS) + ")"
		}
	}
	return "timeline overlap or marker differs"
}

func mismatchError(stage, diff string, inc, full *SessionResult) *Error {
	incDigest, _ := Fingerprint(inc)
	fullDigest, _ := Fingerprint(full)
	return &Error{
		Code: CodeIncrementalMismatch, Stage: stage, Message: "incremental result does not match full rebuild",
		FirstDifference: diff,
		Diagnostics: map[string]string{
			"incremental_digest": incDigest, "full_digest": fullDigest,
			"first_difference": diff,
		},
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
