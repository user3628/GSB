package rewardpack

import (
	"context"
	"encoding/json"
	"sort"
)

// Engine coordinates deterministic builds and publication for one Store.
type Engine struct {
	Store *Store
}

func NewEngine(store *Store) *Engine { return &Engine{Store: store} }

// BuildOptions supports deterministic graceful-stop tests and operational
// cancellation. StopAfterUnits completes the current pair before returning
// E_STOPPED; an unpublished partial stage is never left as a valid checkpoint.
type BuildOptions struct {
	StopAfterUnits int
	OnUnit         func(stage, pairID string)
}

// BuildResult reports which per-pair artifacts were reused or rebuilt.
type BuildResult struct {
	Bundle       ArtifactBundle      `json:"bundle"`
	ChangeKind   string              `json:"change_kind"`
	Invalidation *Invalidation       `json:"invalidation,omitempty"`
	RebuiltPairs map[string][]string `json:"rebuilt_pairs"`
	ReusedPairs  map[string][]string `json:"reused_pairs"`
	StageStates  []StageState        `json:"stage_states"`
}

type stageSummary struct {
	Rebuilt []string
	Reused  []string
}

type buildTracker struct {
	ctx     context.Context
	options BuildOptions
	count   int
}

func (t *buildTracker) checkBefore(stage string) error {
	if t.ctx.Err() != nil {
		return stoppedError(stage, "", "stop requested before stage")
	}
	return nil
}

func (t *buildTracker) completeUnit(stage, pairID string) error {
	t.count++
	if t.options.OnUnit != nil {
		t.options.OnUnit(stage, pairID)
	}
	if t.ctx.Err() != nil {
		return stoppedError(stage, pairID, "stop requested after completed pair unit")
	}
	if t.options.StopAfterUnits > 0 && t.count >= t.options.StopAfterUnits {
		return stoppedError(stage, pairID, "deterministic stop-after-units reached")
	}
	return nil
}

func stoppedError(stage, pairID, reason string) error {
	return &BusinessError{Code: CodeStopped, Stage: stage, PairID: pairID, Context: reason, Message: "build stopped gracefully after a complete pair unit"}
}

// Build runs or resumes the five-stage pipeline under an exclusive OS file lock.
func (e *Engine) Build(ctx context.Context, options BuildOptions) (*BuildResult, error) {
	if e == nil || e.Store == nil {
		return nil, &BusinessError{Code: CodeInternal, Message: "engine has no store"}
	}
	lock, lockErr := acquireFileLock(ctx, e.Store.LockPath())
	if lockErr != nil {
		if ctx.Err() != nil {
			return nil, stoppedError("", "", "stop requested while waiting for publication lock")
		}
		return nil, &BusinessError{Code: CodeLockTimeout, Context: e.Store.LockPath(), Message: lockErr.Error()}
	}
	defer func() { _ = lock.Close() }()

	cfg, err := e.Store.ActiveConfig(ctx)
	if err != nil {
		return nil, err
	}
	annotations, err := e.Store.LoadAnnotations(ctx)
	if err != nil {
		return nil, err
	}
	sources, err := e.Store.LoadSources(ctx)
	if err != nil {
		return nil, err
	}
	currentManifest, err := newManifest(cfg, annotations, sources)
	if err != nil {
		return nil, err
	}
	previousManifest, hasPrevious, err := e.Store.CurrentManifest(ctx)
	if err != nil {
		return nil, err
	}
	var previousPtr *Manifest
	if hasPrevious {
		previousPtr = &previousManifest
	}
	pending, err := e.Store.PendingChanges(ctx)
	if err != nil {
		return nil, err
	}
	historical := false
	if hasPrevious && previousManifest.Digest() != currentManifest.Digest() {
		historical, err = e.Store.ManifestWasCommitted(ctx, currentManifest.Digest())
		if err != nil {
			return nil, err
		}
	}
	change, hasChange, err := analyzeChange(previousPtr, currentManifest, cfg, pending, historical)
	if err != nil {
		if hasChange {
			_ = e.Store.RecordInvalidation(ctx, change)
		}
		return nil, err
	}
	if hasChange {
		if err := e.Store.RecordInvalidation(ctx, change); err != nil {
			return nil, err
		}
	}
	tracker := &buildTracker{ctx: ctx, options: options}
	rebuilt := map[string][]string{}
	reused := map[string][]string{}

	normalized, normalizeState, normalizeSummary, err := runNormalizeStage(ctx, e.Store, cfg, annotations, tracker)
	if err != nil {
		return nil, err
	}
	rebuilt[StageNormalize], reused[StageNormalize] = normalizeSummary.Rebuilt, normalizeSummary.Reused

	scores, scoreState, scoreSummary, err := runScoreStage(ctx, e.Store, cfg, normalized, annotations, normalizeState, tracker)
	if err != nil {
		return nil, err
	}
	rebuilt[StageScore], reused[StageScore] = scoreSummary.Rebuilt, scoreSummary.Reused

	features, featureState, featureSummary, err := runFeatureStage(ctx, e.Store, cfg, scores, scoreState, tracker)
	if err != nil {
		return nil, err
	}
	rebuilt[StageFeatures], reused[StageFeatures] = featureSummary.Rebuilt, featureSummary.Reused

	budget, budgetState, err := runBudgetStage(ctx, e.Store, cfg, scores, features, sources, featureState, tracker)
	if err != nil {
		return nil, err
	}
	rebuilt[StageBudget] = []string{"*"}
	reused[StageBudget] = []string{"*"}

	report, reportState, err := runReportStage(ctx, e.Store, cfg, scores, features, budget, budgetState, tracker)
	if err != nil {
		return nil, err
	}
	rebuilt[StageReport] = []string{"*"}
	reused[StageReport] = []string{"*"}

	if err := e.Store.SaveManifest(ctx, currentManifest); err != nil {
		return nil, err
	}
	kind := ChangeNoop
	var invalidation *Invalidation
	if hasChange {
		kind = change.ChangeKind
		invalidation = &change
	}
	return &BuildResult{
		Bundle:     Bundle(cfg, normalized, scores, features, budget, report),
		ChangeKind: kind, Invalidation: invalidation,
		RebuiltPairs: rebuilt, ReusedPairs: reused,
		StageStates: []StageState{normalizeState, scoreState, featureState, budgetState, reportState},
	}, nil
}

func runNormalizeStage(ctx context.Context, store *Store, cfg Config, annotations []Annotation, tracker *buildTracker) ([]NormalizedPair, StageState, stageSummary, error) {
	if err := tracker.checkBefore(StageNormalize); err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	ordered := append([]Annotation(nil), annotations...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].PairID < ordered[j].PairID })
	existing, err := store.LoadPairArtifacts(ctx, StageNormalize)
	if err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	pairs := make([]NormalizedPair, 0, len(ordered))
	updates := make([]PairArtifact, 0)
	summary := stageSummary{}
	currentIDs := map[string]bool{}
	for _, annotation := range ordered {
		currentIDs[annotation.PairID] = true
		expectedInput := hashBytes([]byte("normalize\x00"), []byte(digestJSON(annotation)), []byte(cfg.NormalizationDigest()))
		if artifact, ok := existing[annotation.PairID]; ok && artifact.InputDigest == expectedInput && digestBytes(artifact.Payload) == artifact.Digest {
			var pair NormalizedPair
			if err := json.Unmarshal(artifact.Payload, &pair); err != nil {
				return nil, StageState{}, stageSummary{}, errorWith(CodeInternal, err)
			}
			pairs = append(pairs, pair)
			summary.Reused = append(summary.Reused, annotation.PairID)
			if err := tracker.completeUnit(StageNormalize, annotation.PairID); err != nil {
				return nil, StageState{}, stageSummary{}, err
			}
			continue
		}
		pair, err := NormalizePair(annotation, cfg)
		if err != nil {
			return nil, StageState{}, stageSummary{}, stageAttach(StageNormalize, err)
		}
		payload, _ := json.Marshal(pair)
		updates = append(updates, PairArtifact{
			Stage: StageNormalize, PairID: pair.PairID, Source: pair.Source,
			InputDigest: pair.InputDigest, Digest: digestBytes(payload), Payload: payload,
		})
		pairs = append(pairs, pair)
		summary.Rebuilt = append(summary.Rebuilt, annotation.PairID)
		if err := tracker.completeUnit(StageNormalize, annotation.PairID); err != nil {
			return nil, StageState{}, stageSummary{}, err
		}
	}
	deletes := stalePairIDs(existing, currentIDs)
	payload, _ := json.Marshal(pairs)
	inputFingerprint := normalizeStageFingerprint(cfg, ordered)
	state, err := publishOrReuse(ctx, store, StageNormalize, inputFingerprint, cfg.Version, payload, updates, deletes, "")
	if err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	return pairs, state, summary, nil
}

func runScoreStage(ctx context.Context, store *Store, cfg Config, normalized []NormalizedPair, annotations []Annotation, previous StageState, tracker *buildTracker) ([]ScoreResult, StageState, stageSummary, error) {
	if err := tracker.checkBefore(StageScore); err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	existing, err := store.LoadPairArtifacts(ctx, StageScore)
	if err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	annByID := annotationMap(annotations)
	scores := make([]ScoreResult, 0, len(normalized))
	updates := make([]PairArtifact, 0)
	summary := stageSummary{}
	currentIDs := map[string]bool{}
	for _, pair := range normalized {
		currentIDs[pair.PairID] = true
		expectedInput := hashBytes([]byte("score\x00"), []byte(pair.Digest), []byte(cfg.ScoringDigest()))
		if artifact, ok := existing[pair.PairID]; ok && artifact.InputDigest == expectedInput && digestBytes(artifact.Payload) == artifact.Digest {
			var score ScoreResult
			if err := json.Unmarshal(artifact.Payload, &score); err != nil {
				return nil, StageState{}, stageSummary{}, errorWith(CodeInternal, err)
			}
			scores = append(scores, score)
			summary.Reused = append(summary.Reused, pair.PairID)
			if err := tracker.completeUnit(StageScore, pair.PairID); err != nil {
				return nil, StageState{}, stageSummary{}, err
			}
			continue
		}
		annotation, ok := annByID[pair.PairID]
		if !ok {
			return nil, StageState{}, stageSummary{}, &BusinessError{Code: CodeMissingArtifact, Stage: StageScore, PairID: pair.PairID, Message: "annotation missing during scoring"}
		}
		score, err := ScorePair(pair, annotation, cfg)
		if err != nil {
			return nil, StageState{}, stageSummary{}, stageAttach(StageScore, err)
		}
		payload, _ := json.Marshal(score)
		updates = append(updates, PairArtifact{
			Stage: StageScore, PairID: score.PairID, Source: score.Source,
			InputDigest: score.InputDigest, Digest: digestBytes(payload), Payload: payload,
		})
		scores = append(scores, score)
		summary.Rebuilt = append(summary.Rebuilt, pair.PairID)
		if err := tracker.completeUnit(StageScore, pair.PairID); err != nil {
			return nil, StageState{}, stageSummary{}, err
		}
	}
	payload, _ := json.Marshal(scores)
	state, err := publishOrReuse(ctx, store, StageScore, scoreStageFingerprint(cfg, normalized), cfg.Version, payload, updates, stalePairIDs(existing, currentIDs), previous.ArtifactDigest)
	if err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	return scores, state, summary, nil
}

func runFeatureStage(ctx context.Context, store *Store, cfg Config, scores []ScoreResult, previous StageState, tracker *buildTracker) ([]FeatureResult, StageState, stageSummary, error) {
	if err := tracker.checkBefore(StageFeatures); err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	existing, err := store.LoadPairArtifacts(ctx, StageFeatures)
	if err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	normalizedArtifacts, err := store.LoadPairArtifacts(ctx, StageNormalize)
	if err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	features := make([]FeatureResult, 0, len(scores))
	updates := make([]PairArtifact, 0)
	summary := stageSummary{}
	currentIDs := map[string]bool{}
	for _, score := range scores {
		currentIDs[score.PairID] = true
		expectedInput := hashBytes([]byte("features\x00"), []byte(score.Digest), []byte(cfg.FeaturesDigest()))
		if artifact, ok := existing[score.PairID]; ok && artifact.InputDigest == expectedInput && digestBytes(artifact.Payload) == artifact.Digest {
			var feature FeatureResult
			if err := json.Unmarshal(artifact.Payload, &feature); err != nil {
				return nil, StageState{}, stageSummary{}, errorWith(CodeInternal, err)
			}
			features = append(features, feature)
			summary.Reused = append(summary.Reused, score.PairID)
			if err := tracker.completeUnit(StageFeatures, score.PairID); err != nil {
				return nil, StageState{}, stageSummary{}, err
			}
			continue
		}
		normalizedArtifact, ok := normalizedArtifacts[score.PairID]
		if !ok {
			return nil, StageState{}, stageSummary{}, &BusinessError{Code: CodeMissingArtifact, Stage: StageNormalize, PairID: score.PairID, Message: "required pair artifact is missing"}
		}
		var pair NormalizedPair
		if err := json.Unmarshal(normalizedArtifact.Payload, &pair); err != nil {
			return nil, StageState{}, stageSummary{}, errorWith(CodeInternal, err)
		}
		feature, err := BuildFeatures(pair, score, cfg)
		if err != nil {
			return nil, StageState{}, stageSummary{}, stageAttach(StageFeatures, err)
		}
		payload, _ := json.Marshal(feature)
		updates = append(updates, PairArtifact{
			Stage: StageFeatures, PairID: feature.PairID, Source: feature.Source,
			InputDigest: feature.InputDigest, Digest: digestBytes(payload), Payload: payload,
		})
		features = append(features, feature)
		summary.Rebuilt = append(summary.Rebuilt, score.PairID)
		if err := tracker.completeUnit(StageFeatures, score.PairID); err != nil {
			return nil, StageState{}, stageSummary{}, err
		}
	}
	payload, _ := json.Marshal(features)
	state, err := publishOrReuse(ctx, store, StageFeatures, featureStageFingerprint(cfg, scores), cfg.Version, payload, updates, stalePairIDs(existing, currentIDs), previous.ArtifactDigest)
	if err != nil {
		return nil, StageState{}, stageSummary{}, err
	}
	return features, state, summary, nil
}

func runBudgetStage(ctx context.Context, store *Store, cfg Config, scores []ScoreResult, features []FeatureResult, sources []SourceRecord, previous StageState, tracker *buildTracker) (BudgetResult, StageState, error) {
	if err := tracker.checkBefore(StageBudget); err != nil {
		return BudgetResult{}, StageState{}, err
	}
	inputFingerprint := budgetInputDigest(scores, cfg)
	state, exists, err := store.StageState(ctx, StageBudget)
	if err != nil {
		return BudgetResult{}, StageState{}, err
	}
	var budget BudgetResult
	if exists && state.InputFingerprint == inputFingerprint {
		if err := json.Unmarshal(state.Payload, &budget); err != nil {
			return BudgetResult{}, StageState{}, errorWith(CodeInternal, err)
		}
		if digestBytes(state.Payload) != state.ArtifactDigest {
			return BudgetResult{}, StageState{}, &BusinessError{Code: CodeInternal, Stage: StageBudget, Message: "budget checkpoint payload digest mismatch"}
		}
	} else {
		budget, err = DecideBudget(scores, cfg, sortedSourceNames(sources))
		if err != nil {
			return BudgetResult{}, StageState{}, stageAttach(StageBudget, err)
		}
		payload, _ := json.Marshal(budget)
		expectedSeq := int64(0)
		if exists {
			expectedSeq = state.Sequence
		}
		if _, err := store.PublishStage(ctx, StagePublication{
			Stage: StageBudget, ExpectedSeq: expectedSeq, InputFingerprint: inputFingerprint,
			ConfigVersion: cfg.Version, ArtifactDigest: digestBytes(payload), Payload: payload,
			PreviousArtifactDigest: previous.ArtifactDigest,
		}); err != nil {
			return BudgetResult{}, StageState{}, stageAttach(StageBudget, err)
		}
		state, _, err = store.StageState(ctx, StageBudget)
		if err != nil {
			return BudgetResult{}, StageState{}, err
		}
	}
	if ctx.Err() != nil {
		return BudgetResult{}, StageState{}, stoppedError(StageBudget, "", "stop requested after indivisible budget unit")
	}
	_ = features
	return budget, state, nil
}

func runReportStage(ctx context.Context, store *Store, cfg Config, scores []ScoreResult, features []FeatureResult, budget BudgetResult, previous StageState, tracker *buildTracker) (Report, StageState, error) {
	if err := tracker.checkBefore(StageReport); err != nil {
		return Report{}, StageState{}, err
	}
	inputFingerprint := reportStageFingerprint(cfg, budget, features)
	state, exists, err := store.StageState(ctx, StageReport)
	if err != nil {
		return Report{}, StageState{}, err
	}
	var report Report
	if exists && state.InputFingerprint == inputFingerprint {
		if err := json.Unmarshal(state.Payload, &report); err != nil {
			return Report{}, StageState{}, errorWith(CodeInternal, err)
		}
		if digestBytes(state.Payload) != state.ArtifactDigest {
			return Report{}, StageState{}, &BusinessError{Code: CodeInternal, Stage: StageReport, Message: "report checkpoint payload digest mismatch"}
		}
	} else {
		report = BuildReport(cfg, scores, features, budget)
		payload, _ := json.Marshal(report)
		expectedSeq := int64(0)
		if exists {
			expectedSeq = state.Sequence
		}
		if _, err := store.PublishStage(ctx, StagePublication{
			Stage: StageReport, ExpectedSeq: expectedSeq, InputFingerprint: inputFingerprint,
			ConfigVersion: cfg.Version, ArtifactDigest: digestBytes(payload), Payload: payload,
			PreviousArtifactDigest: previous.ArtifactDigest,
		}); err != nil {
			return Report{}, StageState{}, stageAttach(StageReport, err)
		}
		state, _, err = store.StageState(ctx, StageReport)
		if err != nil {
			return Report{}, StageState{}, err
		}
	}
	return report, state, nil
}

func publishOrReuse(ctx context.Context, store *Store, stage, inputFingerprint string, configVersion int, payload []byte, updates []PairArtifact, deletes []string, previousArtifactDigest string) (StageState, error) {
	state, exists, err := store.StageState(ctx, stage)
	if err != nil {
		return StageState{}, err
	}
	artifactDigest := digestBytes(payload)
	if exists && state.InputFingerprint == inputFingerprint {
		if state.ArtifactDigest != artifactDigest {
			return StageState{}, &BusinessError{Code: CodeInternal, Stage: stage, Expected: state.ArtifactDigest, Actual: artifactDigest, Context: "same input produced a different artifact digest", Message: "deterministic artifact mismatch"}
		}
		return state, nil
	}
	expectedSeq := int64(0)
	if exists {
		expectedSeq = state.Sequence
	}
	result, err := store.PublishStage(ctx, StagePublication{
		Stage: stage, ExpectedSeq: expectedSeq, InputFingerprint: inputFingerprint,
		ConfigVersion: configVersion, ArtifactDigest: artifactDigest, Payload: payload,
		PreviousArtifactDigest: previousArtifactDigest,
		PairArtifacts:          updates, DeletePairIDs: deletes,
	})
	if err != nil {
		return StageState{}, stageAttach(stage, err)
	}
	return StageState{
		Stage: stage, Sequence: result.Sequence, InputFingerprint: inputFingerprint,
		ConfigVersion: configVersion, ArtifactDigest: artifactDigest, Payload: payload,
	}, nil
}

func normalizeStageFingerprint(cfg Config, annotations []Annotation) string {
	parts := make([]string, 0, len(annotations)+1)
	parts = append(parts, cfg.NormalizationDigest())
	for _, annotation := range annotations {
		parts = append(parts, annotation.PairID+":"+digestJSON(annotation))
	}
	return hashBytes([]byte("normalize-stage\x00"), []byte(CanonicalString(parts)))
}

func scoreStageFingerprint(cfg Config, normalized []NormalizedPair) string {
	parts := make([]string, 0, len(normalized)+1)
	parts = append(parts, cfg.ScoringDigest())
	for _, pair := range normalized {
		parts = append(parts, pair.PairID+":"+pair.Digest)
	}
	return hashBytes([]byte("score-stage\x00"), []byte(CanonicalString(parts)))
}

func featureStageFingerprint(cfg Config, scores []ScoreResult) string {
	parts := make([]string, 0, len(scores)+1)
	parts = append(parts, cfg.FeaturesDigest())
	for _, score := range scores {
		parts = append(parts, score.PairID+":"+score.Digest)
	}
	return hashBytes([]byte("features-stage\x00"), []byte(CanonicalString(parts)))
}

func reportStageFingerprint(cfg Config, budget BudgetResult, features []FeatureResult) string {
	parts := make([]string, 0, len(features)+2)
	parts = append(parts, cfg.Digest(), budget.Digest)
	for _, feature := range features {
		parts = append(parts, feature.PairID+":"+feature.Digest)
	}
	return hashBytes([]byte("report-stage\x00"), []byte(CanonicalString(parts)))
}

func stalePairIDs(existing map[string]PairArtifact, current map[string]bool) []string {
	result := make([]string, 0)
	for pairID := range existing {
		if !current[pairID] {
			result = append(result, pairID)
		}
	}
	sort.Strings(result)
	return result
}

func stageAttach(stage string, err error) error {
	if err == nil {
		return nil
	}
	if be, ok := err.(*BusinessError); ok {
		if be.Stage == "" {
			be.Stage = stage
		}
		return be
	}
	return &BusinessError{Code: CodeInternal, Stage: stage, Message: err.Error()}
}

func annotationMap(annotations []Annotation) map[string]Annotation {
	result := make(map[string]Annotation, len(annotations))
	for _, annotation := range annotations {
		result[annotation.PairID] = annotation
	}
	return result
}

func sortedSourceNames(sources []SourceRecord) []string {
	names := make([]string, len(sources))
	for i, source := range sources {
		names[i] = source.Source
	}
	sort.Strings(names)
	return names
}
