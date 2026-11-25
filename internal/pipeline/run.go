package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	"time"

	"ocrlab/internal/apperr"
	"ocrlab/internal/canonical"
	"ocrlab/internal/engine"
	"ocrlab/internal/model"
	"ocrlab/internal/store"
)

type RunOptions struct {
	Groups         []string
	Rebuild        bool
	StopAfterUnits int
	Progress       func(Progress)
}

type Progress struct {
	ImageID string `json:"image_id"`
	Stage   string `json:"stage"`
	Reused  bool   `json:"reused"`
	Seq     int64  `json:"seq,omitempty"`
}

type StageSummary struct {
	Stage    string `json:"stage"`
	Executed int    `json:"executed"`
	Reused   int    `json:"reused"`
}

type RuntimeIssue struct {
	Code           string `json:"code"`
	Stage          string `json:"stage"`
	ImageID        string `json:"image_id"`
	BoxIndex       int    `json:"box_index,omitempty"`
	CandidateIndex int    `json:"candidate_index,omitempty"`
	Message        string `json:"message"`
}

type RunSummary struct {
	ManifestDigest string         `json:"manifest_digest"`
	ConfigVersion  string         `json:"config_version"`
	Stopped        bool           `json:"stopped"`
	StopReason     string         `json:"stop_reason,omitempty"`
	ImagesComplete int            `json:"images_complete"`
	UnitsExecuted  int            `json:"units_executed"`
	UnitsReused    int            `json:"units_reused"`
	Stages         []StageSummary `json:"stages"`
	Issues         []RuntimeIssue `json:"issues"`
}

type runState struct {
	executed  int
	reused    int
	stopAfter int
	counts    map[string]*StageSummary
	issues    []RuntimeIssue
	stop      bool
	reason    string
}

func newRunState(stopAfter int) *runState {
	state := &runState{stopAfter: stopAfter, counts: make(map[string]*StageSummary)}
	for _, stage := range []string{model.StageDetect, model.StageRecognize, model.StageStrategy, model.StageDiagnostic} {
		state.counts[stage] = &StageSummary{Stage: stage}
	}
	return state
}

func (s *runState) beforeUnit(ctx context.Context) bool {
	if s.stop {
		return true
	}
	if err := ctx.Err(); err != nil {
		s.stop = true
		s.reason = "signal"
		return true
	}
	if s.stopAfter > 0 && s.executed >= s.stopAfter {
		s.stop = true
		s.reason = "unit_limit"
		return true
	}
	return false
}

func (s *runState) account(stage string, reused bool, seq int64) {
	entry := s.counts[stage]
	if reused {
		s.reused++
		entry.Reused++
	} else {
		s.executed++
		entry.Executed++
	}
}

func (s *runState) summary(pipeline *Pipeline, imagesComplete int) RunSummary {
	stages := make([]StageSummary, 0, 4)
	for _, stage := range []string{model.StageDetect, model.StageRecognize, model.StageStrategy, model.StageDiagnostic} {
		stages = append(stages, *s.counts[stage])
	}
	return RunSummary{
		ManifestDigest: pipeline.manifestDigest, ConfigVersion: pipeline.cfg.Version(),
		Stopped: s.stop, StopReason: s.reason, ImagesComplete: imagesComplete,
		UnitsExecuted: s.executed, UnitsReused: s.reused, Stages: stages,
		Issues: append([]RuntimeIssue(nil), s.issues...),
	}
}

func (p *Pipeline) Run(ctx context.Context, options RunOptions) (RunSummary, error) {
	if options.StopAfterUnits < 0 {
		return RunSummary{}, apperr.New(apperr.CodeInvalidArgument, "stop_after_units cannot be negative")
	}
	if options.Rebuild {
		if err := p.store.ClearOutputs(ctx); err != nil {
			return RunSummary{}, apperr.Wrap(apperr.CodeStorage, "clear outputs", err)
		}
	}
	records, err := p.OrderedImageRecords(ctx, options.Groups)
	if err != nil {
		return RunSummary{}, err
	}
	state := newRunState(options.StopAfterUnits)
	completed := 0
	for _, record := range records {
		if state.beforeUnit(ctx) {
			break
		}
		stopped, err := p.processImage(ctx, record, state, options.Progress)
		if err != nil {
			return state.summary(p, completed), err
		}
		if stopped {
			break
		}
		completed++
	}
	return state.summary(p, completed), nil
}

func (p *Pipeline) processImage(ctx context.Context, record store.ImageRecord, state *runState, progress func(Progress)) (bool, error) {
	group, err := p.GroupForImage(record.ImageID)
	if err != nil {
		return false, err
	}
	groupClass := engine.GroupClass(p.cfg.Strategy, group.Observed.ShardCount)
	detSelection := engine.SelectionInput{
		GroupClass: groupClass, ShardCount: group.Observed.ShardCount,
		TotalCharacters: group.Observed.TotalCharacters, ExpectedBoxes: len(record.Spec.Truth),
		BudgetUnits: p.cfg.ComputeBudgetUnits,
	}
	detDecision := engine.ChooseDetection(p.cfg.Strategy, p.cfg.ComputeBudgetUnits, detSelection)
	detInputVersion, detConfigVersion, err := p.detectionVersions(record, detDecision.Chosen)
	if err != nil {
		return false, err
	}

	var img image.Image
	loadImage := func() (image.Image, error) {
		if img != nil {
			return img, nil
		}
		path, err := p.paths.ImagePath(record.File)
		if err != nil {
			return nil, err
		}
		decoded, err := engine.LoadPNG(path)
		if err != nil {
			return nil, apperr.Wrap(apperr.CodeStorage, "load PNG", err)
		}
		bounds := decoded.Bounds()
		if bounds.Dx() != record.Width || bounds.Dy() != record.Height {
			return nil, apperr.WithImage(apperr.CodeInvalidArgument, fmt.Sprintf("PNG dimensions %dx%d do not match manifest %dx%d", bounds.Dx(), bounds.Dy(), record.Width, record.Height), record.ImageID)
		}
		img = decoded
		return img, nil
	}

	if state.beforeUnit(ctx) {
		return true, nil
	}
	detection, detectionDigest, detectionReused, err := p.ensureDetection(ctx, record, detInputVersion, detConfigVersion, detDecision.Chosen, state, loadImage)
	if err != nil {
		return false, err
	}
	state.account(model.StageDetect, detectionReused, 0)
	p.emit(progress, record.ImageID, model.StageDetect, detectionReused)

	if state.beforeUnit(ctx) {
		return true, nil
	}
	recognition, recognitionDigest, recognitionReused, err := p.ensureRecognition(ctx, record, group, groupClass, detection, detectionDigest, state, loadImage)
	if err != nil {
		return false, err
	}
	state.account(model.StageRecognize, recognitionReused, 0)
	p.emit(progress, record.ImageID, model.StageRecognize, recognitionReused)

	if state.beforeUnit(ctx) {
		return true, nil
	}
	recSelection := engine.SelectionInput{
		GroupClass: groupClass, ShardCount: group.Observed.ShardCount,
		TotalCharacters: group.Observed.TotalCharacters, ExpectedBoxes: len(detection.Candidates),
		BudgetUnits: p.cfg.ComputeBudgetUnits,
	}
	recDecision := engine.ChooseRecognition(p.cfg.Strategy, p.cfg.ComputeBudgetUnits, recSelection)
	strategyProduct, strategyDigest, strategyReused, err := p.ensureStrategy(ctx, record, group, groupClass, detection, detectionDigest, recognition, recognitionDigest, detDecision, recDecision)
	if err != nil {
		return false, err
	}
	state.account(model.StageStrategy, strategyReused, 0)
	p.emit(progress, record.ImageID, model.StageStrategy, strategyReused)

	if state.beforeUnit(ctx) {
		return true, nil
	}
	_, diagnosticReused, err := p.ensureDiagnostic(ctx, record, detection, detectionDigest, recognition, recognitionDigest, strategyProduct, strategyDigest)
	if err != nil {
		return false, err
	}
	state.account(model.StageDiagnostic, diagnosticReused, 0)
	p.emit(progress, record.ImageID, model.StageDiagnostic, diagnosticReused)
	return false, nil
}

func (p *Pipeline) emit(progress func(Progress), imageID, stage string, reused bool) {
	if progress != nil {
		progress(Progress{ImageID: imageID, Stage: stage, Reused: reused})
	}
}

type detectionVersionInput struct {
	Schema       string      `json:"schema"`
	Engine       string      `json:"engine"`
	Content      string      `json:"content_sha256"`
	Detection    interface{} `json:"detection"`
	Strategy     string      `json:"strategy"`
	ProbeInvalid bool        `json:"probe_invalid_box"`
}

func (p *Pipeline) detectionVersions(record store.ImageRecord, strategy string) (string, string, error) {
	input := detectionVersionInput{
		Schema: model.SchemaVersion, Engine: model.EngineVersion, Content: record.ContentSHA256,
		Detection: p.cfg.Detection, Strategy: strategy, ProbeInvalid: p.cfg.Diagnostics.ProbeInvalidBox,
	}
	digest, err := canonical.Digest(input)
	if err != nil {
		return "", "", err
	}
	configVersion, err := stageConfigVersion("detcfg", struct {
		Detection    interface{} `json:"detection"`
		Strategy     string      `json:"strategy"`
		ProbeInvalid bool        `json:"probe_invalid_box"`
	}{Detection: p.cfg.Detection, Strategy: strategy, ProbeInvalid: p.cfg.Diagnostics.ProbeInvalidBox})
	return digest, configVersion, err
}

func (p *Pipeline) ensureDetection(ctx context.Context, record store.ImageRecord, inputVersion, configVersion, strategy string, state *runState, loadImage func() (image.Image, error)) (model.DetectionProduct, string, bool, error) {
	unitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	var existing model.DetectionProduct
	reused, err := p.reuseOrInvalidate(unitCtx, record.ImageID, model.StageDetect, inputVersion, &existing)
	if err != nil {
		return model.DetectionProduct{}, "", false, err
	}
	if reused {
		p.collectDetectionIssues(state, existing)
		return existing, p.productDigest(existing), true, nil
	}
	img, err := loadImage()
	if err != nil {
		return model.DetectionProduct{}, "", false, err
	}
	result := engine.Detect(img, record.Spec, p.cfg.Detection.Threshold, p.cfg.Detection.Invert, p.cfg.Detection.MinComponentPixels, p.cfg.Detection.MergeGapRatio, p.cfg.Detection.LineCenterTolerance, strategy, p.cfg.Diagnostics.ProbeInvalidBox)
	product := model.DetectionProduct{
		SchemaVersion: model.SchemaVersion, ImageID: record.ImageID, Group: record.Group,
		ContentSHA256: record.ContentSHA256, ConfigVersion: configVersion, InputVersion: inputVersion,
		Strategy: strategy, Candidates: result.Candidates, Errors: result.Errors,
		InvalidBoxes: len(result.Errors), CandidateCount: len(result.Candidates),
	}
	digest, concurrentReuse, err := p.publishProduct(unitCtx, record, model.StageDetect, inputVersion, product)
	if err != nil {
		return model.DetectionProduct{}, "", false, err
	}
	if !concurrentReuse {
		p.collectDetectionIssues(state, product)
	}
	return product, digest, concurrentReuse, nil
}

type recognitionVersionInput struct {
	Schema          string      `json:"schema"`
	Engine          string      `json:"engine"`
	Content         string      `json:"content_sha256"`
	DetectionDigest string      `json:"detection_digest"`
	TemplateDigest  string      `json:"template_library_digest"`
	Recognition     interface{} `json:"recognition"`
	Strategy        string      `json:"strategy"`
	ProbeEmpty      bool        `json:"probe_empty_templates"`
}

func (p *Pipeline) ensureRecognition(ctx context.Context, record store.ImageRecord, group model.GroupSpec, groupClass string, detection model.DetectionProduct, detectionDigest string, state *runState, loadImage func() (image.Image, error)) (model.RecognitionProduct, string, bool, error) {
	img, err := loadImage()
	if err != nil {
		return model.RecognitionProduct{}, "", false, err
	}
	binary := engine.Binarize(img, p.cfg.Detection.Threshold, p.cfg.Detection.Invert, detection.Strategy)
	components := engine.Segment(binary, detection.Strategy, p.cfg.Detection.MinComponentPixels)
	library, err := engine.BuildTemplateLibrary(binary, components, record.Spec, p.cfg.Recognition, p.cfg.Diagnostics.ProbeEmptyTemplates)
	if err != nil {
		return model.RecognitionProduct{}, "", false, err
	}
	selection := engine.SelectionInput{
		GroupClass: groupClass, ShardCount: group.Observed.ShardCount,
		TotalCharacters: group.Observed.TotalCharacters, ExpectedBoxes: len(detection.Candidates),
		BudgetUnits: p.cfg.ComputeBudgetUnits,
	}
	decision := engine.ChooseRecognition(p.cfg.Strategy, p.cfg.ComputeBudgetUnits, selection)
	input := recognitionVersionInput{
		Schema: model.SchemaVersion, Engine: model.EngineVersion, Content: record.ContentSHA256,
		DetectionDigest: detectionDigest, TemplateDigest: library.Digest,
		Recognition: p.cfg.Recognition, Strategy: decision.Chosen, ProbeEmpty: p.cfg.Diagnostics.ProbeEmptyTemplates,
	}
	inputVersion, err := canonical.Digest(input)
	if err != nil {
		return model.RecognitionProduct{}, "", false, err
	}
	configVersion, err := stageConfigVersion("reccfg", struct {
		Recognition interface{} `json:"recognition"`
		Strategy    string      `json:"strategy"`
		ProbeEmpty  bool        `json:"probe_empty_templates"`
	}{Recognition: p.cfg.Recognition, Strategy: decision.Chosen, ProbeEmpty: p.cfg.Diagnostics.ProbeEmptyTemplates})
	if err != nil {
		return model.RecognitionProduct{}, "", false, err
	}
	unitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	var existing model.RecognitionProduct
	reused, err := p.reuseOrInvalidate(unitCtx, record.ImageID, model.StageRecognize, inputVersion, &existing)
	if err != nil {
		return model.RecognitionProduct{}, "", false, err
	}
	if reused {
		p.collectRecognitionIssues(state, existing)
		return existing, p.productDigest(existing), true, nil
	}
	build := engine.Recognize(binary, components, detection, record.Spec, p.cfg.Recognition, library, decision.Chosen)
	recognized := 0
	for _, result := range build.Results {
		if result.Status == "recognized" {
			recognized++
		}
	}
	product := model.RecognitionProduct{
		SchemaVersion: model.SchemaVersion, ImageID: record.ImageID, Group: record.Group,
		ContentSHA256: record.ContentSHA256, ConfigVersion: configVersion, InputVersion: inputVersion,
		DetectionDigest: detectionDigest, TemplateLibraryDigest: library.Digest, Strategy: decision.Chosen,
		TemplateCount: len(library.Templates), CharacterSet: library.CharSet,
		Results: build.Results, Errors: build.Errors, CandidateCount: len(detection.Candidates), RecognizedCount: recognized,
	}
	digest, concurrentReuse, err := p.publishProduct(unitCtx, record, model.StageRecognize, inputVersion, product)
	if err != nil {
		return model.RecognitionProduct{}, "", false, err
	}
	if !concurrentReuse {
		p.collectRecognitionIssues(state, product)
	}
	return product, digest, concurrentReuse, nil
}

type strategyVersionInput struct {
	Schema              string                     `json:"schema"`
	Engine              string                     `json:"engine"`
	DetectionDigest     string                     `json:"detection_digest"`
	RecognitionDigest   string                     `json:"recognition_digest"`
	DetectionStrategy   string                     `json:"detection_strategy"`
	RecognitionStrategy string                     `json:"recognition_strategy"`
	GroupClass          string                     `json:"group_class"`
	Observations        model.StrategyObservations `json:"observations"`
	Strategy            interface{}                `json:"strategy_config"`
	Budget              int                        `json:"compute_budget_units"`
}

func (p *Pipeline) ensureStrategy(ctx context.Context, record store.ImageRecord, group model.GroupSpec, groupClass string, detection model.DetectionProduct, detectionDigest string, recognition model.RecognitionProduct, recognitionDigest string, detDecision, recDecision model.StrategyDecision) (model.StrategyProduct, string, bool, error) {
	observations := model.StrategyObservations{
		GroupName: group.Name, ShardCount: group.Observed.ShardCount,
		TotalCharacters: group.Observed.TotalCharacters, CandidateCount: len(detection.Candidates),
	}
	input := strategyVersionInput{
		Schema: model.SchemaVersion, Engine: model.EngineVersion,
		DetectionDigest: detectionDigest, RecognitionDigest: recognitionDigest,
		DetectionStrategy: detDecision.Chosen, RecognitionStrategy: recDecision.Chosen,
		GroupClass: groupClass, Observations: observations,
		Strategy: p.cfg.Strategy, Budget: p.cfg.ComputeBudgetUnits,
	}
	inputVersion, err := canonical.Digest(input)
	if err != nil {
		return model.StrategyProduct{}, "", false, err
	}
	configVersion, err := stageConfigVersion("stratcfg", struct {
		Strategy interface{} `json:"strategy_config"`
		Budget   int         `json:"compute_budget_units"`
	}{Strategy: p.cfg.Strategy, Budget: p.cfg.ComputeBudgetUnits})
	if err != nil {
		return model.StrategyProduct{}, "", false, err
	}
	unitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	var existing model.StrategyProduct
	reused, err := p.reuseOrInvalidate(unitCtx, record.ImageID, model.StageStrategy, inputVersion, &existing)
	if err != nil {
		return model.StrategyProduct{}, "", false, err
	}
	if reused {
		return existing, p.productDigest(existing), true, nil
	}
	budgetPerShard := p.cfg.ComputeBudgetUnits / maxInt(1, group.Observed.ShardCount)
	product := model.StrategyProduct{
		SchemaVersion: model.SchemaVersion, ImageID: record.ImageID, Group: record.Group,
		ConfigVersion: configVersion, InputVersion: inputVersion,
		DetectionDigest: detectionDigest, RecognitionDigest: recognitionDigest,
		GroupClass: groupClass, BudgetUnits: p.cfg.ComputeBudgetUnits, BudgetPerShard: budgetPerShard,
		Observations: observations, Detection: detDecision, Recognition: recDecision,
	}
	digest, concurrentReuse, err := p.publishProduct(unitCtx, record, model.StageStrategy, inputVersion, product)
	if err != nil {
		return model.StrategyProduct{}, "", false, err
	}
	return product, digest, concurrentReuse, nil
}

type diagnosticVersionInput struct {
	Schema            string      `json:"schema"`
	Engine            string      `json:"engine"`
	TruthDigest       string      `json:"truth_digest"`
	DetectionDigest   string      `json:"detection_digest"`
	RecognitionDigest string      `json:"recognition_digest"`
	StrategyDigest    string      `json:"strategy_digest"`
	ComputeBudget     int         `json:"compute_budget_units"`
	Diagnostics       interface{} `json:"diagnostics"`
}

func (p *Pipeline) ensureDiagnostic(ctx context.Context, record store.ImageRecord, detection model.DetectionProduct, detectionDigest string, recognition model.RecognitionProduct, recognitionDigest string, strategy model.StrategyProduct, strategyDigest string) (model.ImageDiagnostic, bool, error) {
	input := diagnosticVersionInput{
		Schema: model.SchemaVersion, Engine: model.EngineVersion, TruthDigest: record.TruthDigest,
		DetectionDigest: detectionDigest, RecognitionDigest: recognitionDigest, StrategyDigest: strategyDigest,
		ComputeBudget: p.cfg.ComputeBudgetUnits, Diagnostics: p.cfg.Diagnostics,
	}
	inputVersion, err := canonical.Digest(input)
	if err != nil {
		return model.ImageDiagnostic{}, false, err
	}
	configVersion, err := stageConfigVersion("diagcfg", struct {
		ComputeBudget int         `json:"compute_budget_units"`
		Diagnostics   interface{} `json:"diagnostics"`
	}{ComputeBudget: p.cfg.ComputeBudgetUnits, Diagnostics: p.cfg.Diagnostics})
	if err != nil {
		return model.ImageDiagnostic{}, false, err
	}
	unitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	var existing model.ImageDiagnostic
	reused, err := p.reuseOrInvalidate(unitCtx, record.ImageID, model.StageDiagnostic, inputVersion, &existing)
	if err != nil {
		return model.ImageDiagnostic{}, false, err
	}
	if reused {
		return existing, true, nil
	}
	product := engine.ImageDiagnostic(p.cfg, record.Spec, detection, recognition, strategy, engine.ProductDigests{
		Detection: detectionDigest, Recognition: recognitionDigest, Strategy: strategyDigest,
		Input: inputVersion, ConfigVersion: configVersion,
	})
	_, concurrentReuse, err := p.publishProduct(unitCtx, record, model.StageDiagnostic, inputVersion, product)
	if err != nil {
		return model.ImageDiagnostic{}, false, err
	}
	return product, concurrentReuse, nil
}

func (p *Pipeline) reuseOrInvalidate(ctx context.Context, imageID, stage, inputVersion string, target any) (bool, error) {
	checkpoint, exists, err := p.store.GetCheckpoint(ctx, imageID, stage)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if checkpoint.InputVersion != inputVersion {
		if err := p.store.InvalidateFrom(ctx, imageID, stage, true); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := json.Unmarshal(checkpoint.Product, target); err != nil {
		return false, &apperr.Error{Code: apperr.CodeStaleCheckpoint, Message: "checkpoint product cannot be decoded", ImageID: imageID, Stage: stage, Reason: "product_malformed"}
	}
	return true, nil
}

func (p *Pipeline) publishProduct(ctx context.Context, record store.ImageRecord, stage, inputVersion string, product any) (string, bool, error) {
	productBytes, err := canonical.Marshal(product)
	if err != nil {
		return "", false, err
	}
	productDigest := canonical.DigestBytes(productBytes)
	nextSeq, previousSeq, sequenceToken, err := p.store.NextSequence(ctx, record.ImageID)
	if err != nil {
		return "", false, err
	}
	artifactRel := fmt.Sprintf("state/artifacts/%s/%s/%s.json", record.Group, record.ImageID, stage)
	err = p.store.Publish(ctx, store.PublishRequest{
		ImageID: record.ImageID, Stage: stage, Seq: nextSeq, ExpectedPrevSeq: previousSeq,
		SequenceToken: sequenceToken,
		InputVersion:  inputVersion, ConfigVersion: p.cfg.Version(), ProductDigest: productDigest,
		ArtifactRel: artifactRel, Product: productBytes,
	})
	if err != nil {
		appErr := apperr.From(err)
		if appErr.Code == apperr.CodeStaleCheckpoint {
			checkpoint, exists, loadErr := p.store.GetCheckpoint(ctx, record.ImageID, stage)
			if loadErr == nil && exists && checkpoint.InputVersion == inputVersion && checkpoint.ProductDigest == productDigest {
				return productDigest, true, nil
			}
		}
		return "", false, err
	}
	return productDigest, false, nil
}

func (p *Pipeline) productDigest(product any) string {
	digest, _ := canonical.Digest(product)
	return digest
}

func stageConfigVersion(prefix string, value any) (string, error) {
	digest, err := canonical.Digest(value)
	if err != nil {
		return "", err
	}
	return prefix + "-" + digest[:16], nil
}

func (p *Pipeline) collectDetectionIssues(state *runState, product model.DetectionProduct) {
	for _, issue := range product.Errors {
		state.issues = append(state.issues, RuntimeIssue{
			Code: issue.Code, Stage: model.StageDetect, ImageID: issue.ImageID,
			BoxIndex: issue.BoxIndex, Message: issue.Message,
		})
	}
}

func (p *Pipeline) collectRecognitionIssues(state *runState, product model.RecognitionProduct) {
	for _, issue := range product.Errors {
		state.issues = append(state.issues, RuntimeIssue{
			Code: issue.Code, Stage: model.StageRecognize, ImageID: issue.ImageID,
			CandidateIndex: issue.CandidateIndex, Message: issue.Message,
		})
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
