package speakerdiar

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// Service runs checkpointed, recoverable pipeline executions in one workspace.
type Service struct {
	WorkDir    string
	ProducerID string
	Manifest   *SessionManifest
	Store      *Store
	Snapshots  *SnapshotStore
	Injection  Injection
	StageDelay time.Duration
	stop       atomic.Bool
}

// ChunkRunReport records reuse/recompute decisions for one chunk.
type ChunkRunReport struct {
	ChunkID            string   `json:"chunk_id"`
	ChunkIndex         int      `json:"chunk_index"`
	RecomputedStages   []string `json:"recomputed_stages"`
	ReusedStages       []string `json:"reused_stages"`
	BaseChunkSequence  int64    `json:"base_chunk_sequence"`
	FinalChunkSequence int64    `json:"final_chunk_sequence"`
}

// RunReport is the recoverable execution result.
type RunReport struct {
	SessionID    string           `json:"session_id"`
	WorkDir      string           `json:"work_dir"`
	ProducerID   string           `json:"producer_id"`
	Chunks       []ChunkRunReport `json:"chunks"`
	GracefulStop bool             `json:"graceful_stop"`
	Artifacts    *ArtifactFiles   `json:"-"`
	Final        *FinalArtifacts  `json:"final,omitempty"`
	Products     []*ChunkProducts `json:"-"`
}

// EnsureWorkspace generates the deterministic sample/manifest on first use.
func EnsureWorkspace(workDir string, cfg Config) (*SessionManifest, error) {
	path := filepath.Join(workDir, "manifest.json")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return GenerateSession(workDir, cfg)
	}
	return LoadManifest(workDir)
}

// NewService opens or creates a deterministic workspace and SQLite store.
func NewService(workDir, producerID string, injection Injection) (*Service, error) {
	cfg := DefaultConfig()
	manifest, err := EnsureWorkspace(workDir, cfg)
	if err != nil {
		return nil, err
	}
	store, err := OpenStore(workDir)
	if err != nil {
		return nil, err
	}
	return &Service{
		WorkDir: workDir, ProducerID: producerID, Manifest: manifest,
		Store: store, Snapshots: NewSnapshotStore(workDir), Injection: injection,
	}, nil
}

// ReloadManifest picks up JSON manifest/config edits made by incremental tests or operators.
func (s *Service) ReloadManifest() error {
	m, err := LoadManifest(s.WorkDir)
	if err != nil {
		return err
	}
	s.Manifest = m
	return nil
}

// Close releases SQLite resources.
func (s *Service) Close() error {
	if s.Store == nil {
		return nil
	}
	return s.Store.Close()
}

// RequestStop asks the runner to stop before the next chunk boundary.
func (s *Service) RequestStop() { s.stop.Store(true) }

// ApplyInjection mutates the loaded session according to one deterministic fault entry.
func ApplyInjection(m *SessionManifest, injection Injection) {
	if m == nil || len(m.Chunks) == 0 {
		return
	}
	switch injection {
	case InjectLengthMismatch:
		m.Chunks[0].DeclaredSampleCount += 7331
	case InjectRateMismatch:
		m.SampleRate = 8000
	case InjectZeroSpeakers:
		m.Chunks[0].SpeakerCount = 0
	case InjectNegativeMaskRounds:
		m.Config.MaskMaxIterations = -1
	case InjectProjectionDimMismatch:
		m.Config.ProjectionInputDim = 999
	case InjectTooManySpeakers:
		m.Chunks[0].SpeakerCount = m.Config.MaxSpeakers + 1
	}
}

// Run processes all chunks in manifest order and writes final artifacts.
func (s *Service) Run() (*RunReport, error) {
	ApplyInjection(s.Manifest, s.Injection)
	report := &RunReport{SessionID: s.Manifest.SessionID, WorkDir: s.WorkDir, ProducerID: s.ProducerID}
	embeddings := make([]*EmbeddingResult, len(s.Manifest.Chunks))
	summaries := make([]*ChunkSummary, len(s.Manifest.Chunks))

	for i := range s.Manifest.Chunks {
		if s.stop.Load() {
			report.GracefulStop = true
			break
		}
		products, chunkReport, err := s.runChunk(i)
		if err != nil {
			s.writeDiagnostic(err)
			return report, err
		}
		report.Chunks = append(report.Chunks, *chunkReport)
		report.Products = append(report.Products, products)
		embeddings[i] = products.Embedding
		summaries[i] = products.Summary
	}
	if report.GracefulStop {
		return report, nil
	}

	tl, err := BuildTimeline(s.Manifest.SessionID, s.Manifest.Chunks, embeddings, summaries, s.Manifest.Config)
	if err != nil {
		s.writeDiagnostic(err)
		return report, err
	}
	files, final, err := BuildArtifactFiles(s.Manifest, embeddings, summaries, tl)
	if err != nil {
		return report, err
	}
	if err := s.writeArtifacts(files); err != nil {
		return report, err
	}
	if err := s.Store.VerifySequenceContinuity(); err != nil {
		return report, err
	}
	report.Artifacts = files
	report.Final = final
	return report, nil
}

func (s *Service) writeArtifacts(files *ArtifactFiles) error {
	dir := filepath.Join(s.WorkDir, "outputs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := AtomicWriteFile(filepath.Join(dir, "segments.json"), files.Segments); err != nil {
		return err
	}
	if err := AtomicWriteFile(filepath.Join(dir, "overlaps.json"), files.Overlaps); err != nil {
		return err
	}
	if err := AtomicWriteFile(filepath.Join(dir, "voiceprints.json"), files.Voiceprints); err != nil {
		return err
	}
	return AtomicWriteFile(filepath.Join(dir, "final.json"), files.Final)
}

func (s *Service) writeDiagnostic(err error) {
	de, ok := AsError(err)
	if !ok {
		return
	}
	dir := filepath.Join(s.WorkDir, "diagnostics")
	name := de.ChunkID
	if name == "" {
		name = "session"
	}
	if de.Stage != "" {
		name += "-" + de.Stage
	}
	name += "-" + de.Code + ".json"
	b, mErr := PrettyJSON(de)
	if mErr == nil {
		_ = AtomicWriteFile(filepath.Join(dir, name), b)
	}
}

func (s *Service) runChunk(index int) (*ChunkProducts, *ChunkRunReport, error) {
	chunk := s.Manifest.Chunks[index]
	cfg := s.Manifest.Config
	if s.StageDelay > 0 {
		time.Sleep(s.StageDelay)
	}
	raw, pcm, err := ReadAndValidateChunk(s.WorkDir, s.Manifest, chunk)
	if err != nil {
		return nil, nil, err
	}
	fps := ComputeFingerprints(chunk, raw, cfg)
	baseHigh, err := s.Store.HighWater(chunk.ChunkID)
	if err != nil {
		return nil, nil, err
	}
	cpFrame, err := s.Store.GetCheckpoint(chunk.ChunkID, StageFrame)
	if err != nil {
		return nil, nil, err
	}
	cpSep, err := s.Store.GetCheckpoint(chunk.ChunkID, StageSeparate)
	if err != nil {
		return nil, nil, err
	}
	cpEmb, err := s.Store.GetCheckpoint(chunk.ChunkID, StageEmbed)
	if err != nil {
		return nil, nil, err
	}
	cpSummary, err := s.Store.GetCheckpoint(chunk.ChunkID, StageSummary)
	if err != nil {
		return nil, nil, err
	}

	report := &ChunkRunReport{ChunkID: chunk.ChunkID, ChunkIndex: index, BaseChunkSequence: baseHigh}
	products := &ChunkProducts{ChunkID: chunk.ChunkID, ChunkIndex: index, Fingerprints: fps}
	expected := baseHigh
	firstCommitInvalidates := true
	delay := func() {
		if s.StageDelay > 0 {
			time.Sleep(s.StageDelay)
		}
	}

	frameValid := !s.injectionForces(StageFrame) && cpFrame != nil && cpFrame.Fingerprint == fps.Frame &&
		s.loadSnapshotInto(cpFrame, StageFrame, &products.Spectral)
	if !frameValid {
		delay()
		products.Spectral, err = RunFrameStage(chunk, pcm, cfg, s.Injection)
		if err != nil {
			return nil, report, err
		}
		if expected, err = s.commitStage(chunk, StageFrame, fps.ChunkContent, fps.Frame, fps.ConfigVersion, products.Spectral, expected, firstCommitInvalidates); err != nil {
			return nil, report, err
		}
		firstCommitInvalidates = false
		report.RecomputedStages = append(report.RecomputedStages, StageFrame)
	} else {
		report.ReusedStages = append(report.ReusedStages, StageFrame)
	}

	sepValid := frameValid && !s.injectionForces(StageSeparate) && cpSep != nil && cpSep.Fingerprint == fps.Separation &&
		s.loadSnapshotInto(cpSep, StageSeparate, &products.Separation)
	if !sepValid {
		delay()
		products.Separation, err = SeparateSources(products.Spectral, chunk.SpeakerCount, cfg, s.Injection)
		if err != nil {
			return nil, report, err
		}
		if expected, err = s.commitStage(chunk, StageSeparate, fps.ChunkContent, fps.Separation, fps.ConfigVersion, products.Separation, expected, firstCommitInvalidates); err != nil {
			return nil, report, err
		}
		firstCommitInvalidates = false
		report.RecomputedStages = append(report.RecomputedStages, StageSeparate)
	} else {
		report.ReusedStages = append(report.ReusedStages, StageSeparate)
	}

	embValid := sepValid && !s.injectionForces(StageEmbed) && cpEmb != nil && cpEmb.Fingerprint == fps.Embedding &&
		s.loadSnapshotInto(cpEmb, StageEmbed, &products.Embedding)
	if !embValid {
		delay()
		products.Embedding, err = RunEmbeddingStage(chunk, products.Spectral, products.Separation, cfg, s.Injection)
		if err != nil {
			return nil, report, err
		}
		if expected, err = s.commitStage(chunk, StageEmbed, fps.ChunkContent, fps.Embedding, fps.ConfigVersion, products.Embedding, expected, firstCommitInvalidates); err != nil {
			return nil, report, err
		}
		firstCommitInvalidates = false
		report.RecomputedStages = append(report.RecomputedStages, StageEmbed)
	} else {
		report.ReusedStages = append(report.ReusedStages, StageEmbed)
	}

	summaryValid := embValid && !s.injectionForces(StageSummary) && cpSummary != nil && cpSummary.Fingerprint == fps.Summary &&
		s.loadSnapshotInto(cpSummary, StageSummary, &products.Summary)
	if !summaryValid {
		delay()
		products.Summary, err = RunSummaryStage(chunk, products.Embedding, products.Separation, cfg)
		if err != nil {
			return nil, report, err
		}
		if expected, err = s.commitStage(chunk, StageSummary, fps.ChunkContent, fps.Summary, fps.ConfigVersion, products.Summary, expected, firstCommitInvalidates); err != nil {
			return nil, report, err
		}
		report.RecomputedStages = append(report.RecomputedStages, StageSummary)
	} else {
		report.ReusedStages = append(report.ReusedStages, StageSummary)
	}

	report.FinalChunkSequence = expected
	return products, report, nil
}

func (s *Service) loadSnapshotInto(cp *Checkpoint, stage string, target any) bool {
	if cp == nil {
		return false
	}
	env, err := s.Snapshots.LoadStagePayload(cp.ChunkID, stage, target)
	if err != nil || env == nil {
		return false
	}
	return env.VersionFingerprint == cp.Fingerprint && env.ArtifactDigest == cp.ArtifactDigest
}

func (s *Service) injectionForces(stage string) bool {
	switch s.Injection {
	case InjectNumericSpectrum:
		return stage == StageFrame
	case InjectZeroMaskedSource, InjectTooManySpeakers, InjectZeroSpeakers, InjectNegativeMaskRounds:
		return stage == StageSeparate
	case InjectEmbeddingTie, InjectProjectionDimMismatch:
		return stage == StageEmbed
	default:
		return false
	}
}

func (s *Service) commitStage(chunk ChunkManifest, stage, contentFingerprint, fingerprint, configVersion string, payload any, expected int64, invalidate bool) (int64, error) {
	env, err := s.Snapshots.SaveStageSnapshot(s.Manifest.SessionID, chunk.ChunkID, stage, fingerprint, configVersion, payload)
	if err != nil {
		return expected, err
	}
	cp, err := s.Store.PublishCheckpoint(PublishRequest{
		SessionID: s.Manifest.SessionID, ProducerID: s.ProducerID,
		ChunkID: chunk.ChunkID, ChunkIndex: chunk.Index, Stage: stage,
		ContentFingerprint: contentFingerprint,
		Fingerprint:        fingerprint, ConfigVersion: configVersion,
		ArtifactDigest: env.ArtifactDigest, ExpectedChunkSequence: expected,
		InvalidateDownstream: invalidate,
	})
	if err != nil {
		return expected, err
	}
	return cp.ChunkSequence, nil
}

// ComputeFullArtifacts runs every stage from WAV without reusing checkpoints or snapshots.
func ComputeFullArtifacts(workDir string, manifest *SessionManifest, injection Injection) ([]*ChunkProducts, *ArtifactFiles, error) {
	embeddings := make([]*EmbeddingResult, len(manifest.Chunks))
	summaries := make([]*ChunkSummary, len(manifest.Chunks))
	products := make([]*ChunkProducts, len(manifest.Chunks))
	for i := range manifest.Chunks {
		chunk := manifest.Chunks[i]
		raw, pcm, err := ReadAndValidateChunk(workDir, manifest, chunk)
		if err != nil {
			return nil, nil, err
		}
		p, _, _, err := runChunkWithAudio(manifest, chunk, raw, pcm, manifest.Config, injection)
		if err != nil {
			return nil, nil, err
		}
		products[i] = p
		embeddings[i] = p.Embedding
		summaries[i] = p.Summary
	}
	tl, err := BuildTimeline(manifest.SessionID, manifest.Chunks, embeddings, summaries, manifest.Config)
	if err != nil {
		return nil, nil, err
	}
	files, _, err := BuildArtifactFiles(manifest, embeddings, summaries, tl)
	if err != nil {
		return nil, nil, err
	}
	return products, files, nil
}

// VerifyIncrementalEqualsFull recomputes a zero-state run and identifies the first divergent chunk/stage.
func VerifyIncrementalEqualsFull(workDir string, manifest *SessionManifest, incremental *RunReport) error {
	fullProducts, fullFiles, err := ComputeFullArtifacts(workDir, manifest, InjectNone)
	if err != nil {
		return err
	}
	for i := range manifest.Chunks {
		if i >= len(incremental.Products) || incremental.Products[i] == nil {
			return incrementalMismatch(manifest.Chunks[i].ChunkID, "missing_report_product", "incremental report is missing chunk product")
		}
		inc := incremental.Products[i]
		full := fullProducts[i]
		pairs := []struct {
			stage string
			a, b  string
		}{
			{StageFrame, inc.Spectral.Digest, full.Spectral.Digest},
			{StageSeparate, inc.Separation.Digest, full.Separation.Digest},
			{StageEmbed, inc.Embedding.Digest, full.Embedding.Digest},
			{StageSummary, inc.Summary.Digest, full.Summary.Digest},
		}
		for _, p := range pairs {
			if p.a != p.b {
				de := incrementalMismatch(manifest.Chunks[i].ChunkID, p.stage, "stage digest differs from full recomputation")
				de.Details["incremental_digest"] = p.a
				de.Details["full_digest"] = p.b
				return de
			}
		}
	}
	if !bytes.Equal(incremental.Artifacts.Segments, fullFiles.Segments) {
		return incrementalMismatch("", StageSummary, "segments.json bytes differ")
	}
	if !bytes.Equal(incremental.Artifacts.Overlaps, fullFiles.Overlaps) {
		return incrementalMismatch("", StageSummary, "overlaps.json bytes differ")
	}
	if !bytes.Equal(incremental.Artifacts.Voiceprints, fullFiles.Voiceprints) {
		return incrementalMismatch("", StageEmbed, "voiceprints.json bytes differ")
	}
	if !bytes.Equal(incremental.Artifacts.Final, fullFiles.Final) {
		return incrementalMismatch("", StageSummary, "final.json bytes differ")
	}
	return nil
}

func incrementalMismatch(chunkID, stage, msg string) *Error {
	de := newError(CodeIncrementalMismatch, msg)
	de.ChunkID = chunkID
	de.Stage = stage
	de.Details = map[string]any{}
	return de
}

// NewIncrementalMismatch exposes the stable mismatch error to integration callers.
func NewIncrementalMismatch(chunkID, stage, msg string) *Error {
	return incrementalMismatch(chunkID, stage, msg)
}

// ReadOutputFile loads one committed final artifact.
func ReadOutputFile(workDir, name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(workDir, "outputs", name))
}
