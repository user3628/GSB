// Package appcli exposes the workbench through a deterministic local service
// used by both the HTTP server and the command line client.
package appcli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"headprune/internal/generate"
	"headprune/internal/model"
	"headprune/internal/pipeline"
	"headprune/internal/store"
)

// Service holds the workspace state and guards concurrent mutations.
type Service struct {
	mu      sync.Mutex
	dir     string
	store   *store.Store
	dataset model.Dataset
	last    *pipeline.StageResult
	stopped bool
}

// New opens the workspace, database and any previously generated inputs.
func New(dir string) (*Service, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	st, err := store.Open(dir)
	if err != nil {
		return nil, err
	}
	s := &Service{dir: dir, store: st}
	if err := s.loadDataset(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) Close() error { return s.store.Close() }

func (s *Service) loadDataset() error {
	ds := model.Dataset{}
	if err := model.ReadJSON(filepath.Join(s.dir, "heads.json"), &ds.Heads); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := model.ReadJSON(filepath.Join(s.dir, "sentences.json"), &ds.Sentences); err != nil {
		return err
	}
	if err := model.ReadJSON(filepath.Join(s.dir, "policy.json"), &ds.Policy); err != nil {
		return err
	}
	s.dataset = ds
	return nil
}

// loadLastResult restores the most recent pipeline result across processes.
func (s *Service) loadLastResult() error {
	var res pipeline.StageResult
	if err := model.ReadJSON(filepath.Join(s.dir, "last-result.json"), &res); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	s.last = &res
	return nil
}

func (s *Service) ensureResult() (*pipeline.StageResult, error) {
	if s.last != nil {
		return s.last, nil
	}
	if err := s.loadLastResult(); err != nil {
		return nil, err
	}
	if s.last == nil {
		return nil, &model.BizError{Code: model.CodeConflict, Message: "no pipeline result available, run the pipeline first"}
	}
	return s.last, nil
}

// Generate runs the fixed-seed generator and writes the three input files.
func (s *Service) Generate() (generate.Inputs, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := generate.Build()
	if err := model.WriteJSON(filepath.Join(s.dir, "heads.json"), in.Dataset.Heads); err != nil {
		return in, err
	}
	if err := model.WriteJSON(filepath.Join(s.dir, "sentences.json"), in.Dataset.Sentences); err != nil {
		return in, err
	}
	if err := model.WriteJSON(filepath.Join(s.dir, "policy.json"), in.Dataset.Policy); err != nil {
		return in, err
	}
	s.dataset = in.Dataset
	s.last = nil
	return in, nil
}

// ImportHeads replaces or appends head shards from an external JSON file.
func (s *Service) ImportHeads(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var heads []model.Head
	if err := model.ReadJSON(path, &heads); err != nil {
		return err
	}
	old := s.dataset
	newDS := s.dataset
	newDS.Heads = heads
	verdict, err := pipeline.ClassifyHeads(old, newDS, nil)
	if err != nil {
		return err
	}
	cur, _ := s.store.CurrentVersion()
	if err := s.store.UpsertLineage(verdict.Kind, verdict.ContentHash, cur+1, verdict.Relation, "", cur+1); err != nil {
		return err
	}
	s.dataset.Heads = heads
	s.last = nil
	return model.WriteJSON(filepath.Join(s.dir, "heads.json"), heads)
}

// ImportSentences replaces the masked sentence pair set.
func (s *Service) ImportSentences(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sentences []model.Sentence
	if err := model.ReadJSON(path, &sentences); err != nil {
		return err
	}
	old := s.dataset
	newDS := s.dataset
	newDS.Sentences = sentences
	verdict, err := pipeline.ClassifySentences(old, newDS)
	if err != nil {
		return err
	}
	cur, _ := s.store.CurrentVersion()
	if err := s.store.UpsertLineage(verdict.Kind, verdict.ContentHash, cur+1, verdict.Relation, "", cur+1); err != nil {
		return err
	}
	s.dataset.Sentences = sentences
	s.last = nil
	return model.WriteJSON(filepath.Join(s.dir, "sentences.json"), sentences)
}

// SetPolicy validates and stores a replacement strategy snapshot.
func (s *Service) SetPolicy(p model.Policy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := pipeline.ValidatePolicy(p); err != nil {
		return err
	}
	oldDS := s.dataset
	s.dataset.Policy = p
	s.last = nil
	cur, _ := s.store.CurrentVersion()
	hash, _ := model.Digest(p)
	known := map[string]int64{}
	if rows, err := s.store.Lineage(pipeline.KindPolicy); err == nil {
		for _, r := range rows {
			known[r.ContentHash] = r.Seq
		}
	}
	newDS := oldDS
	newDS.Policy = p
	verdict, err := pipeline.ClassifyPolicy(oldDS, newDS, known)
	if err != nil {
		return err
	}
	if err := s.store.UpsertLineage(pipeline.KindPolicy, hash, p.Version, verdict.Relation, "", cur+1); err != nil {
		return err
	}
	return model.WriteJSON(filepath.Join(s.dir, "policy.json"), p)
}

// RunAll validates and executes the full four-stage pipeline.
func (s *Service) RunAll() (pipeline.StageResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return pipeline.StageResult{}, &model.BizError{Code: model.CodeConflict, Message: "service is stopped, call resume first"}
	}
	wb, err := pipeline.NewWorkbench(s.store, s.dataset)
	if err != nil {
		return pipeline.StageResult{}, err
	}
	res, err := wb.Run()
	if err != nil {
		return pipeline.StageResult{}, err
	}
	s.last = &res
	if err := model.WriteJSON(filepath.Join(s.dir, "last-result.json"), res); err != nil {
		return pipeline.StageResult{}, err
	}
	return res, nil
}

// Commit persists the latest decision as the next monotonic version.
func (s *Service) Commit() (store.VersionRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.ensureResult()
	if err != nil {
		return store.VersionRow{}, err
	}
	wb, err := pipeline.NewWorkbench(s.store, s.dataset)
	if err != nil {
		return store.VersionRow{}, err
	}
	return wb.Commit(*res)
}

// History returns the full version history including rolled-back rows.
func (s *Service) History() ([]store.VersionRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.History()
}

// Rollback restores a prior active version.
func (s *Service) Rollback(target int64) (store.VersionRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Rollback(target, s.dataset.Policy.RollbackGenerations)
}

// RestoreVersion reads and decodes any version, even rolled-back ones.
func (s *Service) RestoreVersion(v int64) (pipeline.Decision, store.VersionRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vr, err := s.store.GetVersion(v)
	if err != nil {
		return pipeline.Decision{}, store.VersionRow{}, err
	}
	d, err := pipeline.Restore(vr)
	return d, vr, err
}

// HeadErrors returns the latest per-head error table.
func (s *Service) HeadErrors() (pipeline.ProbeReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.ensureResult()
	if err != nil {
		return pipeline.ProbeReport{}, err
	}
	return res.Probe, nil
}

// Coupling returns the combination replay and amplification details.
func (s *Service) Coupling() (pipeline.ComboReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.ensureResult()
	if err != nil {
		return pipeline.ComboReport{}, err
	}
	return res.Combos, nil
}

// Quantization returns the per-head quantization table.
func (s *Service) Quantization() (pipeline.QuantTable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.ensureResult()
	if err != nil {
		return pipeline.QuantTable{}, err
	}
	return res.Quant, nil
}

// Stop requests a graceful stop; further runs are refused until Resume.
func (s *Service) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	return nil
}

// Resume clears the graceful-stop flag.
func (s *Service) Resume() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = false
	return nil
}

// Lineage returns the detected append/replace/rollback/fork relations.
func (s *Service) Lineage() (map[string][]store.LineageRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]store.LineageRow{}
	for _, kind := range []string{pipeline.KindHeads, pipeline.KindSentences, pipeline.KindPolicy} {
		rows, err := s.store.Lineage(kind)
		if err != nil {
			return nil, err
		}
		out[kind] = rows
	}
	return out, nil
}

// Status summarizes workspace state for health checks.
func (s *Service) Status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, _ := s.store.CurrentVersion()
	active, _ := s.store.ActiveVersion()
	return map[string]any{
		"workspace":       s.dir,
		"heads":           len(s.dataset.Heads),
		"sentences":       len(s.dataset.Sentences),
		"policy_version":  s.dataset.Policy.Version,
		"current_version": cur,
		"active_version":  active,
		"stopped":         s.stopped,
		"has_result":      s.last != nil,
	}
}

// WriteArtifact dumps canonical JSON under the workspace artifacts folder.
func WriteArtifact(dir, name string, v any) (string, error) {
	path := filepath.Join(dir, name)
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(b, '\n'), 0o644)
}

// EnsureService is a small constructor used by CLI subcommands.
func EnsureService(workspace string) (*Service, error) {
	if workspace == "" {
		return nil, fmt.Errorf("workspace directory is required")
	}
	return New(workspace)
}
