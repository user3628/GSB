package experiment

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync/atomic"

	"regworkbench/internal/canonical"
	"regworkbench/internal/checkpoint"
	"regworkbench/internal/config"
	"regworkbench/internal/cv"
	"regworkbench/internal/dataset"
	"regworkbench/internal/preprocess"
	"regworkbench/internal/workerr"
)

type Hooks struct {
	AfterCheckpoint func(stage string, fold int, seq int64)
	Round           func(fold int, lambda, l1Ratio float64, iteration int)
}

type Session struct {
	Root      string
	ID        string
	Cfg       config.Config
	ConfigVer string
	DS        *dataset.Dataset
	Prepared  *preprocess.Prepared
	Split     *cv.Split
	Store     *checkpoint.Manager
	Cache     *ResponseCache
	Hooks     Hooks

	snap       atomic.Pointer[Snapshot]
	querySeq   atomic.Uint64
	running    atomic.Bool
	reuseFolds map[int]*FoldArtifact
}

func CreateSession(root string, cfg config.Config, ds *dataset.Dataset, hooks Hooks) (*Session, error) {
	cfg, err := cfg.Normalize()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "input"), 0o755); err != nil {
		return nil, err
	}
	if err := writeDatasetCSV(filepath.Join(root, "input", "input.csv"), ds); err != nil {
		return nil, err
	}
	specBytes, err := canonical.JSON(ds.Spec)
	if err != nil {
		return nil, err
	}
	if err := canonical.AtomicWriteFile(filepath.Join(root, "input", "dataset_manifest.json"), append(specBytes, '\n'), 0o644); err != nil {
		return nil, err
	}
	cfgBytes, err := canonical.JSON(cfg)
	if err != nil {
		return nil, err
	}
	if err := canonical.AtomicWriteFile(filepath.Join(root, "config.json"), append(cfgBytes, '\n'), 0o644); err != nil {
		return nil, err
	}
	sessionJSON := []byte(fmt.Sprintf(`{"id":%q,"schema":"regworkbench.session/v1"}`+"\n", filepath.Base(root)))
	if err := canonical.AtomicWriteFile(filepath.Join(root, "session.json"), sessionJSON, 0o644); err != nil {
		return nil, err
	}
	return openWithConfig(root, cfg, hooks)
}

func OpenSession(root string, hooks Hooks) (*Session, error) {
	cfgBytes, err := os.ReadFile(filepath.Join(root, "config.json"))
	if err != nil {
		return nil, workerr.Wrap(workerr.ENotFound, err)
	}
	var cfg config.Config
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		return nil, err
	}
	cfg, err = cfg.Normalize()
	if err != nil {
		return nil, err
	}
	return openWithConfig(root, cfg, hooks)
}

func openWithConfig(root string, cfg config.Config, hooks Hooks) (*Session, error) {
	specBytes, err := os.ReadFile(filepath.Join(root, "input", "dataset_manifest.json"))
	if err != nil {
		return nil, workerr.Wrap(workerr.ENotFound, err)
	}
	var spec dataset.DatasetManifest
	if err := json.Unmarshal(specBytes, &spec); err != nil {
		return nil, err
	}
	ds, err := dataset.LoadDatasetCSV(filepath.Join(root, "input", "input.csv"), spec)
	if err != nil {
		return nil, err
	}
	prepared, err := preprocess.Prepare(ds, cfg)
	if err != nil {
		return nil, err
	}
	ver, err := cfg.Version()
	if err != nil {
		return nil, err
	}
	split, err := cv.Stratified(prepared.Y, cfg.KFolds, cfg.Seed, ds.RawSHA, ver)
	if err != nil {
		return nil, err
	}
	store, err := checkpoint.OpenManager(root, cfg.KFolds)
	if err != nil {
		return nil, err
	}
	s := &Session{
		Root: root, ID: filepath.Base(root), Cfg: cfg, ConfigVer: ver, DS: ds,
		Prepared: prepared, Split: split, Store: store, Cache: NewResponseCache(), Hooks: hooks,
	}
	s.initializeSnapshot()
	if err := s.restoreCheckpoints(context.Background()); err != nil {
		_ = store.Close()
		return nil, err
	}
	return s, nil
}

func writeDatasetCSV(path string, ds *dataset.Dataset) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write(ds.Header); err != nil {
		return err
	}
	if err := w.WriteAll(ds.Records); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return f.Sync()
}

func (s *Session) Close() error {
	if s.Store != nil {
		return s.Store.Close()
	}
	return nil
}

func (s *Session) initializeSnapshot() {
	snap := &Snapshot{
		SessionID:         s.ID,
		State:             "created",
		Config:            s.Cfg,
		ConfigVersion:     s.ConfigVer,
		SampleFingerprint: s.DS.RawSHA,
		CurrentFold:       -1,
		Folds:             map[int]*FoldArtifact{},
		Fingerprints: map[string]string{
			"sample":     s.DS.RawSHA,
			"config":     s.ConfigVer,
			"preprocess": preprocessFingerprint(s.Prepared),
			"split":      s.Split.Fingerprint,
		},
	}
	s.snap.Store(snap)
}

func preprocessFingerprint(p *preprocess.Prepared) string {
	payload := struct {
		Columns      []preprocess.ColumnStat `json:"columns"`
		FeatureNames []string                `json:"feature_names"`
	}{Columns: p.Columns, FeatureNames: p.FeatureNames}
	_, b, _ := canonical.SHA256JSON(payload)
	return canonical.SHA256Bytes(b)
}

func (s *Session) Snapshot() *Snapshot { return s.snap.Load() }

func (s *Session) update(mutate func(*Snapshot)) *Snapshot {
	for {
		old := s.snap.Load()
		next := cloneSnapshot(old)
		mutate(next)
		if s.snap.CompareAndSwap(old, next) {
			return next
		}
	}
}

func cloneSnapshot(in *Snapshot) *Snapshot {
	if in == nil {
		return &Snapshot{Folds: map[int]*FoldArtifact{}, Fingerprints: map[string]string{}}
	}
	out := *in
	if in.Folds != nil {
		out.Folds = make(map[int]*FoldArtifact, len(in.Folds))
		for k, v := range in.Folds {
			out.Folds[k] = v
		}
	} else {
		out.Folds = map[int]*FoldArtifact{}
	}
	if in.Fingerprints != nil {
		out.Fingerprints = make(map[string]string, len(in.Fingerprints))
		for k, v := range in.Fingerprints {
			out.Fingerprints[k] = v
		}
	}
	out.Config.Encoding = maps.Clone(in.Config.Encoding)
	return &out
}

func noCoefficientDigest() string {
	return canonical.SHA256Bytes([]byte("regworkbench:no-coefficients:v1"))
}
