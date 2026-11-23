package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"internalpki/workbench/internal/model"
)

const fileFormatVersion = 1

// Snapshot is the durable state for one sample. Version is the global commit
// generation at which this snapshot was written.
type Snapshot struct {
	Path        string              `json:"path"`
	State       string              `json:"state"`
	Version     int64               `json:"version"`
	InputDigest string              `json:"input_digest,omitempty"`
	Result      *model.SampleResult `json:"result,omitempty"`
}

// File is the checkpoint document. CommitLog must be the contiguous sequence
// 1..Generation; any other sequence is treated as E_SNAPSHOT_GAP.
type File struct {
	FormatVersion int        `json:"format_version"`
	Generation    int64      `json:"generation"`
	CommitLog     []int64    `json:"commit_log"`
	Samples       []Snapshot `json:"samples"`
}

type Store struct {
	mu     sync.Mutex
	path   string
	data   File
	byPath map[string]int
}

// Open loads a checkpoint and reconciles it with the current sample set. A
// damaged or gapped checkpoint is not trusted for recovery; the returned
// diagnostic is included in the final report and all samples are redone.
func Open(path string, digests map[string]string) (*Store, []model.Diagnostic, error) {
	store := &Store{path: path}
	var diagnostics []model.Diagnostic
	loaded, exists, err := readFile(path)
	if err != nil {
		diagnostics = append(diagnostics, model.Diagnostic{
			Code:    model.CodeSnapshotInvalid,
			Path:    "checkpoint",
			Message: "checkpoint is unreadable or malformed; recovery disabled",
		})
		store.reset(digests)
		if saveErr := store.saveLocked(); saveErr != nil {
			return nil, nil, saveErr
		}
		return store, diagnostics, nil
	}
	if !exists {
		store.reset(digests)
		if err := store.saveLocked(); err != nil {
			return nil, nil, err
		}
		return store, nil, nil
	}
	if issue := validate(loaded); issue != "" {
		code := model.CodeSnapshotGap
		if loaded.FormatVersion != fileFormatVersion && loaded.FormatVersion != 0 {
			code = model.CodeSnapshotInvalid
		}
		diagnostics = append(diagnostics, model.Diagnostic{
			Code:    code,
			Path:    "checkpoint",
			Message: issue,
		})
		store.reset(digests)
		if err := store.saveLocked(); err != nil {
			return nil, nil, err
		}
		return store, diagnostics, nil
	}

	store.data = loaded
	store.rebuildIndex()
	store.reconcile(digests)
	if err := store.saveLocked(); err != nil {
		return nil, nil, err
	}
	return store, diagnostics, nil
}

func (s *Store) reset(digests map[string]string) {
	s.data = File{
		FormatVersion: fileFormatVersion,
		Generation:    1,
		CommitLog:     []int64{1},
	}
	paths := sortedPaths(digests)
	for _, path := range paths {
		s.data.Samples = append(s.data.Samples, Snapshot{
			Path:        path,
			State:       model.StatePending,
			Version:     1,
			InputDigest: digests[path],
		})
	}
	s.rebuildIndex()
}

func (s *Store) reconcile(digests map[string]string) {
	existing := make(map[string]Snapshot, len(s.data.Samples))
	for _, snapshot := range s.data.Samples {
		existing[snapshot.Path] = snapshot
	}
	s.data.Samples = s.data.Samples[:0]
	for _, path := range sortedPaths(digests) {
		snapshot, ok := existing[path]
		if !ok || snapshot.InputDigest != digests[path] {
			snapshot = Snapshot{
				Path:        path,
				State:       model.StatePending,
				Version:     1,
				InputDigest: digests[path],
			}
		}
		s.data.Samples = append(s.data.Samples, snapshot)
	}
	sort.Slice(s.data.Samples, func(i, j int) bool {
		return s.data.Samples[i].Path < s.data.Samples[j].Path
	})
	s.rebuildIndex()
}

func (s *Store) rebuildIndex() {
	s.byPath = make(map[string]int, len(s.data.Samples))
	for i := range s.data.Samples {
		s.byPath[s.data.Samples[i].Path] = i
	}
}

func validate(file File) string {
	if file.FormatVersion != fileFormatVersion {
		return fmt.Sprintf("unsupported checkpoint format version %d", file.FormatVersion)
	}
	if file.Generation < 1 {
		return fmt.Sprintf("invalid checkpoint generation %d", file.Generation)
	}
	if int64(len(file.CommitLog)) != file.Generation {
		return fmt.Sprintf("commit log length %d does not match generation %d", len(file.CommitLog), file.Generation)
	}
	for i, generation := range file.CommitLog {
		expected := int64(i + 1)
		if generation != expected {
			return fmt.Sprintf("snapshot commit version jump: expected %d, got %d", expected, generation)
		}
	}
	seen := make(map[string]struct{}, len(file.Samples))
	for _, snapshot := range file.Samples {
		if _, duplicate := seen[snapshot.Path]; duplicate {
			return fmt.Sprintf("duplicate snapshot path %q", snapshot.Path)
		}
		seen[snapshot.Path] = struct{}{}
		if snapshot.Version < 1 || snapshot.Version > file.Generation {
			return fmt.Sprintf("snapshot %q has invalid version %d", snapshot.Path, snapshot.Version)
		}
		if !generationExists(file.CommitLog, snapshot.Version) {
			return fmt.Sprintf("snapshot %q version %d is missing from commit log", snapshot.Path, snapshot.Version)
		}
		switch snapshot.State {
		case model.StatePending, model.StateVerifying:
			if snapshot.Version != 1 && snapshot.State == model.StatePending {
				return fmt.Sprintf("pending snapshot %q has noninitial version %d", snapshot.Path, snapshot.Version)
			}
		case model.StateCompleted, model.StateFailed, model.StateBudgetTerminated:
			if snapshot.Result == nil {
				return fmt.Sprintf("terminal snapshot %q has no result", snapshot.Path)
			}
			if snapshot.Version <= 1 {
				return fmt.Sprintf("terminal snapshot %q has nonincremented version %d", snapshot.Path, snapshot.Version)
			}
		default:
			return fmt.Sprintf("snapshot %q has unknown state %q", snapshot.Path, snapshot.State)
		}
	}
	return ""
}

func generationExists(log []int64, generation int64) bool {
	i := sort.Search(len(log), func(i int) bool { return log[i] >= generation })
	return i < len(log) && log[i] == generation
}

// TerminalResults returns only fully committed terminal snapshots whose input
// digest still matches. Pending and verifying work is intentionally redone.
func (s *Store) TerminalResults(digests map[string]string) map[string]model.SampleResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make(map[string]model.SampleResult)
	for _, snapshot := range s.data.Samples {
		if snapshot.Result == nil || digests[snapshot.Path] != snapshot.InputDigest {
			continue
		}
		switch snapshot.State {
		case model.StateCompleted, model.StateFailed, model.StateBudgetTerminated:
			copy := *snapshot.Result
			results[snapshot.Path] = copy
		}
	}
	return results
}

func (s *Store) MarkVerifying(path, digest string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transitionLocked(path, digest, model.StateVerifying, nil)
}

func (s *Store) Commit(path, digest, state string, result model.SampleResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transitionLocked(path, digest, state, &result)
}

func (s *Store) transitionLocked(path, digest, state string, result *model.SampleResult) error {
	index, ok := s.byPath[path]
	if !ok {
		return fmt.Errorf("checkpoint has no snapshot for %q", path)
	}
	oldGeneration := s.data.Generation
	oldLog := append([]int64(nil), s.data.CommitLog...)
	oldSnapshot := s.data.Samples[index]

	s.data.Generation++
	s.data.CommitLog = append(s.data.CommitLog, s.data.Generation)
	s.data.Samples[index] = Snapshot{
		Path:        path,
		State:       state,
		Version:     s.data.Generation,
		InputDigest: digest,
		Result:      result,
	}
	if err := s.saveLocked(); err != nil {
		s.data.Generation = oldGeneration
		s.data.CommitLog = oldLog
		s.data.Samples[index] = oldSnapshot
		return err
	}
	return nil
}

func (s *Store) File() File {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.data
	data.CommitLog = append([]int64(nil), s.data.CommitLog...)
	data.Samples = append([]Snapshot(nil), s.data.Samples...)
	return data
}

func (s *Store) saveLocked() error {
	if s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create checkpoint directory: %w", err)
	}
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	data = append(data, '\n')
	tempPath := s.path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("write checkpoint temp file: %w", err)
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		if runtime.GOOS != "windows" {
			return fmt.Errorf("replace checkpoint: %w", err)
		}
		if removeErr := os.Remove(s.path); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("replace checkpoint on Windows: %w", err)
		}
		if renameErr := os.Rename(tempPath, s.path); renameErr != nil {
			return fmt.Errorf("replace checkpoint on Windows: %w", renameErr)
		}
	}
	return nil
}

func readFile(path string) (File, bool, error) {
	if path == "" {
		return File{}, false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return File{}, false, nil
		}
		return File{}, false, fmt.Errorf("read checkpoint: %w", err)
	}
	var file File
	if err := json.Unmarshal(data, &file); err != nil {
		return File{}, false, fmt.Errorf("decode checkpoint: %w", err)
	}
	return file, true, nil
}

func sortedPaths(digests map[string]string) []string {
	paths := make([]string, 0, len(digests))
	for path := range digests {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}
