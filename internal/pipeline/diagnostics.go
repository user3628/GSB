package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"imretrace/internal/model"
	"imretrace/internal/search"
	"imretrace/internal/store"
)

func (s *Service) WritePolicySnapshot(p model.Policy) error {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	versioned := filepath.Join(s.Paths.ConfigDir, "policy.v"+itoa(p.Version)+".json")
	current := filepath.Join(s.Paths.ConfigDir, "policy.current.json")
	if err := os.WriteFile(versioned, b, 0o644); err != nil {
		return err
	}
	return os.WriteFile(current, b, 0o644)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

type Status struct {
	OK                bool                 `json:"ok"`
	Workdir           string               `json:"workdir"`
	DBPath            string               `json:"db_path"`
	ManifestPath      string               `json:"manifest_path"`
	Writable          bool                 `json:"writable"`
	Policy            *model.Policy        `json:"policy,omitempty"`
	Policies          []model.Policy       `json:"policies"`
	Items             []model.ManifestItem `json:"items"`
	ShardStates       []map[string]any     `json:"shard_states"`
	ActiveCheckpoints []store.Checkpoint   `json:"active_checkpoints"`
	CheckpointCount   int                  `json:"checkpoint_count"`
	StateHash         string               `json:"state_hash"`
}

func (s *Service) Status(ctx context.Context) (*Status, error) {
	if _, err := s.EnsureDemo(ctx); err != nil {
		return nil, err
	}
	policy, err := s.Store.CurrentPolicy(ctx)
	if err != nil {
		return nil, err
	}
	policies, err := s.Store.ListPolicies(ctx)
	if err != nil {
		return nil, err
	}
	items, _, err := s.Store.ListItems(ctx)
	if err != nil {
		return nil, err
	}
	shards, err := s.Store.ShardStates(ctx)
	if err != nil {
		return nil, err
	}
	version := 0
	if policy != nil {
		version = policy.Version
	}
	checkpoints, err := s.Store.ActiveCheckpoints(ctx, version)
	if err != nil {
		return nil, err
	}
	for i := range checkpoints {
		checkpoints[i].ID = 0
	}
	hash, _, err := s.StateHash(ctx)
	if err != nil {
		return nil, err
	}
	writable := s.WritableCheck() == nil
	return &Status{OK: true, Workdir: s.Paths.Root, DBPath: s.Paths.DBPath, ManifestPath: s.Paths.Manifest, Writable: writable, Policy: policy, Policies: policies, Items: items, ShardStates: shards, ActiveCheckpoints: checkpoints, CheckpointCount: len(checkpoints), StateHash: hash}, nil
}

type CanonicalState struct {
	Schema      string               `json:"schema"`
	Policy      *model.Policy        `json:"policy"`
	Policies    []model.Policy       `json:"policies"`
	Items       []model.ManifestItem `json:"items"`
	ItemHashes  map[string]string    `json:"item_hashes"`
	ShardStates []map[string]any     `json:"shard_states"`
	Checkpoints []store.Checkpoint   `json:"checkpoints"`
}

func (s *Service) CanonicalState(ctx context.Context) (*CanonicalState, []byte, error) {
	policy, err := s.Store.CurrentPolicy(ctx)
	if err != nil {
		return nil, nil, err
	}
	policies, err := s.Store.ListPolicies(ctx)
	if err != nil {
		return nil, nil, err
	}
	items, hashes, err := s.Store.ListItems(ctx)
	if err != nil {
		return nil, nil, err
	}
	shards, err := s.Store.ShardStates(ctx)
	if err != nil {
		return nil, nil, err
	}
	version := 0
	if policy != nil {
		version = policy.Version
	}
	checkpoints, err := s.Store.ActiveCheckpoints(ctx, version)
	if err != nil {
		return nil, nil, err
	}
	for i := range checkpoints {
		checkpoints[i].ID = 0
	}
	state := &CanonicalState{Schema: "imretrace-state-v1", Policy: policy, Policies: policies, Items: items, ItemHashes: hashes, ShardStates: shards, Checkpoints: checkpoints}
	b, err := model.CanonicalBytes(state)
	return state, b, err
}

func (s *Service) StateHash(ctx context.Context) (string, []byte, error) {
	_, b, err := s.CanonicalState(ctx)
	if err != nil {
		return "", nil, err
	}
	return model.HashBytes("canonical-state-v1:", b), b, nil
}

type QueryEnvelope struct {
	Result *search.Result  `json:"result,omitempty"`
	Error  *model.BizError `json:"error,omitempty"`
}

type Report struct {
	OK                bool          `json:"ok"`
	Schema            string        `json:"schema"`
	StateHash         string        `json:"state_hash"`
	StateCanonicalSHA string        `json:"state_canonical_sha256"`
	Status            *Status       `json:"status"`
	DemoTextQuery     string        `json:"demo_text_query"`
	DemoText          QueryEnvelope `json:"demo_text"`
	DemoImageItem     string        `json:"demo_image_item"`
	DemoImage         QueryEnvelope `json:"demo_image"`
	VerificationNotes []string      `json:"verification_notes"`
}

func envelope(res *search.Result, err error) QueryEnvelope {
	env := QueryEnvelope{Result: res}
	if err != nil {
		if biz, ok := model.AsBiz(err); ok {
			env.Error = biz
		} else {
			env.Error = model.NewBiz(model.CodeInternal, "report", "", "%v", err)
		}
	}
	return env
}

func (s *Service) ExportReport(ctx context.Context, outPath string) (*Report, error) {
	status, err := s.Status(ctx)
	if err != nil {
		return nil, err
	}
	items, _, err := s.Store.ListItems(ctx)
	if err != nil {
		return nil, err
	}
	textQuery := "red circle 红色圆形 blue gradient"
	textRes, textErr := s.SearchText(ctx, textQuery, 0)
	imageItem := ""
	if len(items) > 0 {
		imageItem = items[0].ItemID
	}
	imageRes, imageErr := s.SearchImage(ctx, imageItem, 0)
	hash, canonical, err := s.StateHash(ctx)
	if err != nil {
		return nil, err
	}
	report := &Report{
		OK: textErr == nil && imageErr == nil, Schema: "imretrace-diagnostic-report-v1",
		StateHash: hash, StateCanonicalSHA: model.HashBytes("sha256:", canonical), Status: status,
		DemoTextQuery: textQuery, DemoText: envelope(textRes, textErr),
		DemoImageItem: imageItem, DemoImage: envelope(imageRes, imageErr),
		VerificationNotes: []string{
			"Error bounds are deterministic conservative bounds from quantized step size and reference-vector saturation distance.",
			"Recovery progress is read only from committed SQLite checkpoints; flock is released automatically by the kernel after SIGKILL.",
			"Policy version rollback is rejected with E_STALE_VERSION; stale sequence publishes are rejected with E_STALE_CHECKPOINT.",
		},
	}
	if outPath == "" {
		outPath = s.Paths.ReportPath
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return nil, err
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(outPath, append(b, '\n'), 0o644); err != nil {
		return nil, err
	}
	return report, nil
}
