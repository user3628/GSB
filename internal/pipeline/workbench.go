package pipeline

import (
	"encoding/json"

	"headprune/internal/model"
	"headprune/internal/store"
)

// StageResult is the complete deterministic output of one pipeline run.
type StageResult struct {
	InputDigest string            `json:"input_digest"`
	Quant       QuantTable        `json:"quantization"`
	Probe       ProbeReport       `json:"probe_report"`
	Combos      ComboReport       `json:"combo_report"`
	Decision    Decision          `json:"decision"`
	Reuse       ReusePlan         `json:"reuse_plan"`
	Lineage     []LineageVerdict  `json:"lineage"`
	ReusedRows  []model.HeadID    `json:"reused_rows"`
	cacheKeys   map[string]string `json:"-"`
}

// ProbeDigest hashes the deterministic probe selection.
func ProbeDigest(p ProbeReport) (string, error) { return model.Digest(p.Probes) }

func mustDigest(v any) string {
	d, err := model.Digest(v)
	if err != nil {
		panic(err)
	}
	return d
}

// Workbench orchestrates the four stages on top of a version store.
type Workbench struct {
	Store   *store.Store
	Dataset model.Dataset
}

// NewWorkbench validates inputs before any stage can run.
func NewWorkbench(s *store.Store, ds model.Dataset) (*Workbench, error) {
	if err := ValidateDataset(ds); err != nil {
		return nil, err
	}
	return &Workbench{Store: s, Dataset: ds}, nil
}

// headCacheKey identifies one head error row from the head shard content,
// the sentence set and the policy snapshot. Unchanged heads keep the same key
// across head/sentence appends and can therefore be reused safely.
func (wb *Workbench) headCacheKey(h model.Head) (string, error) {
	hh, err := model.Digest(h)
	if err != nil {
		return "", err
	}
	sh, err := model.Digest(wb.Dataset.Sentences)
	if err != nil {
		return "", err
	}
	ph, err := model.Digest(wb.Dataset.Policy)
	if err != nil {
		return "", err
	}
	return hh + "|" + sh + "|" + ph, nil
}

// Run executes all stages. Cached per-head rows are reused only when their
// cache keys prove the underlying head shard, sentences and policy are
// byte-identical; every reused row cites its source version.
func (wb *Workbench) Run() (StageResult, error) {
	ds := wb.Dataset
	inputDigest, err := model.Digest(ds)
	if err != nil {
		return StageResult{}, err
	}
	quant, err := QuantizeHeads(ds.Heads, ds.Policy.QuantBits)
	if err != nil {
		return StageResult{}, err
	}
	probes, err := SampleProbes(ds.Sentences, ds.Policy)
	if err != nil {
		return StageResult{}, err
	}

	cacheKeys := map[string]string{}
	cachedSource := map[model.HeadID]int64{}
	for _, h := range ds.Heads {
		key, err := wb.headCacheKey(h)
		if err != nil {
			return StageResult{}, err
		}
		cacheKeys[h.ID().String()] = key
		if raw, ver, ok, cerr := wb.Store.CachedHeadError(key, h.ID().String()); cerr == nil && ok {
			var cached HeadErrorRow
			if json.Unmarshal(raw, &cached) == nil {
				cachedSource[h.ID()] = ver
			}
		}
	}

	plan := ReusePlan{Reusable: map[string]int64{}}
	for id, ver := range cachedSource {
		plan.Reusable[id.String()] = ver
	}
	headVerdict := LineageVerdict{Kind: KindHeads, ContentHash: mustDigest(ds.Heads)}
	sentVerdict := LineageVerdict{Kind: KindSentences, ContentHash: mustDigest(ds.Sentences)}
	polVerdict := LineageVerdict{Kind: KindPolicy, ContentHash: mustDigest(ds.Policy)}
	if len(cachedSource) == 0 {
		headVerdict.Relation, sentVerdict.Relation, polVerdict.Relation = RelationInitial, RelationInitial, RelationInitial
		headVerdict.FullInvalidation, sentVerdict.FullInvalidation, polVerdict.FullInvalidation = true, true, true
	} else {
		headVerdict.Relation, sentVerdict.Relation, polVerdict.Relation = RelationAppend, RelationAppend, RelationAppend
	}

	fresh, err := RunPerHeadReplay(ds, probes, quant)
	if err != nil {
		return StageResult{}, err
	}
	freshByID := map[model.HeadID]HeadErrorRow{}
	for _, r := range fresh {
		freshByID[r.Head] = r
	}

	rows := make([]HeadErrorRow, 0, len(ds.Heads))
	var reused []model.HeadID
	cacheWrites := map[string][]byte{}
	for _, h := range ds.Heads {
		id := h.ID()
		key := cacheKeys[id.String()]
		if ver, ok := plan.Reusable[id.String()]; ok {
			if raw, _, found, _ := wb.Store.CachedHeadError(key, id.String()); found {
				var cached HeadErrorRow
				if json.Unmarshal(raw, &cached) == nil {
					cached.ReusedFromVer = ver
					rows = append(rows, cached)
					reused = append(reused, id)
					continue
				}
			}
		}
		row := freshByID[id]
		rows = append(rows, row)
		b, _ := model.CanonicalJSON(row)
		cacheWrites[key+"|"+id.String()] = b
	}
	if len(cacheWrites) > 0 {
		cur, _ := wb.Store.ActiveVersion()
		if err := wb.Store.CacheHeadErrors("", cur, cacheWrites); err != nil {
			return StageResult{}, err
		}
	}

	probeReport := ProbeReport{
		Seed:       ds.Policy.Seed,
		Limit:      ds.Policy.ProbeLimit,
		Probes:     probes,
		HeadErrors: rows,
	}
	combos, err := Stage2(ds, probes, quant, rows)
	if err != nil {
		return StageResult{}, err
	}
	decision, err := Stage3(ds, rows, combos)
	if err != nil {
		return StageResult{}, err
	}
	return StageResult{
		InputDigest: inputDigest,
		Quant:       quant,
		Probe:       probeReport,
		Combos:      combos,
		Decision:    decision,
		Reuse:       plan,
		Lineage:     []LineageVerdict{headVerdict, sentVerdict, polVerdict},
		ReusedRows:  reused,
		cacheKeys:   cacheKeys,
	}, nil
}

// Commit persists the decision as the next monotonic version and retags the
// cache rows used by this run with the committed version number.
func (wb *Workbench) Commit(res StageResult) (store.VersionRow, error) {
	cur, err := wb.Store.CurrentVersion()
	if err != nil {
		return store.VersionRow{}, err
	}
	probeDigest, err := ProbeDigest(res.Probe)
	if err != nil {
		return store.VersionRow{}, err
	}
	decisionJSON, err := model.CanonicalJSON(res.Decision)
	if err != nil {
		return store.VersionRow{}, err
	}
	vr, err := wb.Store.CommitVersion(store.CommitInput{
		HeadFP:         res.Decision.Selected.Fingerprint(),
		ProbeDigest:    probeDigest,
		QuantFP:        res.Quant.Fingerprint(),
		DecisionDigest: res.Decision.Fingerprint(),
		InputDigest:    res.InputDigest,
		DecisionJSON:   decisionJSON,
		ExpectedNext:   cur + 1,
	})
	if err != nil {
		return store.VersionRow{}, err
	}
	for _, key := range res.cacheKeys {
		if err := wb.Store.RetagCacheVersion(key, vr.Version); err != nil {
			return store.VersionRow{}, err
		}
	}
	return vr, nil
}

// Restore decodes the decision JSON of a stored version.
func Restore(vr store.VersionRow) (Decision, error) {
	var d Decision
	if err := json.Unmarshal([]byte(vr.DecisionJSON), &d); err != nil {
		return Decision{}, err
	}
	return d, nil
}
