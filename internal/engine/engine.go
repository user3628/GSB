// Package engine orchestrates the four deterministic stages, the SQLite
// checkpoint protocol and crash recovery. All ordering is explicit (sorted
// ticket ids, staged transactions) so concurrent publish attempts and process
// restarts cannot change produced bytes.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"

	"ticketpipeline/internal/bayes"
	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
	"ticketpipeline/internal/normalize"
	"ticketpipeline/internal/priority"
	"ticketpipeline/internal/store"
	"ticketpipeline/internal/validate"
)

// Engine holds workspace paths, the SQLite store and runtime cancellation.
type Engine struct {
	DataDir string
	OutDir  string
	store   *store.Store
	lock    *determ.FileLock

	stopFlag atomic.Bool
	crashAt  string // "stage:ticketID" trigger for the deterministic crash demo
}

// Snapshot is the in-memory view reconstructed from inputs and checkpoints.
type Snapshot struct {
	Policy  model.Policy
	Tickets []model.RawTicket

	Normalize map[string]*normalize.Result
	Intent    map[string]*bayes.Result
	Priority  map[string]*priority.Result
	Report    *validate.Report

	NormErr   map[string]*model.BizError
	IntentErr map[string]*model.BizError
	PrioErr   map[string]*model.BizError
}

// New opens the engine, taking the workspace-wide advisory file lock so only
// one engine instance publishes into a given output directory.
func New(dataDir, outDir string) (*Engine, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	st, err := store.Open(outDir)
	if err != nil {
		return nil, err
	}
	lock, err := determ.NewFileLock(outDir, "pipeline.lock")
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	if err := lock.Lock(); err != nil {
		_ = st.Close()
		return nil, err
	}
	return &Engine{DataDir: dataDir, OutDir: outDir, store: st, lock: lock}, nil
}

// Store exposes the underlying store for verification tooling.
func (e *Engine) Store() *store.Store { return e.store }

// Close releases lock and database.
func (e *Engine) Close() error {
	err1 := e.lock.Close()
	err2 := e.store.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// RequestStop marks the engine for graceful stop at the next ticket boundary.
func (e *Engine) RequestStop() { e.stopFlag.Store(true) }

// SetCrashAt arms a hard-exit trigger ("stage:ticketID"). The process exits
// immediately after the preceding transaction committed, so recovery relies
// only on durable facts and never on an exit report.
func (e *Engine) SetCrashAt(spec string) { e.crashAt = spec }

// Run executes the pipeline end to end. Upstream stages whose fingerprints
// still match are reused; the earliest changed stage and every downstream
// stage is rebuilt, producing bytes identical to a fresh full run.
func (e *Engine) Run(ctx context.Context) (*Snapshot, error) {
	policy, err := LoadPolicy(e.DataDir)
	if err != nil {
		return nil, err
	}
	shards, err := LoadShards(e.DataDir)
	if err != nil {
		return nil, err
	}
	tickets, err := FlattenShards(shards)
	if err != nil {
		return nil, err
	}
	sort.Slice(tickets, func(i, j int) bool { return tickets[i].TicketID < tickets[j].TicketID })
	ids := make([]string, len(tickets))
	byID := make(map[string]model.RawTicket, len(tickets))
	for i, t := range tickets {
		ids[i] = t.TicketID
		byID[t.TicketID] = t
	}

	inputFP, err := determ.Fingerprint(tickets)
	if err != nil {
		return nil, err
	}
	// Expected effective checkpoint counts per stage, derived purely from the
	// inputs: every ticket reaches normalize, while later stages skip tickets
	// that failed upstream (e.g. empty text never reaches the Bayes scorer).
	expectedCount := e.expectedCounts(tickets, policy)
	configFP := map[string]string{}
	for _, st := range model.Stages {
		fp, err := StageConfigFingerprint(st, policy)
		if err != nil {
			return nil, err
		}
		configFP[st] = fp
	}

	snap := &Snapshot{
		Policy: policy, Tickets: tickets,
		Normalize: map[string]*normalize.Result{}, Intent: map[string]*bayes.Result{},
		Priority:  map[string]*priority.Result{},
		NormErr:   map[string]*model.BizError{},
		IntentErr: map[string]*model.BizError{},
		PrioErr:   map[string]*model.BizError{},
	}
	startStage, err := e.planReuse(policy, inputFP, configFP, expectedCount)
	if err != nil {
		return nil, err
	}
	if startStage == "" {
		// Every stage is reusable: hydrate committed facts and return.
		if err := e.hydrate(snap); err != nil {
			return nil, err
		}
		return snap, nil
	}
	// Always clear the earliest affected stage and its downstream so rebuilt
	// stage_seq restarts at 1 and stays byte-identical to a fresh workspace.
	if err := e.store.DeleteStage(startStage); err != nil {
		return nil, err
	}
	if err := e.hydrate(snap); err != nil {
		return nil, err
	}

	rebuiltFrom := stageIndex(startStage)
	for idx, stage := range model.Stages {
		if err := ctx.Err(); err != nil {
			return snap, err
		}
		if idx < rebuiltFrom {
			continue // reused upstream stages keep their existing generation
		}
		gen, err := e.nextGeneration(stage)
		if err != nil {
			return snap, err
		}
		// Deterministic logical timestamps: no wall clock in persisted state.
		started := int64(gen) * 10
		if err := e.runStage(ctx, stage, snap, byID, ids, policy, inputFP, configFP[stage]); err != nil {
			return snap, err
		}
		// The stage-level artifact fingerprint is an ordered digest over the
		// committed checkpoint rows, so it exists as soon as the stage ends and
		// is identical on replay, incremental rebuild and crash recovery.
		artifactFP, err := e.stageDigest(stage)
		if err != nil {
			return snap, err
		}
		if err := e.store.RecordStageRun(stage, gen, policy.Version, configFP[stage],
			inputFP, artifactFP, started, started+1); err != nil {
			return snap, err
		}
	}
	return snap, nil
}

// planReuse returns the earliest stage that must rebuild. A stage is reusable
// only when it has checkpoints whose config/input fingerprints match the
// current run AND a stage_run marker with the current policy version exists.
// Once one stage must rebuild, every downstream stage rebuilds too, so the
// returned value is simply the first mismatch; an empty string means the
// whole pipeline is reusable.
func (e *Engine) planReuse(policy model.Policy, inputFP string, configFP map[string]string, expected map[string]int) (string, error) {
	firstMismatch := ""
	for _, stage := range model.Stages {
		if firstMismatch != "" {
			// Downstream of the first affected stage always rebuilds.
			return firstMismatch, nil
		}
		cps, err := e.store.ListCheckpoints(stage)
		if err != nil {
			return "", err
		}
		// A partial stage (e.g. process killed mid-stage) is never reused:
		// rebuilding it guarantees the trained vocabulary and every aggregate
		// match a continuous run byte-for-byte.
		if len(cps) == 0 || len(cps) != expected[stage] {
			return stage, nil
		}
		for _, cp := range cps {
			// Reuse depends solely on the stage-relevant config fingerprint and
			// the input fingerprint; policy_version is recorded for audit but a
			// mere version bump with identical effective config must not rebuild.
			if cp.ConfigFP != configFP[stage] || cp.InputFP != inputFP {
				firstMismatch = stage
				break
			}
		}
		if firstMismatch != "" {
			continue
		}
		_, fp, err := e.latestStageRun(stage)
		if err != nil {
			return "", err
		}
		if fp == "" {
			firstMismatch = stage
		}
	}
	return firstMismatch, nil
}

func (e *Engine) nextGeneration(stage string) (int, error) {
	gen, err := e.store.LatestGeneration(stage)
	if err != nil {
		return 0, err
	}
	return gen + 1, nil
}

// hydrate loads committed artifacts back into the snapshot so reused upstream
// stages are never recomputed.
func (e *Engine) hydrate(snap *Snapshot) error {
	for _, stage := range model.Stages {
		cps, err := e.store.ListCheckpoints(stage)
		if err != nil {
			return err
		}
		for _, cp := range cps {
			if cp.Status == store.StatusBizError {
				be := model.NewBizError(cp.ErrorCode, stage, cp.TicketID, cp.Summary, nil)
				recordBiz(stage, cp.TicketID, be, snap)
				continue
			}
			path := filepath.Join(e.OutDir, stage, cp.TicketID+".json")
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if err := decodeArtifact(stage, raw, snap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) runStage(ctx context.Context, stage string, snap *Snapshot,
	byID map[string]model.RawTicket, ids []string, policy model.Policy,
	inputFP, configFP string) error {

	stageDir := filepath.Join(e.OutDir, stage)
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return err
	}
	maxSeq, err := e.store.MaxStageSeqDB(stage)
	if err != nil {
		return err
	}

	switch stage {
	case model.StageNormalize:
		if err := normalize.ValidateConfig(policy); err != nil {
			return err
		}
		for _, id := range ids {
			if err := checkCtx(ctx, e); err != nil {
				return err
			}
			t := byID[id]
			res, bizErr := normalize.Process(t, policy.HashDim)
			summary := "tokens=" + strconv.Itoa(resTokens(res))
			if bizErr != nil {
				summary = bizErr.Message
			}
			if err := e.persist(snap, stage, id, policy, inputFP, configFP, maxSeq, summary, res, bizErr); err != nil {
				return err
			}
			maxSeq++
			e.maybeCrash(stage, id)
		}
	case model.StageIntent:
		if err := bayes.ValidateConfig(policy); err != nil {
			return err
		}
		trained, err := bayes.Train(snap.Tickets, snap.Normalize, policy)
		if err != nil {
			return err
		}
		if err := writeCanonical(filepath.Join(stageDir, "model.json"), trained); err != nil {
			return err
		}
		for _, id := range ids {
			if err := checkCtx(ctx, e); err != nil {
				return err
			}
			nr, ok := snap.Normalize[id]
			if !ok {
				continue // stage-1 failure already recorded; downstream skips by design
			}
			res, bizErr := trained.Score(nr, policy.IntentWeights)
			summary := "top=" + safeTop(res)
			if bizErr != nil {
				summary = bizErr.Message
			}
			if err := e.persist(snap, stage, id, policy, inputFP, configFP, maxSeq, summary, res, bizErr); err != nil {
				return err
			}
			maxSeq++
			e.maybeCrash(stage, id)
		}
	case model.StagePriority:
		if err := priority.ValidateConfig(policy); err != nil {
			return err
		}
		for _, id := range ids {
			if err := checkCtx(ctx, e); err != nil {
				return err
			}
			ir, ok := snap.Intent[id]
			if !ok {
				continue
			}
			res, bizErr := priority.Decide(byID[id], ir, policy)
			summary := "priority=" + strconv.Itoa(res.FinalPriority) + " verdict=" + res.Verdict
			if bizErr != nil {
				summary = bizErr.Message
			}
			if err := e.persist(snap, stage, id, policy, inputFP, configFP, maxSeq, summary, res, bizErr); err != nil {
				return err
			}
			maxSeq++
			e.maybeCrash(stage, id)
		}
	case model.StageValidate:
		stageDigests := map[string]string{}
		for _, st := range model.Stages {
			fp, err := e.store.StageArtifactFP(st)
			if err != nil {
				return err
			}
			stageDigests[st] = fp
		}
		report, err := validate.Build(policy.Version, validate.Inputs{
			Tickets:      snap.Tickets,
			Norm:         snap.Normalize,
			Intent:       snap.Intent,
			Priority:     snap.Priority,
			NormErr:      snap.NormErr,
			IntentErr:    snap.IntentErr,
			PrioErr:      snap.PrioErr,
			StageDigests: stageDigests,
		})
		if err != nil {
			return err
		}
		if err := writeCanonical(filepath.Join(stageDir, "report.json"), report); err != nil {
			return err
		}
		snap.Report = report
		summary := "rows=" + strconv.Itoa(len(report.Rows))
		if err := e.persist(snap, stage, "REPORT", policy, inputFP, configFP, maxSeq, summary, report, nil); err != nil {
			return err
		}
	}
	return nil
}

// persist writes the artifact file (when present) and publishes one
// checkpoint transactionally. stageSeq must be exactly current max+1; stale or
// replaced publishes surface as E_STALE_CHECKPOINT.
func (e *Engine) persist(snap *Snapshot, stage, ticketID string, policy model.Policy,
	inputFP, configFP string, currentMax int64, summary string, artifact any, bizErr *model.BizError) error {

	var artifactFP string
	status := store.StatusOK
	code := ""
	if bizErr != nil {
		status = store.StatusBizError
		code = bizErr.Code
		fp, err := determ.Fingerprint(bizErr)
		if err != nil {
			return err
		}
		artifactFP = fp
		recordBiz(stage, ticketID, bizErr, snap)
	} else {
		fp, err := determ.Fingerprint(artifact)
		if err != nil {
			return err
		}
		artifactFP = fp
		path := filepath.Join(e.OutDir, stage, ticketID+".json")
		if err := writeCanonical(path, artifact); err != nil {
			return err
		}
		recordArtifact(stage, ticketID, artifact, snap)
	}
	cp := store.Checkpoint{
		StageSeq:      currentMax + 1,
		Stage:         stage,
		TicketID:      ticketID,
		PolicyVersion: policy.Version,
		InputFP:       inputFP,
		ConfigFP:      configFP,
		ArtifactFP:    artifactFP,
		Summary:       summary,
		ErrorCode:     code,
		Status:        status,
	}
	if outcome := e.store.Publish(cp); outcome.Error != nil {
		return outcome.Error
	}
	return nil
}

func (e *Engine) maybeCrash(stage, ticketID string) {
	if e.crashAt == "" {
		return
	}
	if e.crashAt == stage+":"+ticketID {
		_ = e.store.Close() // release SQLite; WAL guarantees durability
		os.Exit(97)
	}
}

func checkCtx(ctx context.Context, e *Engine) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.stopFlag.Load() {
		return context.Canceled
	}
	return nil
}

func stageIndex(stage string) int {
	for i, s := range model.Stages {
		if s == stage {
			return i
		}
	}
	return -1
}
