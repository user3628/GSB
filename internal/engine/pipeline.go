package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"queryrewrite/internal/policy"
	"queryrewrite/internal/spec"
	"queryrewrite/internal/store"
)

// Engine orchestrates the deterministic four-stage pipeline over SQLite.
type Engine struct {
	store      *store.Store
	cache      *Cache
	policy     spec.Policy
	CrashAfter string // when set, os.Exit(137) right after this stage commits
}

// New wires an engine and rebuilds the in-memory cache from committed slots.
func New(st *store.Store, p spec.Policy) (*Engine, error) {
	e := &Engine{store: st, policy: p, cache: NewCache(p.CacheCapacity)}
	if err := e.restoreCache(); err != nil {
		return nil, err
	}
	return e, nil
}

// Policy returns the active policy snapshot.
func (e *Engine) Policy() spec.Policy { return e.policy }

// SetPolicy switches the active snapshot. A full replacement invalidates all
// slots and adjudication caches; callers have already classified the kind.
func (e *Engine) SetPolicy(p spec.Policy, kind string) error {
	if err := e.store.SavePolicy(p, kind); err != nil {
		return err
	}
	if kind == "replacement" {
		e.cache = NewCache(p.CacheCapacity)
		if err := e.store.ReplaceSlots(nil); err != nil {
			return err
		}
	} else {
		e.cache.capacity = p.CacheCapacity
	}
	e.policy = p
	return e.store.SetMeta("active_policy_version", fmt.Sprintf("%d", p.Version))
}

func digestJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// inputDigest fingerprints the turn input plus the active policy content.
func inputDigest(qt spec.QueryTurn, p spec.Policy) string {
	return digestJSON(struct {
		Turn   spec.QueryTurn `json:"turn"`
		Policy string         `json:"policy_digest"`
	}{qt, policy.Digest(p)})
}

// RunTurn executes all four stages for one turn, checkpointing each stage in
// its own transaction. Already-committed stages are reloaded untouched, which
// makes a forced-kill restart byte-identical to an uninterrupted run.
func (e *Engine) RunTurn(qt spec.QueryTurn) (spec.TurnOutput, error) {
	out := spec.TurnOutput{
		SessionID: qt.SessionID, Turn: qt.Turn,
		PolicyVer: e.policy.Version, Digests: map[string]string{},
	}
	inDig := inputDigest(qt, e.policy)

	// Consecutive-turn gate. On a fresh turn we advance the index; on recovery
	// (the turn already has at least one committed stage) we skip the advance
	// but still reject out-of-order turns, then simply fill missing stages.
	hasFact, err := e.store.HasCheckpoint(qt.SessionID, qt.Turn)
	if err != nil {
		return out, err
	}
	if hasFact {
		next, err := e.store.ExpectedNextTurn(qt.SessionID)
		if err != nil {
			return out, err
		}
		if qt.Turn > next {
			return out, spec.NewBizError(spec.CodeStaleCheckpoint,
				"non-consecutive recovery turn", qt.SessionID, qt.Turn, "")
		}
	} else {
		if err := e.store.AdvanceTurn(qt.SessionID, qt.Turn); err != nil {
			return out, err
		}
	}

	// Recovery short-circuit: if a prior process already recorded a terminal
	// stage-1 business error for this turn, do not synthesize later stages.
	if gcp, _ := e.store.GetCheckpoint(spec.StageGenerate, qt.SessionID, qt.Turn); gcp != nil {
		var gprod struct {
			Tokens     []spec.Token     `json:"tokens"`
			Candidates []spec.Candidate `json:"candidates"`
			Error      *spec.BizError   `json:"error"`
		}
		_ = json.Unmarshal([]byte(gcp.Payload), &gprod)
		if gprod.Error != nil {
			out.StageError = gprod.Error
			out.Digests[spec.StageGenerate] = gcp.ProductDigest
			out.Tokens, out.Candidates = []spec.Token{}, []spec.Candidate{}
			out.Hits, out.Adj = []spec.HitResult{}, []spec.AdjResult{}
			return out, gprod.Error
		}
	}

	// ---- Stage 1 ----
	tokens, cands, berr := e.loadOrGenerate(qt, inDig, &out)
	if berr != nil {
		out.StageError = berr
		out.Tokens = []spec.Token{}
		out.Candidates = []spec.Candidate{}
		out.Hits = []spec.HitResult{}
		out.Adj = []spec.AdjResult{}
		e.persistStageError(berr, inDig, &out)
		return out, berr
	}
	out.Tokens, out.Candidates = tokens, cands

	// ---- Stage 2 ----
	hits, err := e.loadOrPrefix(qt, inDig, cands, &out)
	if err != nil {
		return out, err
	}
	out.Hits = hits

	// ---- Stage 3 ----
	adjs, err := e.loadOrAdjudge(qt, inDig, cands, hits, &out)
	if err != nil {
		return out, err
	}
	out.Adj = adjs

	// ---- Stage 4 ----
	ledger, err := e.loadOrSchedule(qt, inDig, cands, hits, adjs, &out)
	if err != nil {
		return out, err
	}
	out.Ledger = ledger
	return out, nil
}

func (e *Engine) loadOrGenerate(qt spec.QueryTurn, inDig string,
	out *spec.TurnOutput) ([]spec.Token, []spec.Candidate, *spec.BizError) {
	if cp, _ := e.store.GetCheckpoint(spec.StageGenerate, qt.SessionID, qt.Turn); cp != nil {
		var prod struct {
			Tokens     []spec.Token     `json:"tokens"`
			Candidates []spec.Candidate `json:"candidates"`
		}
		_ = json.Unmarshal([]byte(cp.Payload), &prod)
		out.Digests[spec.StageGenerate] = cp.ProductDigest
		return prod.Tokens, prod.Candidates, nil
	}
	tokens, cands, berr := Stage1(qt, e.policy)
	if berr != nil {
		return tokens, cands, berr
	}
	prod := struct {
		Tokens     []spec.Token     `json:"tokens"`
		Candidates []spec.Candidate `json:"candidates"`
	}{tokens, cands}
	e.commit(spec.StageGenerate, qt, inDig, prod, out)
	return tokens, cands, nil
}

func (e *Engine) loadOrPrefix(qt spec.QueryTurn, inDig string,
	cands []spec.Candidate, out *spec.TurnOutput) ([]spec.HitResult, error) {
	if cp, _ := e.store.GetCheckpoint(spec.StagePrefix, qt.SessionID, qt.Turn); cp != nil {
		var prod struct {
			Hits []spec.HitResult `json:"hits"`
		}
		_ = json.Unmarshal([]byte(cp.Payload), &prod)
		out.Digests[spec.StagePrefix] = cp.ProductDigest
		return prod.Hits, nil
	}
	hits := Stage2(cands, e.policy, e.cache, qt.Turn)
	prod := struct {
		Hits []spec.HitResult `json:"hits"`
	}{hits}
	e.commit(spec.StagePrefix, qt, inDig, prod, out)
	return hits, nil
}

func (e *Engine) loadOrAdjudge(qt spec.QueryTurn, inDig string,
	cands []spec.Candidate, hits []spec.HitResult,
	out *spec.TurnOutput) ([]spec.AdjResult, error) {
	if cp, _ := e.store.GetCheckpoint(spec.StageAdjudge, qt.SessionID, qt.Turn); cp != nil {
		var prod struct {
			Adj []spec.AdjResult `json:"adjudications"`
		}
		_ = json.Unmarshal([]byte(cp.Payload), &prod)
		out.Digests[spec.StageAdjudge] = cp.ProductDigest
		return prod.Adj, nil
	}
	adjs := Stage3(cands, hits, e.policy, e.cache, qt.Turn)
	prod := struct {
		Adj []spec.AdjResult `json:"adjudications"`
	}{adjs}
	e.commit(spec.StageAdjudge, qt, inDig, prod, out)
	return adjs, nil
}

func (e *Engine) loadOrSchedule(qt spec.QueryTurn, inDig string,
	cands []spec.Candidate, hits []spec.HitResult, adjs []spec.AdjResult,
	out *spec.TurnOutput) (spec.BudgetLedger, error) {
	if cp, _ := e.store.GetCheckpoint(spec.StageSchedule, qt.SessionID, qt.Turn); cp != nil {
		var prod struct {
			Ledger spec.BudgetLedger `json:"ledger"`
		}
		_ = json.Unmarshal([]byte(cp.Payload), &prod)
		out.Digests[spec.StageSchedule] = cp.ProductDigest
		e.replayScheduleSideEffects(qt, prod.Ledger)
		return prod.Ledger, nil
	}
	ledger := Stage4(qt.SessionID, qt.Turn, cands, hits, adjs, e.policy, e.cache)
	prod := struct {
		Ledger spec.BudgetLedger `json:"ledger"`
	}{ledger}
	e.commitSchedule(qt, inDig, prod, out)
	return ledger, nil
}

func (e *Engine) commit(stage string, qt spec.QueryTurn, inDig string,
	prod any, out *spec.TurnOutput) {
	payload, _ := json.Marshal(prod)
	prodDig := digestJSON(json.RawMessage(payload))
	cp := spec.Checkpoint{
		Stage: stage, SessionID: qt.SessionID, Turn: qt.Turn,
		InputDigest: inDig, PolicyVersion: e.policy.Version,
		ProductDigest: prodDig, Payload: string(payload),
	}
	seq, err := e.store.CommitCheckpoint(cp)
	if err != nil {
		// Duplicate on restart is expected; surface other errors.
		if berr, ok := err.(*spec.BizError); !ok || berr.Code != spec.CodeStaleCheckpoint {
			panic(err)
		}
	}
	out.Digests[stage] = fmt.Sprintf("%s#%d", prodDig, seq)
	if e.CrashAfter == stage {
		// Simulate a hard SIGKILL immediately after this durable commit.
		fmt.Fprintln(os.Stderr, "SIMULATED SIGKILL after committed "+stage)
		os.Exit(137)
	}
}

// commitSchedule atomically writes the stage-4 fact together with the durable
// slot table it produced, so a forced kill cannot commit one without the other.
func (e *Engine) commitSchedule(qt spec.QueryTurn, inDig string, prod any, out *spec.TurnOutput) {
	payload, _ := json.Marshal(prod)
	prodDig := digestJSON(json.RawMessage(payload))
	cp := spec.Checkpoint{
		Stage: spec.StageSchedule, SessionID: qt.SessionID, Turn: qt.Turn,
		InputDigest: inDig, PolicyVersion: e.policy.Version,
		ProductDigest: prodDig, Payload: string(payload),
	}
	seq, err := e.store.CommitCheckpointWithSlots(cp, e.cache.DurableSlots())
	if err != nil {
		if berr, ok := err.(*spec.BizError); !ok || berr.Code != spec.CodeStaleCheckpoint {
			panic(err)
		}
	}
	out.Digests[spec.StageSchedule] = fmt.Sprintf("%s#%d", prodDig, seq)
}

func (e *Engine) persistStageError(berr *spec.BizError, inDig string, out *spec.TurnOutput) {
	payload, _ := json.Marshal(struct {
		Error *spec.BizError `json:"error"`
	}{berr})
	prodDig := digestJSON(json.RawMessage(payload))
	cp := spec.Checkpoint{
		Stage: berr.Stage, SessionID: berr.SessionID, Turn: berr.Turn,
		InputDigest: inDig, PolicyVersion: e.policy.Version,
		ProductDigest: prodDig, Payload: string(payload),
	}
	_, _ = e.store.CommitCheckpoint(cp)
	out.Digests[berr.Stage] = prodDig
}

// replayScheduleSideEffects mirrors the cache mutations a continuous run would
// have performed in stage 4 for an already-committed turn, without altering the
// ledger. This keeps post-restart cache state byte-identical.
func (e *Engine) replayScheduleSideEffects(qt spec.QueryTurn, ledger spec.BudgetLedger) {
	hcp, _ := e.store.GetCheckpoint(spec.StagePrefix, qt.SessionID, qt.Turn)
	if hcp == nil {
		return
	}
	var hprod struct {
		Hits []spec.HitResult `json:"hits"`
	}
	if err := json.Unmarshal([]byte(hcp.Payload), &hprod); err != nil {
		return
	}
	accepted := map[string]bool{}
	for _, id := range ledger.Accepted {
		accepted[id] = true
	}
	for _, hr := range hprod.Hits {
		if !hr.Cacheable || !accepted[hr.CandidateID] {
			continue
		}
		fp := CandidateFingerprint(hr)
		if sl, live := e.cache.Lookup(fp); live {
			sl.LastUsedTurn = qt.Turn
			continue
		}
		if e.cache.durableSize() >= e.policy.CacheCapacity {
			if victim := e.cache.evictionVictim(qt.Turn); victim != nil {
				e.cache.ApplyEviction(victim.SlotID)
			}
		}
		if e.cache.durableSize() < e.policy.CacheCapacity {
			e.cache.Put(fp, len(hr.Blocks), qt.SessionID, qt.Turn, qt.Turn)
		}
	}
}

// restoreCache rebuilds the in-memory cache by replaying committed stage-4
// ledgers in monotonic checkpoint order. Because slot ids are derived from the
// fingerprint and eviction is deterministic, the rebuilt state matches a
// continuous run byte-for-byte even after a forced kill.
func (e *Engine) restoreCache() error {
	e.cache = NewCache(e.policy.CacheCapacity)
	cps, err := e.store.Checkpoints()
	if err != nil {
		return err
	}
	for _, cp := range cps {
		if cp.Stage != spec.StageSchedule {
			continue
		}
		var prod struct {
			Ledger spec.BudgetLedger `json:"ledger"`
		}
		if err := json.Unmarshal([]byte(cp.Payload), &prod); err != nil {
			return err
		}
		// Re-insert every accepted candidate's durable fingerprint by re-deriving
		// it from the stage-2 checkpoint of the same turn.
		hcp, err := e.store.GetCheckpoint(spec.StagePrefix, cp.SessionID, cp.Turn)
		if err != nil || hcp == nil {
			continue
		}
		var hprod struct {
			Hits []spec.HitResult `json:"hits"`
		}
		if err := json.Unmarshal([]byte(hcp.Payload), &hprod); err != nil {
			return err
		}
		accepted := map[string]bool{}
		for _, id := range prod.Ledger.Accepted {
			accepted[id] = true
		}
		for _, hr := range hprod.Hits {
			if !hr.Cacheable || !accepted[hr.CandidateID] {
				continue
			}
			fp := CandidateFingerprint(hr)
			if sl, live := e.cache.Lookup(fp); live {
				// Exact hits still refresh LRU age; keep replay identical.
				sl.LastUsedTurn = cp.Turn
				continue
			}
			if e.cache.durableSize() >= e.policy.CacheCapacity {
				victim := e.cache.evictionVictim(cp.Turn)
				if victim != nil {
					e.cache.ApplyEviction(victim.SlotID)
				}
			}
			if e.cache.durableSize() < e.policy.CacheCapacity {
				e.cache.Put(fp, len(hr.Blocks), cp.SessionID, cp.Turn, cp.Turn)
			}
		}
	}
	return e.store.ReplaceSlots(e.cache.DurableSlots())
}

// ReadAdjudication performs an observed-version read. Reads below the active
// version return E_STALE_READ (must recompute) instead of a stale conclusion.
func (e *Engine) ReadAdjudication(sessionID string, turn int, observedVersion int) (
	*spec.AdjResult, *spec.BizError) {
	curVer, _, err := e.store.CurrentPolicyVersion()
	if err != nil {
		return nil, spec.NewBizError(spec.CodeStaleRead, err.Error(), sessionID, turn, spec.StageAdjudge)
	}
	if observedVersion < curVer {
		return nil, spec.NewBizError(spec.CodeStaleRead,
			"observed policy version is stale; recompute required",
			sessionID, turn, spec.StageAdjudge).
			WithDetail("observed_version", fmt.Sprintf("%d", observedVersion)).
			WithDetail("current_version", fmt.Sprintf("%d", curVer))
	}
	cp, err := e.store.GetCheckpoint(spec.StageAdjudge, sessionID, turn)
	if err != nil || cp == nil {
		return nil, spec.NewBizError(spec.CodeStaleRead,
			"no committed adjudication for turn", sessionID, turn, spec.StageAdjudge)
	}
	var prod struct {
		Adj []spec.AdjResult `json:"adjudications"`
	}
	if err := json.Unmarshal([]byte(cp.Payload), &prod); err != nil {
		return nil, spec.NewBizError(spec.CodeStaleRead, err.Error(), sessionID, turn, spec.StageAdjudge)
	}
	if len(prod.Adj) == 0 {
		return nil, spec.NewBizError(spec.CodeStaleRead, "empty adjudication", sessionID, turn, spec.StageAdjudge)
	}
	return &prod.Adj[0], nil
}

// StageStatus reports the checkpoint digest/seq for each stage of a turn.
func (e *Engine) StageStatus(sessionID string, turn int) (map[string]string, error) {
	status := map[string]string{}
	for _, st := range spec.AllStages() {
		cp, err := e.store.GetCheckpoint(st, sessionID, turn)
		if err != nil {
			return nil, err
		}
		if cp != nil {
			status[st] = fmt.Sprintf("%s#%d", cp.ProductDigest, cp.Seq)
		} else {
			status[st] = ""
		}
	}
	return status, nil
}

// VersionFingerprint returns the active policy version and content digest.
func (e *Engine) VersionFingerprint() (int, string) {
	return e.policy.Version, policy.Digest(e.policy)
}
