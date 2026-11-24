package workspace

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"maskrevision/internal/model"
	"maskrevision/internal/reconcile"
	"maskrevision/internal/replay"
	"maskrevision/internal/revlog"
)

type UpgradeVerification struct {
	ShardID            string                    `json:"shard_id"`
	SourceVersion      int                       `json:"source_version"`
	ExportedV2         string                    `json:"exported_v2"`
	RevisionCount      int                       `json:"revision_count"`
	TraceEqual         bool                      `json:"trace_equal"`
	V1TraceDigest      string                    `json:"v1_trace_digest"`
	V2TraceDigest      string                    `json:"v2_trace_digest"`
	FinalSnapshot      string                    `json:"final_snapshot_digest"`
	UnrecoverableCount int                       `json:"unrecoverable_count"`
	Inferences         []revlog.UpgradeInference `json:"inferences"`
}

type FixtureClassification struct {
	File                string               `json:"file"`
	Result              reconcile.Result     `json:"result"`
	ReplayErrorCode     string               `json:"replay_error_code,omitempty"`
	ReplayErrorMessage  string               `json:"replay_error_message,omitempty"`
	ForkDecisions       []model.ForkDecision `json:"fork_decisions,omitempty"`
	RevisionsReplayed   int                  `json:"revisions_replayed"`
	FinalSnapshotDigest string               `json:"final_snapshot_digest,omitempty"`
}

func (ws *Workspace) ExportAndVerifyV1(id string) (UpgradeVerification, *model.BusinessError) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	rt, be := ws.shard(id)
	if be != nil {
		return UpgradeVerification{}, be
	}
	if rt.OriginalVersion != model.SchemaV1 || rt.Upgrade == nil {
		return UpgradeVerification{}, model.WithContext(model.NewBusinessError("E_SCHEMA_NOT_V1", "shard log is not a v1 log", nil), id, 0)
	}
	data, err := revlog.EncodeV2(rt.Log)
	if err != nil {
		return UpgradeVerification{}, model.NewBusinessError("E_UPGRADE_EXPORT", err.Error(), nil)
	}
	path := filepath.Join(ws.Layout.Exports, id+".upgraded.v2.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return UpgradeVerification{}, model.NewBusinessError("E_UPGRADE_EXPORT", err.Error(), nil)
	}
	reportData, _ := json.MarshalIndent(rt.Upgrade, "", "  ")
	_ = os.WriteFile(filepath.Join(ws.Layout.Exports, id+".upgrade-report.json"), append(reportData, '\n'), 0o644)
	parsed, _, err := revlog.Parse(data)
	if err != nil {
		return UpgradeVerification{}, model.NewBusinessError("E_UPGRADE_REIMPORT", err.Error(), nil)
	}
	e1, r1, be := replayTrace(rt.Initial, rt.Log, ws.opts.Replay)
	if be != nil {
		return UpgradeVerification{}, be
	}
	e2, r2, be := replayTrace(rt.Initial, parsed, ws.opts.Replay)
	if be != nil {
		return UpgradeVerification{}, be
	}
	d1, d2 := traceDigest(r1), traceDigest(r2)
	final1 := finalStateDigest(e1.State())
	final2 := finalStateDigest(e2.State())
	return UpgradeVerification{
		ShardID: id, SourceVersion: rt.OriginalVersion, ExportedV2: path,
		RevisionCount: len(rt.Log.Revisions), TraceEqual: d1 == d2 && final1 == final2,
		V1TraceDigest: d1, V2TraceDigest: d2, FinalSnapshot: final1,
		UnrecoverableCount: rt.Upgrade.UnrecoverableCount, Inferences: rt.Upgrade.Inferences,
	}, nil
}

func (ws *Workspace) UnrecoverableFixture() (revlog.UpgradeReport, *model.BusinessError) {
	rt, be := ws.shard("shard-01")
	if be != nil {
		return revlog.UpgradeReport{}, be
	}
	data, err := os.ReadFile(filepath.Join(ws.Layout.Data, "legacy", "v1-unrecoverable.json"))
	if err != nil {
		return revlog.UpgradeReport{}, model.NewBusinessError("E_FIXTURE_READ", err.Error(), nil)
	}
	upgradeOpts := ws.opts.Upgrade
	upgradeOpts.Width = rt.Initial.Width
	upgradeOpts.Height = rt.Initial.Height
	upgradeOpts.Pixels = rt.Initial.Pixels
	res, err := revlog.UpgradeV1(data, upgradeOpts)
	if err != nil {
		return revlog.UpgradeReport{}, model.NewBusinessError("E_UPGRADE", err.Error(), nil)
	}
	return res.Report, nil
}

func (ws *Workspace) ClassifyFixture(id, relativePath string) (FixtureClassification, *model.BusinessError) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	rt, be := ws.shard(id)
	if be != nil {
		return FixtureClassification{}, be
	}
	data, err := os.ReadFile(filepath.Join(ws.Layout.Data, filepath.FromSlash(relativePath)))
	if err != nil {
		return FixtureClassification{}, model.NewBusinessError("E_FIXTURE_READ", err.Error(), nil)
	}
	log, _, err := parseLogData(rt.Initial, data, ws.opts.Upgrade)
	if err != nil {
		return FixtureClassification{}, model.NewBusinessError("E_FIXTURE_PARSE", err.Error(), nil)
	}
	metas, err := ws.Store.List(id)
	if err != nil {
		return FixtureClassification{}, model.NewBusinessError("E_CHECKPOINT_READ", err.Error(), nil)
	}
	result := reconcile.ClassifyFingerprints(historyFromMetas(metas), id, rt.Initial.InputFingerprint, log, rt.Initial.InputFingerprint)
	engine, trace, replayErr := replayTrace(rt.Initial, log, ws.opts.Replay)
	out := FixtureClassification{
		File: relativePath, Result: result, RevisionsReplayed: len(trace), ForkDecisions: engine.State().ForkDecisions,
	}
	if replayErr != nil {
		out.ReplayErrorCode = replayErr.Code
		out.ReplayErrorMessage = replayErr.Error()
	} else {
		out.FinalSnapshotDigest = finalStateDigest(engine.State())
	}
	return out, nil
}

func (ws *Workspace) ApplyIncomingLog(id string, raw []byte) (SyncReport, *model.BusinessError) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	rt, be := ws.shard(id)
	if be != nil {
		return SyncReport{}, be
	}
	log, upgrade, err := parseLogData(rt.Initial, raw, ws.opts.Upgrade)
	if err != nil {
		return SyncReport{}, model.NewBusinessError("E_LOG_INVALID", err.Error(), nil)
	}
	if log.ShardID != id {
		return SyncReport{}, model.NewBusinessError("E_LOG_SHARD_MISMATCH", "incoming log belongs to a different shard", map[string]any{
			"expected_shard_id": id, "actual_shard_id": log.ShardID,
		})
	}
	if err := ws.persistEffectiveLog(id, rt.Initial.InputFingerprint, model.SHA256Hex(raw), detectVersion(raw), log, upgrade); err != nil {
		return SyncReport{}, model.NewBusinessError("E_LOG_PERSIST", err.Error(), nil)
	}
	old := *rt
	rt.Log = log
	rt.RawLog = raw
	rt.OriginalVersion = detectVersion(raw)
	rt.Upgrade = upgrade
	if err := ws.syncShard(rt); err != nil {
		rt.Log = old.Log
		rt.RawLog = old.RawLog
		rt.OriginalVersion = old.OriginalVersion
		rt.Upgrade = old.Upgrade
		if be := model.AsBusinessError(err); be != nil {
			return SyncReport{}, be
		}
		return SyncReport{}, model.NewBusinessError("E_REPLAY", err.Error(), nil)
	}
	ws.Shards[id] = rt
	return rt.Sync, nil
}

func parseLogData(initial replay.Initial, raw []byte, upgradeOpts revlog.UpgradeOptions) (model.Log, *revlog.UpgradeReport, error) {
	switch detectVersion(raw) {
	case model.SchemaV2:
		log, _, err := revlog.Parse(raw)
		return log, nil, err
	case model.SchemaV1:
		upgradeOpts.Width = initial.Width
		upgradeOpts.Height = initial.Height
		upgradeOpts.Pixels = initial.Pixels
		res, err := revlog.UpgradeV1(raw, upgradeOpts)
		if err != nil {
			return model.Log{}, nil, err
		}
		report := res.Report
		return res.Log, &report, nil
	default:
		return model.Log{}, nil, fmt.Errorf("unsupported log schema")
	}
}

func replayTrace(initial replay.Initial, log model.Log, opts replay.Options) (*replay.Engine, []model.RevisionRecord, *model.BusinessError) {
	engine, err := replay.NewEngine(initial, log, opts)
	if err != nil {
		return nil, nil, model.AsBusinessError(err)
	}
	records := make([]model.RevisionRecord, 0, len(log.Revisions))
	for engine.Remaining() > 0 {
		rec, be := engine.ApplyNext()
		if be != nil {
			return engine, records, be
		}
		records = append(records, *rec)
	}
	return engine, records, nil
}

func finalStateDigest(state *model.ReplayState) string {
	if state == nil || len(state.RevisionRecords) == 0 {
		return ""
	}
	return state.RevisionRecords[len(state.RevisionRecords)-1].SnapshotDigest
}

func traceDigest(records []model.RevisionRecord) string {
	var b []byte
	for _, rec := range records {
		b = append(b, []byte(rec.SnapshotDigest)...)
		b = append(b, 0)
	}
	return model.SHA256Hex(b)
}

func (ws *Workspace) verifyTraceDigest(records []model.RevisionRecord) string {
	return traceDigest(records)
}
