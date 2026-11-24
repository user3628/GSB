package workspace

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"maskrevision/internal/imageio"
	"maskrevision/internal/model"
	"maskrevision/internal/reconcile"
	"maskrevision/internal/replay"
	"maskrevision/internal/revlog"
	"maskrevision/internal/store"
)

type Options struct {
	Replay       replay.Options
	Upgrade      revlog.UpgradeOptions
	CrashAfter   int
	CrashFunc    func() error
	OnCheckpoint func(store.Checkpoint) error
}

type Layout struct {
	Root     string
	Data     string
	Database string
	Locks    string
	Exports  string
}

type ShardRuntime struct {
	Definition      model.ManifestShard
	Initial         replay.Initial
	Log             model.Log
	RawLog          []byte
	OriginalVersion int
	Upgrade         *revlog.UpgradeReport
	State           *model.ReplayState
	Engine          *replay.Engine
	Sync            SyncReport
}

type SyncReport struct {
	ShardID               string   `json:"shard_id"`
	OriginalSchemaVersion int      `json:"original_schema_version"`
	Upgraded              bool     `json:"upgraded"`
	Kind                  string   `json:"kind"`
	EarliestAffected      int      `json:"earliest_affected_revision"`
	ReusedRevision        int      `json:"reused_revision"`
	RemovedCheckpoints    int64    `json:"removed_checkpoints"`
	ReplayedRevisions     []int    `json:"replayed_revisions"`
	HeadRevision          int      `json:"head_revision"`
	InputFP               string   `json:"input_fp"`
	LogFP                 string   `json:"log_fp"`
	SnapshotDigest        string   `json:"final_snapshot_digest"`
	UndoneRevisions       []int    `json:"undone_revisions"`
	Reasons               []string `json:"reasons,omitempty"`
}

type Workspace struct {
	mu              sync.RWMutex
	Layout          Layout
	Manifest        model.Manifest
	Store           *store.Store
	Shards          map[string]*ShardRuntime
	Order           []string
	opts            Options
	checkpointCount int
}

func Prepare(root string, clean bool) (Layout, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return Layout{}, err
	}
	if clean {
		if err := os.RemoveAll(abs); err != nil {
			return Layout{}, err
		}
	}
	layout := Layout{
		Root: abs, Data: filepath.Join(abs, "data"), Database: filepath.Join(abs, "checkpoints.db"),
		Locks: filepath.Join(abs, "locks"), Exports: filepath.Join(abs, "exports"),
	}
	for _, dir := range []string{layout.Root, layout.Data, layout.Locks, layout.Exports} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return Layout{}, err
		}
	}
	return layout, nil
}

func Open(root string, opts Options) (*Workspace, error) {
	layout, err := Prepare(root, false)
	if err != nil {
		return nil, err
	}
	manifestData, err := os.ReadFile(filepath.Join(layout.Data, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read manifest (run init/demo first): %w", err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.SchemaVersion != model.SchemaV2 {
		return nil, fmt.Errorf("manifest schema_version %d is unsupported", manifest.SchemaVersion)
	}
	st, err := store.Open(layout.Database)
	if err != nil {
		return nil, err
	}
	ws := &Workspace{Layout: layout, Manifest: manifest, Store: st, Shards: map[string]*ShardRuntime{}, opts: opts}
	for _, def := range manifest.Shards {
		rt, err := ws.loadShard(def)
		if err != nil {
			_ = st.Close()
			return nil, err
		}
		ws.Shards[def.ShardID] = rt
		ws.Order = append(ws.Order, def.ShardID)
	}
	sort.Strings(ws.Order)
	return ws, nil
}

func (ws *Workspace) Close() error {
	if ws == nil || ws.Store == nil {
		return nil
	}
	return ws.Store.Close()
}

func (ws *Workspace) loadShard(def model.ManifestShard) (*ShardRuntime, error) {
	initial, err := ws.loadInitial(def)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", def.ShardID, err)
	}
	raw, err := os.ReadFile(filepath.Join(ws.Layout.Data, filepath.FromSlash(def.Log)))
	if err != nil {
		return nil, err
	}
	version := detectVersion(raw)
	var log model.Log
	var upgrade *revlog.UpgradeReport
	sourceSHA := model.SHA256Hex(raw)
	stored, hasStored, storeErr := ws.Store.LoadLog(def.ShardID)
	if storeErr != nil {
		return nil, storeErr
	}
	usedStored := false
	if hasStored && stored.InputFP == initial.InputFingerprint && stored.SourceSHA256 == sourceSHA {
		if err := json.Unmarshal([]byte(stored.LogJSON), &log); err == nil {
			version = stored.OriginalVersion
			if stored.UpgradeReportJSON != "" {
				var report revlog.UpgradeReport
				if err := json.Unmarshal([]byte(stored.UpgradeReportJSON), &report); err == nil {
					upgrade = &report
				}
			}
			usedStored = true
		}
	}
	if !usedStored {
		switch version {
		case model.SchemaV2:
			log, _, err = revlog.Parse(raw)
		case model.SchemaV1:
			upgradeOpts := ws.opts.Upgrade
			upgradeOpts.Width = initial.Width
			upgradeOpts.Height = initial.Height
			upgradeOpts.Pixels = initial.Pixels
			res, uerr := revlog.UpgradeV1(raw, upgradeOpts)
			if uerr != nil {
				err = uerr
			} else {
				log = res.Log
				report := res.Report
				upgrade = &report
			}
		default:
			err = fmt.Errorf("unsupported log schema version %d", version)
		}
		if err != nil {
			return nil, fmt.Errorf("parse %s log: %w", def.ShardID, err)
		}
		if err := ws.persistEffectiveLog(def.ShardID, initial.InputFingerprint, sourceSHA, version, log, upgrade); err != nil {
			return nil, err
		}
	}
	if log.ShardID != def.ShardID || log.Width != initial.Width || log.Height != initial.Height {
		return nil, fmt.Errorf("log metadata does not match shard definition for %s", def.ShardID)
	}
	if upgrade != nil && !usedStored {
		if err := ws.Store.RecordUpgrade(def.ShardID, upgrade); err != nil {
			return nil, err
		}
	}
	rt := &ShardRuntime{
		Definition: def, Initial: initial, Log: log, RawLog: raw, OriginalVersion: version, Upgrade: upgrade,
	}
	if err := ws.syncShard(rt); err != nil {
		return nil, err
	}
	return rt, nil
}

func (ws *Workspace) persistEffectiveLog(shardID, inputFP, sourceSHA string, version int, log model.Log, upgrade *revlog.UpgradeReport) error {
	logJSON, err := json.Marshal(log)
	if err != nil {
		return err
	}
	reportJSON := ""
	if upgrade != nil {
		data, err := json.Marshal(upgrade)
		if err != nil {
			return err
		}
		reportJSON = string(data)
	}
	return ws.Store.SaveLog(store.StoredLog{
		ShardID: shardID, InputFP: inputFP, SourceSHA256: sourceSHA, OriginalVersion: version,
		LogJSON: string(logJSON), UpgradeReportJSON: reportJSON,
	})
}

func (ws *Workspace) loadInitial(def model.ManifestShard) (replay.Initial, error) {
	w, h, pixels, pngData, err := imageio.ReadIndexed(filepath.Join(ws.Layout.Data, filepath.FromSlash(def.Image)))
	if err != nil {
		return replay.Initial{}, err
	}
	if w != def.Width || h != def.Height {
		return replay.Initial{}, fmt.Errorf("PNG dimensions %dx%d do not match manifest %dx%d", w, h, def.Width, def.Height)
	}
	if digest := model.SHA256Hex(pngData); digest != def.ImageSHA256 {
		return replay.Initial{}, fmt.Errorf("PNG digest %s does not match manifest %s", digest, def.ImageSHA256)
	}
	if digest := model.PixelDigest(w, h, pixels); digest != def.InitialPixelFP {
		return replay.Initial{}, fmt.Errorf("initial pixel digest %s does not match manifest %s", digest, def.InitialPixelFP)
	}
	fp := model.InputFingerprint(w, h, pngData, def.Segments)
	return replay.Initial{
		ShardID: def.ShardID, Width: w, Height: h, Pixels: pixels, Segments: def.Segments, InputFingerprint: fp,
	}, nil
}

func (ws *Workspace) syncShard(rt *ShardRuntime) error {
	metas, err := ws.Store.List(rt.Definition.ShardID)
	if err != nil {
		return err
	}
	history := historyFromMetas(metas)
	oldInput := ""
	oldShard := ""
	if len(metas) > 0 {
		oldInput = metas[len(metas)-1].InputFP
		oldShard = metas[len(metas)-1].ShardID
	}
	change := reconcile.ClassifyFingerprints(history, oldShard, oldInput, rt.Log, rt.Initial.InputFingerprint)
	report := SyncReport{
		ShardID: rt.Definition.ShardID, OriginalSchemaVersion: rt.OriginalVersion,
		Upgraded: rt.Upgrade != nil, Kind: change.Kind, EarliestAffected: change.EarliestAffectedRevision,
		ReusedRevision: change.ReusableRevision, InputFP: rt.Initial.InputFingerprint,
		LogFP: model.LogPrefixDigest(rt.Log.Revisions), Reasons: change.Reasons,
	}
	if len(metas) == 0 {
		report.Kind = "initial"
		if len(rt.Log.Revisions) > 0 {
			report.EarliestAffected = rt.Log.Revisions[0].Revision
		}
	}
	if change.Kind == reconcile.KindReplacement || change.Kind == reconcile.KindRollback || change.Kind == reconcile.KindFork {
		n, err := ws.Store.DeleteFrom(rt.Definition.ShardID, change.EarliestAffectedRevision)
		if err != nil {
			return err
		}
		report.RemovedCheckpoints = n
	}

	latest, ok, err := ws.Store.Latest(rt.Definition.ShardID)
	if err != nil {
		return err
	}
	var engine *replay.Engine
	if ok {
		engine, err = replay.Resume(model.CloneState(latest.State), rt.Log, ws.opts.Replay)
		if err != nil {
			// A checkpoint that is not a valid prefix cannot be trusted. Find the
			// newest prefix that does resume before falling back to revision zero.
			engine, err = ws.resumeNewestPrefix(rt, &report)
			if err != nil {
				return err
			}
		}
	} else {
		engine, err = replay.NewEngine(rt.Initial, rt.Log, ws.opts.Replay)
		if err != nil {
			return err
		}
	}
	replayed := []int{}
	for engine.Remaining() > 0 {
		rec, be := engine.ApplyNext()
		if be != nil {
			_ = ws.Store.RecordAudit(rt.Definition.ShardID, be.Revision, "replay", be.Code, be.Error())
			return be
		}
		state := model.CloneState(engine.State())
		snap := model.SnapshotFromState(state, *rec)
		seq, be := ws.Store.SaveCheckpoint(*rec, state, snap)
		if be != nil {
			return be
		}
		ws.checkpointCount++
		replayed = append(replayed, rec.Revision)
		if ws.opts.OnCheckpoint != nil {
			if err := ws.opts.OnCheckpoint(store.Checkpoint{Seq: seq, ShardID: state.ShardID, Revision: rec.Revision, SnapshotDigest: rec.SnapshotDigest}); err != nil {
				return err
			}
		}
		if ws.opts.CrashAfter > 0 && ws.checkpointCount >= ws.opts.CrashAfter && ws.opts.CrashFunc != nil {
			if err := ws.opts.CrashFunc(); err != nil {
				return err
			}
		}
	}
	state := model.CloneState(engine.State())
	rt.Engine = engine
	rt.State = state
	report.ReplayedRevisions = replayed
	report.HeadRevision = state.LastRevision
	if n := len(state.RevisionRecords); n > 0 {
		report.SnapshotDigest = state.RevisionRecords[n-1].SnapshotDigest
	}
	for rev := range state.Undone {
		report.UndoneRevisions = append(report.UndoneRevisions, rev)
	}
	sort.Ints(report.UndoneRevisions)
	rt.Sync = report
	return nil
}

func (ws *Workspace) resumeNewestPrefix(rt *ShardRuntime, report *SyncReport) (*replay.Engine, error) {
	metas, err := ws.Store.List(rt.Definition.ShardID)
	if err != nil {
		return nil, err
	}
	for i := len(metas) - 1; i >= 0; i-- {
		cp, ok, err := ws.Store.At(rt.Definition.ShardID, metas[i].Revision)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		engine, err := replay.Resume(model.CloneState(cp.State), rt.Log, ws.opts.Replay)
		if err == nil {
			report.ReusedRevision = cp.Revision
			return engine, nil
		}
		if _, err := ws.Store.DeleteFrom(rt.Definition.ShardID, metas[i].Revision); err != nil {
			return nil, err
		}
		report.RemovedCheckpoints++
	}
	engine, err := replay.NewEngine(rt.Initial, rt.Log, ws.opts.Replay)
	if err != nil {
		return nil, err
	}
	report.ReusedRevision = 0
	return engine, nil
}

func historyFromMetas(metas []store.CheckpointMeta) []reconcile.RevisionHistoryEntry {
	out := make([]reconcile.RevisionHistoryEntry, len(metas))
	for i, meta := range metas {
		out[i] = reconcile.RevisionHistoryEntry{
			Revision: meta.Revision, ParentRevision: meta.ParentRevision, RevisionFP: meta.RevisionFP,
		}
	}
	return out
}

func detectVersion(data []byte) int {
	var header struct {
		SchemaVersion int `json:"schema_version"`
		Version       int `json:"version"`
		LegacyVersion int `json:"legacy_version"`
	}
	if err := json.Unmarshal(data, &header); err == nil {
		if header.SchemaVersion != 0 {
			return header.SchemaVersion
		}
		if header.Version != 0 {
			return header.Version
		}
		return header.LegacyVersion
	}
	return 0
}
