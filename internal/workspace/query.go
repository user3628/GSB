package workspace

import (
	"fmt"
	"sort"
	"strings"

	"maskrevision/internal/model"
	"maskrevision/internal/replay"
	"maskrevision/internal/store"
)

type RevisionView struct {
	Revision           int          `json:"revision"`
	ParentRevision     int          `json:"parent_revision,omitempty"`
	Action             string       `json:"action"`
	Status             string       `json:"status"`
	UndoOf             int          `json:"undo_of,omitempty"`
	Actor              string       `json:"actor,omitempty"`
	Region             model.Region `json:"region,omitempty"`
	CategoryID         int          `json:"category_id,omitempty"`
	TargetSegmentID    int          `json:"target_segment_id,omitempty"`
	Confidence         float64      `json:"confidence,omitempty"`
	InferenceBasis     string       `json:"inference_basis,omitempty"`
	Unrecoverable      string       `json:"unrecoverable,omitempty"`
	SnapshotDigest     string       `json:"snapshot_digest,omitempty"`
	PixelDigest        string       `json:"pixel_digest,omitempty"`
	SegmentTableDigest string       `json:"segment_table_digest,omitempty"`
}

type ShardSummary struct {
	ShardID             string `json:"shard_id"`
	Image               string `json:"image"`
	Log                 string `json:"log"`
	SchemaVersion       int    `json:"schema_version"`
	Upgraded            bool   `json:"upgraded"`
	Revisions           int    `json:"revisions"`
	HeadRevision        int    `json:"head_revision"`
	UndoneRevisions     []int  `json:"undone_revisions"`
	InputFP             string `json:"input_fp"`
	LogFP               string `json:"log_fp"`
	FinalSnapshotDigest string `json:"final_snapshot_digest"`
	Checkpoints         int    `json:"checkpoints"`
	SyncKind            string `json:"sync_kind"`
	SyncReplayed        []int  `json:"sync_replayed_revisions"`
	UnrecoverableCount  int    `json:"unrecoverable_v1_actions"`
}

func (ws *Workspace) shard(id string) (*ShardRuntime, *model.BusinessError) {
	rt, ok := ws.Shards[id]
	if !ok {
		return nil, model.WithContext(model.NewBusinessError("E_SHARD_NOT_FOUND", "unknown shard", nil), id, 0)
	}
	return rt, nil
}

func (ws *Workspace) accessibleCheckpoint(id string, revision int) (*ShardRuntime, *store.Checkpoint, *model.BusinessError) {
	rt, be := ws.shard(id)
	if be != nil {
		return nil, nil, be
	}
	if revision <= 0 || revision > rt.State.LastRevision {
		return nil, nil, model.WithContext(model.NewBusinessError("E_REVISION_OUT_OF_RANGE", "revision is outside the replayed range", map[string]any{
			"requested_revision": revision, "head_revision": rt.State.LastRevision,
		}), id, revision)
	}
	if rt.State.Undone[revision] {
		reason := "revision was undone"
		for _, rec := range rt.State.RevisionRecords {
			if rec.UndoOf == revision {
				reason = fmt.Sprintf("revision %d was undone by revision %d", revision, rec.Revision)
			}
		}
		return nil, nil, model.WithContext(model.NewBusinessError("E_REVISION_UNDONE", reason, map[string]any{
			"requested_revision": revision, "reason": reason,
		}), id, revision)
	}
	cp, ok, err := ws.Store.At(id, revision)
	if err != nil {
		return nil, nil, model.WithContext(model.NewBusinessError("E_CHECKPOINT_READ", err.Error(), nil), id, revision)
	}
	if !ok {
		return nil, nil, model.WithContext(model.NewBusinessError("E_REVISION_OUT_OF_RANGE", "no checkpoint exists for this revision", map[string]any{
			"requested_revision": revision, "head_revision": rt.State.LastRevision,
		}), id, revision)
	}
	return rt, cp, nil
}

func (ws *Workspace) Snapshot(id string, revision int) (model.Snapshot, *model.BusinessError) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	_, cp, be := ws.accessibleCheckpoint(id, revision)
	if be != nil {
		return model.Snapshot{}, be
	}
	snap := *cp.Snapshot
	if snap.SnapshotDigest != cp.SnapshotDigest || model.SnapshotDigest(&snap) != snap.SnapshotDigest {
		return model.Snapshot{}, model.WithContext(model.NewBusinessError("E_CHECKPOINT_CORRUPT", "snapshot digest does not match checkpoint", map[string]any{
			"checkpoint_digest": cp.SnapshotDigest, "snapshot_digest": snap.SnapshotDigest,
		}), id, revision)
	}
	return snap, nil
}

func (ws *Workspace) Diff(id string, fromRevision, toRevision int) (replay.DiffResult, *model.BusinessError) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	_, fromCP, be := ws.accessibleCheckpoint(id, fromRevision)
	if be != nil {
		return replay.DiffResult{}, be
	}
	_, toCP, be := ws.accessibleCheckpoint(id, toRevision)
	if be != nil {
		return replay.DiffResult{}, be
	}
	return replay.Diff(fromRevision, toRevision, fromCP.State, toCP.State, toCP.State.RevisionRecords)
}

func (ws *Workspace) Revisions(id string) ([]RevisionView, *model.BusinessError) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	rt, be := ws.shard(id)
	if be != nil {
		return nil, be
	}
	records := map[int]model.RevisionRecord{}
	for _, rec := range rt.State.RevisionRecords {
		records[rec.Revision] = rec
	}
	out := make([]RevisionView, 0, len(rt.Log.Revisions))
	for _, rev := range rt.Log.Revisions {
		v := RevisionView{
			Revision: rev.Revision, ParentRevision: rev.ParentRevision, Action: rev.Action,
			UndoOf: rev.UndoOf, Actor: rev.Actor, Region: rev.Region, CategoryID: rev.CategoryID,
			TargetSegmentID: rev.TargetSegmentID, Confidence: rev.Confidence, InferenceBasis: rev.InferenceBasis,
			Unrecoverable: rev.UnrecoverableReason, Status: model.StatusActive,
		}
		if rec, ok := records[rev.Revision]; ok {
			v.UndoOf = rec.UndoOf
			v.SnapshotDigest = rec.SnapshotDigest
			v.PixelDigest = rec.PixelDigest
			v.SegmentTableDigest = rec.SegmentDigest
		}
		if rt.State.Undone[rev.Revision] {
			v.Status = "undone"
		}
		out = append(out, v)
	}
	return out, nil
}

func (ws *Workspace) ShardsSummary() ([]ShardSummary, error) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	ids := append([]string(nil), ws.Order...)
	sort.Strings(ids)
	out := make([]ShardSummary, 0, len(ids))
	for _, id := range ids {
		rt := ws.Shards[id]
		metas, err := ws.Store.List(id)
		if err != nil {
			return nil, err
		}
		undone := make([]int, 0, len(rt.State.Undone))
		for rev := range rt.State.Undone {
			undone = append(undone, rev)
		}
		sort.Ints(undone)
		unrec := 0
		if rt.Upgrade != nil {
			unrec = rt.Upgrade.UnrecoverableCount
		}
		out = append(out, ShardSummary{
			ShardID: id, Image: rt.Definition.Image, Log: rt.Definition.Log,
			SchemaVersion: rt.OriginalVersion, Upgraded: rt.Upgrade != nil,
			Revisions: len(rt.Log.Revisions), HeadRevision: rt.State.LastRevision,
			UndoneRevisions: undone, InputFP: rt.Initial.InputFingerprint,
			LogFP: model.LogPrefixDigest(rt.Log.Revisions), FinalSnapshotDigest: rt.Sync.SnapshotDigest,
			Checkpoints: len(metas), SyncKind: rt.Sync.Kind, SyncReplayed: rt.Sync.ReplayedRevisions,
			UnrecoverableCount: unrec,
		})
	}
	return out, nil
}

func (ws *Workspace) Checkpoints(id string) ([]store.CheckpointMeta, *model.BusinessError) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	if _, be := ws.shard(id); be != nil {
		return nil, be
	}
	metas, err := ws.Store.List(id)
	if err != nil {
		return nil, model.NewBusinessError("E_CHECKPOINT_READ", err.Error(), nil)
	}
	return metas, nil
}

func (ws *Workspace) Shard(id string) (*ShardRuntime, *model.BusinessError) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	rt, be := ws.shard(id)
	if be != nil {
		return nil, be
	}
	cp := *rt
	cp.State = model.CloneState(rt.State)
	return &cp, nil
}

func (ws *Workspace) Digest() string {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	ids := append([]string(nil), ws.Order...)
	sort.Strings(ids)
	var b strings.Builder
	b.WriteString("workspace-digest-v1\x00")
	for _, id := range ids {
		rt := ws.Shards[id]
		b.WriteString(id)
		b.WriteByte(0)
		b.WriteString(rt.Initial.InputFingerprint)
		b.WriteByte(0)
		b.WriteString(model.LogPrefixDigest(rt.Log.Revisions))
		b.WriteByte(0)
		fmt.Fprintf(&b, "%d\x00", rt.State.LastRevision)
		b.WriteString(rt.Sync.SnapshotDigest)
		b.WriteByte(0)
		for _, rev := range rt.Sync.UndoneRevisions {
			fmt.Fprintf(&b, "u%d,", rev)
		}
		b.WriteByte(0)
		if rt.Upgrade != nil {
			b.WriteString(model.SHA256Hex([]byte(fmt.Sprintf("%+v", rt.Upgrade.Inferences))))
		}
		b.WriteByte(0)
	}
	return model.SHA256Hex([]byte(b.String()))
}
