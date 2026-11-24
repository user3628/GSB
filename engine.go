package cockpitaudio

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type ShardTimelineArtifact struct {
	ShardID  string   `json:"shard_id"`
	Timeline Timeline `json:"timeline"`
}

type QueryBatchItem struct {
	Request  QueryRequest    `json:"request"`
	CacheKey string          `json:"cache_key"`
	Result   json.RawMessage `json:"result"`
}

type QueryBatchArtifact struct {
	ShardID string           `json:"shard_id"`
	Items   []QueryBatchItem `json:"items"`
}

type RunReport struct {
	Result      *SessionResult `json:"result,omitempty"`
	Committed   []string       `json:"committed_units"`
	Skipped     []string       `json:"skipped_units"`
	Stopped     bool           `json:"stopped"`
	Checkpoints int            `json:"checkpoints_written"`
}

type Engine struct {
	ds          *Dataset
	cfg         Config
	store       *Store
	publisherID string
	edit        *EditState

	OnUnitCommitted func(committedTotal int, shardID, stage string, sequence int)
}

func NewEngine(ds *Dataset, cfg Config, store *Store, publisherID string) *Engine {
	if publisherID == "" {
		publisherID = "local-worker"
	}
	return &Engine{ds: ds, cfg: cfg, store: store, publisherID: publisherID}
}

func (e *Engine) WithEdit(edit *EditState) *Engine {
	e.edit = edit
	return e
}

func (e *Engine) currentEditVersion() int {
	if e.edit != nil {
		return e.edit.CurrentVersion()
	}
	return 0
}

func (e *Engine) editLog() []EditRecord {
	if e.edit != nil {
		return e.edit.LogCopy()
	}
	return []EditRecord{}
}

func (e *Engine) Run(ctx context.Context) (*RunReport, *Error) {
	configShardID := ""
	if len(e.ds.Manifest.Shards) > 0 {
		configShardID = e.ds.Manifest.Shards[0].ShardID
	}
	rc, rerr := e.cfg.ResolveVAD(configShardID)
	if rerr != nil {
		return nil, rerr
	}
	inputFP, ierr := InputFingerprint(e.ds)
	if ierr != nil {
		return nil, NewError(CodeInternal, StageIngest, "", "input fingerprint: %v", ierr)
	}
	report := &RunReport{Committed: []string{}, Skipped: []string{}}
	shardResults := []ShardResult{}
	timelineArtifacts := []ShardTimelineArtifact{}

	for shardIndex := range e.ds.Manifest.Shards {
		shard := e.ds.Manifest.Shards[shardIndex]
		shardID := shard.ShardID

		if stopErr := checkStopped(ctx, report); stopErr != nil {
			return report, stopErr
		}
		vad, vadFP, err := e.runVADStage(ctx, &shard, rc, report)
		if err != nil {
			return report, err
		}

		if stopErr := checkStopped(ctx, report); stopErr != nil {
			return report, stopErr
		}
		attr, err := e.runAttributionStage(ctx, &shard, vad, rc, report)
		if err != nil {
			return report, err
		}

		if stopErr := checkStopped(ctx, report); stopErr != nil {
			return report, stopErr
		}
		tlArtifact, err := e.runTimelineStage(ctx, &shard, vad, attr, rc, report)
		if err != nil {
			return report, err
		}
		timelineArtifacts = append(timelineArtifacts, *tlArtifact)

		if stopErr := checkStopped(ctx, report); stopErr != nil {
			return report, stopErr
		}
		if _, err := e.runQueryStage(ctx, &shard, vad, attr, tlArtifact, rc, report); err != nil {
			return report, err
		}

		shardResults = append(shardResults, ShardResult{
			ShardID: shardID, ShardFingerprint: vadFP, VAD: vad, Attribution: attr,
		})
	}

	allIntervals := []TimelineInterval{}
	allAttributions := []Attribution{}
	for _, ta := range timelineArtifacts {
		allIntervals = append(allIntervals, ta.Timeline.Intervals...)
		allAttributions = append(allAttributions, ta.Timeline.Attributions...)
	}
	timeline := BuildTimeline(allIntervals, allAttributions)
	editVersion := 0
	if e.edit != nil {
		editVersion = e.edit.CurrentVersion()
	}
	summary := summarize(shardResults, timeline, rc, editVersion)
	report.Result = &SessionResult{
		SessionID: e.ds.Manifest.SessionID, InputFingerprint: inputFP,
		ConfigVersion: rc.ConfigVersion, ConfigFingerprint: rc.ConfigFingerprint,
		EditVersion: editVersion, EditLog: e.editLog(), Shards: shardResults, Timeline: timeline, Summary: summary,
	}
	return report, nil
}

func checkStopped(ctx context.Context, report *RunReport) *Error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			report.Stopped = true
			return &Error{Code: CodeStopped, Stage: "scheduler", Message: "stop requested at indivisible-unit boundary"}
		default:
		}
	}
	return nil
}

func (e *Engine) runVADStage(ctx context.Context, shard *ShardManifest, rc ResolvedConfig, report *RunReport) (*ShardVAD, string, *Error) {
	unit := shard.ShardID + "/" + StageVAD
	if cp, ok, err := e.loadCurrent(shard.ShardID, StageVAD, rc.ConfigVersion, &ShardVAD{}); err != nil {
		return nil, "", err
	} else if ok {
		report.Skipped = append(report.Skipped, unit+"@"+fmt.Sprint(cp.Sequence))
		var vad ShardVAD
		if uerr := json.Unmarshal([]byte(cp.PayloadJSON), &vad); uerr != nil {
			return nil, "", NewError(CodeInternal, StageVAD, shard.ShardID, "rehydrate: %v", uerr)
		}
		return &vad, cp.ShardFingerprint, nil
	}

	wavPath := e.ds.Root + "/" + shard.WAVFile
	wavBytes, rerr := readFileBytes(wavPath)
	if rerr != nil {
		return nil, "", NewError(CodeInternal, StageVAD, shard.ShardID, "read wav: %v", rerr)
	}
	wav, rerr := ReadWAV(wavPath)
	if rerr != nil {
		return nil, "", NewError(CodeInternal, StageVAD, shard.ShardID, "parse wav: %v", rerr)
	}
	fp, ferr := ShardFingerprint(shard, wavBytes)
	if ferr != nil {
		return nil, "", NewError(CodeInternal, StageVAD, shard.ShardID, "fingerprint: %v", ferr)
	}
	vad, verr := RunVAD(shard, wav, e.cfg)
	if verr != nil {
		return nil, "", verr
	}
	vad.ShardFingerprint = fp
	rehydrated, rerr := roundTripJSON(vad)
	if rerr != nil {
		return nil, "", NewError(CodeInternal, StageVAD, shard.ShardID, "canonicalize: %v", rerr)
	}
	cp, perr := e.publish(shard.ShardID, StageVAD, fp, rc.ConfigVersion, e.currentEditVersion(), rehydrated)
	if perr != nil {
		return nil, "", perr
	}
	e.recordCommit(report, unit, cp)
	return rehydrated, fp, nil
}

func (e *Engine) runAttributionStage(ctx context.Context, shard *ShardManifest, vad *ShardVAD, rc ResolvedConfig, report *RunReport) (*ShardAttribution, *Error) {
	unit := shard.ShardID + "/" + StageAttribution
	if cp, ok, err := e.loadCurrent(shard.ShardID, StageAttribution, rc.ConfigVersion, &ShardAttribution{}); err != nil {
		return nil, err
	} else if ok {
		report.Skipped = append(report.Skipped, unit+"@"+fmt.Sprint(cp.Sequence))
		var attr ShardAttribution
		_ = json.Unmarshal([]byte(cp.PayloadJSON), &attr)
		return &attr, nil
	}

	opts := AttributionOptions{EditVersion: 0}
	if e.edit != nil {
		opts.Intervals = e.edit.EffectiveIntervals()
		opts.PriorityOverrides = e.edit.PriorityOverrideMap()
		opts.HitOverrides = e.edit.HitOverrides()
		opts.EditVersion = e.edit.CurrentVersion()
	} else {
		intervals := []Interval{}
		for _, z := range vad.Zones {
			intervals = append(intervals, z.Intervals...)
		}
		opts.Intervals = intervals
	}
	attr, aerr := RunAttribution(shard, vad, e.cfg, opts)
	if aerr != nil {
		return nil, aerr
	}
	attr.ConfigFingerprint = rc.ConfigFingerprint
	rehydrated, rerr := roundTripJSON(attr)
	if rerr != nil {
		return nil, NewError(CodeInternal, StageAttribution, shard.ShardID, "canonicalize: %v", rerr)
	}
	cp, perr := e.publish(shard.ShardID, StageAttribution, vad.ShardFingerprint, rc.ConfigVersion, opts.EditVersion, rehydrated)
	if perr != nil {
		return nil, perr
	}
	e.recordCommit(report, unit, cp)
	return rehydrated, nil
}

func (e *Engine) runTimelineStage(ctx context.Context, shard *ShardManifest, vad *ShardVAD, attr *ShardAttribution, rc ResolvedConfig, report *RunReport) (*ShardTimelineArtifact, *Error) {
	unit := shard.ShardID + "/" + StageTimeline
	if cp, ok, err := e.loadCurrent(shard.ShardID, StageTimeline, rc.ConfigVersion, &ShardTimelineArtifact{}); err != nil {
		return nil, err
	} else if ok {
		report.Skipped = append(report.Skipped, unit+"@"+fmt.Sprint(cp.Sequence))
		var tl ShardTimelineArtifact
		_ = json.Unmarshal([]byte(cp.PayloadJSON), &tl)
		return &tl, nil
	}

	var edit *EditState
	if e.edit != nil {
		edit = e.edit
	} else {
		intervals := []Interval{}
		for _, z := range vad.Zones {
			intervals = append(intervals, z.Intervals...)
		}
		edit = NewEditState(intervals, e.cfg.Priorities)
	}
	intervals := []TimelineInterval{}
	start, end := shard.TimelineStartMS, shard.TimelineEndMS
	for _, in := range edit.TimelineIntervals() {
		if in.EndMS > start && in.StartMS < end {
			intervals = append(intervals, in)
		}
	}
	tl := BuildTimeline(intervals, attr.Items)
	artifact := &ShardTimelineArtifact{ShardID: shard.ShardID, Timeline: tl}
	rehydrated, rerr := roundTripJSON(artifact)
	if rerr != nil {
		return nil, NewError(CodeInternal, StageTimeline, shard.ShardID, "canonicalize: %v", rerr)
	}
	editVersion := 0
	if e.edit != nil {
		editVersion = e.edit.CurrentVersion()
	}
	cp, perr := e.publish(shard.ShardID, StageTimeline, vad.ShardFingerprint, rc.ConfigVersion, editVersion, rehydrated)
	if perr != nil {
		return nil, perr
	}
	e.recordCommit(report, unit, cp)
	return rehydrated, nil
}

func (e *Engine) runQueryStage(ctx context.Context, shard *ShardManifest, vad *ShardVAD, attr *ShardAttribution, tl *ShardTimelineArtifact, rc ResolvedConfig, report *RunReport) (*QueryBatchArtifact, *Error) {
	unit := shard.ShardID + "/" + StageQuery
	if cp, ok, err := e.loadCurrent(shard.ShardID, StageQuery, rc.ConfigVersion, &QueryBatchArtifact{}); err != nil {
		return nil, err
	} else if ok {
		report.Skipped = append(report.Skipped, unit+"@"+fmt.Sprint(cp.Sequence))
		var batch QueryBatchArtifact
		_ = json.Unmarshal([]byte(cp.PayloadJSON), &batch)
		return &batch, nil
	}

	editVersion := 0
	if e.edit != nil {
		editVersion = e.edit.CurrentVersion()
	}
	tmpResult := &SessionResult{
		ConfigVersion: rc.ConfigVersion, ConfigFingerprint: rc.ConfigFingerprint, EditVersion: editVersion,
		Shards:   []ShardResult{{ShardID: shard.ShardID, VAD: vad, Attribution: attr}},
		Timeline: tl.Timeline,
	}
	svc, qerr := NewQueryService(e.cfg, tmpResult)
	if qerr != nil {
		return nil, qerr
	}
	requests := []QueryRequest{
		{QueryID: shard.ShardID + "-active-250", Type: QueryActiveZones, AtMS: shard.TimelineStartMS + 250, ShardID: shard.ShardID},
		{QueryID: shard.ShardID + "-active-650", Type: QueryActiveZones, AtMS: shard.TimelineStartMS + 650, ShardID: shard.ShardID},
	}
	for _, a := range attr.Items {
		requests = append(requests,
			QueryRequest{QueryID: a.CommandID + "-basis", Type: QueryAttributionBasis, CommandID: a.CommandID, ShardID: shard.ShardID},
			QueryRequest{QueryID: a.CommandID + "-delay", Type: QueryDelayDetails, CommandID: a.CommandID, ShardID: shard.ShardID},
		)
	}
	batch := &QueryBatchArtifact{ShardID: shard.ShardID, Items: []QueryBatchItem{}}
	for _, req := range requests {
		key, _, kerr := svc.cacheKey(req)
		if kerr != nil {
			return nil, NewError(CodeInternal, StageQuery, shard.ShardID, "cache key: %v", kerr)
		}
		raw, cerr := svc.computeCanonical(req)
		if cerr != nil {
			return nil, cerr
		}
		batch.Items = append(batch.Items, QueryBatchItem{Request: req, CacheKey: key, Result: raw})
	}
	rehydrated, rerr := roundTripJSON(batch)
	if rerr != nil {
		return nil, NewError(CodeInternal, StageQuery, shard.ShardID, "canonicalize: %v", rerr)
	}
	cp, perr := e.publish(shard.ShardID, StageQuery, vad.ShardFingerprint, rc.ConfigVersion, editVersion, rehydrated)
	if perr != nil {
		return nil, perr
	}
	e.recordCommit(report, unit, cp)
	return rehydrated, nil
}

func (e *Engine) loadCurrent(shardID, stage, configVersion string, prototype any) (*Checkpoint, bool, *Error) {
	cp, err := e.store.LatestCheckpoint(shardID, stage)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, NewError(CodeInternal, stage, shardID, "latest checkpoint: %v", err)
	}
	if cp.ConfigVersion != configVersion {
		return nil, false, nil
	}
	if err := json.Unmarshal([]byte(cp.PayloadJSON), prototype); err != nil {
		return nil, false, NewError(CodeInternal, stage, shardID, "decode checkpoint: %v", err)
	}
	if ver, verr := e.store.VerifySnapshot(shardID, stage); verr != nil {
		return cp, false, verr
	} else if ver == nil || !ver.Healthy {
		return cp, false, NewError(CodeStaleCheckpoint, stage, shardID, "snapshot verification failed")
	}
	return cp, true, nil
}

func (e *Engine) publish(shardID, stage, fp, configVersion string, editVersion int, artifact any) (*Checkpoint, *Error) {
	out, err := e.store.Publish(PublishInput{
		ShardID: shardID, Stage: stage, Sequence: 0, ShardFingerprint: fp,
		ConfigVersion: configVersion, EditVersion: editVersion, Artifact: artifact,
		PublisherID: e.publisherID,
	})
	if err != nil {
		return nil, err
	}
	return &out.Checkpoint, nil
}

func (e *Engine) recordCommit(report *RunReport, unit string, cp *Checkpoint) {
	report.Committed = append(report.Committed, fmt.Sprintf("%s@%d", unit, cp.Sequence))
	report.Checkpoints++
	if e.OnUnitCommitted != nil {
		e.OnUnitCommitted(len(report.Committed), cp.ShardID, cp.Stage, cp.Sequence)
	}
}

func roundTripJSON[T any](v *T) (*T, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return nil, err
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func readFileBytes(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func summarize(shards []ShardResult, timeline Timeline, rc ResolvedConfig, editVersion int) Summary {
	commandCount, resolved, ambiguous := 0, 0, 0
	responsible := map[string]int{}
	for _, s := range shards {
		if s.Attribution == nil {
			continue
		}
		for _, a := range s.Attribution.Items {
			commandCount++
			if a.Ambiguous {
				ambiguous++
			} else {
				resolved++
				responsible[a.ResponsibleZoneID]++
			}
		}
	}
	return Summary{
		ShardCount: len(shards), ZoneCount: len(shards) * len(ZoneOrder),
		IntervalCount: len(timeline.Intervals), OverlapCount: len(timeline.Overlaps),
		CommandCount: commandCount, ResolvedCount: resolved, AmbiguousCount: ambiguous,
		ResponsibleByZone: responsible, MaxEditVersion: editVersion,
		ConfigVersion: rc.ConfigVersion, ConfigFingerprint: rc.ConfigFingerprint,
	}
}
