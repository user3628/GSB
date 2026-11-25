package engine

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"driftengine/internal/batch"
	"driftengine/internal/drift"
	"driftengine/internal/errs"
	"driftengine/internal/spec"
	"driftengine/internal/store"
)

type Engine struct {
	store       *store.Store
	endpointURL string

	writeMu sync.Mutex
	view    atomic.Pointer[View]

	progressMu sync.Mutex

	inflightMu sync.Mutex
	inflight   map[string]*batchRun

	schedMu sync.RWMutex
	sched   *Scheduler
}

type batchRun struct {
	requestHash string
	done        chan struct{}
	result      *BatchTriggerResult
	err         error
}

type BatchTriggerResult struct {
	BatchID      string             `json:"batch_id"`
	Status       string             `json:"status"`
	TriggerCount int                `json:"trigger_count"`
	ObjectPhases map[string]string  `json:"object_phases"`
	Errors       []BatchObjectError `json:"errors,omitempty"`
}

type BatchObjectError struct {
	ObjectID string `json:"object_id"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

func New(st *store.Store, endpointURL string) *Engine {
	e := &Engine{store: st, endpointURL: endpointURL, inflight: map[string]*batchRun{}}
	e.sched = NewScheduler(2, 16)
	return e
}

func (e *Engine) Bootstrap(ctx context.Context) error {
	version, err := e.store.CurrentVersion(ctx)
	if err != nil || version == 0 {
		return err
	}
	row, err := e.store.LoadCommittedVersion(ctx, version)
	if err != nil {
		return err
	}
	view, err := viewFromRow(row)
	if err != nil {
		return err
	}
	objectStates, err := e.store.ListObjectStates(ctx, version)
	if err != nil {
		return err
	}
	for id, raw := range objectStates {
		var state ObjectState
		if err := json.Unmarshal([]byte(raw), &state); err == nil {
			view.ObjectStates[id] = &state
		}
	}
	batchStates, err := e.store.ListBatchStates(ctx, version)
	if err != nil {
		return err
	}
	for id, raw := range batchStates {
		var state BatchState
		if err := json.Unmarshal([]byte(raw), &state); err == nil {
			view.BatchStates[id] = &state
		}
	}
	view.Summary = buildSummary(view)
	e.view.Store(view)
	e.resetScheduler(view.Snapshot.Policy)
	return nil
}

func viewFromRow(row store.CommittedVersion) (*View, error) {
	var snapshot spec.Snapshot
	if err := json.Unmarshal([]byte(row.SnapshotJSON), &snapshot); err != nil {
		return nil, err
	}
	driftReports := map[string]drift.Report{}
	if row.DriftJSON != "" {
		if err := json.Unmarshal([]byte(row.DriftJSON), &driftReports); err != nil {
			return nil, err
		}
	}
	var plan batch.Plan
	if row.PlanJSON != "" {
		if err := json.Unmarshal([]byte(row.PlanJSON), &plan); err != nil {
			return nil, err
		}
	}
	var summary Summary
	if row.SummaryJSON != "" {
		if err := json.Unmarshal([]byte(row.SummaryJSON), &summary); err != nil {
			return nil, err
		}
	}
	return &View{
		Version:      row.Version,
		BaseVersion:  row.BaseVersion,
		Kind:         row.Kind,
		Snapshot:     &snapshot,
		Drift:        driftReports,
		Plan:         &plan,
		Summary:      summary,
		ObjectStates: map[string]*ObjectState{},
		BatchStates:  map[string]*BatchState{},
		CommittedAt:  row.CreatedAt,
	}, nil
}

func (e *Engine) View() *View { return e.view.Load() }

func (e *Engine) CurrentVersion() int64 {
	if view := e.View(); view != nil {
		return view.Version
	}
	return 0
}

func (e *Engine) SubmitSnapshotData(ctx context.Context, data []byte) (*View, error) {
	snapshot, err := spec.ParseSnapshot(data)
	if err != nil {
		return nil, err
	}
	return e.SubmitSnapshot(ctx, snapshot)
}

func (e *Engine) SubmitSnapshot(ctx context.Context, snapshot *spec.Snapshot) (*View, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	current := e.CurrentVersion()
	if snapshot.Version <= current {
		return nil, errs.New(errs.StaleVersion, "snapshot version must be greater than current committed version", map[string]int64{
			"submitted": snapshot.Version,
			"current":   current,
		})
	}
	intentID := "snapshot-switch:v" + itoa(snapshot.Version)
	if err := e.store.UpsertIntent(ctx, store.Intent{
		ID: intentID, Type: "snapshot_switch", Version: snapshot.Version,
		PayloadJSON: `{"digest":` + jsonString(snapshot.Digest) + `}`, Status: "prepared",
	}); err != nil {
		return nil, errs.New(errs.Internal, "cannot persist snapshot intent", err.Error())
	}

	view := buildView(snapshot, 0, "snapshot", time.Now().UTC())
	if err := e.commitView(ctx, view); err != nil {
		return nil, err
	}
	if err := e.store.SetIntentStatus(ctx, intentID, "committed", ""); err != nil {
		return nil, err
	}
	e.view.Store(view)
	e.resetScheduler(snapshot.Policy)
	return view, nil
}

func (e *Engine) SubmitIncrementData(ctx context.Context, data []byte) (*View, error) {
	increment, err := spec.ParseIncrement(data)
	if err != nil {
		return nil, err
	}
	return e.SubmitIncrement(ctx, increment)
}

func (e *Engine) SubmitIncrement(ctx context.Context, increment *spec.Increment) (*View, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	current := e.View()
	if current == nil || current.Snapshot == nil {
		return nil, errs.New(errs.StaleBase, "cannot apply increment without a committed base snapshot")
	}
	if increment.BaseVersion != current.Version {
		return nil, errs.New(errs.StaleBase, "increment base_version is not the current committed version", map[string]int64{
			"base_version": increment.BaseVersion,
			"current":      current.Version,
		})
	}

	mergedObjects := mergeObjects(current.Snapshot.Objects, increment.Objects)
	if err := spec.ValidateObjects(mergedObjects); err != nil {
		return nil, err
	}
	nextVersion := current.Version + 1
	generatedAt := increment.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}
	next := &spec.Snapshot{
		Schema:      current.Snapshot.Schema,
		Version:     nextVersion,
		GeneratedAt: generatedAt,
		Policy:      current.Snapshot.Policy,
		Objects:     mergedObjects,
	}
	digest, _, err := spec.DigestOf(next)
	if err != nil {
		return nil, errs.New(errs.Internal, err.Error())
	}
	next.Digest = digest

	switchIntent := "snapshot-switch:v" + itoa(nextVersion)
	if err := e.store.UpsertIntent(ctx, store.Intent{
		ID: switchIntent, Type: "snapshot_switch", Version: nextVersion,
		PayloadJSON: `{"base_version":` + itoa(nextVersion-1) + `,"increment_digest":` + jsonString(increment.Digest) + `}`,
		Status:      "prepared",
	}); err != nil {
		return nil, errs.New(errs.Internal, err.Error())
	}
	incIntent := "increment-commit:" + increment.IdempotencyKey
	if increment.IdempotencyKey == "" {
		incIntent = "increment-commit:v" + itoa(nextVersion) + ":" + shortDigest(increment.Digest)
	}
	_ = e.store.UpsertIntent(ctx, store.Intent{
		ID: incIntent, Type: "increment_commit", Version: nextVersion,
		PayloadJSON: `{"base_version":` + itoa(increment.BaseVersion) + `,"digest":` + jsonString(increment.Digest) + `}`,
		Status:      "committed",
	})

	view := buildView(next, increment.BaseVersion, "increment", time.Now().UTC())
	if err := e.commitView(ctx, view); err != nil {
		return nil, err
	}
	if err := e.store.SetIntentStatus(ctx, switchIntent, "committed", ""); err != nil {
		return nil, err
	}
	e.view.Store(view)
	e.resetScheduler(next.Policy)
	return view, nil
}

func (e *Engine) commitView(ctx context.Context, view *View) error {
	snapshotJSON, err := json.Marshal(view.Snapshot)
	if err != nil {
		return errs.New(errs.Internal, err.Error())
	}
	driftJSON, err := json.Marshal(view.Drift)
	if err != nil {
		return errs.New(errs.Internal, err.Error())
	}
	planJSON, err := json.Marshal(view.Plan)
	if err != nil {
		return errs.New(errs.Internal, err.Error())
	}
	view.Summary = buildSummary(view)
	summaryJSON, err := json.Marshal(view.Summary)
	if err != nil {
		return errs.New(errs.Internal, err.Error())
	}
	objectStates := map[string]string{}
	for id, state := range view.ObjectStates {
		raw, err := json.Marshal(state)
		if err != nil {
			return errs.New(errs.Internal, err.Error())
		}
		objectStates[id] = string(raw)
	}
	batchStates := map[string]string{}
	for id, state := range view.BatchStates {
		raw, err := json.Marshal(state)
		if err != nil {
			return errs.New(errs.Internal, err.Error())
		}
		batchStates[id] = string(raw)
	}
	return e.store.SaveCommitBundle(ctx, store.CommitBundle{
		Version: store.CommittedVersion{
			Version:      view.Version,
			BaseVersion:  view.BaseVersion,
			Kind:         view.Kind,
			Digest:       view.Snapshot.Digest,
			SnapshotJSON: string(snapshotJSON),
			DriftJSON:    string(driftJSON),
			PlanJSON:     string(planJSON),
			SummaryJSON:  string(summaryJSON),
			CreatedAt:    view.CommittedAt,
		},
		ObjectStates: objectStates,
		BatchStates:  batchStates,
	})
}

func buildView(snapshot *spec.Snapshot, baseVersion int64, kind string, committedAt time.Time) *View {
	reports := drift.EvaluateAll(snapshot.Objects, snapshot.Policy)
	plan, cycleErr := batch.Build(snapshot.Version, snapshot.Objects, snapshot.Policy)
	if cycleErr != nil && plan == nil {
		plan = &batch.Plan{Version: snapshot.Version, DependencyOrderRule: snapshot.Policy.DependencyOrderRule}
	}
	view := &View{
		Version:      snapshot.Version,
		BaseVersion:  baseVersion,
		Kind:         kind,
		Snapshot:     snapshot,
		Drift:        reports,
		Plan:         plan,
		ObjectStates: map[string]*ObjectState{},
		BatchStates:  map[string]*BatchState{},
		CommittedAt:  committedAt,
	}
	for _, object := range snapshot.Objects {
		report := reports[object.ID]
		phase := PhaseDriftDetected
		if report.InSync {
			phase = PhaseObservedInSync
		}
		view.ObjectStates[object.ID] = &ObjectState{
			ObjectID:        object.ID,
			Phase:           phase,
			PrimaryCategory: report.PrimaryCategory,
			HitCategories:   report.Categories,
			Attempts:        0,
			MaxRetries:      snapshot.Policy.MaxRetries,
			Differences:     report.Differences,
			FieldDigest:     digestFields(object.Desired),
			UpdatedAt:       committedAt,
		}
	}
	for _, b := range plan.Batches {
		hash, _, _ := spec.DigestOf(b)
		phases := map[string]string{}
		for _, id := range b.ObjectIDs {
			if state := view.ObjectStates[id]; state != nil {
				phases[id] = state.Phase
			}
		}
		now := committedAt
		view.BatchStates[b.BatchID] = &BatchState{
			BatchID:      b.BatchID,
			BatchNumber:  b.BatchNumber,
			RequestHash:  hash,
			Status:       "planned",
			TriggerCount: 0,
			ObjectIDs:    append([]string(nil), b.ObjectIDs...),
			ObjectPhases: phases,
			UpdatedAt:    now,
		}
	}
	view.Summary = buildSummary(view)
	return view
}

func buildSummary(view *View) Summary {
	snapshot := view.Snapshot
	summary := Summary{
		Version:               view.Version,
		SnapshotDigest:        snapshot.Digest,
		TotalObjects:          len(snapshot.Objects),
		ObjectRefCount:        len(snapshot.Objects),
		BatchCount:            len(view.Plan.Batches),
		DependencyCycle:       view.Plan.DependencyCycle,
		CommittedAt:           view.CommittedAt,
		PhaseCounts:           map[string]int{},
		PrimaryCategoryCounts: map[string]int{},
		BatchStatusCounts:     map[string]int{},
	}
	edgeCount := 0
	edgeRefs := []map[string]string{}
	for _, object := range snapshot.Objects {
		edgeCount += len(object.Dependencies)
		for _, dep := range object.Dependencies {
			edgeRefs = append(edgeRefs, map[string]string{"edge_id": object.ID + "->" + dep})
		}
		state := view.ObjectStates[object.ID]
		if state == nil {
			continue
		}
		summary.PhaseCounts[state.Phase]++
		switch state.Phase {
		case PhaseObservedInSync:
			summary.ObservedInSync++
		case PhaseDriftDetected:
			summary.DriftedObjects++
		case PhaseConverged:
			summary.VerifiedConverged++
		case PhasePendingWrite, PhaseWriteTimeout:
			if state.Phase == PhasePendingWrite {
				summary.PendingWrite++
			}
		case PhaseRetryExhausted, PhaseFailed:
			summary.FailedObjects++
		}
		if state.PrimaryCategory != "" && state.PrimaryCategory != spec.CategoryConverged {
			summary.PrimaryCategoryCounts[state.PrimaryCategory]++
		}
	}
	summary.EdgeRefCount = edgeCount
	refDigest, _, _ := spec.DigestOf(map[string]any{
		"snapshot_digest": snapshot.Digest,
		"object_count":    summary.ObjectRefCount,
		"edge_refs":       edgeRefs,
	})
	summary.RefsDigest = refDigest
	for _, batchState := range view.BatchStates {
		summary.BatchStatusCounts[batchState.Status]++
	}
	return summary
}

func mergeObjects(base, changes []spec.Object) []spec.Object {
	byID := map[string]spec.Object{}
	order := []string{}
	for _, object := range base {
		byID[object.ID] = object
		order = append(order, object.ID)
	}
	for _, object := range changes {
		if _, exists := byID[object.ID]; !exists {
			order = append(order, object.ID)
		}
		byID[object.ID] = object
	}
	sort.Strings(order)
	out := make([]spec.Object, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out
}

func digestFields(fields spec.Fields) string {
	digest, _, _ := spec.DigestOf(fields)
	return digest
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}

func shortDigest(digest string) string {
	if len(digest) > 18 {
		return digest[7:19]
	}
	return digest
}
