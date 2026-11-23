package configimpact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func loadSampleDocument(t *testing.T) *Document {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("testdata", "sample.json"))
	if err != nil {
		t.Fatalf("read sample: %v", err)
	}
	doc, err := DecodeDocument(payload)
	if err != nil {
		t.Fatalf("decode sample: %v", err)
	}
	return doc
}

func loadSamplePatch(t *testing.T) Patch {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("testdata", "patch.json"))
	if err != nil {
		t.Fatalf("read patch: %v", err)
	}
	patch, err := DecodePatch(payload)
	if err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	return *patch
}

func newTestEngine(t *testing.T, options Options) (*Engine, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "impact.db")
	engine, err := Open(context.Background(), path, options)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	return engine, path
}

func loadSample(t *testing.T, engine *Engine) LoadResult {
	t.Helper()
	result, err := engine.LoadDocument(context.Background(), loadSampleDocument(t))
	if err != nil {
		t.Fatalf("load sample: %v", err)
	}
	return result
}

func requiredValue(t *testing.T, config MaterializedConfig, key string) EffectiveValue {
	t.Helper()
	value, exists := config.Values[key]
	if !exists {
		t.Fatalf("component %s has no value for %s", config.ComponentID, key)
	}
	return value
}

func jsonString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode raw value: %v", err)
	}
	return fmt.Sprint(value)
}

func TestLoadValidationPreservesOldDocument(t *testing.T) {
	engine, _ := newTestEngine(t, Options{})
	loadSample(t, engine)

	base := loadSampleDocument(t)
	cases := []struct {
		name string
		doc  *Document
	}{
		{
			name: "duplicate component id",
			doc: func() *Document {
				doc, _ := cloneDocument(base)
				doc.Version = 2
				doc.Components = append(doc.Components, doc.Components[0])
				return doc
			}(),
		},
		{
			name: "missing dependency",
			doc: func() *Document {
				doc, _ := cloneDocument(base)
				doc.Version = 2
				doc.Components[0].DependsOn = []string{"does-not-exist"}
				return doc
			}(),
		},
		{
			name: "invalid key path",
			doc: func() *Document {
				doc, _ := cloneDocument(base)
				doc.Version = 2
				doc.Components[0].Watches = append(doc.Components[0].Watches, "bad..path")
				return doc
			}(),
		},
		{
			name: "dependency cycle",
			doc: func() *Document {
				doc, _ := cloneDocument(base)
				doc.Version = 2
				doc.Components[0].DependsOn = []string{"worker"}
				return doc
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := engine.LoadDocument(context.Background(), tc.doc)
			if !IsCode(err, ErrDocInvalid) {
				t.Fatalf("expected %s, got %v", ErrDocInvalid, err)
			}
			version, err := engine.CurrentVersion(context.Background())
			if err != nil || version != 1 {
				t.Fatalf("old version not preserved: version=%d err=%v", version, err)
			}
			read, err := engine.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 1})
			if err != nil {
				t.Fatalf("old document is not readable: %v", err)
			}
			if got := jsonString(t, requiredValue(t, read.Config, "db.dsn").Value); got != "sqlite://primary" {
				t.Fatalf("unexpected old value %s", got)
			}
		})
	}
}

func TestEffectiveConfigConflictSourceChainAndUndefined(t *testing.T) {
	doc := loadSampleDocument(t)
	var worker *Component
	for i := range doc.Components {
		if doc.Components[i].ID == "worker" {
			worker = &doc.Components[i]
		}
	}
	worker.Watches = append(worker.Watches, "missing.key")

	api, err := EffectiveConfig(doc, "api")
	if err != nil {
		t.Fatalf("api effective config: %v", err)
	}
	logLevel := requiredValue(t, api, "log.level")
	if got := jsonString(t, logLevel.Value); got != "debug" {
		t.Fatalf("log.level winner = %s, want debug", got)
	}
	if logLevel.Winner.Owner != "auth" || logLevel.Winner.Kind != "component" {
		t.Fatalf("unexpected winner: %+v", logLevel.Winner)
	}
	if logLevel.Resolution != "highest effective order" {
		t.Fatalf("unexpected resolution: %s", logLevel.Resolution)
	}
	if !containsSourceOwner(logLevel.Sources, "edge", "warn") || !containsSourceOwner(logLevel.Sources, "auth", "debug") {
		t.Fatalf("source chain does not contain both inheritance and dependency candidates: %+v", logLevel.Sources)
	}

	auth, err := EffectiveConfig(doc, "auth")
	if err != nil {
		t.Fatalf("auth effective config: %v", err)
	}
	authLog := requiredValue(t, auth, "log.level")
	if got := jsonString(t, authLog.Value); got != "debug" {
		t.Fatalf("auth log.level = %s, want debug", got)
	}
	if len(authLog.Sources) < 2 {
		t.Fatalf("expected group and explicit source candidates, got %+v", authLog.Sources)
	}

	workerConfig, err := EffectiveConfig(doc, "worker")
	if err != nil {
		t.Fatalf("worker effective config: %v", err)
	}
	timeout := requiredValue(t, workerConfig, "service.timeout")
	if got := jsonString(t, timeout.Value); got != "45" {
		t.Fatalf("worker service.timeout = %s, want 45", got)
	}
	if timeout.Winner.Owner != "api" {
		t.Fatalf("worker timeout should come through dependency api, got %+v", timeout.Winner)
	}
	if _, exists := workerConfig.Values["missing.key"]; exists {
		t.Fatal("missing watched key was materialized as a value")
	}
	if !reflect.DeepEqual(workerConfig.Undefined, []string{"missing.key"}) {
		t.Fatalf("undefined = %v", workerConfig.Undefined)
	}
}

func containsSourceOwner(sources []Source, owner, value string) bool {
	for _, source := range sources {
		if source.Owner == owner && jsonStringNoTest(source.Value) == value {
			return true
		}
	}
	return false
}

func jsonStringNoTest(raw json.RawMessage) string {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return fmt.Sprint(value)
}

func TestPatchPropagationMaterializationAndReadVisibility(t *testing.T) {
	engine, _ := newTestEngine(t, Options{})
	loadSample(t, engine)
	patch := Patch{
		ID:          "pool-only",
		BaseVersion: 1,
		Changes: []PatchChange{{
			Key:       "db.pool_size",
			Scope:     "component",
			Component: "db",
			Type:      "int",
			Value:     json.RawMessage("16"),
			Order:     20,
		}},
	}
	result, err := engine.SubmitPatch(context.Background(), patch)
	if err != nil {
		t.Fatalf("submit patch: %v", err)
	}
	if result.DocumentVersion != 2 {
		t.Fatalf("document version = %d", result.DocumentVersion)
	}
	affected := make(map[string]AffectedComponent)
	for _, entry := range result.Impact.AffectedComponents {
		affected[entry.ComponentID] = entry
	}
	for _, id := range []string{"db", "auth", "api", "worker", "audit"} {
		if _, exists := affected[id]; !exists {
			t.Fatalf("expected %s to be affected: %+v", id, result.Impact.AffectedComponents)
		}
	}
	if _, exists := affected["metrics"]; exists {
		t.Fatal("metrics should not be affected by db.pool_size")
	}
	if affected["db"].Hops != 0 || affected["auth"].Hops != 1 || affected["api"].Hops != 1 || affected["worker"].Hops != 2 || affected["audit"].Hops != 2 {
		t.Fatalf("unexpected hop counts: %+v", affected)
	}
	if !reflect.DeepEqual(affected["db"].TriggerKeys, []string{"db.pool_size"}) {
		t.Fatalf("trigger keys = %v", affected["db"].TriggerKeys)
	}
	if !reflect.DeepEqual(result.Impact.Downstream["db"], []string{"api", "audit", "auth", "worker"}) {
		t.Fatalf("downstream = %v", result.Impact.Downstream["db"])
	}
	if affected["db"].Path != "incremental" || affected["auth"].Path != "full" {
		t.Fatalf("unexpected path selection: db=%s auth=%s", affected["db"].Path, affected["auth"].Path)
	}

	materials := make(map[string]MaterializedConfig)
	for _, config := range result.Materializations {
		materials[config.ComponentID] = config
	}
	if got := jsonString(t, requiredValue(t, materials["worker"], "db.pool_size").Value); got != "16" {
		t.Fatalf("worker inherited pool size = %s", got)
	}
	if materials["metrics"].Decision.Path != "reuse" || materials["metrics"].MaterializedVersion != 1 {
		t.Fatalf("metrics should be reused at materialized version 1: %+v", materials["metrics"].Decision)
	}

	historical, err := engine.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 1})
	if err != nil {
		t.Fatalf("historical read: %v", err)
	}
	if got := jsonString(t, requiredValue(t, historical.Config, "db.pool_size").Value); got != "8" {
		t.Fatalf("historical pool size = %s", got)
	}
	current, err := engine.Read(context.Background(), ReadRequest{ComponentID: "worker", ObservedVersion: 2})
	if err != nil {
		t.Fatalf("current read: %v", err)
	}
	if current.MaterializedVersion != 2 {
		t.Fatalf("worker materialized version = %d", current.MaterializedVersion)
	}
	reused, err := engine.Read(context.Background(), ReadRequest{ComponentID: "metrics", ObservedVersion: 2})
	if err != nil {
		t.Fatalf("reused read: %v", err)
	}
	if reused.MaterializedVersion != 1 {
		t.Fatalf("metrics materialized version = %d, want 1", reused.MaterializedVersion)
	}
	_, err = engine.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 3})
	if !IsCode(err, ErrStaleRead) {
		t.Fatalf("future read should be stale, got %v", err)
	}
	var be *BusinessError
	if !asBusinessError(err, &be) || be.Details["committed_version"] != int64(2) {
		t.Fatalf("stale details = %#v", be)
	}
}

func TestIncrementalAndFullMaterializationAreEquivalent(t *testing.T) {
	optionsIncremental := Options{IncrementalThreshold: 1.0}
	optionsFull := Options{IncrementalThreshold: 0.01}
	engineIncremental, _ := newTestEngine(t, optionsIncremental)
	engineFull, _ := newTestEngine(t, optionsFull)
	loadSample(t, engineIncremental)
	loadSample(t, engineFull)
	patch := Patch{
		ID:          "equivalence",
		BaseVersion: 1,
		Changes: []PatchChange{{
			Key: "db.pool_size", Scope: "component", Component: "db",
			Value: json.RawMessage("16"), Order: 20,
		}},
	}
	incremental, err := engineIncremental.SubmitPatch(context.Background(), patch)
	if err != nil {
		t.Fatalf("incremental patch: %v", err)
	}
	full, err := engineFull.SubmitPatch(context.Background(), patch)
	if err != nil {
		t.Fatalf("full patch: %v", err)
	}
	incConfigs := materializationMap(incremental.Materializations)
	fullConfigs := materializationMap(full.Materializations)
	for id, inc := range incConfigs {
		want, exists := fullConfigs[id]
		if !exists {
			t.Fatalf("full result missing %s", id)
		}
		if !reflect.DeepEqual(inc.Values, want.Values) {
			t.Fatalf("component %s values differ between paths\nincremental=%#v\nfull=%#v", id, inc.Values, want.Values)
		}
		if !reflect.DeepEqual(inc.Undefined, want.Undefined) {
			t.Fatalf("component %s undefined keys differ", id)
		}
	}
	if got := jsonString(t, requiredValue(t, incConfigs["api"], "db.pool_size").Value); got != "16" {
		t.Fatalf("api pool size = %s", got)
	}
}

func TestPatchIdempotencyConflictAndStaleBase(t *testing.T) {
	engine, _ := newTestEngine(t, Options{})
	loadSample(t, engine)
	patch := Patch{
		ID:          "same-patch",
		BaseVersion: 1,
		Changes: []PatchChange{{
			Key: "db.pool_size", Scope: "component", Component: "db", Type: "int",
			Value: json.RawMessage("16"), Order: 20,
		}},
	}
	const workers = 8
	results := make(chan PatchResult, workers)
	errorsCh := make(chan error, workers)
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < workers; i++ {
		go func() {
			start.Wait()
			result, err := engine.SubmitPatch(context.Background(), patch)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}()
	}
	start.Done()
	var firstVersion int64
	duplicates := 0
	for i := 0; i < workers; i++ {
		select {
		case err := <-errorsCh:
			t.Fatalf("concurrent submit: %v", err)
		case result := <-results:
			if firstVersion == 0 {
				firstVersion = result.DocumentVersion
			}
			if result.DocumentVersion != firstVersion {
				t.Fatalf("reused result version %d, want %d", result.DocumentVersion, firstVersion)
			}
			if result.Duplicate {
				duplicates++
			}
		}
	}
	if duplicates != workers-1 {
		t.Fatalf("duplicate count = %d, want %d", duplicates, workers-1)
	}
	canonicalDuplicate := patch
	canonicalDuplicate.Changes = append([]PatchChange(nil), patch.Changes...)
	canonicalDuplicate.Changes[0].Type = "integer"
	canonicalDuplicate.Changes[0].Value = json.RawMessage(" 16 ")
	duplicateResult, err := engine.SubmitPatch(context.Background(), canonicalDuplicate)
	if err != nil {
		t.Fatalf("canonical duplicate: %v", err)
	}
	if !duplicateResult.Duplicate || duplicateResult.DocumentVersion != 2 {
		t.Fatalf("canonical duplicate result: %+v", duplicateResult)
	}
	version, err := engine.CurrentVersion(context.Background())
	if err != nil || version != 2 {
		t.Fatalf("version after duplicate submits = %d err=%v", version, err)
	}

	conflicting := patch
	conflicting.Changes = append([]PatchChange(nil), patch.Changes...)
	conflicting.Changes[0].Value = json.RawMessage("17")
	_, err = engine.SubmitPatch(context.Background(), conflicting)
	if !IsCode(err, ErrConflictingPatch) {
		t.Fatalf("expected conflicting patch, got %v", err)
	}

	stale := Patch{
		ID:          "stale",
		BaseVersion: 1,
		Changes: []PatchChange{{
			Key: "db.pool_size", Scope: "component", Component: "db",
			Value: json.RawMessage("32"), Order: 20,
		}},
	}
	_, err = engine.SubmitPatch(context.Background(), stale)
	if !IsCode(err, ErrStaleBase) {
		t.Fatalf("expected stale base, got %v", err)
	}
	version, _ = engine.CurrentVersion(context.Background())
	if version != 2 {
		t.Fatalf("stale patch changed version to %d", version)
	}
}

func TestReadersUseBoundVersionDuringCommit(t *testing.T) {
	engine, _ := newTestEngine(t, Options{})
	loadSample(t, engine)
	intentWritten := make(chan struct{})
	release := make(chan struct{})
	engine.testHookAfterIntent = func() {
		close(intentWritten)
		<-release
	}
	patch := Patch{
		ID:          "blocked",
		BaseVersion: 1,
		Changes: []PatchChange{{
			Key: "db.pool_size", Scope: "component", Component: "db",
			Value: json.RawMessage("16"), Order: 20,
		}},
	}
	submitted := make(chan struct {
		result PatchResult
		err    error
	}, 1)
	go func() {
		result, err := engine.SubmitPatch(context.Background(), patch)
		submitted <- struct {
			result PatchResult
			err    error
		}{result: result, err: err}
	}()
	select {
	case <-intentWritten:
	case <-time.After(5 * time.Second):
		t.Fatal("patch did not reach durable intent")
	}

	oldRead, err := engine.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 1})
	if err != nil {
		t.Fatalf("read at bound old version: %v", err)
	}
	if got := jsonString(t, requiredValue(t, oldRead.Config, "db.pool_size").Value); got != "8" {
		t.Fatalf("old read = %s", got)
	}
	_, err = engine.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 2})
	if !IsCode(err, ErrStaleRead) {
		t.Fatalf("uncommitted version read should be stale, got %v", err)
	}
	if _, err := engine.ImpactAt(context.Background(), 2, "db"); !IsCode(err, ErrNotFound) {
		t.Fatalf("uncommitted impact row should be invisible, got %v", err)
	}
	if _, err := engine.ReferencesAt(context.Background(), 2, "db"); !IsCode(err, ErrNotFound) {
		t.Fatalf("uncommitted reference rows should be invisible, got %v", err)
	}
	close(release)
	outcome := <-submitted
	if outcome.err != nil {
		t.Fatalf("submit after release: %v", outcome.err)
	}
	newRead, err := engine.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 2})
	if err != nil {
		t.Fatalf("read new version: %v", err)
	}
	if got := jsonString(t, requiredValue(t, newRead.Config, "db.pool_size").Value); got != "16" {
		t.Fatalf("new read = %s", got)
	}
	impact, err := engine.ImpactAt(context.Background(), 2, "db")
	if err != nil {
		t.Fatalf("committed impact: %v", err)
	}
	if impact.Path != "incremental" || !reflect.DeepEqual(impact.TriggerKeys, []string{"db.pool_size"}) {
		t.Fatalf("unexpected committed impact: %+v", impact)
	}
	refs, err := engine.ReferencesAt(context.Background(), 2, "worker")
	if err != nil {
		t.Fatalf("committed references: %v", err)
	}
	if !hasRef(refs, "upstream_component", "api", "") || !hasRef(refs, "group_default", "base", "service.timeout") {
		t.Fatalf("worker references missing upstream/group edges: %+v", refs)
	}
}

func hasRef(refs []PatchRef, kind, target, key string) bool {
	for _, ref := range refs {
		if ref.Kind == kind && ref.TargetID == target && ref.Key == key {
			return true
		}
	}
	return false
}

func TestFixedSamplePatchAndDocumentReplacement(t *testing.T) {
	engine, _ := newTestEngine(t, Options{})
	loadResult := loadSample(t, engine)
	if !loadResult.Changed || loadResult.Version != 1 {
		t.Fatalf("unexpected load result: %+v", loadResult)
	}
	again, err := engine.LoadDocument(context.Background(), loadSampleDocument(t))
	if err != nil {
		t.Fatalf("idempotent load: %v", err)
	}
	if again.Changed || again.Version != 1 {
		t.Fatalf("same document should be unchanged: %+v", again)
	}

	patch := loadSamplePatch(t)
	result, err := engine.SubmitPatch(context.Background(), patch)
	if err != nil {
		t.Fatalf("fixed sample patch: %v", err)
	}
	if len(result.Impact.AffectedComponents) != 6 {
		t.Fatalf("fixed patch affected %d components, want 6: %+v", len(result.Impact.AffectedComponents), result.Impact.AffectedComponents)
	}
	materials := materializationMap(result.Materializations)
	if got := jsonString(t, requiredValue(t, materials["auth"], "log.level").Value); got != "error" {
		t.Fatalf("auth log.level = %s, want error", got)
	}
	if got := jsonString(t, requiredValue(t, materials["api"], "http.retries").Value); got != "4" {
		t.Fatalf("api retries = %s, want 4", got)
	}
	if got := jsonString(t, requiredValue(t, materials["worker"], "db.pool_size").Value); got != "16" {
		t.Fatalf("worker pool size = %s, want 16", got)
	}
	for _, entry := range result.Impact.AffectedComponents {
		if entry.Hops != 0 {
			t.Fatalf("all fixed patch seeds should be direct, got %+v", entry)
		}
	}

	replacement := loadSampleDocument(t)
	replacement.Version = 3
	replacement.Components[0].Config[0].Value = json.RawMessage(`"sqlite://replica"`)
	replaceResult, err := engine.LoadDocument(context.Background(), replacement)
	if err != nil {
		t.Fatalf("replace document: %v", err)
	}
	if !replaceResult.Changed || replaceResult.Version != 3 {
		t.Fatalf("unexpected replacement result: %+v", replaceResult)
	}
	read, err := engine.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 3})
	if err != nil {
		t.Fatalf("read replacement: %v", err)
	}
	if got := jsonString(t, requiredValue(t, read.Config, "db.dsn").Value); got != "sqlite://replica" {
		t.Fatalf("replacement dsn = %s", got)
	}
}

func asBusinessError(err error, target **BusinessError) bool {
	if err == nil {
		return false
	}
	be, ok := err.(*BusinessError)
	if ok {
		*target = be
	}
	return ok
}

func TestDecodeRejectsUnknownFieldsAndNullValues(t *testing.T) {
	if _, err := DecodeDocument([]byte(`{"version":1,"unknown":true,"components":[]}`)); !IsCode(err, ErrDocInvalid) {
		t.Fatalf("unknown field should be invalid, got %v", err)
	}
	nullDoc := []byte(`{
		"version": 1,
		"groups": [{"id":"g","defaults":[{"key":"a.b","type":"string","value":null,"order":1}]}],
		"components": [{"id":"c","group":"g","watches":["a.b"]}]
	}`)
	if _, err := DecodeDocument(nullDoc); !IsCode(err, ErrDocInvalid) {
		t.Fatalf("null value should be invalid, got %v", err)
	}
}

func TestReadFutureVersionDetailsIncludeLaggingVersion(t *testing.T) {
	engine, _ := newTestEngine(t, Options{})
	loadSample(t, engine)
	_, err := engine.Read(context.Background(), ReadRequest{ComponentID: "db", ObservedVersion: 9})
	if !IsCode(err, ErrStaleRead) {
		t.Fatalf("expected stale read, got %v", err)
	}
	var be *BusinessError
	if !asBusinessError(err, &be) {
		t.Fatalf("expected business error, got %T", err)
	}
	if be.Details["committed_version"] != int64(1) || be.Details["lagging_version"] != int64(1) {
		t.Fatalf("missing lagging version details: %#v", be.Details)
	}
}

func TestSQLiteStateSurvivesReopen(t *testing.T) {
	engine, path := newTestEngine(t, Options{})
	loadSample(t, engine)
	patch := Patch{
		ID: "persistent", BaseVersion: 1,
		Changes: []PatchChange{{Key: "db.pool_size", Scope: "component", Component: "db", Value: json.RawMessage("16"), Order: 20}},
	}
	if _, err := engine.SubmitPatch(context.Background(), patch); err != nil {
		t.Fatalf("patch: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	version, err := reopened.CurrentVersion(context.Background())
	if err != nil || version != 2 {
		t.Fatalf("reopened version = %d err=%v", version, err)
	}
	read, err := reopened.Read(context.Background(), ReadRequest{ComponentID: "auth", ObservedVersion: 2})
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if got := jsonString(t, requiredValue(t, read.Config, "db.pool_size").Value); got != "16" {
		t.Fatalf("reopened value = %s", got)
	}
	duplicate, err := reopened.SubmitPatch(context.Background(), patch)
	if err != nil {
		t.Fatalf("duplicate after reopen: %v", err)
	}
	if !duplicate.Duplicate || duplicate.DocumentVersion != 2 {
		t.Fatalf("unexpected duplicate result: %+v", duplicate)
	}
}

func TestDatabasePathWithSpaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config impact database.db")
	engine, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open path with spaces: %v", err)
	}
	if _, err := engine.LoadDocument(context.Background(), loadSampleDocument(t)); err != nil {
		t.Fatalf("load path with spaces: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("reopen path with spaces: %v", err)
	}
	defer reopened.Close()
	version, err := reopened.CurrentVersion(context.Background())
	if err != nil || version != 1 {
		t.Fatalf("version=%d err=%v", version, err)
	}
}

func TestRecoveryDiscardsIncompleteVersionRows(t *testing.T) {
	engine, path := newTestEngine(t, Options{})
	loadSample(t, engine)
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	db, err := openDatabase(path)
	if err != nil {
		t.Fatalf("open raw database: %v", err)
	}
	ctx := context.Background()
	doc, _, _, err := loadDocumentAt(ctx, db, 1)
	if err != nil {
		db.Close()
		t.Fatalf("load version 1: %v", err)
	}
	doc.Version = 2
	hash, payload, err := documentHash(doc)
	if err != nil {
		db.Close()
		t.Fatalf("hash version 2: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO documents(version, doc_hash, payload, created_at) VALUES(2,?,?,?)`, hash, payload, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		db.Close()
		t.Fatalf("insert incomplete document: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO materializations(snapshot_version, component_id, materialized_version, config_json, path)
		SELECT 2, component_id, materialized_version, config_json, path
		FROM materializations WHERE snapshot_version=1 LIMIT 1`); err != nil {
		db.Close()
		t.Fatalf("insert partial materialization: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE meta SET value='2' WHERE key='current_version'`); err != nil {
		db.Close()
		t.Fatalf("advance raw current version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw database: %v", err)
	}

	recovered, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("reopen with incomplete rows: %v", err)
	}
	defer recovered.Close()
	report := recovered.RecoveryReport()
	if !reflect.DeepEqual(report.CompleteDocumentVersions, []int64{1}) {
		t.Fatalf("complete versions = %v", report.CompleteDocumentVersions)
	}
	if !reflect.DeepEqual(report.DiscardedIncompleteVersions, []int64{2}) {
		t.Fatalf("discarded versions = %v", report.DiscardedIncompleteVersions)
	}
	if report.LastCompleteVersion != 1 {
		t.Fatalf("last complete version = %d", report.LastCompleteVersion)
	}
	version, err := recovered.CurrentVersion(ctx)
	if err != nil || version != 1 {
		t.Fatalf("current version=%d err=%v", version, err)
	}
	var documentRows int
	if err := recovered.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents`).Scan(&documentRows); err != nil {
		t.Fatalf("count documents: %v", err)
	}
	if documentRows != 1 {
		t.Fatalf("document rows = %d, want 1", documentRows)
	}
	if _, err := recovered.Read(ctx, ReadRequest{ComponentID: "db", ObservedVersion: 2}); !IsCode(err, ErrStaleRead) {
		t.Fatalf("discarded version should be stale, got %v", err)
	}
}

func TestDecodePatchStrict(t *testing.T) {
	if _, err := DecodePatch([]byte(`{"id":"x","base_version":1,"changes":[],"extra":1}`)); !IsCode(err, ErrPatchInvalid) {
		t.Fatalf("unknown patch field should fail, got %v", err)
	}
}

func FuzzLoadDocumentDoesNotPanic(f *testing.F) {
	f.Add([]byte(`{"version":1,"components":[{"id":"c","group":"g"}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeDocument(bytes.Clone(data))
	})
}
