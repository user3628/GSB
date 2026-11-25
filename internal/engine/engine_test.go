package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"localsearch/multirecall/internal/model"
	"localsearch/multirecall/internal/recall"
	"localsearch/multirecall/internal/sample"
	"localsearch/multirecall/internal/store"
	"localsearch/multirecall/internal/worker"
)

func TestMain(m *testing.M) {
	if worker.IsWorkerProcess() {
		os.Exit(worker.Main())
	}
	os.Exit(m.Run())
}

func generateDataset(t *testing.T, items, queries int, seed int64) string {
	t.Helper()
	dir := t.TempDir()
	if _, err := sample.Generate(sample.GenerateOptions{
		OutDir: dir, Seed: seed, ItemCount: items, QueryCount: queries, VectorDim: 12, Strategy: "weighted",
	}); err != nil {
		t.Fatal(err)
	}
	return dir
}

func runExperiment(t *testing.T, request Request) *model.RunOutput {
	t.Helper()
	output, err := (&Engine{}).Run(context.Background(), request)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	return output
}

func canonical(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestProcessAndInlineOutputsAreByteIdentical(t *testing.T) {
	dataDir := generateDataset(t, 90, 18, 11)
	root := t.TempDir()
	processOutput := runExperiment(t, Request{
		DataDir: dataDir, DBPath: filepath.Join(root, "process.db"), Workers: 3,
		Mode: "process", VerifyIncremental: true,
	})
	inlineOutput := runExperiment(t, Request{
		DataDir: dataDir, DBPath: filepath.Join(root, "inline.db"), Workers: 1,
		Mode: "inline", VerifyIncremental: true,
	})
	if string(canonical(t, processOutput)) != string(canonical(t, inlineOutput)) {
		t.Fatal("process and inline outputs differ byte-for-byte")
	}
	if len(processOutput.Results) != 18 || processOutput.Summary.ProcessedCount != 18 {
		t.Fatalf("unexpected result count: %d", len(processOutput.Results))
	}
	for _, result := range processOutput.Results {
		if !recall.VerifyFusion(result.Recall, model.FusionConfig{Strategy: "weighted", Weights: model.RouteWeights{Lexical: 0.45, Category: 0.25, Vector: 0.30}, RRFK: 60, TopK: 10, DedupeByItemID: true}, result.Fusion) {
			t.Fatalf("fusion details mismatch for %s", result.QueryID)
		}
	}
}

func TestBudgetUnprocessedAndDecisionTotals(t *testing.T) {
	dataDir := generateDataset(t, 50, 8, 12)
	config := sample.DefaultConfig(12, 12)
	config.Budget.GlobalCandidateEvaluations = 4
	config.Execution.MaxConcurrentQueries = 2
	output := runExperiment(t, Request{
		DataDir: dataDir, DBPath: filepath.Join(t.TempDir(), "budget.db"), Config: &config,
		Mode: "inline", VerifyIncremental: false,
	})
	if len(output.Results)+len(output.Unprocessed) != 8 {
		t.Fatalf("processed and unprocessed queries do not account for all inputs: %d + %d", len(output.Results), len(output.Unprocessed))
	}
	if len(output.Unprocessed) == 0 {
		t.Fatal("tight budget should leave explicitly unprocessed queries")
	}
	predicted := 0
	for _, decision := range output.BudgetDecisions {
		predicted += decision.PredictedEvaluations
	}
	if predicted > 4 {
		t.Fatalf("predicted evaluations exceed budget: %d", predicted)
	}
	if output.Summary.UnprocessedCount != len(output.Unprocessed) || len(output.Summary.UnprocessedReasonCounts) == 0 {
		t.Fatalf("summary does not record unprocessed reasons: %+v", output.Summary)
	}
}

func TestWeightChangeInvalidatesFusionButReusesRecall(t *testing.T) {
	dataDir := generateDataset(t, 60, 10, 21)
	root := t.TempDir()
	dbPath := filepath.Join(root, "incremental.db")
	first := runExperiment(t, Request{DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: true})
	mutateConfig(t, dataDir, func(config *model.Config) {
		config.Fusion.Weights.Lexical = 0.70
		config.Fusion.Weights.Category = 0.10
	})
	second := runExperiment(t, Request{DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: true})
	fresh := runExperiment(t, Request{DataDir: dataDir, DBPath: filepath.Join(root, "fresh.db"), Mode: "inline", VerifyIncremental: true})
	if string(canonical(t, second)) != string(canonical(t, fresh)) {
		t.Fatal("incremental weight result differs from from-scratch run")
	}
	if string(canonical(t, first)) == string(canonical(t, second)) {
		t.Fatal("weight change did not alter experiment output")
	}
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	checkpoints, err := st.ListCheckpoints(context.Background(), "q-000001")
	if err != nil {
		t.Fatal(err)
	}
	assertValidRevision(t, checkpoints, model.StageRecall, 1)
	assertValidRevision(t, checkpoints, model.StageFusion, 2)
	assertValidRevision(t, checkpoints, model.StageEvaluation, 2)
}

func TestQueryAndQrelChangesPropagateFromEarliestStage(t *testing.T) {
	dataDir := generateDataset(t, 60, 8, 22)
	root := t.TempDir()
	dbPath := filepath.Join(root, "lineage.db")
	_ = runExperiment(t, Request{DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: true})
	mutateQuery(t, dataDir, 0, func(query *model.Query) { query.Text += " uniquechange" })
	queryChanged := runExperiment(t, Request{DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: true})
	queryFresh := runExperiment(t, Request{DataDir: dataDir, DBPath: filepath.Join(root, "query-fresh.db"), Mode: "inline", VerifyIncremental: true})
	if string(canonical(t, queryChanged)) != string(canonical(t, queryFresh)) {
		t.Fatal("query incremental result differs from full rebuild")
	}
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	changedChecks, _ := st.ListCheckpoints(context.Background(), "q-000001")
	untouchedChecks, _ := st.ListCheckpoints(context.Background(), "q-000002")
	assertValidRevision(t, changedChecks, model.StageRecall, 2)
	assertValidRevision(t, untouchedChecks, model.StageRecall, 1)
	st.Close()

	mutateQrels(t, dataDir, func(judgments []model.Judgment) []model.Judgment {
		if len(judgments) > 0 {
			judgments[0].Relevance++
		}
		return judgments
	})
	qrelChanged := runExperiment(t, Request{DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: true})
	qrelFresh := runExperiment(t, Request{DataDir: dataDir, DBPath: filepath.Join(root, "qrel-fresh.db"), Mode: "inline", VerifyIncremental: true})
	if string(canonical(t, qrelChanged)) != string(canonical(t, qrelFresh)) {
		t.Fatal("qrel incremental result differs from full rebuild")
	}
	st, err = store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	checks, _ := st.ListCheckpoints(context.Background(), "q-000002")
	assertValidRevision(t, checks, model.StageRecall, 1)
	assertValidRevision(t, checks, model.StageFusion, 1)
	assertValidRevision(t, checks, model.StageEvaluation, 2)
}

func TestGracefulStopResumeMatchesFreshRun(t *testing.T) {
	dataDir := generateDataset(t, 100, 14, 31)
	root := t.TempDir()
	dbPath := filepath.Join(root, "resume.db")
	stop := make(chan struct{})
	var once sync.Once
	partial, err := (&Engine{}).Run(context.Background(), Request{
		DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: false,
		Stop: stop, OnCheckpoint: func(queryID, stage string) { once.Do(func() { close(stop) }) },
	})
	if err != nil {
		t.Fatalf("stopped run returned error: %v", err)
	}
	if !partial.Stopped || len(partial.Results) == 0 || len(partial.Results) == 14 {
		t.Fatalf("expected a partial run with explicit stop: stopped=%v results=%d unprocessed=%d", partial.Stopped, len(partial.Results), len(partial.Unprocessed))
	}
	resumed := runExperiment(t, Request{DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: true})
	fresh := runExperiment(t, Request{DataDir: dataDir, DBPath: filepath.Join(root, "fresh.db"), Mode: "inline", VerifyIncremental: true})
	if resumed.Stopped || len(resumed.Results) != 14 {
		t.Fatalf("resume did not complete: %+v", resumed.Summary)
	}
	if string(canonical(t, resumed)) != string(canonical(t, fresh)) {
		t.Fatal("resumed output differs from from-scratch output")
	}
}

func TestIncrementalMismatchCodeWithFirstQueryID(t *testing.T) {
	dataDir := generateDataset(t, 50, 6, 41)
	dbPath := filepath.Join(t.TempDir(), "mismatch.db")
	_ = runExperiment(t, Request{DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: true})
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, ok, err := st.ValidCheckpoint(context.Background(), "q-000001", model.StageFusion)
	if err != nil || !ok {
		t.Fatalf("load fusion checkpoint: ok=%v err=%v", ok, err)
	}
	var fusion model.FusionResult
	if err := json.Unmarshal(checkpoint.Payload, &fusion); err != nil {
		t.Fatal(err)
	}
	fusion.Strategy = "tampered-but-canonical"
	tampered, _ := json.Marshal(fusion)
	digest := model.SHA256(tampered)
	if _, err := st.DB().Exec(`UPDATE checkpoints SET payload=?, artifact_digest=? WHERE query_id=? AND stage=? AND valid=1`, tampered, digest, "q-000001", model.StageFusion); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`UPDATE checkpoint_heads SET artifact_digest=? WHERE query_id=? AND stage=?`, digest, "q-000001", model.StageFusion); err != nil {
		t.Fatal(err)
	}
	st.Close()
	_, err = (&Engine{}).Run(context.Background(), Request{DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: true})
	appErr := model.AsAppError(err)
	if appErr.Code != model.CodeIncrementalMismatch || appErr.QueryID != "q-000001" {
		t.Fatalf("expected E_INCREMENTAL_MISMATCH for q-000001, got %+v", appErr)
	}
}

type failingExecutor struct{}

func (f *failingExecutor) RunRound(ctx context.Context, tasks []worker.Task) ([]worker.Result, error) {
	results := make([]worker.Result, len(tasks))
	for i, task := range tasks {
		results[i] = worker.Result{
			Shard: 7, QueryID: task.Query.QueryID,
			Error: &model.WorkerFailure{Code: model.CodeWorkerFailure, Message: "simulated crash", Shard: 7, QueryID: task.Query.QueryID},
		}
	}
	return results, errors.New("simulated worker crash")
}

func (f *failingExecutor) Close() error { return nil }

func TestWorkerFailureIsStructuredAndPersisted(t *testing.T) {
	dataDir := generateDataset(t, 30, 3, 51)
	dbPath := filepath.Join(t.TempDir(), "worker.db")
	_, err := (&Engine{}).Run(context.Background(), Request{
		DataDir: dataDir, DBPath: dbPath, Mode: "inline", VerifyIncremental: false, Executor: &failingExecutor{},
	})
	appErr := model.AsAppError(err)
	if appErr.Code != model.CodeWorkerFailure || appErr.QueryID == "" || appErr.Details["shard"] != 7 {
		t.Fatalf("unexpected worker error: %+v", appErr)
	}
	st, openErr := store.Open(dbPath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer st.Close()
	var count int
	if queryErr := st.DB().QueryRow(`SELECT COUNT(*) FROM failures WHERE code=?`, model.CodeWorkerFailure).Scan(&count); queryErr != nil {
		t.Fatal(queryErr)
	}
	if count != 3 {
		t.Fatalf("expected every failed query in the round to be persisted, got %d", count)
	}
}

func mutateConfig(t *testing.T, dataDir string, mutate func(*model.Config)) {
	t.Helper()
	path := filepath.Join(dataDir, sample.ConfigFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config model.Config
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	mutate(&config)
	data, _ = json.MarshalIndent(config, "", "  ")
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mutateQuery(t *testing.T, dataDir string, index int, mutate func(*model.Query)) {
	t.Helper()
	path := filepath.Join(dataDir, sample.QueriesFile)
	queries := readJSONL[model.Query](t, path)
	mutate(&queries[index])
	writeJSONL(t, path, queries)
}

func mutateQrels(t *testing.T, dataDir string, mutate func([]model.Judgment) []model.Judgment) {
	t.Helper()
	path := filepath.Join(dataDir, sample.JudgmentsFile)
	judgments := mutate(readJSONL[model.Judgment](t, path))
	writeJSONL(t, path, judgments)
}

func readJSONL[T any](t *testing.T, path string) []T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := splitLines(data)
	out := make([]T, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var value T
		if err := json.Unmarshal(line, &value); err != nil {
			t.Fatal(err)
		}
		out = append(out, value)
	}
	return out
}

func writeJSONL[T any](t *testing.T, path string, values []T) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func splitLines(data []byte) [][]byte {
	lines := make([][]byte, 0)
	start := 0
	for i, value := range data {
		if value == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

func assertValidRevision(t *testing.T, checkpoints []store.Checkpoint, stage string, revision int) {
	t.Helper()
	for _, checkpoint := range checkpoints {
		if checkpoint.Stage == stage && checkpoint.Valid {
			if checkpoint.Revision != revision {
				t.Fatalf("%s valid revision=%d want=%d", stage, checkpoint.Revision, revision)
			}
			return
		}
	}
	t.Fatalf("no valid checkpoint for stage %s", stage)
}
