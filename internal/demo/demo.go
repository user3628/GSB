package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"featureplatform/internal/engine"
	"featureplatform/internal/httpapi"
	"featureplatform/internal/model"
	"featureplatform/internal/samplegen"
)

type Probe struct {
	SampleID        string `json:"sample_id"`
	ObservedVersion int64  `json:"observed_version"`
	ExpectedCode    string `json:"expected_code,omitempty"`
	ActualCode      string `json:"actual_code"`
	Pass            bool   `json:"pass"`
}

type HTTPReport struct {
	HealthOK       bool   `json:"health_ok"`
	CurrentReadOK  bool   `json:"current_read_ok"`
	StaleReadCode  string `json:"stale_read_code"`
	GracefulStopOK bool   `json:"graceful_stop_ok"`
}

type Report struct {
	Schema                      string         `json:"schema"`
	GeneratedSamples            int            `json:"generated_samples"`
	Tasks                       []string       `json:"tasks"`
	ContinuousExportDigest      string         `json:"continuous_export_digest"`
	RecoveryExportDigest        string         `json:"recovery_export_digest"`
	RecoveryMatchesContinuous   bool           `json:"recovery_matches_continuous"`
	ForcedTermination           string         `json:"forced_termination"`
	KilledAfterCheckpointWrites int            `json:"killed_after_checkpoint_writes"`
	IncrementFragmentID         string         `json:"increment_fragment_id"`
	IncrementAffectedTasks      []string       `json:"increment_affected_tasks"`
	IncrementAffectedCount      int            `json:"increment_affected_count"`
	IncrementReusedCount        int            `json:"increment_reused_count"`
	StaleReadProbe              Probe          `json:"stale_read_probe"`
	StaleCheckpointProbe        Probe          `json:"stale_checkpoint_probe"`
	UnaffectedCurrentProbe      Probe          `json:"unaffected_current_probe"`
	RebuiltCount                int            `json:"rebuilt_count"`
	PostIncrementReadOK         bool           `json:"post_increment_read_ok"`
	FinalExportDigest           string         `json:"final_export_digest"`
	Summary                     engine.Summary `json:"summary"`
	HTTP                        HTTPReport     `json:"http"`
	FixedSummaryDigest          string         `json:"fixed_summary_digest"`
}

func Run(dir, executable string) (*Report, error) {
	ctx := context.Background()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	continuousDB := filepath.Join(absDir, "continuous.db")
	recoveryDB := filepath.Join(absDir, "recovery.db")
	readyFile := filepath.Join(absDir, "hang.ready")
	for _, p := range []string{continuousDB, recoveryDB, continuousDB + "-wal", continuousDB + "-shm", continuousDB + ".lock", recoveryDB + "-wal", recoveryDB + "-shm", recoveryDB + ".lock", readyFile} {
		_ = os.Remove(p)
	}

	shards := samplegen.GenerateShards()
	sampleCount := 0
	tasks := []string{}
	for _, shard := range shards {
		sampleCount += len(shard.Samples)
		tasks = append(tasks, shard.Task)
	}

	continuousDigest, err := seedMigrateExport(ctx, continuousDB, shards)
	if err != nil {
		return nil, fmt.Errorf("continuous run: %w", err)
	}
	if err := seedOnly(ctx, recoveryDB, shards); err != nil {
		return nil, fmt.Errorf("seed recovery db: %w", err)
	}
	if err := forceKillMigrate(executable, recoveryDB, readyFile); err != nil {
		return nil, err
	}

	recoveryEng, err := engine.Open(recoveryDB)
	if err != nil {
		return nil, err
	}
	if _, err := recoveryEng.RunMigration(ctx, engine.RunOptions{Budget: 1000}); err != nil {
		_ = recoveryEng.Close()
		return nil, fmt.Errorf("recovery migration: %w", err)
	}
	_, recoveryDigest, _, err := recoveryEng.ExportCanonical(ctx)
	if err != nil {
		_ = recoveryEng.Close()
		return nil, err
	}
	if recoveryDigest != continuousDigest {
		_ = recoveryEng.Close()
		return nil, fmt.Errorf("recovery digest %s != continuous %s", recoveryDigest, continuousDigest)
	}

	fragment := samplegen.SentimentV2Fragment()
	_, plan, err := recoveryEng.SubmitFragment(ctx, fragment)
	if err != nil {
		_ = recoveryEng.Close()
		return nil, err
	}
	mplan, err := recoveryEng.PlanMigration(ctx)
	if err != nil {
		_ = recoveryEng.Close()
		return nil, err
	}

	staleRead := probeRead(ctx, recoveryEng, "SA-0001", 1, model.CodeStaleRead)
	staleCP := probeRead(ctx, recoveryEng, "SA-0001", 2, model.CodeStaleCheckpoint)
	unaffected := probeRead(ctx, recoveryEng, "TC-0001", 2, "")
	if !staleRead.Pass || !staleCP.Pass || !unaffected.Pass {
		_ = recoveryEng.Close()
		return nil, fmt.Errorf("read adjudication probes failed: %+v %+v %+v", staleRead, staleCP, unaffected)
	}

	runReport, err := recoveryEng.RunMigration(ctx, engine.RunOptions{Budget: 1000})
	if err != nil {
		_ = recoveryEng.Close()
		return nil, err
	}
	postRec, coded := recoveryEng.Store.ReadAdjudicated(ctx, "SA-0001", 2)
	postOK := coded == nil && postRec != nil
	if !postOK {
		_ = recoveryEng.Close()
		return nil, fmt.Errorf("post-increment read failed: %+v", coded)
	}
	_, finalDigest, _, err := recoveryEng.ExportCanonical(ctx)
	if err != nil {
		_ = recoveryEng.Close()
		return nil, err
	}
	summary, err := recoveryEng.Summary(ctx)
	if err != nil {
		_ = recoveryEng.Close()
		return nil, err
	}

	httpReport, err := runHTTPProbe(ctx, recoveryEng)
	if err != nil {
		_ = recoveryEng.Close()
		return nil, err
	}
	if err := recoveryEng.Close(); err != nil {
		return nil, err
	}

	report := &Report{
		Schema:                      "deterministic-demo/v1",
		GeneratedSamples:            sampleCount,
		Tasks:                       tasks,
		ContinuousExportDigest:      continuousDigest,
		RecoveryExportDigest:        recoveryDigest,
		RecoveryMatchesContinuous:   true,
		ForcedTermination:           "force_kill",
		KilledAfterCheckpointWrites: 2,
		IncrementFragmentID:         fragment.FragmentID,
		IncrementAffectedTasks:      plan.AffectedTasks,
		IncrementAffectedCount:      len(mplan.Units),
		IncrementReusedCount:        mplan.ReusedCount,
		StaleReadProbe:              staleRead,
		StaleCheckpointProbe:        staleCP,
		UnaffectedCurrentProbe:      unaffected,
		RebuiltCount:                runReport.RebuiltCount,
		PostIncrementReadOK:         postOK,
		FinalExportDigest:           finalDigest,
		Summary:                     summary,
		HTTP:                        httpReport,
	}
	digest, _, err := model.DigestValue("demo-summary/v1:", report)
	if err != nil {
		return nil, err
	}
	report.FixedSummaryDigest = digest
	return report, nil
}

func seedMigrateExport(ctx context.Context, dbPath string, shards []model.Shard) (string, error) {
	eng, err := engine.Open(dbPath)
	if err != nil {
		return "", err
	}
	defer eng.Close()
	if _, err := eng.Store.ImportShards(ctx, shards, "demo-seed"); err != nil {
		return "", err
	}
	if _, err := eng.CommitInitialConfig(ctx, samplegen.DefaultConfig()); err != nil {
		return "", err
	}
	if _, err := eng.RunMigration(ctx, engine.RunOptions{Budget: 1000}); err != nil {
		return "", err
	}
	_, digest, _, err := eng.ExportCanonical(ctx)
	return digest, err
}

func seedOnly(ctx context.Context, dbPath string, shards []model.Shard) error {
	eng, err := engine.Open(dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	if _, err := eng.Store.ImportShards(ctx, shards, "demo-seed"); err != nil {
		return err
	}
	_, err = eng.CommitInitialConfig(ctx, samplegen.DefaultConfig())
	return err
}

func forceKillMigrate(executable, dbPath, readyFile string) error {
	cmd := exec.Command(executable, "migrate", "--db", dbPath, "--budget", "1000",
		"--hang-after-writes", "2", "--ready-file", readyFile)
	logFile, err := os.Create(dbPath + ".kill-worker.log")
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start kill worker: %w", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		if _, err := os.Stat(readyFile); err == nil {
			ready = true
			break
		}
		if cmd.ProcessState != nil {
			return fmt.Errorf("kill worker exited before reaching hang point; see %s", logFile.Name())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return fmt.Errorf("timed out waiting for hang point; see %s", logFile.Name())
	}
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("force kill worker: %w", err)
	}
	_ = cmd.Wait()
	return nil
}

func probeRead(ctx context.Context, eng *engine.Engine, sampleID string, version int64, expected string) Probe {
	_, coded := eng.Store.ReadAdjudicated(ctx, sampleID, version)
	actual := ""
	pass := false
	if coded != nil {
		actual = coded.Code
		pass = expected != "" && actual == expected
	} else {
		actual = "OK"
		pass = expected == ""
	}
	return Probe{SampleID: sampleID, ObservedVersion: version, ExpectedCode: expected, ActualCode: actual, Pass: pass}
}

func runHTTPProbe(ctx context.Context, eng *engine.Engine) (HTTPReport, error) {
	report := HTTPReport{}
	apiServer := httpapi.New(eng)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return report, err
	}
	httpServer := &http.Server{Handler: apiServer.Handler()}
	stopped := make(chan struct{})
	apiServer.OnStop = func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}
	go func() {
		_ = httpServer.Serve(listener)
		close(stopped)
	}()
	baseURL := "http://" + listener.Addr().String()

	healthResp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		return report, err
	}
	report.HealthOK = healthResp.StatusCode == 200
	healthResp.Body.Close()

	readResp, err := http.Get(fmt.Sprintf("%s/api/v1/read?sample_id=SA-0001&version=2", baseURL))
	if err != nil {
		return report, err
	}
	report.CurrentReadOK = readResp.StatusCode == 200
	readResp.Body.Close()

	staleResp, err := http.Get(fmt.Sprintf("%s/api/v1/read?sample_id=SA-0001&version=1", baseURL))
	if err != nil {
		return report, err
	}
	var staleBody struct {
		Error *model.CodedError `json:"error"`
	}
	_ = json.NewDecoder(staleResp.Body).Decode(&staleBody)
	staleResp.Body.Close()
	if staleResp.StatusCode == http.StatusConflict && staleBody.Error != nil {
		report.StaleReadCode = staleBody.Error.Code
	}

	stopResp, err := http.Post(baseURL+"/api/v1/admin/stop", "application/json", nil)
	if err != nil {
		return report, err
	}
	stopResp.Body.Close()
	select {
	case <-stopped:
		report.GracefulStopOK = true
	case <-time.After(5 * time.Second):
		return report, fmt.Errorf("graceful HTTP stop timed out")
	}
	_ = ctx
	return report, nil
}
