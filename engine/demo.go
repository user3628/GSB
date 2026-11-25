package engine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type DemoOptions struct {
	DBDir       string
	DBPath      string
	ReportPath  string
	SummaryPath string
	WorkerExe   string
	KeepDB      bool
}

type DemoSummary struct {
	Format               string   `json:"format"`
	OK                   bool     `json:"ok"`
	Driver               string   `json:"driver"`
	BatchCount           int      `json:"batch_count"`
	SampleCount          int      `json:"sample_count"`
	KilledAfterBatches   int      `json:"killed_after_batches"`
	RecoveredBatches     []string `json:"recovered_batches"`
	ResumeImported       []string `json:"resume_imported_batches"`
	CorrectionSample     string   `json:"correction_sample"`
	AffectedUnits        []int    `json:"affected_units"`
	RebuiltUnits         []int    `json:"rebuilt_units"`
	ReusedUnits          []int    `json:"reused_units"`
	ByteEqual            bool     `json:"byte_equal"`
	FirstDiffByte        int64    `json:"first_diff_byte"`
	OldModelVersion      int64    `json:"old_model_version"`
	NewModelVersion      int64    `json:"new_model_version"`
	InvalidatedCacheKeys int      `json:"invalidated_cache_keys"`
	AfterScore           float64  `json:"after_score"`
	AfterObservedError   float64  `json:"after_observed_error"`
	AfterTotalBound      float64  `json:"after_total_bound"`
	AfterWithinBound     bool     `json:"after_within_bound"`
	ConfigDigest         string   `json:"config_digest"`
	Fingerprint          string   `json:"fingerprint"`
	SummaryDigest        string   `json:"summary_digest"`
	DBPath               string   `json:"db_path,omitempty"`
	ReportPath           string   `json:"report_path,omitempty"`
	SummaryPath          string   `json:"summary_path,omitempty"`
}

func fixedDemoDigest(s *DemoSummary) (string, error) {
	copy := *s
	copy.SummaryDigest = ""
	copy.DBPath = ""
	copy.ReportPath = ""
	copy.SummaryPath = ""
	b, err := CanonicalJSON(copy)
	if err != nil {
		return "", err
	}
	return DigestBytes(b), nil
}

func prepareDemoDB(opt DemoOptions) (string, string, func(), error) {
	dbPath := opt.DBPath
	dir := opt.DBDir
	if dbPath == "" {
		if dir == "" {
			var err error
			dir, err = os.MkdirTemp("", "sampleengine-demo-")
			if err != nil {
				return "", "", nil, err
			}
		} else if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", "", nil, err
		}
		dbPath = filepath.Join(dir, "sampleengine.db")
	} else {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return "", "", nil, err
		}
		dir = filepath.Dir(dbPath)
	}
	_ = os.Remove(dbPath)
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")
	reportPath := opt.ReportPath
	if reportPath == "" {
		reportPath = filepath.Join(dir, "rebuild-report.json")
	}
	cleanup := func() {}
	if !opt.KeepDB && opt.DBPath == "" && opt.DBDir == "" {
		cleanup = func() { _ = os.RemoveAll(dir) }
	}
	return dbPath, reportPath, cleanup, nil
}

// RunCrashWorker is the child process entry used to prove hard-kill recovery.
func RunCrashWorker(args []string) int {
	dbPath := ""
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--db":
			if i+1 < len(args) {
				dbPath = args[i+1]
				i++
			}
		}
	}
	if dbPath == "" {
		fmt.Fprintln(os.Stderr, "crash worker: missing --db")
		return 2
	}
	eng, err := OpenDB(dbPath, DefaultConfig())
	if err != nil {
		fmt.Fprintln(os.Stderr, "crash worker open:", err)
		return 1
	}
	data := GenerateDemoData(DefaultConfig().Seed)
	for i := 0; i < 2; i++ {
		if _, err := eng.ImportBatch(data.Batches[i]); err != nil {
			fmt.Fprintln(os.Stderr, "crash worker import:", err)
			return 1
		}
	}
	st, err := eng.Status()
	if err != nil {
		fmt.Fprintln(os.Stderr, "crash worker status:", err)
		return 1
	}
	fmt.Printf("READY %s %d\n", st.Fingerprint, st.CurrentModelVersion)
	// Intentionally keep the SQLite handle open. The parent hard-kills this
	// process; deferred Close/Unlock must not run.
	select {}
}

func runKilledWorker(exe, dbPath string) error {
	if exe == "" {
		var err error
		exe, err = os.Executable()
		if err != nil {
			return err
		}
	}
	cmd := exec.Command(exe, "_crash_worker", "--db", dbPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start crash worker %q: %w (stderr=%s)", exe, err, stderr.String())
	}
	scanner := bufio.NewScanner(stdout)
	ready := make(chan string, 1)
	go func() {
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	select {
	case line := <-ready:
		if !strings.HasPrefix(line, "READY ") {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return fmt.Errorf("crash worker did not become ready: %q stderr=%s", line, stderr.String())
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("timed out waiting for crash worker; stderr=%s", stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("hard kill crash worker: %w", err)
	}
	_ = cmd.Wait()
	return nil
}

// RunDemo executes the deterministic end-to-end acceptance scenario.
func RunDemo(opt DemoOptions) (*DemoSummary, error) {
	dbPath, reportPath, cleanup, err := prepareDemoDB(opt)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	cfg := DefaultConfig()
	data := GenerateDemoData(cfg.Seed)
	summary := &DemoSummary{
		Format:             "sampleengine.demo.v1",
		Driver:             "modernc.org/sqlite",
		BatchCount:         len(data.Batches),
		SampleCount:        len(FlattenBatches(data.Batches)),
		KilledAfterBatches: 2,
		CorrectionSample:   data.Correction.SampleID,
		FirstDiffByte:      -1,
	}

	if err := runKilledWorker(opt.WorkerExe, dbPath); err != nil {
		return nil, err
	}

	eng, err := OpenDB(dbPath, cfg)
	if err != nil {
		return nil, err
	}
	defer eng.Close()

	recovery, err := eng.Recover()
	if err != nil {
		return nil, err
	}
	summary.RecoveredBatches = append([]string(nil), recovery.CommittedBatches...)
	if len(recovery.CommittedBatches) != 2 {
		return nil, fmt.Errorf("expected exactly two committed batches after kill, got %v", recovery.CommittedBatches)
	}
	for i := 2; i < len(data.Batches); i++ {
		if _, err := eng.ImportBatch(data.Batches[i]); err != nil {
			return nil, err
		}
		summary.ResumeImported = append(summary.ResumeImported, data.Batches[i].ID)
	}

	target, err := findDemoSample(eng, data.Correction.SampleID)
	if err != nil {
		return nil, err
	}
	beforeScore, err := eng.Score(ScoreRequest{
		RequestID: "req-before-backfill",
		Text:      target.Text,
	})
	if err != nil {
		return nil, err
	}
	beforeKey := beforeScore.CacheKey

	bf, err := eng.Backfill(BackfillRequest{
		SampleID: data.Correction.SampleID,
		Label:    data.Correction.Label,
		Fields:   data.Correction.Fields,
	})
	if err != nil {
		return nil, err
	}
	summary.AffectedUnits = bf.AffectedUnits
	summary.RebuiltUnits = bf.RebuiltUnits
	summary.ReusedUnits = bf.ReusedUnits
	summary.ByteEqual = bf.ByteEqual
	summary.FirstDiffByte = bf.FirstDiffByte
	summary.OldModelVersion = bf.OldModelVersion
	summary.NewModelVersion = bf.NewModelVersion
	summary.InvalidatedCacheKeys = len(bf.InvalidatedCacheKeys)
	if len(bf.InvalidatedCacheKeys) != 1 || bf.InvalidatedCacheKeys[0] != beforeKey {
		return nil, fmt.Errorf("expected before-score cache %s to be invalidated, got %v", beforeKey, bf.InvalidatedCacheKeys)
	}
	if _, err := eng.CachedScore(beforeKey); err == nil {
		return nil, fmt.Errorf("invalidated cache %s was still readable", beforeKey)
	}

	after, err := eng.Score(ScoreRequest{
		RequestID: "req-after-backfill",
		Text:      data.QueryText,
		Fields:    data.QueryFields,
	})
	if err != nil {
		return nil, err
	}
	summary.AfterScore = after.Score
	summary.AfterObservedError = after.ObservedError
	summary.AfterTotalBound = after.TotalBound
	summary.AfterWithinBound = after.ObservedError <= after.TotalBound+1e-12

	st, err := eng.Status()
	if err != nil {
		return nil, err
	}
	summary.ConfigDigest = st.ConfigDigest
	summary.Fingerprint = st.Fingerprint
	if _, err := eng.ExportReport(bf.ReportID, reportPath); err != nil {
		return nil, err
	}
	summary.ReportPath = reportPath
	summary.DBPath = dbPath
	if opt.SummaryPath != "" {
		summary.SummaryPath = opt.SummaryPath
	}
	digest, err := fixedDemoDigest(summary)
	if err != nil {
		return nil, err
	}
	summary.SummaryDigest = digest
	summary.OK = summary.ByteEqual && summary.AfterWithinBound && len(summary.RecoveredBatches) == 2

	out, err := CanonicalJSON(summary)
	if err != nil {
		return nil, err
	}
	if opt.SummaryPath != "" {
		if err := os.MkdirAll(filepath.Dir(opt.SummaryPath), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(opt.SummaryPath, append(out, '\n'), 0o644); err != nil {
			return nil, err
		}
	}
	return summary, nil
}

func findDemoSample(e *Engine, id string) (StoredSample, error) {
	samples, err := e.ListSamples()
	if err != nil {
		return StoredSample{}, err
	}
	for _, s := range samples {
		if s.ID == id {
			return s, nil
		}
	}
	return StoredSample{}, newError(CodeNotFound, "demo sample %s", id)
}

// ConcurrentPublish opens independent engine handles and races unit publications.
func ConcurrentPublish(dbPath string, unitID, workers, iterations int) ([]PublishUnitResult, []error) {
	if workers <= 0 {
		workers = 4
	}
	if iterations <= 0 {
		iterations = 5
	}
	results := make(chan PublishUnitResult, workers*iterations)
	errs := make(chan error, workers*iterations)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			eng, err := OpenDB(dbPath, DefaultConfig())
			if err != nil {
				errs <- err
				return
			}
			defer eng.Close()
			for i := 0; i < iterations; i++ {
				r, err := eng.PublishUnit(PublishUnitRequest{UnitID: unitID, Producer: fmt.Sprintf("worker-%d", worker)})
				if err != nil {
					errs <- err
					return
				}
				results <- *r
			}
		}(w)
	}
	wg.Wait()
	close(results)
	close(errs)
	var out []PublishUnitResult
	for r := range results {
		out = append(out, r)
	}
	var errOut []error
	for err := range errs {
		errOut = append(errOut, err)
	}
	return out, errOut
}

var _ = json.Marshal
