package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"vpnbench/internal/anomaly"
	"vpnbench/internal/app"
	"vpnbench/internal/samplegen"
)

const Schema = "vpnbench.acceptance/v1"

type Baseline struct {
	Runs          int    `json:"runs"`
	ByteIdentical bool   `json:"byte_identical"`
	SHA256        string `json:"sha256"`
	Sessions      int    `json:"sessions"`
	Requests      int    `json:"requests"`
}

type CrashRecovery struct {
	PolicyCrashExit                          int     `json:"policy_crash_exit"`
	SettlementCrashExit                      int     `json:"settlement_crash_exit"`
	PolicyVersionsAfterPolicyCrash           []int64 `json:"policy_versions_after_policy_crash"`
	PolicyVersionsAfterPolicyRestart         []int64 `json:"policy_versions_after_policy_restart"`
	SettlementVersionsAfterPolicyRestart     []int64 `json:"settlement_versions_after_policy_restart"`
	PolicyVersionsAfterSettlementCrash       []int64 `json:"policy_versions_after_settlement_crash"`
	SettlementVersionsAfterSettlementCrash   []int64 `json:"settlement_versions_after_settlement_crash"`
	PolicyVersionsAfterSettlementRestart     []int64 `json:"policy_versions_after_settlement_restart"`
	SettlementVersionsAfterSettlementRestart []int64 `json:"settlement_versions_after_settlement_restart"`
	WindowsAfterSettlementCrash              int     `json:"windows_after_settlement_crash"`
	WindowsAfterRestart                      int     `json:"windows_after_restart"`
	Passed                                   bool    `json:"passed"`
}

type Result struct {
	Schema        string           `json:"schema"`
	Passed        bool             `json:"passed"`
	Baseline      Baseline         `json:"baseline"`
	Anomalies     []anomaly.Result `json:"anomalies"`
	CrashRecovery CrashRecovery    `json:"crash_recovery"`
}

func Run(ctx context.Context, samplesDir, workDir string) (Result, error) {
	if _, err := samplegen.Generate(samplesDir); err != nil {
		return Result{}, fmt.Errorf("generate samples: %w", err)
	}
	if workDir == "" {
		var err error
		workDir, err = os.MkdirTemp("", "vpnbench-verify-")
		if err != nil {
			return Result{}, fmt.Errorf("create work directory: %w", err)
		}
		defer os.RemoveAll(workDir)
	} else if err := os.MkdirAll(workDir, 0o755); err != nil {
		return Result{}, fmt.Errorf("create work directory: %w", err)
	}
	configPath := filepath.Join(samplesDir, "configs", "workbench.json")
	runSpec, err := app.LoadRunConfig(configPath)
	if err != nil {
		return Result{}, err
	}

	first, firstBytes, err := baselineRun(ctx, runSpec, filepath.Join(workDir, "baseline-1.db"), filepath.Join(workDir, "baseline-1.json"))
	if err != nil {
		return Result{}, err
	}
	_, secondBytes, err := baselineRun(ctx, runSpec, filepath.Join(workDir, "baseline-2.db"), filepath.Join(workDir, "baseline-2.json"))
	if err != nil {
		return Result{}, err
	}
	requests := 0
	for _, session := range first.Report.Sessions {
		requests += len(session.Requests)
	}
	digest := sha256.Sum256(firstBytes)
	result := Result{
		Schema: "vpnbench.acceptance/v1",
		Baseline: Baseline{
			Runs:          2,
			ByteIdentical: bytes.Equal(firstBytes, secondBytes),
			SHA256:        hex.EncodeToString(digest[:]),
			Sessions:      len(first.Report.Sessions),
			Requests:      requests,
		},
	}

	patterns, err := filepath.Glob(filepath.Join(samplesDir, "anomalies", "*.json"))
	if err != nil {
		return Result{}, err
	}
	sort.Strings(patterns)
	for _, paramsPath := range patterns {
		caseName := strings.TrimSuffix(filepath.Base(paramsPath), filepath.Ext(paramsPath))
		dbPath := filepath.Join(workDir, "anomaly-"+caseName+".db")
		anomalyResult, err := anomaly.Run(ctx, paramsPath, dbPath)
		if err != nil {
			return Result{}, fmt.Errorf("run anomaly %s: %w", caseName, err)
		}
		result.Anomalies = append(result.Anomalies, anomalyResult)
	}

	crash, err := verifyCrashRecovery(ctx, configPath, workDir)
	if err != nil {
		return Result{}, err
	}
	result.CrashRecovery = crash
	result.Passed = result.Baseline.ByteIdentical && crash.Passed
	for _, anomalyResult := range result.Anomalies {
		if !anomalyResult.Passed {
			result.Passed = false
		}
	}
	return result, nil
}

func baselineRun(ctx context.Context, spec app.RunSpec, dbPath, reportPath string) (app.RunResult, []byte, error) {
	for _, candidate := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		_ = os.Remove(candidate)
	}
	run, err := app.Run(ctx, spec, dbPath)
	if err != nil {
		return app.RunResult{}, nil, err
	}
	if err := run.Report.Write(reportPath); err != nil {
		return app.RunResult{}, nil, err
	}
	encoded, err := run.Report.Bytes()
	if err != nil {
		return app.RunResult{}, nil, err
	}
	return run, encoded, nil
}

func verifyCrashRecovery(ctx context.Context, configPath, workDir string) (CrashRecovery, error) {
	executable, err := os.Executable()
	if err != nil {
		return CrashRecovery{}, fmt.Errorf("resolve executable: %w", err)
	}
	spec, err := app.LoadRunConfig(configPath)
	if err != nil {
		return CrashRecovery{}, err
	}
	var result CrashRecovery

	policyDB := filepath.Join(workDir, "crash-policy.db")
	if _, _, err := baselineRun(ctx, spec, policyDB, filepath.Join(workDir, "crash-policy-seed.json")); err != nil {
		return CrashRecovery{}, err
	}
	exit, err := runChild(ctx, executable, configPath, policyDB, filepath.Join(workDir, "crash-policy-report.json"), "policy_after_write")
	if err != nil {
		return CrashRecovery{}, err
	}
	result.PolicyCrashExit = exit
	afterPolicyCrash, err := app.Recover(ctx, policyDB)
	if err != nil {
		return CrashRecovery{}, err
	}
	result.PolicyVersionsAfterPolicyCrash = recoveryVersions(afterPolicyCrash.Policy.Snapshots)
	if _, err := runChild(ctx, executable, configPath, policyDB, filepath.Join(workDir, "crash-policy-restart.json"), ""); err != nil {
		return CrashRecovery{}, err
	}
	afterPolicyRestart, err := app.Recover(ctx, policyDB)
	if err != nil {
		return CrashRecovery{}, err
	}
	result.PolicyVersionsAfterPolicyRestart = recoveryVersions(afterPolicyRestart.Policy.Snapshots)
	result.SettlementVersionsAfterPolicyRestart = recoveryVersions(afterPolicyRestart.Settlement.Snapshots)

	settlementDB := filepath.Join(workDir, "crash-settlement.db")
	for _, candidate := range []string{settlementDB, settlementDB + "-wal", settlementDB + "-shm"} {
		_ = os.Remove(candidate)
	}
	exit, err = runChild(ctx, executable, configPath, settlementDB, filepath.Join(workDir, "crash-settlement-report.json"), "settlement_after_write")
	if err != nil {
		return CrashRecovery{}, err
	}
	result.SettlementCrashExit = exit
	afterSettlementCrash, err := app.Recover(ctx, settlementDB)
	if err != nil {
		return CrashRecovery{}, err
	}
	result.PolicyVersionsAfterSettlementCrash = recoveryVersions(afterSettlementCrash.Policy.Snapshots)
	result.SettlementVersionsAfterSettlementCrash = recoveryVersions(afterSettlementCrash.Settlement.Snapshots)
	result.WindowsAfterSettlementCrash = len(afterSettlementCrash.Windows)
	if _, err := runChild(ctx, executable, configPath, settlementDB, filepath.Join(workDir, "crash-settlement-restart.json"), ""); err != nil {
		return CrashRecovery{}, err
	}
	afterSettlementRestart, err := app.Recover(ctx, settlementDB)
	if err != nil {
		return CrashRecovery{}, err
	}
	result.PolicyVersionsAfterSettlementRestart = recoveryVersions(afterSettlementRestart.Policy.Snapshots)
	result.SettlementVersionsAfterSettlementRestart = recoveryVersions(afterSettlementRestart.Settlement.Snapshots)
	result.WindowsAfterRestart = len(afterSettlementRestart.Windows)

	result.Passed = result.PolicyCrashExit == 86 && result.SettlementCrashExit == 86 &&
		equalInt64s(result.PolicyVersionsAfterPolicyCrash, []int64{1}) &&
		equalInt64s(result.PolicyVersionsAfterPolicyRestart, []int64{1, 2}) &&
		equalInt64s(result.SettlementVersionsAfterPolicyRestart, []int64{1, 2, 3, 4}) &&
		equalInt64s(result.PolicyVersionsAfterSettlementCrash, []int64{1}) &&
		len(result.SettlementVersionsAfterSettlementCrash) == 0 && result.WindowsAfterSettlementCrash == 0 &&
		equalInt64s(result.PolicyVersionsAfterSettlementRestart, []int64{1, 2}) &&
		equalInt64s(result.SettlementVersionsAfterSettlementRestart, []int64{1, 2}) && result.WindowsAfterRestart == 2
	return result, nil
}

func runChild(ctx context.Context, executable, configPath, dbPath, reportPath, crashPoint string) (int, error) {
	commandContext, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	command := exec.CommandContext(commandContext, executable, "run", "--config", configPath, "--db", dbPath, "--report", reportPath)
	command.Stdout = nil
	command.Stderr = nil
	command.Env = os.Environ()
	if crashPoint != "" {
		command.Env = append(command.Env, "VPNBENCH_CRASH_POINT="+crashPoint)
	} else {
		filtered := make([]string, 0, len(command.Env))
		for _, value := range command.Env {
			if !strings.HasPrefix(value, "VPNBENCH_CRASH_POINT=") {
				filtered = append(filtered, value)
			}
		}
		command.Env = filtered
	}
	err := command.Run()
	if err == nil {
		return 0, nil
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		if crashPoint != "" && exitError.ExitCode() == 86 {
			return 86, nil
		}
		return exitError.ExitCode(), fmt.Errorf("child %s exited with %d: %w", crashPoint, exitError.ExitCode(), err)
	}
	return 0, err
}

func recoveryVersions(snapshots []app.RecoverySnapshot) []int64 {
	versions := make([]int64, 0, len(snapshots))
	for _, snapshot := range snapshots {
		versions = append(versions, snapshot.Version)
	}
	return versions
}

func equalInt64s(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (r Result) Bytes() ([]byte, error) {
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func (r Result) Write(path string) error {
	encoded, err := r.Bytes()
	if err != nil {
		return err
	}
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, encoded, 0o644)
}
