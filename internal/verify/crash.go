package verify

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"edgeconfig/pkg/edgeconfig"
)

func CrashChild(samplesDir, dbPath, scenario string) error {
	samples, err := LoadSampleSet(samplesDir)
	if err != nil {
		return err
	}
	resetSQLiteFiles(dbPath)
	store, err := edgeconfig.OpenStore(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	service := edgeconfig.NewService(store)
	ctx := context.Background()
	if _, err := service.SaveTopology(ctx, samples.Topology); err != nil {
		return err
	}
	if _, err := service.SubmitSnapshot(ctx, samples.Snapshot); err != nil {
		return err
	}
	intent, err := service.CreatePendingIntent(ctx, scenario, "crash-"+scenario+"-1", 1, map[string]any{
		"scenario": scenario,
		"note":     "intentionally interrupted before completion",
	})
	if err != nil {
		return err
	}

	if scenario == "probe_round" {
		mock := edgeconfig.NewMockNode("crash-hang-node", "sha256:crash", 1)
		mock.SetHang(10 * time.Minute)
		server, err := edgeconfig.StartFreeMockServer(mock)
		if err != nil {
			return err
		}
		go func() {
			client := &http.Client{}
			_, _ = client.Get(server.URL)
		}()
		waitForCondition(time.Second, 5*time.Millisecond, func() bool {
			_, active, _ := mock.Stats()
			return active == 1
		})
	}

	fmt.Println("READY " + intent.IntentID)
	select {}
}

func runForceKillCheck(parent context.Context, exe, samplesDir, baseDBPath, scenario string) ForceKillResult {
	dbPath := baseDBPath + "-crash-" + scenario + ".db"
	resetSQLiteFiles(dbPath)
	result := ForceKillResult{Scenario: scenario, DBPath: dbPath}
	if exe == "" {
		result.Error = "executable path is unavailable"
		return result
	}

	cmd := exec.Command(exe, "__crash_child", "--samples", samplesDir, "--db", dbPath, "--scenario", scenario)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if err := cmd.Start(); err != nil {
		result.Error = err.Error()
		return result
	}

	readyCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) >= 5 && line[:5] == "READY" {
				readyCh <- line
				return
			}
		}
	}()

	select {
	case line := <-readyCh:
		result.ReadyLine = line
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		result.Error = "timed out waiting for READY; stderr=" + stderr.String()
		return result
	case <-parent.Done():
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		result.Error = parent.Err().Error()
		return result
	}

	if err := cmd.Process.Kill(); err != nil {
		result.Error = "force kill failed: " + err.Error()
		return result
	}
	_ = cmd.Wait()

	store, err := edgeconfig.OpenStore(dbPath)
	if err != nil {
		result.Error = "open after kill: " + err.Error()
		return result
	}
	service := edgeconfig.NewService(store)
	pending, err := service.ListIntents(context.Background(), edgeconfig.IntentPending)
	if err != nil {
		_ = store.Close()
		result.Error = "list pending after kill: " + err.Error()
		return result
	}
	result.PendingBefore = len(pending)
	recovery, err := service.Recover(context.Background())
	if err != nil {
		_ = store.Close()
		result.Error = "recover: " + err.Error()
		return result
	}
	result.Recovery = recovery
	_ = store.Close()

	result.OK = result.PendingBefore == 1 &&
		len(recovery.AbortedIntentIDs) == 1 &&
		recovery.LastCompleteVersion == 1 &&
		recovery.RolledBackToVersion == 1
	if !result.OK {
		raw, _ := json.Marshal(recovery)
		result.Error = "recovery assertions failed: " + string(raw)
	}
	return result
}
