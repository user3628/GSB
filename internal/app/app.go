// Package app wires the deterministic sample generator, the engine and the
// report exporter together, and drives the fault-injection scenarios.
package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"iotrust/internal/engine"
	"iotrust/internal/samples"
)

// GenSamples writes every scenario sample plus the local key material into
// dir (created when empty) and returns the directory used.
func GenSamples(dir string) (string, error) {
	if dir == "" {
		d, err := os.MkdirTemp("", "iotrust-")
		if err != nil {
			return "", err
		}
		dir = d
	}
	if err := os.MkdirAll(filepath.Join(dir, "samples"), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "key.hex"), []byte(samples.KeyHex()), 0o600); err != nil {
		return "", err
	}
	for _, name := range samples.Scenarios {
		ss, err := samples.Generate(name)
		if err != nil {
			return "", err
		}
		raw, err := json.MarshalIndent(ss, "", "  ")
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(filepath.Join(dir, "samples", name+".json"), raw, 0o644); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func cleanDB(dir, name string) string {
	dbPath := filepath.Join(dir, name+".db")
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		os.Remove(dbPath + suffix)
	}
	return dbPath
}

// RunScenario executes one scenario end to end and returns the
// deterministic report plus the work directory used.
func RunScenario(dir, name string, workers int) (*engine.Report, string, error) {
	if dir == "" {
		d, err := os.MkdirTemp("", "iotrust-")
		if err != nil {
			return nil, "", err
		}
		dir = d
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", err
	}
	ss, err := samples.Generate(name)
	if err != nil {
		return nil, "", err
	}
	if _, err := GenSamples(dir); err != nil {
		return nil, "", err
	}
	params := ss.Params
	if workers > 0 {
		params.Workers = workers
	}
	dbPath := cleanDB(dir, name)

	eng, err := engine.NewEngine(dbPath, ss.Policies, params, samples.Key())
	if err != nil {
		return nil, "", err
	}
	var rep *engine.Report
	claims := ss.Claims
	for round := 0; round < ss.Rounds; round++ {
		if round > 0 && len(ss.Claims2) > 0 {
			claims = ss.Claims2
		}
		rep = eng.RunRound(claims, ss.Now, ss.VerifyAt)
	}
	if ss.Contention > 0 {
		key := ss.Claims[0].TenantID + "/" + ss.Claims[0].DeviceID
		rep.Contention = eng.RunContentionCheck(key, ss.Contention)
	}
	eng.Close()

	if name == "snapshot-gap" {
		rep, err = runSnapshotGap(dbPath, ss, params)
		if err != nil {
			return nil, "", err
		}
	}
	return rep, dir, nil
}

// runSnapshotGap injects a version-skipping snapshot row through the
// store's fault-injection entry point (input-driven, no source change),
// then reloads: the engine must refuse with E_SNAPSHOT_VERSION, report the
// latest continuous version, and fall back to it for the next commit.
func runSnapshotGap(dbPath string, ss *engine.SampleSet, params engine.Params) (*engine.Report, error) {
	eng, err := engine.NewEngine(dbPath, ss.Policies, params, samples.Key())
	if err != nil {
		return nil, err
	}
	defer eng.Close()
	fake := &engine.Snapshot{Version: 3, PrevHash: strings.Repeat("0", 64), Devices: []engine.DeviceState{}}
	payload, _ := json.Marshal(fake)
	if err := eng.Store().InjectRawSnapshot(3, strings.Repeat("0", 64), strings.Repeat("1", 64), string(payload)); err != nil {
		return nil, err
	}
	eng2, err := engine.NewEngine(dbPath, ss.Policies, params, samples.Key())
	if err != nil {
		return nil, err
	}
	defer eng2.Close()
	if eng2.Fault() == nil || eng2.Fault().Code != engine.CodeSnapshotVersion {
		return nil, fmt.Errorf("snapshot-gap: expected E_SNAPSHOT_VERSION fault, got %+v", eng2.Fault())
	}
	return eng2.RunRound(ss.Claims, ss.Now, ss.VerifyAt), nil
}
