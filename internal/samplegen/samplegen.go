package samplegen

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"vpnbench/internal/config"
	"vpnbench/internal/model"
	"vpnbench/internal/requests"
)

const (
	basePolicy = `{
  "schema": "vpnbench.policy/v1",
  "policies": [
    {"tunnel_id": "tun-alpha", "destination_cidr": "10.0.0.0/8", "priority": 100, "bandwidth": 7, "forwarding_cost": 2},
    {"tunnel_id": "tun-beta", "destination_cidr": "10.0.0.0/8", "priority": 100, "bandwidth": 11, "forwarding_cost": 3},
    {"tunnel_id": "tun-gamma", "destination_cidr": "10.0.0.0/8", "priority": 50, "bandwidth": 13, "forwarding_cost": 1},
    {"tunnel_id": "tun-delta", "destination_cidr": "192.168.0.0/16", "priority": 100, "bandwidth": 17, "forwarding_cost": 5},
    {"tunnel_id": "tun-epsilon", "destination_cidr": "172.16.0.0/12", "priority": 100, "bandwidth": 19, "forwarding_cost": 2}
  ]
}
`
	overflowPolicy = `{
  "schema": "vpnbench.policy/v1",
  "policies": [
    {"tunnel_id": "tun-overflow", "destination_cidr": "10.0.0.0/8", "priority": 100, "bandwidth": 10000000000000, "forwarding_cost": 1},
    {"tunnel_id": "tun-safe", "destination_cidr": "192.168.0.0/16", "priority": 100, "bandwidth": 5, "forwarding_cost": 1}
  ]
}
`
	replacementPolicy = `{
  "schema": "vpnbench.policy/v1",
  "policies": [
    {"tunnel_id": "tun-new-alpha", "destination_cidr": "198.18.0.0/15", "priority": 100, "bandwidth": 5, "forwarding_cost": 2},
    {"tunnel_id": "tun-new-beta", "destination_cidr": "198.19.0.0/16", "priority": 100, "bandwidth": 9, "forwarding_cost": 4}
  ]
}
`
)

type FileRecord struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Schema string       `json:"schema"`
	Files  []FileRecord `json:"files"`
}

func Generate(dir string) (Manifest, error) {
	files := map[string][]byte{
		"policies/base.json":        []byte(basePolicy),
		"policies/overflow.json":    []byte(overflowPolicy),
		"policies/replacement.json": []byte(replacementPolicy),
		"requests/base.txt":         []byte(baseRequests()),
		"requests/no_match.txt":     []byte(noMatchRequests()),
		"requests/overflow.txt":     []byte(overflowRequests()),
		"requests/replacement.txt":  []byte(replacementRequests()),
		"requests/single.txt":       []byte(singleRequest()),
	}

	runConfig := config.RunConfig{
		Schema:           "vpnbench.run/v1",
		PolicyFile:       "../policies/base.json",
		ConcurrencyLimit: 2,
		LatencyClock:     "fixed",
		RequestBudget:    64,
		Sessions: []config.SessionSpec{
			{SessionID: "session-a", RequestsFile: "../requests/base.txt", RequestBudget: 64},
			{SessionID: "session-b", RequestsFile: "../requests/single.txt", RequestBudget: 64},
		},
	}
	raceConfig := config.RunConfig{
		Schema:           "vpnbench.run/v1",
		PolicyFile:       "../policies/base.json",
		ConcurrencyLimit: 1,
		LatencyClock:     "fixed",
		RequestBudget:    64,
		Sessions: []config.SessionSpec{
			{SessionID: "race-a", RequestsFile: "../requests/base.txt", RequestBudget: 2},
			{SessionID: "race-b", RequestsFile: "../requests/single.txt", RequestBudget: 64},
			{SessionID: "race-b", RequestsFile: "../requests/single.txt", RequestBudget: 64},
			{SessionID: "race-c", RequestsFile: "../requests/single.txt", RequestBudget: 64},
		},
	}
	addJSON(files, "configs/workbench.json", runConfig)
	addJSON(files, "configs/race.json", raceConfig)

	anomalies := map[string]config.AnomalyParams{
		"no_match.json": {
			Schema:                 "vpnbench.anomaly/v1",
			Case:                   "no_match",
			PolicyFile:             "../policies/base.json",
			RequestsFile:           "../requests/no_match.txt",
			ExpectedCodes:          []string{"E_NO_TUNNEL"},
			ExpectSnapshotClean:    true,
			ExpectOthersUnaffected: true,
		},
		"fixed_overflow.json": {
			Schema:                 "vpnbench.anomaly/v1",
			Case:                   "fixed_overflow",
			PolicyFile:             "../policies/overflow.json",
			RequestsFile:           "../requests/overflow.txt",
			ExpectedCodes:          []string{"E_FIXED_OVERFLOW"},
			ExpectSnapshotClean:    true,
			ExpectOthersUnaffected: true,
		},
		"snapshot_gap.json": {
			Schema:                 "vpnbench.anomaly/v1",
			Case:                   "snapshot_gap",
			PolicyFile:             "../policies/base.json",
			RequestsFile:           "../requests/single.txt",
			ExpectedCodes:          []string{"E_SNAPSHOT_VERSION"},
			ExpectSnapshotClean:    true,
			ExpectOthersUnaffected: true,
		},
		"whole_replacement.json": {
			Schema:                 "vpnbench.anomaly/v1",
			Case:                   "whole_replacement",
			PolicyFile:             "../policies/base.json",
			ReplacementPolicyFile:  "../policies/replacement.json",
			RequestsFile:           "../requests/replacement.txt",
			ExpectedCodes:          []string{"E_POLICY_REPLACED"},
			ExpectSnapshotClean:    true,
			ExpectOthersUnaffected: true,
		},
		"session_race.json": {
			Schema:                 "vpnbench.anomaly/v1",
			Case:                   "session_race",
			PolicyFile:             "../policies/base.json",
			Sessions:               raceConfig.Sessions,
			ConcurrencyLimit:       1,
			RequestBudget:          64,
			ExpectedCodes:          []string{"E_SESSION_CONFLICT", "E_REQUEST_BUDGET"},
			ExpectSnapshotClean:    true,
			ExpectOthersUnaffected: true,
		},
	}
	for name, anomaly := range anomalies {
		addJSON(files, "anomalies/"+name, anomaly)
	}

	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	manifest := Manifest{Schema: "vpnbench.samples/v1"}
	for _, relative := range paths {
		fullPath := filepath.Join(dir, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			return Manifest{}, err
		}
		if err := os.WriteFile(fullPath, files[relative], 0o644); err != nil {
			return Manifest{}, err
		}
		sum := sha256.Sum256(files[relative])
		manifest.Files = append(manifest.Files, FileRecord{Path: relative, SHA256: hex.EncodeToString(sum[:])})
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Manifest{}, err
	}
	encoded = append(encoded, '\n')
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, encoded, 0o644); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func addJSON(files map[string][]byte, path string, value any) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		panic(fmt.Sprintf("marshal sample %s: %v", path, err))
	}
	files[path] = append(encoded, '\n')
}

func baseRequests() string {
	blocks := []string{
		requests.FormatBlock(request("flow-a", 2, "10.1.0.10", "10.2.0.9", 1200)),
		requests.FormatBlock(request("flow-a", 1, "10.1.0.10", "10.1.0.8", 900)),
		requests.FormatBlock(request("flow-b", 1, "10.2.0.10", "192.168.10.2", 700)),
		requests.FormatBlock(request("flow-c", 1, "10.3.0.10", "172.20.0.5", 500)),
		requests.FormatBlock(request("flow-b", 2, "10.2.0.10", "10.3.0.4", 800)),
	}
	return joinBlocks(blocks)
}

func noMatchRequests() string {
	return joinBlocks([]string{
		requests.FormatBlock(request("flow-no-match", 1, "10.9.0.1", "203.0.113.9", 640)),
		requests.FormatBlock(request("flow-no-match", 2, "10.9.0.1", "192.168.1.9", 640)),
	})
}

func overflowRequests() string {
	return joinBlocks([]string{
		requests.FormatBlock(request("flow-overflow", 1, "10.9.0.1", "10.8.0.1", 1500)),
		requests.FormatBlock(request("flow-safe", 1, "10.9.0.1", "192.168.8.1", 1500)),
	})
}

func replacementRequests() string {
	return requests.FormatBlock(request("flow-replacement", 1, "198.18.0.5", "198.18.1.20", 1024))
}

func singleRequest() string {
	return requests.FormatBlock(request("flow-single", 1, "10.4.0.10", "10.4.0.20", 512))
}

func request(streamID string, seq int64, source, destination string, packetBytes int64) model.Request {
	return model.Request{
		StreamID:    streamID,
		RequestSeq:  seq,
		Source:      source,
		Destination: destination,
		PacketBytes: packetBytes,
	}
}

func joinBlocks(blocks []string) string {
	result := ""
	for _, block := range blocks {
		result += block
	}
	return result
}
