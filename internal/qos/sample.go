package qos

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const DefaultSampleSeed int64 = 20261125

func samplePolicy() []Queue {
	parent := func(v string) *string { return &v }
	return []Queue{
		{
			ID: "wan", CIRBPS: 100_000_123, PIRBPS: 100_000_123, CBSBytes: 64_000, PBSBytes: 128_000,
			Priority: 1, Match: MatchRule{SrcPortMin: 0, SrcPortMax: 65535, DSCPValues: []int{}},
		},
		{
			ID: "voice", CIRBPS: 5_000_123, PIRBPS: 8_000_123, CBSBytes: 8_000, PBSBytes: 16_000,
			Priority: 7, Match: MatchRule{SrcPortMin: 5060, SrcPortMax: 5061, DSCPValues: []int{46}}, ParentID: parent("wan"),
		},
		{
			ID: "voice-sip", CIRBPS: 4_000_321, PIRBPS: 6_000_321, CBSBytes: 4_000, PBSBytes: 8_000,
			Priority: 7, Match: MatchRule{SrcPortMin: 5060, SrcPortMax: 5060, DSCPValues: []int{46}}, ParentID: parent("voice"),
		},
		{
			ID: "video", CIRBPS: 20_000_377, PIRBPS: 30_000_377, CBSBytes: 32_000, PBSBytes: 64_000,
			Priority: 5, Match: MatchRule{SrcPortMin: 2000, SrcPortMax: 4000, DSCPValues: []int{34, 46}}, ParentID: parent("wan"),
		},
		{
			ID: "video-rtp", CIRBPS: 12_000_123, PIRBPS: 15_000_123, CBSBytes: 16_000, PBSBytes: 32_000,
			Priority: 6, Match: MatchRule{SrcPortMin: 5004, SrcPortMax: 5005, DSCPValues: []int{34}}, ParentID: parent("video"),
		},
		{
			ID: "bulk", CIRBPS: 10_000_111, PIRBPS: 15_000_111, CBSBytes: 48_000, PBSBytes: 96_000,
			Priority: 2, Match: MatchRule{SrcPortMin: 1, SrcPortMax: 1023, DSCPValues: []int{8}}, ParentID: parent("wan"),
		},
		{
			ID: "guest", CIRBPS: 30_000_123, PIRBPS: 40_000_123, CBSBytes: 32_000, PBSBytes: 64_000,
			Priority: 3, Match: MatchRule{SrcPortMin: 1024, SrcPortMax: 65535, DSCPValues: []int{0, 10}}, ParentID: parent("wan"),
		},
	}
}

func sampleTraffic(seed int64, policies []Queue) ([]TrafficSample, error) {
	baseTS := int64(1_760_000_000_000)
	var all []TrafficSample
	for _, q := range policies {
		if q.ID == "wan" {
			continue
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(q.ID))
		r := rand.New(rand.NewSource(seed ^ int64(h.Sum64())))
		step := 4 + r.Intn(5)
		count := 72
		for i := 0; i < count; i++ {
			bytes := int64(700 + r.Intn(1900))
			if q.ID == "video" || q.ID == "video-rtp" {
				bytes = int64(1500 + r.Intn(1200))
			}
			dscp := 0
			if len(q.Match.DSCPValues) > 0 {
				dscp = q.Match.DSCPValues[r.Intn(len(q.Match.DSCPValues))]
			}
			ts := baseTS + int64(i*step) + int64(r.Intn(2))
			all = append(all, TrafficSample{TimestampMS: ts, QueueID: q.ID, Bytes: bytes, DSCP: dscp})
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].TimestampMS != all[j].TimestampMS {
			return all[i].TimestampMS < all[j].TimestampMS
		}
		pi, pj := 0, 0
		for _, q := range policies {
			if q.ID == all[i].QueueID {
				pi = q.Priority
			}
			if q.ID == all[j].QueueID {
				pj = q.Priority
			}
		}
		if pi != pj {
			return pi > pj
		}
		return all[i].QueueID < all[j].QueueID
	})
	return all, nil
}

// EnsureSamples regenerates deterministic policy and traffic files in dir.
func EnsureSamples(dir string, seed int64) (SampleMeta, error) {
	if seed == 0 {
		seed = DefaultSampleSeed
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return SampleMeta{}, err
	}
	queues := samplePolicy()
	normalized, _, biz := NormalizeAndValidate(queues)
	if biz != nil {
		return SampleMeta{}, biz
	}
	policy := SnapshotRequest{RequestID: "sample-snapshot-v1", Version: 1, Queues: normalized}
	policyBytes, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return SampleMeta{}, err
	}
	policyBytes = append(policyBytes, '\n')
	traffic, err := sampleTraffic(seed, normalized)
	if err != nil {
		return SampleMeta{}, err
	}
	var trafficBytes []byte
	for _, p := range traffic {
		line, err := json.Marshal(p)
		if err != nil {
			return SampleMeta{}, err
		}
		trafficBytes = append(trafficBytes, line...)
		trafficBytes = append(trafficBytes, '\n')
	}
	policyFile := filepath.Join(dir, "policy_sample.json")
	trafficFile := filepath.Join(dir, "traffic_sample.jsonl")
	if err := atomicWrite(policyFile, policyBytes, 0o644); err != nil {
		return SampleMeta{}, err
	}
	if err := atomicWrite(trafficFile, trafficBytes, 0o644); err != nil {
		return SampleMeta{}, err
	}
	ph := sha256.Sum256(policyBytes)
	th := sha256.Sum256(trafficBytes)
	meta := SampleMeta{
		Seed: seed, PolicyFile: policyFile, TrafficFile: trafficFile,
		PolicySHA256: hex.EncodeToString(ph[:]), TrafficSHA256: hex.EncodeToString(th[:]),
		TrafficPackets: len(traffic), QueueCount: len(normalized),
		GeneratedAt: "deterministic-fixed-seed",
	}
	metaBytes, _ := json.MarshalIndent(meta, "", "  ")
	seedBytes := []byte("seed=" + strconv.FormatInt(seed, 10) + "\n" +
		"policy_sha256=" + meta.PolicySHA256 + "\n" +
		"traffic_sha256=" + meta.TrafficSHA256 + "\n")
	if err := atomicWrite(filepath.Join(dir, "sample_meta.json"), append(metaBytes, '\n'), 0o644); err != nil {
		return SampleMeta{}, err
	}
	if err := atomicWrite(filepath.Join(dir, "seed.txt"), seedBytes, 0o644); err != nil {
		return SampleMeta{}, err
	}
	return meta, nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func LoadTrafficFile(path string) ([]TrafficSample, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	var out []TrafficSample
	lines := 0
	for _, raw := range splitLines(data) {
		if len(raw) == 0 {
			continue
		}
		lines++
		var p TrafficSample
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, "", fmt.Errorf("traffic line %d: %w", lines, err)
		}
		if p.TimestampMS < 0 || p.QueueID == "" || p.Bytes <= 0 || p.DSCP < 0 || p.DSCP > 63 {
			return nil, "", fmt.Errorf("traffic line %d: invalid packet", lines)
		}
		out = append(out, p)
	}
	return out, hex.EncodeToString(sum[:]), nil
}

func splitLines(data []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			out = append(out, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		out = append(out, data[start:])
	}
	return out
}
