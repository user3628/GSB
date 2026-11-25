package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

type digestInstance struct {
	ServiceName      string    `json:"serviceName"`
	InstanceID       string    `json:"instanceId"`
	Endpoint         string    `json:"endpoint"`
	Weight           int       `json:"weight"`
	LeaseTTLMS       int64     `json:"leaseTtlMs"`
	Probe            ProbeSpec `json:"probe"`
	Priority         int       `json:"priority"`
	SuccessThreshold int       `json:"successThreshold"`
	FailureThreshold int       `json:"failureThreshold"`
}

type digestDocument struct {
	Version     int64            `json:"version"`
	GeneratedAt string           `json:"generatedAt"`
	Instances   []digestInstance `json:"instances"`
}

func digestBytes(version int64, generatedAt string, instances []Instance) []byte {
	sorted := SortedInstances(instances)
	doc := digestDocument{Version: version, GeneratedAt: generatedAt, Instances: make([]digestInstance, 0, len(sorted))}
	for _, in := range sorted {
		doc.Instances = append(doc.Instances, digestInstance{
			ServiceName: in.ServiceName, InstanceID: in.InstanceID, Endpoint: in.Endpoint,
			Weight: in.Weight, LeaseTTLMS: in.LeaseTTLMS, Probe: in.Probe, Priority: in.Priority,
			SuccessThreshold: in.SuccessThreshold, FailureThreshold: in.FailureThreshold,
		})
	}
	b, _ := json.Marshal(doc)
	return b
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ComputeDigest computes the canonical SHA-256 summary of snapshot fields.
// Digest and requestId are intentionally excluded. Instances are sorted by
// serviceName and instanceId before hashing.
func ComputeDigest(version int64, generatedAt string, instances []Instance) string {
	return sha256Hex(digestBytes(version, generatedAt, instances))
}

func verifyDigest(req SnapshotRequest) error {
	if strings.TrimSpace(req.Digest) == "" {
		return fmt.Errorf("digest is required")
	}
	got := ComputeDigest(req.Version, req.GeneratedAt, req.Instances)
	if !strings.EqualFold(req.Digest, got) {
		return fmt.Errorf("digest mismatch: declared=%s computed=%s", req.Digest, got)
	}
	return nil
}
