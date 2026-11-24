package blockpool

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
)

// NonNegativeInt64 accepts only a JSON integer in [0, 2^63-1]. Negative,
// fractional, exponent-form, quoted, and overflowing values are all rejected.
type NonNegativeInt64 int64

var jsonIntegerPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

func (v *NonNegativeInt64) UnmarshalJSON(data []byte) error {
	if v == nil {
		return fmt.Errorf("nil NonNegativeInt64")
	}
	s := string(data)
	if !jsonIntegerPattern.MatchString(s) {
		return fmt.Errorf("must be a non-negative integer")
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("non-negative integer overflows int64: %w", err)
	}
	*v = NonNegativeInt64(n)
	return nil
}

func (v NonNegativeInt64) MarshalJSON() ([]byte, error) {
	if v < 0 {
		return nil, fmt.Errorf("negative integer")
	}
	return []byte(strconv.FormatInt(int64(v), 10)), nil
}

type Sample struct {
	Seed       NonNegativeInt64 `json:"seed"`
	Namespaces []string         `json:"namespaces"`
	Nodes      []SampleNode     `json:"nodes"`
	Blocks     []SampleBlock    `json:"blocks"`
	Actions    []SampleAction   `json:"actions"`
}

type SampleNode struct {
	Namespace string `json:"namespace"`
	Path      string `json:"path"`
}

type SampleBlock struct {
	Digest      string           `json:"digest"`
	SizeBytes   NonNegativeInt64 `json:"size_bytes"`
	RetentionMS NonNegativeInt64 `json:"retention_ms"`
	DataBase64  string           `json:"data_base64"`
}

type SampleAction struct {
	Op        string           `json:"op"`
	Namespace string           `json:"namespace,omitempty"`
	Path      string           `json:"path,omitempty"`
	Digest    string           `json:"digest,omitempty"`
	Digests   []string         `json:"digests,omitempty"`
	Version   NonNegativeInt64 `json:"version,omitempty"`
	LeaseMS   NonNegativeInt64 `json:"lease_ms,omitempty"`
	RequestID string           `json:"request_id"`
}

type SampleActionOutcome struct {
	Index int    `json:"index"`
	Op    string `json:"op"`
	Error string `json:"error,omitempty"`
}

func ReadSample(path string) (Sample, *Error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Sample{}, Errorf(CodeSpecInvalid, "read sample file", "path", path, "cause", err.Error())
	}
	return ParseSample(data)
}

func ParseSample(data []byte) (Sample, *Error) {
	var sample Sample
	if err := json.Unmarshal(data, &sample); err != nil {
		return Sample{}, Errorf(CodeSpecInvalid, "parse sample JSON", "cause", err.Error())
	}
	if err := ValidateSample(sample); err != nil {
		return Sample{}, err
	}
	return sample, nil
}

func ValidateSample(sample Sample) *Error {
	namespaces := make(map[string]struct{}, len(sample.Namespaces))
	for _, namespace := range sample.Namespaces {
		if namespace == "" {
			return Errorf(CodeSpecInvalid, "sample declares an empty namespace")
		}
		if _, duplicate := namespaces[namespace]; duplicate {
			return Errorf(CodeSpecInvalid, "sample declares a namespace more than once", "namespace", namespace)
		}
		namespaces[namespace] = struct{}{}
	}
	for i, node := range sample.Nodes {
		if err := validateNode(node.Namespace, node.Path); err != nil {
			return Errorf(CodeSpecInvalid, "sample node is invalid", "index", i, "cause", err.Message)
		}
		if _, ok := namespaces[node.Namespace]; !ok {
			return Errorf(CodeSpecInvalid, "sample node uses an undeclared namespace",
				"index", i, "namespace", node.Namespace)
		}
	}
	digests := make(map[string]struct{}, len(sample.Blocks))
	for i, block := range sample.Blocks {
		if err := validateDigest(block.Digest); err != nil {
			return Errorf(CodeSpecInvalid, "sample block digest is invalid", "index", i, "digest", block.Digest)
		}
		if _, exists := digests[block.Digest]; exists {
			return Errorf(CodeSpecInvalid, "sample declares a block digest more than once", "digest", block.Digest)
		}
		digests[block.Digest] = struct{}{}
		content, err := base64.StdEncoding.DecodeString(block.DataBase64)
		if err != nil {
			return Errorf(CodeSpecInvalid, "sample block content is not valid base64", "digest", block.Digest, "cause", err.Error())
		}
		if int64(len(content)) != int64(block.SizeBytes) {
			return Errorf(CodeContentCorrupt, "sample size_bytes does not match decoded content size",
				"digest", block.Digest, "declared_size", int64(block.SizeBytes), "actual_size", len(content))
		}
		sum := sha256.Sum256(content)
		if hex.EncodeToString(sum[:]) != block.Digest {
			return Errorf(CodeContentCorrupt, "sample content does not match digest", "digest", block.Digest)
		}
	}
	for i, action := range sample.Actions {
		if action.Namespace != "" {
			if _, ok := namespaces[action.Namespace]; !ok {
				return Errorf(CodeSpecInvalid, "sample action uses an undeclared namespace",
					"index", i, "namespace", action.Namespace)
			}
		}
		switch action.Op {
		case "mount", "renew", "unmount":
			if err := validateNode(action.Namespace, action.Path); err != nil {
				return Errorf(CodeSpecInvalid, "sample action node is invalid", "index", i, "cause", err.Message)
			}
			if err := validateDigest(action.Digest); err != nil {
				return Errorf(CodeSpecInvalid, "sample action digest is invalid", "index", i, "digest", action.Digest)
			}
		case "replace":
			if err := validateNode(action.Namespace, action.Path); err != nil {
				return Errorf(CodeSpecInvalid, "sample replace node is invalid", "index", i, "cause", err.Message)
			}
			for _, digest := range action.Digests {
				if err := validateDigest(digest); err != nil {
					return Errorf(CodeSpecInvalid, "sample replace digest is invalid", "index", i, "digest", digest)
				}
			}
		case "reclaim":
			if err := validateDigest(action.Digest); err != nil {
				return Errorf(CodeSpecInvalid, "sample reclaim digest is invalid", "index", i, "digest", action.Digest)
			}
		default:
			return Errorf(CodeSpecInvalid, "sample action op is unknown", "index", i, "op", action.Op)
		}
	}
	return nil
}

// SeedSample creates namespaces, nodes, and blocks from a validated sample. It
// intentionally does not replay actions; callers can inspect the state before
// applying the deterministic action sequence.
func SeedSample(ctx context.Context, p *Pool, sample Sample) *Error {
	if err := ValidateSample(sample); err != nil {
		return err
	}
	for _, namespace := range sample.Namespaces {
		if _, err := p.CreateNamespace(ctx, namespace); err != nil {
			return err
		}
	}
	for _, node := range sample.Nodes {
		if _, err := p.CreateNode(ctx, NodeRef{Namespace: node.Namespace, Path: node.Path}); err != nil {
			return err
		}
	}
	for _, block := range sample.Blocks {
		content, _ := base64.StdEncoding.DecodeString(block.DataBase64)
		if content == nil {
			content = []byte{}
		}
		if _, err := p.Ingest(ctx, IngestRequest{
			Digest:      block.Digest,
			Data:        content,
			RetentionMS: int64(block.RetentionMS),
		}); err != nil {
			return err
		}
	}
	return nil
}

func ApplySampleAction(ctx context.Context, p *Pool, action SampleAction) *Error {
	switch action.Op {
	case "mount":
		_, err := p.Mount(ctx, MountRequest{RequestID: action.RequestID, Namespace: action.Namespace,
			Path: action.Path, Digest: action.Digest, LeaseTTLMS: int64(action.LeaseMS)})
		return err
	case "renew":
		_, err := p.Renew(ctx, RenewRequest{RequestID: action.RequestID, Namespace: action.Namespace,
			Path: action.Path, Digest: action.Digest, Version: int64(action.Version), LeaseTTLMS: int64(action.LeaseMS)})
		return err
	case "unmount":
		_, err := p.Unmount(ctx, UnmountRequest{RequestID: action.RequestID, Namespace: action.Namespace,
			Path: action.Path, Digest: action.Digest})
		return err
	case "replace":
		blocks := make([]BatchBlock, 0, len(action.Digests))
		for _, digest := range action.Digests {
			blocks = append(blocks, BatchBlock{Digest: digest})
		}
		_, err := p.ReplaceNodeRefs(ctx, ReplaceRequest{RequestID: action.RequestID, Namespace: action.Namespace,
			Path: action.Path, Blocks: blocks, LeaseTTLMS: int64(action.LeaseMS)})
		return err
	case "reclaim":
		_, err := p.Reclaim(ctx, ReclaimRequest{RequestID: action.RequestID, Digest: action.Digest})
		return err
	default:
		return Errorf(CodeSpecInvalid, "unknown sample action", "op", action.Op)
	}
}
