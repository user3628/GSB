package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	CodeSpecInvalid     = "E_SPEC_INVALID"
	CodeStaleBase       = "E_STALE_BASE"
	CodeEnvDrift        = "E_ENV_DRIFT"
	CodeDependencyCycle = "E_DEPENDENCY_CYCLE"
	CodeProbeQueueFull  = "E_PROBE_QUEUE_FULL"
	CodeProbeTimeout    = "E_PROBE_TIMEOUT"
	CodeProbeFailed     = "E_PROBE_FAILED"
	CodeProbeBudget     = "E_PROBE_BUDGET_EXCEEDED"
	CodeNotFound        = "E_NOT_FOUND"
	CodeConflict        = "E_CONFLICT"
	CodeInternal        = "E_INTERNAL"
)

type Ref struct {
	Environment string `json:"environment"`
	Service     string `json:"service"`
}

func (r Ref) String() string { return r.Environment + "/" + r.Service }

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

func NewError(code, message string, details any) *APIError {
	return &APIError{Code: code, Message: message, Details: details}
}

type ProbeSpec struct {
	Path      string `json:"path,omitempty"`
	Method    string `json:"method,omitempty"`
	TimeoutMS int    `json:"timeoutMs,omitempty"`
}

type Unit struct {
	Service        string            `json:"service"`
	ImageDigest    string            `json:"imageDigest"`
	Config         map[string]string `json:"config,omitempty"`
	DependsOn      []string          `json:"dependsOn,omitempty"`
	DesiredVersion string            `json:"desiredVersion"`
	Probe          *ProbeSpec        `json:"probe,omitempty"`
}

type Environment struct {
	Name     string `json:"name"`
	Priority int    `json:"priority"`
	Units    []Unit `json:"units"`
}

type Snapshot struct {
	Version      int64         `json:"version"`
	GeneratedAt  time.Time     `json:"generatedAt"`
	Checksum     string        `json:"checksum,omitempty"`
	Environments []Environment `json:"environments"`
}

type UnitChange struct {
	Environment string `json:"environment"`
	Unit
}

type Increment struct {
	BaseVersion int64        `json:"baseVersion"`
	GeneratedAt time.Time    `json:"generatedAt"`
	Checksum    string       `json:"checksum,omitempty"`
	Changes     []UnitChange `json:"changes"`
}

type Issue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

var digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var servicePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}[a-z0-9]$|^[a-z]$`)

func ValidImageDigest(value string) bool { return digestPattern.MatchString(value) }

func ValidServiceName(value string) bool { return servicePattern.MatchString(value) }

// Checksum returns sha256:<hex> over deterministic JSON with the checksum field blank.
func Checksum(value any) (string, error) {
	switch v := value.(type) {
	case Snapshot:
		v.Checksum = ""
		value = v
	case *Snapshot:
		cp := *v
		cp.Checksum = ""
		value = cp
	case Increment:
		v.Checksum = ""
		value = v
	case *Increment:
		cp := *v
		cp.Checksum = ""
		value = cp
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func VerifyChecksum(value any, supplied string) error {
	if supplied == "" {
		return NewError(CodeSpecInvalid, "checksum is required", []Issue{{Path: "checksum", Message: "missing checksum"}})
	}
	actual, err := Checksum(value)
	if err != nil {
		return NewError(CodeSpecInvalid, "cannot compute checksum", []Issue{{Path: "checksum", Message: err.Error()}})
	}
	if actual != supplied {
		return NewError(CodeSpecInvalid, "checksum mismatch", map[string]string{"expected": actual, "actual": supplied})
	}
	return nil
}

type SemVer struct {
	Major int
	Minor int
	Patch int
	Raw   string
}

func ParseSemVer(raw string) (SemVer, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(raw), "v")
	parts := strings.Split(trimmed, ".")
	if len(parts) != 3 {
		return SemVer{Raw: raw}, fmt.Errorf("invalid semver %q", raw)
	}
	nums := make([]int, 3)
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return SemVer{Raw: raw}, fmt.Errorf("invalid semver %q", raw)
		}
		nums[i] = n
	}
	return SemVer{Major: nums[0], Minor: nums[1], Patch: nums[2], Raw: raw}, nil
}

func CompareVersion(a, b string) int {
	va, erra := ParseSemVer(a)
	vb, errb := ParseSemVer(b)
	if erra != nil || errb != nil {
		return strings.Compare(a, b)
	}
	if va.Major != vb.Major {
		if va.Major < vb.Major {
			return -1
		}
		return 1
	}
	if va.Minor != vb.Minor {
		if va.Minor < vb.Minor {
			return -1
		}
		return 1
	}
	if va.Patch != vb.Patch {
		if va.Patch < vb.Patch {
			return -1
		}
		return 1
	}
	return 0
}
