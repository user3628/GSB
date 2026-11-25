package qos

import (
	"fmt"
	"net/http"
	"sort"
)

// BusinessError is serialized verbatim as an API failure response.
type BusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	Status  int            `json:"-"`
}

func (e *BusinessError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewError(code, message string, status int, context map[string]any) *BusinessError {
	if context == nil {
		context = map[string]any{}
	}
	if status == 0 {
		switch code {
		case CodeStaleBase, CodeStaleSnapshot, CodeStaleRead, CodeRequestConflict, CodeAborted:
			status = http.StatusConflict
		case CodePolicyInvalid, CodeDependencyCycle, CodeSimulationFailed:
			status = http.StatusUnprocessableEntity
		case CodeNoPolicy, CodeNotFound:
			status = http.StatusNotFound
		default:
			status = http.StatusInternalServerError
		}
	}
	return &BusinessError{Code: code, Message: message, Context: context, Status: status}
}

func policyInvalid(message string, context map[string]any) *BusinessError {
	return NewError(CodePolicyInvalid, message, http.StatusUnprocessableEntity, context)
}

func staleBase(base, current int64) *BusinessError {
	return NewError(CodeStaleBase, "increment base version is not the current committed version", http.StatusConflict, map[string]any{
		"base_version": base, "current_version": current,
	})
}

func staleSnapshot(version, current int64) *BusinessError {
	return NewError(CodeStaleSnapshot, "snapshot version must be greater than the current version", http.StatusConflict, map[string]any{
		"snapshot_version": version, "current_version": current,
	})
}

func staleRead(observed, viewVersion, current int64, reason string, extra map[string]any) *BusinessError {
	ctx := map[string]any{
		"observed_version": observed,
		"view_version":     viewVersion,
		"current_version":  current,
		"reason":           reason,
	}
	for k, v := range extra {
		ctx[k] = v
	}
	return NewError(CodeStaleRead, "the observed version is no longer a complete readable version", http.StatusConflict, ctx)
}

func cycleError(path []string) *BusinessError {
	cycle := append([]string(nil), path...)
	seen := map[string]bool{}
	ids := make([]string, 0, len(path))
	for _, id := range path {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return NewError(CodeDependencyCycle, "policy dependency graph contains a cycle", http.StatusUnprocessableEntity, map[string]any{
		"cycle":     cycle,
		"queue_ids": ids,
	})
}
