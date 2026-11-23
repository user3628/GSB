package codes

import (
	"fmt"
	"sort"
)

// Code is a stable business code emitted by the workbench.
type Code string

const (
	None            Code = ""
	PolicyMalformed Code = "E_POLICY_MALFORMED"
	UnitUnknown     Code = "E_UNIT_UNKNOWN"
	NumericOverflow Code = "E_NUMERIC_OVERFLOW"
	ItemBudget      Code = "E_ITEM_BUDGET"
	SnapshotVersion Code = "E_SNAPSHOT_VERSION"
	ErrorBound      Code = "E_ERROR_BOUND"
)

var priority = map[Code]int{
	None:            0,
	ErrorBound:      1,
	PolicyMalformed: 2,
	UnitUnknown:     3,
	NumericOverflow: 4,
	ItemBudget:      5,
	SnapshotVersion: 6,
}

// BusinessError carries an assessable error without relying on error strings.
type BusinessError struct {
	Code     Code
	Message  string
	ItemID   string
	Offset   int
	Original error
}

func (e *BusinessError) Error() string {
	prefix := string(e.Code)
	if prefix == "" {
		prefix = "E_INTERNAL"
	}
	if e.ItemID != "" {
		prefix += " item=" + e.ItemID
	}
	if e.Message != "" {
		return prefix + ": " + e.Message
	}
	return prefix
}

func (e *BusinessError) Unwrap() error { return e.Original }

func New(code Code, format string, args ...any) *BusinessError {
	return &BusinessError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// SortUnique returns codes in severity order, with the most severe first.
func SortUnique(in []Code) []Code {
	seen := make(map[Code]bool, len(in))
	out := make([]Code, 0, len(in))
	for _, code := range in {
		if code == None || seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, code)
	}
	sort.Slice(out, func(i, j int) bool {
		pi, pj := priority[out[i]], priority[out[j]]
		if pi != pj {
			return pi > pj
		}
		return out[i] < out[j]
	})
	return out
}

func Primary(in []Code) Code {
	sorted := SortUnique(in)
	if len(sorted) == 0 {
		return None
	}
	return sorted[0]
}
