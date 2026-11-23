// Package codes defines the stable business codes used across the pipeline.
package codes

const (
	OK                 = "OK"
	EUnknownVersion    = "E_UNKNOWN_VERSION"
	ETruncated         = "E_TRUNCATED"
	EEventFieldMissing = "E_EVENT_FIELD_MISSING"
	EFlowEscaped       = "E_FLOW_ESCAPED"
	ESnapshotVersion   = "E_SNAPSHOT_VERSION"
	EEventBudget       = "E_EVENT_BUDGET"
)

// IsTerminal reports whether a code terminates a window.
func IsTerminal(code string) bool {
	return code == EEventBudget || code == ESnapshotVersion
}
