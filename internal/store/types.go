package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"meshmigrate/internal/model"
)

type SnapshotMeta struct {
	SnapshotVersion         uint64
	SnapshotID              string
	GeneratedAtMs           int64
	SourceFormat            string
	FormatVersion           int
	SourceDigest            string
	ContentDigest           string
	PolicyCount             int
	CumulativeRoundingError int64
	DefaultDetails          []model.DefaultDetail
	ImportedAtMs            int64
}

type SnapshotData struct {
	Meta     SnapshotMeta
	Policies []model.Policy
}

type RequestRecord struct {
	RequestID    string
	RequestHash  string
	Status       string
	HTTPStatus   int
	ResponseJSON string
	ReportID     string
	CreatedAtMs  int64
	UpdatedAtMs  int64
}

type IntentRecord struct {
	IntentID        string
	RequestID       string
	Kind            string
	SnapshotVersion uint64
	Status          string
	CreatedAtMs     int64
	CompletedAtMs   *int64
	Facts           map[string]any
}

type ReportItem struct {
	PolicyID string         `json:"policyId"`
	Outcome  string         `json:"outcome"`
	Basis    []string       `json:"basis"`
	Detail   map[string]any `json:"detail,omitempty"`
}

type ReportRecord struct {
	ReportID        string
	RequestID       string
	SnapshotVersion uint64
	Status          string
	Decision        string
	SourceFormat    string
	FormatVersion   int
	SourceDigest    string
	ContentDigest   string
	CreatedAtMs     int64
	ReportJSON      []byte
	Items           []ReportItem
}

type RouteRow struct {
	VirtualServiceID string            `json:"virtualServiceId"`
	RouteKey         string            `json:"routeKey"`
	PolicyID         string            `json:"policyId"`
	Method           string            `json:"method"`
	PathPrefix       string            `json:"pathPrefix"`
	Headers          map[string]string `json:"headers"`
}

type RateWindowRow struct {
	VirtualServiceID string   `json:"virtualServiceId"`
	WindowStartMs    int64    `json:"windowStartMs"`
	WindowEndMs      int64    `json:"windowEndMs"`
	WindowMs         int64    `json:"windowMs"`
	LimitBps         uint64   `json:"limitBps"`
	PolicyCount      int      `json:"policyCount"`
	PolicyIDs        []string `json:"policyIds"`
}

type CircuitRow struct {
	VirtualServiceID string   `json:"virtualServiceId"`
	State            string   `json:"state"`
	WindowMs         int64    `json:"windowMs"`
	PolicyIDs        []string `json:"policyIds"`
}

type SummaryRow struct {
	VirtualServiceID string   `json:"virtualServiceId"`
	PolicyCount      int      `json:"policyCount"`
	RouteCount       int      `json:"routeCount"`
	RateWindowCount  int      `json:"rateWindowCount"`
	TotalRateBps     uint64   `json:"totalRateBps"`
	CircuitState     string   `json:"circuitState"`
	PolicyIDs        []string `json:"policyIds"`
}

type DerivedData struct {
	SnapshotVersion uint64
	Routes          []RouteRow
	RateWindows     []RateWindowRow
	Circuits        []CircuitRow
	Summaries       []SummaryRow
	Affected        []string
	RebuiltAtMs     int64
}

type ImportCommit struct {
	Snapshot         *model.Snapshot
	AffectedServices []string
	ReportID         string
	ReportJSON       []byte
	ReportItems      []ReportItem
	RequestID        string
	ResponseJSON     []byte
	HTTPStatus       int
	IntentID         string
	Decision         string
	NowMs            int64
	// MidCommitHook exists only behind an explicit test-only server flag. It
	// runs after all policies/derived rows are written and before the active
	// pointer switches, inside the same transaction.
	MidCommitHook func(context.Context, *sql.Tx) error
}

func marshalStrings(v []string) string {
	if v == nil {
		v = []string{}
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func marshalAny(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
