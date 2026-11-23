package engine

import "meshmigrate/internal/model"

type ImportItem struct {
	PolicyID string         `json:"policyId"`
	Outcome  string         `json:"outcome"`
	Basis    []string       `json:"basis"`
	Detail   map[string]any `json:"detail,omitempty"`
}

type ImportReport struct {
	ReportID         string                `json:"reportId"`
	RequestID        string                `json:"requestId"`
	SnapshotVersion  uint64                `json:"snapshotVersion"`
	ActiveVersion    uint64                `json:"activeVersion"`
	Status           string                `json:"status"`
	Decision         string                `json:"decision"`
	Format           string                `json:"format"`
	FormatVersion    int                   `json:"formatVersion"`
	SourceDigest     string                `json:"sourceDigest"`
	ContentDigest    string                `json:"contentDigest"`
	CreatedAtMs      int64                 `json:"createdAtMs"`
	DefaultsApplied  []model.DefaultDetail `json:"defaultsApplied,omitempty"`
	AffectedServices []string              `json:"affectedServices,omitempty"`
	Items            []ImportItem          `json:"items"`
}

type ImportSuccessResponse struct {
	RequestID        string                `json:"requestId"`
	ReportID         string                `json:"reportId"`
	Status           string                `json:"status"`
	SnapshotVersion  uint64                `json:"snapshotVersion"`
	ActiveVersion    uint64                `json:"activeVersion"`
	Decision         string                `json:"decision"`
	Format           string                `json:"format"`
	FormatVersion    int                   `json:"formatVersion"`
	SourceDigest     string                `json:"sourceDigest"`
	ContentDigest    string                `json:"contentDigest"`
	AffectedServices []string              `json:"affectedServices"`
	DefaultsApplied  []model.DefaultDetail `json:"defaultsApplied,omitempty"`
	Items            []ImportItem          `json:"items"`
}
