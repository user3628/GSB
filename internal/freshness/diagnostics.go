package freshness

type OccupancyRecord struct {
	ClientID               string `json:"client_id"`
	Nonce                  string `json:"nonce"`
	FirstOccupiedAtMS      int64  `json:"first_occupied_at_ms"`
	WindowMS               int64  `json:"window_ms"`
	ExpiresAtMS            int64  `json:"expires_at_ms"`
	RemainingWindowMS      int64  `json:"remaining_window_ms"`
	Weight                 int64  `json:"weight"`
	FirstBatchID           string `json:"first_batch_id"`
	FirstRequestID         string `json:"first_request_id"`
	WindowVersion          int64  `json:"window_version"`
	ToleranceConfigVersion string `json:"tolerance_config_version"`
}

type OccupancySection struct {
	Records                []OccupancyRecord `json:"records"`
	ActiveCount            int               `json:"active_count"`
	ExpiredUnreclaimed     int               `json:"expired_unreclaimed"`
	TotalOutstandingWeight int64             `json:"total_outstanding_weight"`
	AsOfMS                 int64             `json:"as_of_ms"`
}

type NonceRecord struct {
	ClientID               string `json:"client_id"`
	Nonce                  string `json:"nonce"`
	State                  string `json:"state"`
	FirstOccupiedAtMS      int64  `json:"first_occupied_at_ms"`
	ExpiresAtMS            int64  `json:"expires_at_ms"`
	WindowMS               int64  `json:"window_ms"`
	Weight                 int64  `json:"weight"`
	FirstBatchID           string `json:"first_batch_id"`
	FirstRequestID         string `json:"first_request_id"`
	LastEventVersion       int64  `json:"last_event_version"`
	LastEventAtMS          int64  `json:"last_event_at_ms"`
	WindowVersion          int64  `json:"first_window_version"`
	ToleranceConfigVersion string `json:"tolerance_config_version"`
}

type NonceTableSection struct {
	Records        []NonceRecord `json:"records"`
	ActiveCount    int           `json:"active_count"`
	ReclaimedCount int           `json:"reclaimed_count"`
}

type TrailRecord struct {
	ID                     int64  `json:"id"`
	BatchID                string `json:"batch_id"`
	WindowVersion          int64  `json:"window_version"`
	ToleranceConfigVersion string `json:"tolerance_config_version"`
	Ordinal                int    `json:"ordinal"`
	RequestID              string `json:"request_id"`
	Stage                  string `json:"stage"`
	Outcome                string `json:"outcome"`
	Code                   string `json:"code"`
	AtMS                   int64  `json:"at_ms"`
	DetailsJSON            string `json:"details_json"`
}

type TrailSection struct {
	Records []TrailRecord `json:"records"`
	Count   int           `json:"count"`
}

type DiagnosticSnapshot struct {
	SnapshotWindowVersion  int64              `json:"snapshot_window_version"`
	ToleranceConfigVersion string             `json:"tolerance_config_version"`
	AsOfMS                 int64              `json:"as_of_ms"`
	Complete               bool               `json:"complete"`
	Partial                bool               `json:"partial"`
	BudgetMS               int64              `json:"budget_ms"`
	ElapsedMS              int64              `json:"elapsed_ms"`
	CompletedSections      []string           `json:"completed_sections"`
	IncompleteSections     []string           `json:"incomplete_sections"`
	Reason                 string             `json:"reason,omitempty"`
	Occupancy              *OccupancySection  `json:"occupancy,omitempty"`
	Nonces                 *NonceTableSection `json:"nonces,omitempty"`
	Trails                 *TrailSection      `json:"trails,omitempty"`
}

type ModeDifferenceItem struct {
	Ordinal     int    `json:"ordinal"`
	RequestID   string `json:"request_id"`
	FullResult  string `json:"full_result"`
	FullCode    string `json:"full_code"`
	FastResult  string `json:"fast_result"`
	FastCode    string `json:"fast_code"`
	Differs     bool   `json:"differs"`
	Explanation string `json:"explanation"`
}

type ModeDifferenceReport struct {
	BatchID                string               `json:"batch_id"`
	WindowVersion          int64                `json:"window_version"`
	ToleranceConfigVersion string               `json:"tolerance_config_version"`
	ActualMode             string               `json:"actual_mode"`
	PrecheckLimit          int                  `json:"precheck_limit"`
	FastSampleRate         float64              `json:"fast_sample_rate"`
	PredictedFastOutcome   string               `json:"predicted_fast_outcome"`
	FullResultAvailable    bool                 `json:"full_result_available"`
	Items                  []ModeDifferenceItem `json:"items"`
	Differences            int                  `json:"differences"`
	Complete               bool                 `json:"complete"`
	Partial                bool                 `json:"partial"`
	Reason                 string               `json:"reason,omitempty"`
	Note                   string               `json:"note"`
}

type ClientView struct {
	ClientID            string `json:"client_id"`
	PickedWindowVersion int64  `json:"picked_window_version"`
	PickedConfigVersion string `json:"picked_config_version,omitempty"`
}

type FieldDifference struct {
	Field             string `json:"field"`
	DecisionValue     any    `json:"decision_value"`
	ClientViewedValue any    `json:"client_viewed_value"`
	Explanation       string `json:"explanation"`
}

type ClientInconsistency struct {
	ClientID              string            `json:"client_id"`
	PickedWindowVersion   int64             `json:"picked_window_version"`
	PickedConfigVersion   string            `json:"picked_config_version"`
	BatchWindowVersion    int64             `json:"batch_window_version"`
	DecisionConfigVersion string            `json:"decision_config_version"`
	BatchVisibleAtVersion bool              `json:"batch_visible_at_version"`
	DecisionBatchCode     string            `json:"decision_batch_code"`
	ViewedBatchCode       string            `json:"viewed_batch_code"`
	Differs               bool              `json:"differs"`
	Differences           []FieldDifference `json:"differences"`
	Explanation           string            `json:"explanation"`
}

type InconsistencyReport struct {
	BatchID                string                `json:"batch_id"`
	WindowVersion          int64                 `json:"window_version"`
	ToleranceConfigVersion string                `json:"tolerance_config_version"`
	Views                  []ClientInconsistency `json:"views"`
	Differs                bool                  `json:"differs"`
	Complete               bool                  `json:"complete"`
	Partial                bool                  `json:"partial"`
	Reason                 string                `json:"reason,omitempty"`
	Note                   string                `json:"note"`
}
