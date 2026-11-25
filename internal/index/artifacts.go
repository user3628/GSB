package index

type DocStat struct {
	DocID     int64 `json:"doc_id"`
	TitleLen  int   `json:"title_len"`
	BodyLen   int   `json:"body_len"`
	Length    int   `json:"length"`
	Tokenized bool  `json:"tokenized"`
}

type QuantPosting struct {
	DocID          int64   `json:"doc_id"`
	Quantized      int64   `json:"quantized"`
	Reference      string  `json:"reference"`
	ReferenceFloat float64 `json:"reference_float"`
	Dequantized    float64 `json:"dequantized"`
	ErrorBound     float64 `json:"error_bound"`
}

type TermStat struct {
	TermID          int32          `json:"term_id"`
	Term            string         `json:"term"`
	DF              int            `json:"df"`
	IDF             string         `json:"idf"`
	IDFFloat        float64        `json:"idf_float"`
	MaxWeight       string         `json:"max_weight"`
	MaxWeightFloat  float64        `json:"max_weight_float"`
	Scale           float64        `json:"scale"`
	QuantStep       float64        `json:"quant_step"`
	QuantErrorBound float64        `json:"quant_error_bound"`
	Postings        []QuantPosting `json:"postings"`
}

type StatsArtifact struct {
	ConfigVersion string     `json:"config_version"`
	StatsDigest   string     `json:"stats_digest"`
	DocCount      int        `json:"doc_count"`
	TotalLength   int        `json:"total_length"`
	AverageLength string     `json:"average_length"`
	AverageFloat  float64    `json:"average_float"`
	Docs          []DocStat  `json:"docs"`
	Terms         []TermStat `json:"terms"`
}
