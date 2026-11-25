package model

const (
	SchemaVersion    = "ocrlab/1.0"
	GeneratorVersion = "generator/1.1"
	EngineVersion    = "engine/1.0"
)

const (
	StageDetect     = "detect"
	StageRecognize  = "recognize"
	StageStrategy   = "strategy"
	StageDiagnostic = "diagnostic"
)

var StageOrder = map[string]int{
	StageDetect:     1,
	StageRecognize:  2,
	StageStrategy:   3,
	StageDiagnostic: 4,
}

// Rect uses normalized image coordinates. X and Y are the top-left corner.
type Rect struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	W float64 `json:"w"`
	H float64 `json:"h"`
}

type TruthBox struct {
	Index int    `json:"index"`
	Box   Rect   `json:"box"`
	Text  string `json:"text"`
}

type ImageSpec struct {
	ImageID  string     `json:"image_id"`
	Group    string     `json:"group"`
	File     string     `json:"file"`
	Width    int        `json:"width"`
	Height   int        `json:"height"`
	Language string     `json:"language"`
	Truth    []TruthBox `json:"truth_boxes"`
}

type GroupObserved struct {
	ShardCount      int `json:"shard_count"`
	TotalCharacters int `json:"total_characters"`
}

type GroupSpec struct {
	Name         string        `json:"name"`
	TargetShards int           `json:"target_shards"`
	Observed     GroupObserved `json:"observed"`
}

type Manifest struct {
	SchemaVersion    string      `json:"schema_version"`
	Seed             int64       `json:"seed"`
	GeneratorVersion string      `json:"generator_version"`
	Groups           []GroupSpec `json:"groups"`
	Images           []ImageSpec `json:"images"`
}

func (m Manifest) Group(name string) (GroupSpec, bool) {
	for _, group := range m.Groups {
		if group.Name == name {
			return group, true
		}
	}
	return GroupSpec{}, false
}

func (m Manifest) Image(id string) (ImageSpec, bool) {
	for _, image := range m.Images {
		if image.ImageID == id {
			return image, true
		}
	}
	return ImageSpec{}, false
}

type CandidateBox struct {
	Index          int     `json:"index"`
	Box            Rect    `json:"box"`
	Confidence     float64 `json:"confidence"`
	SourceStrategy string  `json:"source_strategy"`
	ComponentCount int     `json:"component_count"`
	InkPixels      int     `json:"ink_pixels"`
}

type DetectionError struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	ImageID  string `json:"image_id"`
	BoxIndex int    `json:"box_index"`
}

type DetectionProduct struct {
	SchemaVersion  string           `json:"schema_version"`
	ImageID        string           `json:"image_id"`
	Group          string           `json:"group"`
	ContentSHA256  string           `json:"content_sha256"`
	ConfigVersion  string           `json:"config_version"`
	InputVersion   string           `json:"input_version"`
	Strategy       string           `json:"strategy"`
	Candidates     []CandidateBox   `json:"candidates"`
	Errors         []DetectionError `json:"errors"`
	InvalidBoxes   int              `json:"invalid_boxes"`
	CandidateCount int              `json:"candidate_count"`
}

type RecognitionResult struct {
	CandidateIndex  int     `json:"candidate_index"`
	Box             Rect    `json:"box"`
	Status          string  `json:"status"`
	Text            string  `json:"text"`
	Confidence      float64 `json:"confidence"`
	MatchDistance   float64 `json:"match_distance"`
	TemplateCount   int     `json:"template_count"`
	GlyphCount      int     `json:"glyph_count"`
	WithinThreshold bool    `json:"within_threshold"`
	ErrorCode       string  `json:"error_code,omitempty"`
}

type RecognitionError struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	ImageID        string `json:"image_id"`
	CandidateIndex int    `json:"candidate_index"`
}

type RecognitionProduct struct {
	SchemaVersion         string              `json:"schema_version"`
	ImageID               string              `json:"image_id"`
	Group                 string              `json:"group"`
	ContentSHA256         string              `json:"content_sha256"`
	ConfigVersion         string              `json:"config_version"`
	InputVersion          string              `json:"input_version"`
	DetectionDigest       string              `json:"detection_digest"`
	TemplateLibraryDigest string              `json:"template_library_digest"`
	Strategy              string              `json:"strategy"`
	TemplateCount         int                 `json:"template_count"`
	CharacterSet          []string            `json:"character_set"`
	Results               []RecognitionResult `json:"results"`
	Errors                []RecognitionError  `json:"errors"`
	CandidateCount        int                 `json:"candidate_count"`
	RecognizedCount       int                 `json:"recognized_count"`
}

type StrategyObservations struct {
	GroupName       string `json:"group_name"`
	ShardCount      int    `json:"shard_count"`
	TotalCharacters int    `json:"total_characters"`
	CandidateCount  int    `json:"candidate_count"`
}

type StrategyEvaluation struct {
	Code              string  `json:"code"`
	Quality           float64 `json:"quality"`
	EstimatedCost     int     `json:"estimated_cost"`
	BudgetUtilization float64 `json:"budget_utilization"`
	Score             float64 `json:"score"`
	Admissible        bool    `json:"admissible"`
	Selected          bool    `json:"selected"`
	EliminatedReason  string  `json:"eliminated_reason,omitempty"`
}

type StrategyDecision struct {
	Stage       string               `json:"stage"`
	Chosen      string               `json:"chosen"`
	Reason      string               `json:"reason"`
	Evaluations []StrategyEvaluation `json:"evaluations"`
}

type StrategyProduct struct {
	SchemaVersion     string               `json:"schema_version"`
	ImageID           string               `json:"image_id"`
	Group             string               `json:"group"`
	ConfigVersion     string               `json:"config_version"`
	InputVersion      string               `json:"input_version"`
	DetectionDigest   string               `json:"detection_digest"`
	RecognitionDigest string               `json:"recognition_digest"`
	GroupClass        string               `json:"group_class"`
	BudgetUnits       int                  `json:"budget_units"`
	BudgetPerShard    int                  `json:"budget_per_shard"`
	Observations      StrategyObservations `json:"observations"`
	Detection         StrategyDecision     `json:"detection"`
	Recognition       StrategyDecision     `json:"recognition"`
}

type MetricValue struct {
	Applicable bool     `json:"applicable"`
	Value      *float64 `json:"value"`
}

func Metric(value float64) MetricValue {
	v := round9(value)
	return MetricValue{Applicable: true, Value: &v}
}

func NotApplicable() MetricValue { return MetricValue{Applicable: false, Value: nil} }

type ErrorBand struct {
	Applicable bool     `json:"applicable"`
	Method     string   `json:"method"`
	Low        *float64 `json:"low"`
	High       *float64 `json:"high"`
	SampleSize int      `json:"sample_size"`
}

type DetectionMetrics struct {
	TruthBoxes      int         `json:"truth_boxes"`
	MatchedBoxes    int         `json:"matched_boxes"`
	Recall          MetricValue `json:"recall"`
	RecallBand      ErrorBand   `json:"recall_band"`
	InvalidBoxes    int         `json:"invalid_boxes"`
	DetectionErrors int         `json:"detection_errors"`
}

type RecognitionMetrics struct {
	Candidates           int         `json:"candidates"`
	Recognized           int         `json:"recognized"`
	ExactMatches         int         `json:"exact_matches"`
	Accuracy             MetricValue `json:"accuracy"`
	AccuracyBand         ErrorBand   `json:"accuracy_band"`
	CharacterEdits       int         `json:"character_edits"`
	CharacterTotal       int         `json:"character_total"`
	CharacterAccuracy    MetricValue `json:"character_accuracy"`
	AverageMatchDistance MetricValue `json:"average_match_distance"`
	DistanceSum          float64     `json:"distance_sum"`
	DistanceSquaredSum   float64     `json:"distance_squared_sum"`
	DistanceSamples      int         `json:"distance_samples"`
	DistanceBand         ErrorBand   `json:"distance_band"`
	TemplateMisses       int         `json:"template_misses"`
	NotApplicableCount   int         `json:"not_applicable_count"`
}

type StrategyHits struct {
	Detection   []StrategyHit `json:"detection"`
	Recognition []StrategyHit `json:"recognition"`
}

type StrategyHit struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

type ImageDiagnostic struct {
	SchemaVersion      string             `json:"schema_version"`
	ImageID            string             `json:"image_id"`
	Group              string             `json:"group"`
	ConfigVersion      string             `json:"config_version"`
	InputVersion       string             `json:"input_version"`
	DetectionDigest    string             `json:"detection_digest"`
	RecognitionDigest  string             `json:"recognition_digest"`
	StrategyDigest     string             `json:"strategy_digest"`
	TruthBoxes         int                `json:"truth_boxes"`
	Detection          DetectionMetrics   `json:"detection"`
	Recognition        RecognitionMetrics `json:"recognition"`
	BudgetUnitsUsed    int                `json:"budget_units_used"`
	BudgetUnitsLimit   int                `json:"budget_units_limit"`
	Strategy           StrategyHits       `json:"strategy"`
	Issues             []DiagnosticIssue  `json:"issues"`
	NotApplicableCount int                `json:"not_applicable_count"`
}

type DiagnosticIssue struct {
	Code           string `json:"code"`
	Stage          string `json:"stage"`
	ImageID        string `json:"image_id"`
	BoxIndex       int    `json:"box_index,omitempty"`
	CandidateIndex int    `json:"candidate_index,omitempty"`
	Message        string `json:"message"`
}

type GroupMetrics struct {
	Group                string       `json:"group"`
	ShardCount           int          `json:"shard_count"`
	TotalCharacters      int          `json:"total_characters"`
	TruthBoxes           int          `json:"truth_boxes"`
	Candidates           int          `json:"candidates"`
	MatchedBoxes         int          `json:"matched_boxes"`
	DetectionRecall      MetricValue  `json:"detection_recall"`
	DetectionRecallBand  ErrorBand    `json:"detection_recall_band"`
	ExactMatches         int          `json:"exact_matches"`
	RecognitionAccuracy  MetricValue  `json:"recognition_accuracy"`
	RecognitionBand      ErrorBand    `json:"recognition_accuracy_band"`
	AverageMatchDistance MetricValue  `json:"average_match_distance"`
	AverageDistanceBand  ErrorBand    `json:"average_match_distance_band"`
	BudgetUnitsUsed      int          `json:"budget_units_used"`
	BudgetUnitsLimit     int          `json:"budget_units_limit"`
	BudgetUtilization    MetricValue  `json:"budget_utilization"`
	StrategyHits         StrategyHits `json:"strategy_hits"`
	NotApplicableCount   int          `json:"not_applicable_count"`
}

type TotalsMetrics struct {
	ShardCount           int         `json:"shard_count"`
	TotalCharacters      int         `json:"total_characters"`
	TruthBoxes           int         `json:"truth_boxes"`
	Candidates           int         `json:"candidates"`
	MatchedBoxes         int         `json:"matched_boxes"`
	DetectionRecall      MetricValue `json:"detection_recall"`
	DetectionRecallBand  ErrorBand   `json:"detection_recall_band"`
	ExactMatches         int         `json:"exact_matches"`
	RecognitionAccuracy  MetricValue `json:"recognition_accuracy"`
	RecognitionBand      ErrorBand   `json:"recognition_accuracy_band"`
	AverageMatchDistance MetricValue `json:"average_match_distance"`
	AverageDistanceBand  ErrorBand   `json:"average_match_distance_band"`
	BudgetUnitsUsed      int         `json:"budget_units_used"`
	BudgetUnitsLimit     int         `json:"budget_units_limit"`
	BudgetUtilization    MetricValue `json:"budget_utilization"`
	NotApplicableCount   int         `json:"not_applicable_count"`
}

type Report struct {
	SchemaVersion   string            `json:"schema_version"`
	EngineVersion   string            `json:"engine_version"`
	ManifestDigest  string            `json:"manifest_digest"`
	ConfigVersion   string            `json:"config_version"`
	LatencyBudgetMS int               `json:"latency_budget_ms"`
	ComputeBudget   int               `json:"compute_budget_units"`
	Groups          []GroupMetrics    `json:"groups"`
	Totals          TotalsMetrics     `json:"totals"`
	Images          []ImageDiagnostic `json:"images"`
}

func round9(v float64) float64 {
	// Keep reports stable across identical runs without carrying binary noise.
	const scale = 1e9
	if v >= 0 {
		return float64(int64(v*scale+0.5)) / scale
	}
	return float64(int64(v*scale-0.5)) / scale
}
