package spec

// QueryTurn is one turn read from a JSONL session shard.
type QueryTurn struct {
	SessionID string   `json:"session_id"`
	Turn      int      `json:"turn"`
	RawQuery  string   `json:"raw_query"`
	Locale    string   `json:"locale"`
	History   []string `json:"history_rewrites"`
}

// Policy is the cache/rewrite strategy snapshot.
type Policy struct {
	Version           int             `yaml:"version" json:"version"`
	ParentVersion     int             `yaml:"parent_version" json:"parent_version"`
	ContentDigest     string          `yaml:"content_digest" json:"content_digest"`
	CandidateDepth    int             `yaml:"candidate_depth" json:"candidate_depth"`
	StrategyPerMille  map[string]int  `yaml:"strategy_per_mille" json:"strategy_per_mille"`
	CacheCapacity     int             `yaml:"cache_capacity" json:"cache_capacity"`
	MinPrefixRunes    int             `yaml:"min_prefix_runes" json:"min_prefix_runes"`
	PrefixBlockRunes  int             `yaml:"prefix_block_runes" json:"prefix_block_runes"`
	HitGainThreshold  float64         `yaml:"hit_gain_threshold" json:"hit_gain_threshold"`
	FailureUpperLimit float64         `yaml:"failure_upper_limit" json:"failure_upper_limit"`
	EvalBudgetPerTurn int             `yaml:"eval_budget_per_turn" json:"eval_budget_per_turn"`
	Weights           DecisionWeights `yaml:"weights" json:"weights"`
	Kind              string          `yaml:"kind,omitempty" json:"kind,omitempty"`
}

// DecisionWeights are the log-domain adjudication weights. All must be > 0.
type DecisionWeights struct {
	Overlap     float64 `yaml:"overlap" json:"overlap"`
	Hit         float64 `yaml:"hit" json:"hit"`
	Age         float64 `yaml:"age" json:"age"`
	FailPenalty float64 `yaml:"fail_penalty" json:"fail_penalty"`
}

// Token is a segmented token with rune offsets into the normalized text.
type Token struct {
	Text  string `json:"text"`
	Start int    `json:"start"`
	End   int    `json:"end"`
	Kind  string `json:"kind"` // "han" | "latin"
}

// Candidate is one generated rewrite candidate.
type Candidate struct {
	ID       string  `json:"candidate_id"`
	Text     string  `json:"candidate_text"`
	Strategy string  `json:"strategy"`
	Overlap  float64 `json:"term_overlap"`
	Rank     int     `json:"rank"`
}

// PrefixBlock records one hashed block and its rune span.
type PrefixBlock struct {
	Index       int    `json:"block_index"`
	Start       int    `json:"rune_start"`
	End         int    `json:"rune_end"`
	Fingerprint string `json:"fingerprint"`
}

// Hit outcome constants.
const (
	HitExact    = "EXACT_HIT"
	HitPartial  = "PARTIAL_HIT"
	HitReusable = "NOT_REUSABLE"
	PrefixBelow = "PREFIX_BELOW_THRESHOLD"
)

// HitResult is the stage-2 per-candidate cache decision input.
type HitResult struct {
	CandidateID    string        `json:"candidate_id"`
	Cacheable      bool          `json:"cacheable"`
	Reason         string        `json:"reason"`
	Outcome        string        `json:"outcome"`
	PrefixRunes    int           `json:"prefix_runes"`
	Blocks         []PrefixBlock `json:"blocks"`
	MatchedSlot    string        `json:"matched_slot,omitempty"`
	MatchedBlocks  int           `json:"matched_blocks"`
	ContentionWith []string      `json:"contention_with,omitempty"`
	EvictionOf     string        `json:"eviction_of,omitempty"`
	EvictionReason string        `json:"eviction_reason,omitempty"`
}

// AdjResult is the stage-3 per-candidate adjudication.
type AdjResult struct {
	CandidateID  string             `json:"candidate_id"`
	Decision     string             `json:"decision"` // ACCEPT | REJECT
	BenefitScore float64            `json:"benefit_score"`
	FailureUpper float64            `json:"failure_upper"`
	Components   map[string]float64 `json:"components"`
	Derivation   map[string]string  `json:"derivation"`
	NumericError *BizError          `json:"numeric_error,omitempty"`
}

// Skip records a deterministic budget skip.
type Skip struct {
	CandidateID string `json:"candidate_id"`
	Strategy    string `json:"strategy"`
	Reason      string `json:"reason"`
}

// BudgetLedger makes per-turn consumption fully recomputable.
type BudgetLedger struct {
	Turn           int            `json:"turn"`
	EvalUsed       int            `json:"eval_used"`
	EvalBudget     int            `json:"eval_budget"`
	PerMilleBudget map[string]int `json:"per_mille_budget"`
	PerMilleUsed   map[string]int `json:"per_mille_used"`
	CapacityTotal  int            `json:"capacity_total"`
	CapacityUsed   int            `json:"capacity_used"`
	Accepted       []string       `json:"accepted"`
	Skips          []Skip         `json:"skips"`
	Degraded       bool           `json:"degraded"`
	DegradeReasons []string       `json:"degrade_reasons"`
}

// TurnOutput bundles the four stages' products for one turn.
type TurnOutput struct {
	SessionID  string            `json:"session_id"`
	Turn       int               `json:"turn"`
	PolicyVer  int               `json:"policy_version"`
	Tokens     []Token           `json:"tokens"`
	Candidates []Candidate       `json:"candidates"`
	Hits       []HitResult       `json:"hits"`
	Adj        []AdjResult       `json:"adjudications"`
	Ledger     BudgetLedger      `json:"ledger"`
	StageError *BizError         `json:"stage_error,omitempty"`
	Digests    map[string]string `json:"digests"`
}

// Checkpoint is one committed stage fact row.
type Checkpoint struct {
	Seq           int64  `json:"seq"`
	Stage         string `json:"stage"`
	SessionID     string `json:"session_id"`
	Turn          int    `json:"turn"`
	InputDigest   string `json:"input_digest"`
	PolicyVersion int    `json:"policy_version"`
	ProductDigest string `json:"product_digest"`
	Payload       string `json:"payload"`
}

// Slot is a cache prefix slot.
type Slot struct {
	SlotID       string `json:"slot_id"`
	Fingerprint  string `json:"fingerprint"`
	BlockCount   int    `json:"block_count"`
	OwnerSession string `json:"owner_session"`
	OwnerTurn    int    `json:"owner_turn"`
	BirthTurn    int    `json:"birth_turn"`
	LastUsedTurn int    `json:"last_used_turn"`
}

// PolicyPatch is a single-field incremental patch.
type PolicyPatch struct {
	Version          int            `json:"version" yaml:"version"`
	CandidateDepth   *int           `json:"candidate_depth,omitempty" yaml:"candidate_depth,omitempty"`
	StrategyPerMille map[string]int `json:"strategy_per_mille,omitempty" yaml:"strategy_per_mille,omitempty"`
	CacheCapacity    *int           `json:"cache_capacity,omitempty" yaml:"cache_capacity,omitempty"`
	MinPrefixRunes   *int           `json:"min_prefix_runes,omitempty" yaml:"min_prefix_runes,omitempty"`
	HitGainThreshold *float64       `json:"hit_gain_threshold,omitempty" yaml:"hit_gain_threshold,omitempty"`
}
