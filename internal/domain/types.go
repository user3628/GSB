package domain

// ValueKind is the wire-level type of a semantic slot value.
type ValueKind string

const (
	KindInteger   ValueKind = "integer"
	KindEnum      ValueKind = "enum"
	KindDateRange ValueKind = "date_range"
)

// ValueRange preserves the role boundary used by either generation of the
// semantic role system. For integer ranges Min and Max are inclusive. For enum
// ranges Enum is the complete accepted set. For date ranges the two endpoints
// are inclusive.
type ValueRange struct {
	Kind    ValueKind `json:"kind"`
	Min     *int64    `json:"min,omitempty"`
	Max     *int64    `json:"max,omitempty"`
	Enum    []string  `json:"enum,omitempty"`
	DateMin string    `json:"date_min,omitempty"`
	DateMax string    `json:"date_max,omitempty"`
}

// SlotValue contains both the canonical normalized value and, when applicable,
// the original text. Canonical is the only representation used by the state
// engine.
type SlotValue struct {
	Kind      ValueKind `json:"kind"`
	Canonical string    `json:"canonical"`
	Raw       string    `json:"raw,omitempty"`
}

// ValueSource makes a confirmed value traceable to its input generation.
type ValueSource struct {
	Kind           string `json:"kind"` // new, legacy, rule, codec
	Field          string `json:"field,omitempty"`
	CharOffset     int    `json:"char_offset"`
	MapVersion     string `json:"map_version,omitempty"`
	LegacyRoleCode *int   `json:"legacy_role_code,omitempty"`
	DeclaredRole   string `json:"declared_role,omitempty"`
}

type ConfirmedSlot struct {
	Role       string      `json:"role"`
	Range      ValueRange  `json:"value_range"`
	Value      SlotValue   `json:"value"`
	Source     ValueSource `json:"source"`
	SourceTurn int         `json:"source_turn"`
}

type SlotCandidate struct {
	Role       string     `json:"role"`
	Priority   int        `json:"priority"`
	Range      ValueRange `json:"value_range"`
	Compatible bool       `json:"compatible"`
	Reason     string     `json:"reason"`
}

type PendingSlot struct {
	Role          string          `json:"role,omitempty"`
	Range         ValueRange      `json:"value_range,omitempty"`
	Field         string          `json:"field,omitempty"`
	Reason        string          `json:"reason"`
	Candidates    []SlotCandidate `json:"candidates,omitempty"`
	Source        ValueSource     `json:"source"`
	RequestedTurn int             `json:"requested_turn"`
	ResolvedTurn  *int            `json:"resolved_turn,omitempty"`
}

type NewSlotAnnotation struct {
	Role       string     `json:"role"`
	Range      ValueRange `json:"value_range"`
	Value      string     `json:"value"`
	Field      string     `json:"field,omitempty"`
	CharOffset int        `json:"char_offset"`
}

type LegacySlotAnnotation struct {
	RoleCode   int        `json:"role_code"`
	MapVersion string     `json:"map_version"`
	RawValue   string     `json:"raw_value"`
	Range      ValueRange `json:"value_range"`
	Field      string     `json:"field,omitempty"`
	CharOffset int        `json:"char_offset"`
}

func (s LegacySlotAnnotation) FieldName() string {
	if s.Field != "" {
		return s.Field
	}
	return "legacy_slot"
}

type TurnInput struct {
	SessionID  string                 `json:"session_id"`
	TurnIndex  int                    `json:"turn_index"`
	UserText   string                 `json:"user_text"`
	BotReply   string                 `json:"bot_reply"`
	Language   string                 `json:"language"`
	IntentHint string                 `json:"intent_hint,omitempty"`
	NewSlots   []NewSlotAnnotation    `json:"new_slots,omitempty"`
	OldSlots   []LegacySlotAnnotation `json:"old_slots,omitempty"`
}

type ParseArtifact struct {
	Turn TurnInput `json:"turn"`
}

type UpgradedSlot struct {
	Role         string      `json:"role"`
	Range        ValueRange  `json:"value_range"`
	Value        SlotValue   `json:"value"`
	Source       ValueSource `json:"source"`
	DeclaredRole string      `json:"declared_role,omitempty"`
	Order        int         `json:"order"`
}

type Ambiguity struct {
	SessionID      string          `json:"session_id"`
	TurnIndex      int             `json:"turn_index"`
	Field          string          `json:"field"`
	LegacyRoleCode int             `json:"legacy_role_code"`
	RawValue       string          `json:"raw_value"`
	CharOffset     int             `json:"char_offset"`
	MapVersion     string          `json:"map_version"`
	Candidates     []SlotCandidate `json:"candidates"`
	Reason         string          `json:"reason"`
}

type Diagnostic struct {
	Code         string   `json:"code"`
	Severity     string   `json:"severity"`
	Message      string   `json:"message"`
	SessionID    string   `json:"session_id"`
	TurnIndex    int      `json:"turn_index"`
	Field        string   `json:"field,omitempty"`
	Role         string   `json:"role,omitempty"`
	PreviousRole string   `json:"previous_role,omitempty"`
	CurrentRole  string   `json:"current_role,omitempty"`
	IntentFrom   string   `json:"intent_from,omitempty"`
	IntentTo     string   `json:"intent_to,omitempty"`
	AtTurn       *int     `json:"at_turn,omitempty"`
	PreviousVal  string   `json:"previous_value,omitempty"`
	CurrentVal   string   `json:"current_value,omitempty"`
	Candidates   []string `json:"candidates,omitempty"`
	Evidence     string   `json:"evidence,omitempty"`
}

type UpgradeArtifact struct {
	SessionID    string         `json:"session_id"`
	TurnIndex    int            `json:"turn_index"`
	MapVersion   string         `json:"map_version"`
	Slots        []UpgradedSlot `json:"slots"`
	Ambiguities  []Ambiguity    `json:"ambiguities"`
	Diagnostics  []Diagnostic   `json:"diagnostics"`
	InputCount   int            `json:"input_count"`
	ResolvedRole []string       `json:"resolved_roles"`
}

type Change struct {
	Kind       string `json:"kind"`
	Role       string `json:"role,omitempty"`
	OldValue   string `json:"old_value,omitempty"`
	NewValue   string `json:"new_value,omitempty"`
	RuleID     string `json:"rule_id,omitempty"`
	Diagnostic string `json:"diagnostic,omitempty"`
}

type IntentVisit struct {
	TurnIndex int    `json:"turn_index"`
	Intent    string `json:"intent"`
	RuleID    string `json:"rule_id,omitempty"`
}

type TurnState struct {
	SessionID   string          `json:"session_id"`
	TurnIndex   int             `json:"turn_index"`
	Intent      string          `json:"intent"`
	Confirmed   []ConfirmedSlot `json:"confirmed_slots"`
	Pending     []PendingSlot   `json:"pending_slots"`
	Changes     []Change        `json:"changes"`
	Rules       []string        `json:"triggered_rules"`
	MapVersion  string          `json:"map_version"`
	Diagnostics []Diagnostic    `json:"diagnostics"`
	IntentLog   []IntentVisit   `json:"intent_history"`
}

type SessionState struct {
	SessionID  string      `json:"session_id"`
	MapVersion string      `json:"map_version"`
	Turns      []TurnState `json:"turns"`
}

type RangeFinding struct {
	SessionID  string     `json:"session_id"`
	TurnIndex  int        `json:"turn_index"`
	Role       string     `json:"role"`
	Field      string     `json:"field"`
	Value      SlotValue  `json:"value"`
	Range      ValueRange `json:"value_range"`
	CharOffset int        `json:"char_offset"`
	Reason     string     `json:"reason"`
}

type DriftFinding struct {
	SessionID    string `json:"session_id"`
	TurnIndex    int    `json:"turn_index"`
	SourceTurn   int    `json:"source_turn"`
	Role         string `json:"role"`
	PreviousRole string `json:"previous_role"`
	Field        string `json:"field"`
	LegacyCode   int    `json:"legacy_role_code"`
	MapVersion   string `json:"map_version"`
	CharOffset   int    `json:"char_offset"`
	Reason       string `json:"reason"`
	Evidence     string `json:"evidence"`
}

type AmbiguityFinding struct {
	SessionID      string          `json:"session_id"`
	TurnIndex      int             `json:"turn_index"`
	Field          string          `json:"field"`
	LegacyRoleCode int             `json:"legacy_role_code"`
	RawValue       string          `json:"raw_value"`
	CharOffset     int             `json:"char_offset"`
	Candidates     []SlotCandidate `json:"candidates"`
	Reason         string          `json:"reason"`
}

type AlignmentReport struct {
	SessionID   string             `json:"session_id"`
	TurnIndex   int                `json:"turn_index"`
	MapVersion  string             `json:"map_version"`
	OutOfRange  []RangeFinding     `json:"out_of_range"`
	RoleDrifts  []DriftFinding     `json:"role_drifts"`
	Ambiguous   []AmbiguityFinding `json:"ambiguous"`
	Diagnostics []Diagnostic       `json:"diagnostics"`
}

type RoleDef struct {
	Name        string     `json:"name"`
	Range       ValueRange `json:"value_range"`
	Description string     `json:"description,omitempty"`
}

type RoleCatalog struct {
	Version string    `json:"version"`
	Roles   []RoleDef `json:"roles"`
}

type RoleMapEntry struct {
	LegacyCode  int        `json:"legacy_code"`
	Role        string     `json:"role"`
	Priority    int        `json:"priority"`
	LegacyRange ValueRange `json:"legacy_range"`
	NewRange    ValueRange `json:"new_range"`
	Note        string     `json:"note,omitempty"`
}

type RoleMap struct {
	Version                string         `json:"version"`
	CompatibleDeclarations []string       `json:"compatible_declarations"`
	Entries                []RoleMapEntry `json:"entries"`
}

type IntentRule struct {
	ID             string   `json:"id"`
	Priority       int      `json:"priority"`
	From           []string `json:"from,omitempty"`
	KeywordsAny    []string `json:"keywords_any"`
	To             string   `json:"to"`
	Clarify        []string `json:"clarify,omitempty"`
	ResolvePending []string `json:"resolve_pending,omitempty"`
}

type RulesConfig struct {
	Version       string       `json:"version"`
	InitialIntent string       `json:"initial_intent"`
	Rules         []IntentRule `json:"rules"`
}

type NormalizationConfig struct {
	Version            string            `json:"version"`
	IntegerPrecision   int               `json:"integer_precision"`
	EnumCaseFold       bool              `json:"enum_case_fold"`
	EnumAliases        map[string]string `json:"enum_aliases,omitempty"`
	DateLayouts        []string          `json:"date_layouts,omitempty"`
	DateRangeSplitters []string          `json:"date_range_splitters,omitempty"`
}

type AlignmentConfig struct {
	Version                string `json:"version"`
	OutOfRangeEnabled      bool   `json:"out_of_range_enabled"`
	DriftEnabled           bool   `json:"drift_enabled"`
	AmbiguityEnabled       bool   `json:"ambiguity_enabled"`
	MaxFindingsPerCategory int    `json:"max_findings_per_category"`
}

type WorkbenchConfig struct {
	Version       string              `json:"version"`
	Rules         RulesConfig         `json:"rules"`
	Normalization NormalizationConfig `json:"normalization"`
	Alignment     AlignmentConfig     `json:"alignment"`
}

type ActiveMap struct {
	Version string `json:"version"`
}

type Dataset struct {
	Turns       []TurnInput
	Catalog     RoleCatalog
	Maps        map[string]RoleMap
	ActiveMap   RoleMap
	Config      WorkbenchConfig
	ContentHash string
	MapFileHash string
	RolesHash   string
	ConfigHash  string
}

type StageFingerprint struct {
	Stage        string `json:"stage"`
	InputDigest  string `json:"input_digest"`
	OutputDigest string `json:"output_digest"`
}

type TurnFingerprint struct {
	SessionID string             `json:"session_id"`
	TurnIndex int                `json:"turn_index"`
	Stages    []StageFingerprint `json:"stages"`
	Combined  string             `json:"combined"`
}

type SessionFingerprint struct {
	SessionID    string            `json:"session_id"`
	MapVersion   string            `json:"map_version"`
	ConfigDigest string            `json:"config_digest"`
	Turns        []TurnFingerprint `json:"turns"`
	Combined     string            `json:"combined"`
}

type TurnRef struct {
	SessionID string `json:"session_id"`
	TurnIndex int    `json:"turn_index"`
}

type RunReport struct {
	RunMode        string               `json:"run_mode"`
	Stopped        bool                 `json:"stopped"`
	CompletedTurns int                  `json:"completed_turns"`
	ReusedTurns    int                  `json:"reused_turns"`
	RebuiltTurns   int                  `json:"rebuilt_turns"`
	MapVersion     string               `json:"map_version"`
	ConfigDigest   string               `json:"config_digest"`
	Sessions       []string             `json:"sessions"`
	Completed      []TurnRef            `json:"completed"`
	Invalidations  []InvalidationRecord `json:"invalidations,omitempty"`
}

type InvalidationRecord struct {
	Seq             int64  `json:"seq"`
	SessionID       string `json:"session_id"`
	TurnIndex       int    `json:"turn_index"`
	Stage           string `json:"stage"`
	Reason          string `json:"reason"`
	OldInputDigest  string `json:"old_input_digest,omitempty"`
	NewInputDigest  string `json:"new_input_digest,omitempty"`
	OldOutputDigest string `json:"old_output_digest,omitempty"`
	NewOutputDigest string `json:"new_output_digest,omitempty"`
}

type ExportBundle struct {
	Version      string               `json:"version"`
	MapVersion   string               `json:"map_version"`
	ConfigDigest string               `json:"config_digest"`
	Sessions     []SessionState       `json:"sessions"`
	Alignments   []AlignmentReport    `json:"alignments"`
	Fingerprints []SessionFingerprint `json:"fingerprints"`
}
