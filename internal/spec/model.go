package spec

import "encoding/json"

// ConfigItem declares one configuration entry in a snapshot or delta.
type ConfigItem struct {
	KeyPath string          `json:"key_path"`
	Type    string          `json:"type"`
	Value   json.RawMessage `json:"value"`
	Order   int64           `json:"order"`
}

// Component declares a component's subscriptions and upstream dependencies.
type Component struct {
	ID          string   `json:"id"`
	SubPrefixes []string `json:"sub_prefixes"`
	Wildcard    bool     `json:"wildcard"`
	DependsOn   []string `json:"depends_on"`
}

// Snapshot is a full, versioned configuration + subscription image.
type Snapshot struct {
	Version     int64        `json:"version"`
	GeneratedAt string       `json:"generated_at"`
	Checksum    string       `json:"checksum"`
	Items       []ConfigItem `json:"items"`
	Components  []Component  `json:"components"`
}

// ItemChange is a localized key-path change inside a delta.
type ItemChange struct {
	Op   string     `json:"op"` // upsert | delete
	Item ConfigItem `json:"item"`
}

// SubscriptionChange re-points a component's subscriptions or dependency edges.
type SubscriptionChange struct {
	Op        string    `json:"op"` // replace
	Component Component `json:"component"`
}

// Delta is an incremental fragment based on a specific base version.
type Delta struct {
	BaseVersion   int64                `json:"base_version"`
	TargetVersion int64                `json:"target_version"`
	Items         []ItemChange         `json:"item_changes"`
	Subscriptions []SubscriptionChange `json:"subscription_changes"`
}

// Error is the business error carrying a stable code and structured context.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func NewError(code, message string, ctx map[string]any) *Error {
	return &Error{Code: code, Message: message, Context: ctx}
}
