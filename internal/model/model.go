package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	KindAccount  = "account_policy"
	KindAudit    = "audit_policy"
	KindService  = "service_start_type"
	KindRegistry = "registry_key_permissions"
)

const (
	CodeOK                   = "OK"
	CodeConfigParse          = "E_CONFIG_PARSE"
	CodeDepMissing           = "E_DEP_MISSING"
	CodeDepCycle             = "E_DEP_CYCLE"
	CodeProbeEscape          = "E_PROBE_ESCAPE"
	CodeProbeMismatch        = "E_PROBE_MISMATCH"
	CodeProbeExec            = "E_PROBE_EXEC"
	CodeSnapshotVersion      = "E_SNAPSHOT_VERSION"
	CodeSnapshotReplacement  = "E_SNAPSHOT_REPLACEMENT"
	CodeSnapshotCorrupt      = "E_SNAPSHOT_CORRUPT"
	CodeChangeContention     = "E_CHANGE_CONTENTION"
	CodeNamespaceUnavailable = "E_NAMESPACE_UNAVAILABLE"
	CodeStore                = "E_STORE"
)

const SampleMarker = "T0_FIXED_SAMPLE"

// Item is one normalized baseline entry. SourceOffset is not part of the
// canonical snapshot payload; it is retained only for explainable diagnostics.
type Item struct {
	ID           string          `json:"id"`
	Kind         string          `json:"kind"`
	Refs         []string        `json:"refs,omitempty"`
	Account      *AccountPolicy  `json:"account,omitempty"`
	Audit        *AuditPolicy    `json:"audit,omitempty"`
	Service      *ServicePolicy  `json:"service,omitempty"`
	Registry     *RegistryPolicy `json:"registry,omitempty"`
	SourceOffset int             `json:"-"`
}

type AccountPolicy struct {
	Name        string   `json:"name"`
	Value       *float64 `json:"value,omitempty"`
	Unit        string   `json:"unit"`
	Min         *float64 `json:"min,omitempty"`
	Max         *float64 `json:"max,omitempty"`
	InheritFrom string   `json:"inherit_from,omitempty"`
	Objective   string   `json:"objective"`

	NormalizedValue float64 `json:"-"`
	NormalizedUnit  string  `json:"-"`
}

type AuditPolicy struct {
	Name           string   `json:"name"`
	Category       string   `json:"category"`
	Path           string   `json:"path"`
	Mode           string   `json:"mode"`
	Enabled        bool     `json:"enabled"`
	RetentionValue *float64 `json:"retention_value,omitempty"`
	RetentionUnit  string   `json:"retention_unit,omitempty"`
}

type ServicePolicy struct {
	Name      string `json:"name"`
	StartType string `json:"start_type"`
	ImagePath string `json:"image_path,omitempty"`
}

type RegistryPolicy struct {
	Key   string     `json:"key"`
	Path  string     `json:"path"`
	Mode  string     `json:"mode"`
	ACL   []ACLEntry `json:"acl"`
	Hive  string     `json:"hive,omitempty"`
	Value string     `json:"value,omitempty"`
}

type ACLEntry struct {
	Principal string `json:"principal"`
	Access    string `json:"access"`
}

type Edge struct {
	From string `json:"from"` // dependent item
	To   string `json:"to"`   // referenced dependency
}

type Diff struct {
	ID            string   `json:"id"`
	Action        string   `json:"action"`
	ChangedFields []string `json:"changed_fields"`
	BeforeHash    string   `json:"before_hash,omitempty"`
	AfterHash     string   `json:"after_hash,omitempty"`
	Tightened     bool     `json:"tightened"`
}

type TraceEntry struct {
	Trigger string `json:"trigger"`
	ItemID  string `json:"item_id"`
	Depth   int    `json:"depth"`
	Chain   []Edge `json:"chain"`
}

type Invalidation struct {
	Triggers []string     `json:"triggers"`
	Items    []string     `json:"items"`
	Edges    []Edge       `json:"edges"`
	Traces   []TraceEntry `json:"traces"`
}

type BizError struct {
	Code            string
	ItemID          string
	SourceOffset    int
	Message         string
	Items           []string
	Edges           []Edge
	FallbackVersion int64
}

func (e *BizError) Error() string {
	if e == nil {
		return ""
	}
	parts := []string{e.Code}
	if e.ItemID != "" {
		parts = append(parts, "item="+e.ItemID)
	}
	if e.SourceOffset >= 0 {
		parts = append(parts, fmt.Sprintf("offset=%d", e.SourceOffset))
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	return strings.Join(parts, ": ")
}

func NewError(code, itemID string, offset int, message string) *BizError {
	return &BizError{Code: code, ItemID: itemID, SourceOffset: offset, Message: message, FallbackVersion: -1}
}

func CodeOf(err error) string {
	if err == nil {
		return CodeOK
	}
	if be, ok := err.(*BizError); ok {
		return be.Code
	}
	return CodeStore
}

func CloneItem(in Item) Item {
	out := in
	out.Refs = append([]string(nil), in.Refs...)
	if in.Account != nil {
		v := *in.Account
		v.Value = cloneFloat(in.Account.Value)
		v.Min = cloneFloat(in.Account.Min)
		v.Max = cloneFloat(in.Account.Max)
		out.Account = &v
	}
	if in.Audit != nil {
		v := *in.Audit
		v.RetentionValue = cloneFloat(in.Audit.RetentionValue)
		out.Audit = &v
	}
	if in.Service != nil {
		v := *in.Service
		out.Service = &v
	}
	if in.Registry != nil {
		v := *in.Registry
		v.ACL = append([]ACLEntry(nil), in.Registry.ACL...)
		out.Registry = &v
	}
	return out
}

func cloneFloat(in *float64) *float64 {
	if in == nil {
		return nil
	}
	v := *in
	return &v
}

func SortItems(items []Item) {
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
}

func ItemMap(items []Item) map[string]Item {
	m := make(map[string]Item, len(items))
	for _, it := range items {
		m[it.ID] = it
	}
	return m
}

func SortedIDs(m map[string]Item) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func SortedEdges(edges []Edge) []Edge {
	out := append([]Edge(nil), edges...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

func DedupEdges(edges []Edge) []Edge {
	seen := make(map[Edge]struct{}, len(edges))
	out := make([]Edge, 0, len(edges))
	for _, e := range edges {
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		out = append(out, e)
	}
	return SortedEdges(out)
}

func ItemHash(it Item) string {
	cp := CloneItem(it)
	cp.SourceOffset = 0
	data, _ := json.Marshal(cp)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func HashJSON(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func MarshalCanonical(v any) ([]byte, error) {
	return json.Marshal(v)
}

// DependencyIDs returns explicit references plus the inheritance edge. The
// result is sorted and de-duplicated so graph construction is deterministic.
func DependencyIDs(it Item) []string {
	seen := make(map[string]struct{}, len(it.Refs)+1)
	for _, ref := range it.Refs {
		seen[ref] = struct{}{}
	}
	if it.Account != nil && it.Account.InheritFrom != "" {
		seen[it.Account.InheritFrom] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for ref := range seen {
		out = append(out, ref)
	}
	sort.Strings(out)
	return out
}

func ContainsString(items []string, target string) bool {
	for _, s := range items {
		if s == target {
			return true
		}
	}
	return false
}
