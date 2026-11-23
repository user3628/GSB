// Package domain defines the portable model used to simulate NTFS security
// descriptors. The model intentionally contains no OS-specific ACL calls so the
// same deterministic fixture can be audited on Windows, Linux and macOS.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

const (
	ACETypeAllow = "ALLOW"
	ACETypeDeny  = "DENY"
	ACETypeAudit = "AUDIT"

	OriginExplicit  = "EXPLICIT"
	OriginInherited = "INHERITED"

	DecisionAllow      = "ALLOW"
	DecisionDeny       = "DENY"
	DecisionNotGranted = "NOT_GRANTED"

	// FileAllAccess is the NTFS FILE_ALL_ACCESS mask. The fixture deliberately
	// rejects bits outside this portable mask so an out-of-range mask is
	// detectable without calling Windows APIs.
	FileAllAccess uint32 = 0x001F01FF

	FileReadData    uint32 = 0x00000001
	FileWriteData   uint32 = 0x00000002
	FileExecute     uint32 = 0x00000020
	FileDelete      uint32 = 0x00010000
	ReadControl     uint32 = 0x00020000
	WriteDAC        uint32 = 0x00040000
	WriteOwner      uint32 = 0x00080000
	Synchronize     uint32 = 0x00100000
	FileReadAccess  uint32 = FileReadData | ReadControl | Synchronize
	FileWriteAccess uint32 = FileWriteData | WriteDAC | WriteOwner | Synchronize
)

// InheritFlags models the four NTFS inheritance bits used by this project.
type InheritFlags struct {
	OI bool `json:"oi"` // object inherit
	CI bool `json:"ci"` // container inherit
	IO bool `json:"io"` // inherit only
	NP bool `json:"np"` // no propagate inherit
}

func (f InheritFlags) AnyInheritance() bool { return f.OI || f.CI }
func (f InheritFlags) Empty() bool          { return !f.OI && !f.CI && !f.IO && !f.NP }

func (f InheritFlags) String() string {
	var b strings.Builder
	if f.OI {
		b.WriteString("OI")
	}
	if f.CI {
		if b.Len() > 0 {
			b.WriteByte('|')
		}
		b.WriteString("CI")
	}
	if f.IO {
		if b.Len() > 0 {
			b.WriteByte('|')
		}
		b.WriteString("IO")
	}
	if f.NP {
		if b.Len() > 0 {
			b.WriteByte('|')
		}
		b.WriteString("NP")
	}
	if b.Len() == 0 {
		return "NONE"
	}
	return b.String()
}

// ACE is an access control entry as stored in the simulated DACL.
type ACE struct {
	Index      int          `json:"index"`
	Type       string       `json:"type"`
	SID        string       `json:"sid"`
	AccessMask uint32       `json:"access_mask"`
	Flags      InheritFlags `json:"flags"`
}

// Intent is the declared permission target attached to a node by the fixture.
// ExpectedSource is optional and gives the audit engine a formally declared
// inheritance source. Its absence is useful for detecting a broken chain.
type Intent struct {
	SID            string `json:"sid"`
	Access         string `json:"access"`
	Expected       string `json:"expected"`
	ExpectedSource string `json:"expected_source,omitempty"`
}

// Descriptor is the portable placeholder for a Windows security descriptor.
// Protected corresponds to SE_DACL_PROTECTED: inherited ACEs are not accepted
// by this node and are not propagated through it.
type Descriptor struct {
	Schema      string   `json:"schema"`
	DACLPresent bool     `json:"dacl_present"`
	Protected   bool     `json:"protected"`
	ACEs        []ACE    `json:"aces"`
	Intents     []Intent `json:"intents,omitempty"`
}

// Policy is stored once per fixture and declares the principals known to the
// host. ACEs that reference another SID are orphan references.
type Policy struct {
	Schema    string   `json:"schema"`
	FixtureID string   `json:"fixture_id"`
	KnownSIDs []string `json:"known_sids"`
}

type SourceRef struct {
	NodePath        string `json:"node_path"`
	ACENodePath     string `json:"ace_node_path"`
	ACEIndex        int    `json:"ace_index"`
	Origin          string `json:"origin"`
	InheritedFrom   string `json:"inherited_from,omitempty"`
	InheritanceStep int    `json:"inheritance_step"`
}

// EffectiveACE is a descriptor ACE after inheritance reconstruction.
type EffectiveACE struct {
	Sequence        int          `json:"sequence"`
	OriginalIndex   int          `json:"original_index"`
	Type            string       `json:"type"`
	SID             string       `json:"sid"`
	AccessMask      uint32       `json:"access_mask"`
	AccessMaskHex   string       `json:"access_mask_hex"`
	Flags           InheritFlags `json:"flags"`
	Origin          string       `json:"origin"`
	Effective       bool         `json:"effective"`
	SourcePath      string       `json:"source_path"`
	InheritedFrom   string       `json:"inherited_from,omitempty"`
	InheritanceStep int          `json:"inheritance_step"`
	SourceChain     []SourceRef  `json:"source_chain"`
}

func (a EffectiveACE) MarshalJSON() ([]byte, error) {
	type alias EffectiveACE
	v := alias(a)
	if v.SourceChain == nil {
		v.SourceChain = []SourceRef{}
	}
	return marshalNoEscape(v)
}

// Decision is the effective result for one declared intent.
type Decision struct {
	SID           string      `json:"sid"`
	Access        string      `json:"access"`
	AccessMask    uint32      `json:"access_mask"`
	AccessMaskHex string      `json:"access_mask_hex"`
	Expected      string      `json:"expected"`
	Effective     string      `json:"effective"`
	AllowedMask   uint32      `json:"allowed_mask"`
	DeniedMask    uint32      `json:"denied_mask"`
	Matched       bool        `json:"matched"`
	Sources       []SourceRef `json:"sources"`
	Contributors  []int       `json:"contributors"`
}

func (d Decision) MarshalJSON() ([]byte, error) {
	type alias Decision
	v := alias(d)
	if v.Sources == nil {
		v.Sources = []SourceRef{}
	}
	if v.Contributors == nil {
		v.Contributors = []int{}
	}
	return marshalNoEscape(v)
}

// Node is the filesystem view consumed by the audit engine.
type Node struct {
	RelPath       string
	Kind          string // DIRECTORY or FILE
	Depth         int
	Descriptor    Descriptor
	DescriptorSHA string
	Children      []string
}

type Conflict struct {
	Code           string      `json:"code"`
	NodePath       string      `json:"node_path"`
	ACEIndex       int         `json:"ace_index"`
	SubjectSID     string      `json:"subject_sid"`
	Access         string      `json:"access,omitempty"`
	Detail         string      `json:"detail"`
	FirstDeviation SourceRef   `json:"first_deviation"`
	Closure        []SourceRef `json:"closure"`
}

func (c Conflict) MarshalJSON() ([]byte, error) {
	type alias Conflict
	v := alias(c)
	if v.Closure == nil {
		v.Closure = []SourceRef{}
	}
	return marshalNoEscape(v)
}

// ConflictSummary records the stable first deviation for each business code.
type ConflictSummary struct {
	Code             string `json:"code"`
	Count            int    `json:"count"`
	FirstNodePath    string `json:"first_node_path"`
	FirstACENodePath string `json:"first_ace_node_path"`
	FirstACEIndex    int    `json:"first_ace_index"`
	FirstSubjectSID  string `json:"first_subject_sid"`
}

// NodeReport contains canonical ACEs and intent decisions for a node.
type NodeReport struct {
	RelPath   string         `json:"rel_path"`
	Kind      string         `json:"kind"`
	ACLHash   string         `json:"acl_hash"`
	Protected bool           `json:"protected"`
	ACEs      []EffectiveACE `json:"aces"`
	Decisions []Decision     `json:"decisions"`
}

func (n NodeReport) MarshalJSON() ([]byte, error) {
	type alias NodeReport
	v := alias(n)
	if v.ACEs == nil {
		v.ACEs = []EffectiveACE{}
	}
	if v.Decisions == nil {
		v.Decisions = []Decision{}
	}
	return marshalNoEscape(v)
}

func marshalNoEscape(v any) ([]byte, error) {
	// A tiny indirection makes it impossible to accidentally use an encoder
	// that escapes <, > and & differently between Go versions.
	return marshalJSON(v)
}

// DecisionRank provides a deterministic order for conflict summaries.
func DecisionRank(s string) int {
	switch s {
	case DecisionDeny:
		return 0
	case DecisionAllow:
		return 1
	default:
		return 2
	}
}

// ParseAccess maps the small declared vocabulary to portable NTFS masks.
func ParseAccess(s string) (uint32, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "READ":
		return FileReadAccess, nil
	case "WRITE":
		return FileWriteAccess, nil
	case "EXECUTE":
		return FileExecute, nil
	case "DELETE":
		return FileDelete, nil
	case "FULL", "FULL_CONTROL":
		return FileAllAccess, nil
	default:
		return 0, fmt.Errorf("unknown access type %q", s)
	}
}

func ValidAccessMask(m uint32) bool {
	return m != 0 && m&^FileAllAccess == 0
}

func MaskCovers(granted, requested uint32) bool { return granted&requested == requested }

func SortStrings(values []string) {
	sort.Strings(values)
}

func HashBytes(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
