package dnsdetect

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

// TimeScale is the only time representation used by the domain model: an
// int64 count of 100 ns ticks relative to the Unix epoch.
const TimeScale = "100ns-unix"

// SamplingMarker is emitted in reports to make the fixed time basis explicit.
const SamplingMarker = "fixed-100ns"

type SourceKind string

const (
	SourceBinary SourceKind = "binary"
	SourceJSONL  SourceKind = "jsonl"
	SourceText   SourceKind = "text"
	SourceAuto   SourceKind = "auto"
)

type RecordRole string

const (
	RoleQuery    RecordRole = "query"
	RoleResponse RecordRole = "response"
	RoleCache    RecordRole = "cache"
)

type RecordSection string

const (
	SectionAnswer     RecordSection = "answer"
	SectionAuthority  RecordSection = "authority"
	SectionAdditional RecordSection = "additional"
	SectionCache      RecordSection = "cache"
)

const (
	TypeA    uint16 = 1
	TypeAAAA uint16 = 28
)

// ResourceRecord is a normalized DNS resource record. TTL is seconds. Binary
// sources decode TTL little-endian; JSON and text sources use their native
// decimal representation.
type ResourceRecord struct {
	Section RecordSection `json:"section"`
	Name    string        `json:"name"`
	Type    uint16        `json:"type"`
	TTL     uint32        `json:"ttl"`
	Address string        `json:"address,omitempty"`
	DataHex string        `json:"data_hex,omitempty"`
	Offset  int64         `json:"offset"`
}

// Event is the unified observation model. QuestionKey is the binary label
// length-prefixed canonical key used for pairing and stable sorting.
type Event struct {
	SourcePath     string           `json:"source_path"`
	SourceKind     SourceKind       `json:"source_kind"`
	SourcePriority int              `json:"source_priority"`
	FormatVersion  int              `json:"format_version"`
	Offset         int64            `json:"offset"`
	TXID           uint32           `json:"txid"`
	Flags          uint16           `json:"flags"`
	Question       string           `json:"question"`
	QuestionKey    string           `json:"-"`
	QType          uint16           `json:"qtype"`
	Role           RecordRole       `json:"role"`
	TimeTicks      int64            `json:"time_ticks"`
	TimeErrorNS    int64            `json:"time_error_ns"`
	TimeErrorTicks int64            `json:"time_error_ticks"`
	Records        []ResourceRecord `json:"records,omitempty"`
	SourceAddress  string           `json:"source_address,omitempty"`
	ResolverID     string           `json:"resolver_id,omitempty"`
}

// EventRef is the stable, report-safe identity of an event.
type EventRef struct {
	SourcePath     string     `json:"source_path"`
	SourceKind     SourceKind `json:"source_kind"`
	SourcePriority int        `json:"source_priority"`
	Offset         int64      `json:"offset"`
	Role           RecordRole `json:"role"`
	TimeTicks      int64      `json:"time_ticks"`
}

func (e Event) Ref() EventRef {
	return EventRef{
		SourcePath:     e.SourcePath,
		SourceKind:     e.SourceKind,
		SourcePriority: e.SourcePriority,
		Offset:         e.Offset,
		Role:           e.Role,
		TimeTicks:      e.TimeTicks,
	}
}

type Window struct {
	Index      int64 `json:"index"`
	StartTicks int64 `json:"start_ticks"`
	EndTicks   int64 `json:"end_ticks"`
}

type Pair struct {
	Window      Window     `json:"window"`
	TXID        uint32     `json:"txid"`
	Question    string     `json:"question"`
	QuestionKey string     `json:"question_key_hex"`
	Primary     EventRef   `json:"primary"`
	Evidence    []EventRef `json:"evidence,omitempty"`
}

type Finding struct {
	Code       string     `json:"code"`
	Window     Window     `json:"window"`
	TXID       uint32     `json:"txid"`
	Question   string     `json:"question"`
	SourcePath string     `json:"source_path"`
	Offset     int64      `json:"offset"`
	TimeTicks  int64      `json:"time_ticks"`
	Related    []EventRef `json:"related,omitempty"`
	Addresses  []string   `json:"addresses,omitempty"`
	Owners     []string   `json:"owners,omitempty"`
	Detail     string     `json:"detail"`
}

type UncertainAssociation struct {
	Code       string   `json:"code"`
	Window     Window   `json:"window"`
	TXID       uint32   `json:"txid"`
	Question   string   `json:"question"`
	SourcePath string   `json:"source_path"`
	Offset     int64    `json:"offset"`
	TimeTicks  int64    `json:"time_ticks"`
	Query      EventRef `json:"query"`
	Response   EventRef `json:"response"`
	LowerTicks int64    `json:"lower_ticks"`
	UpperTicks int64    `json:"upper_ticks"`
	Detail     string   `json:"detail"`
}

type Config struct {
	AssociationWindowTicks int64              `json:"association_window_ticks"`
	WindowSpanTicks        int64              `json:"window_span_ticks"`
	WindowAnchorTicks      int64              `json:"window_anchor_ticks"`
	MaxConcurrency         int                `json:"max_concurrency"`
	MaxEventsPerSession    int                `json:"max_events_per_session"`
	MaxSpanTicksPerSession int64              `json:"max_span_ticks_per_session"`
	StrictTXIDQName        bool               `json:"strict_txid_qname"`
	AllowTXIDQNameConflict bool               `json:"-"`
	SourcePriorities       map[SourceKind]int `json:"-"`
}

const (
	DefaultAssociationWindowTicks = int64(5_000_000)  // 500 ms
	DefaultWindowSpanTicks        = int64(10_000_000) // 1 s
	DefaultMaxConcurrency         = 4
	DefaultMaxEventsPerSession    = 100_000
	DefaultMaxSpanTicksPerSession = int64(86_400_000_000_000) // 100 days
)

func DefaultConfig() Config {
	return Config{
		AssociationWindowTicks: DefaultAssociationWindowTicks,
		WindowSpanTicks:        DefaultWindowSpanTicks,
		WindowAnchorTicks:      0,
		MaxConcurrency:         DefaultMaxConcurrency,
		MaxEventsPerSession:    DefaultMaxEventsPerSession,
		MaxSpanTicksPerSession: DefaultMaxSpanTicksPerSession,
		StrictTXIDQName:        true,
		SourcePriorities: map[SourceKind]int{
			SourceBinary: 30,
			SourceJSONL:  20,
			SourceText:   10,
		},
	}
}

func (c Config) WithDefaults() Config {
	d := DefaultConfig()
	c.StrictTXIDQName = !c.AllowTXIDQNameConflict
	if c.AssociationWindowTicks <= 0 {
		c.AssociationWindowTicks = d.AssociationWindowTicks
	}
	if c.WindowSpanTicks <= 0 {
		c.WindowSpanTicks = d.WindowSpanTicks
	}
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = d.MaxConcurrency
	}
	if c.MaxEventsPerSession <= 0 {
		c.MaxEventsPerSession = d.MaxEventsPerSession
	}
	if c.MaxSpanTicksPerSession <= 0 {
		c.MaxSpanTicksPerSession = d.MaxSpanTicksPerSession
	}
	if c.SourcePriorities == nil {
		c.SourcePriorities = d.SourcePriorities
	} else {
		merged := make(map[SourceKind]int, len(d.SourcePriorities))
		for k, v := range d.SourcePriorities {
			merged[k] = v
		}
		for k, v := range c.SourcePriorities {
			merged[k] = v
		}
		c.SourcePriorities = merged
	}
	return c
}

func (c Config) Priority(kind SourceKind) int {
	if p, ok := c.SourcePriorities[kind]; ok {
		return p
	}
	return 0
}

func (c Config) WindowForTicks(ticks int64) Window {
	span := c.WindowSpanTicks
	if span <= 0 {
		span = DefaultWindowSpanTicks
	}
	index := floorDiv(ticks-c.WindowAnchorTicks, span)
	start := c.WindowAnchorTicks + index*span
	return Window{Index: index, StartTicks: start, EndTicks: start + span}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

type SourceSpec struct {
	Path             string     `json:"path"`
	Kind             SourceKind `json:"kind"`
	Priority         *int       `json:"priority,omitempty"`
	ObservationStart int64      `json:"observation_start_ticks,omitempty"`
}

func (s SourceSpec) EffectiveKind() SourceKind {
	if s.Kind != "" && s.Kind != SourceAuto {
		return s.Kind
	}
	ext := strings.ToLower(filepath.Ext(s.Path))
	switch ext {
	case ".dnb", ".bin", ".dnsb":
		return SourceBinary
	case ".jsonl", ".ndjson":
		return SourceJSONL
	case ".txt", ".blocks", ".log":
		return SourceText
	default:
		return SourceAuto
	}
}

type LoadOptions struct {
	MaxEvents         int
	MaxSpanTicks      int64
	MaxBlockBytes     int
	WindowSpanTicks   int64
	WindowAnchorTicks int64
	SourcePriorities  map[SourceKind]int
}

func (o LoadOptions) WithDefaults() LoadOptions {
	if o.MaxEvents <= 0 {
		o.MaxEvents = DefaultMaxEventsPerSession
	}
	if o.MaxSpanTicks <= 0 {
		o.MaxSpanTicks = DefaultMaxSpanTicksPerSession
	}
	if o.MaxBlockBytes <= 0 {
		o.MaxBlockBytes = 4 << 20
	}
	if o.WindowSpanTicks <= 0 {
		o.WindowSpanTicks = DefaultWindowSpanTicks
	}
	return o
}

type SourceSession struct {
	Path                  string      `json:"path"`
	Kind                  SourceKind  `json:"kind"`
	Priority              int         `json:"priority"`
	FormatVersion         int         `json:"format_version"`
	Bytes                 int64       `json:"bytes"`
	ObservationStartTicks int64       `json:"observation_start_ticks,omitempty"`
	Events                []Event     `json:"-"`
	Ledger                ErrorLedger `json:"-"`
}

func sortEventRefs(refs []EventRef) {
	sort.SliceStable(refs, func(i, j int) bool {
		if refs[i].SourcePath != refs[j].SourcePath {
			return refs[i].SourcePath < refs[j].SourcePath
		}
		if refs[i].Offset != refs[j].Offset {
			return refs[i].Offset < refs[j].Offset
		}
		return refs[i].Role < refs[j].Role
	})
}

func roleFromByte(b byte) (RecordRole, error) {
	switch b {
	case 0:
		return RoleQuery, nil
	case 1:
		return RoleResponse, nil
	case 2:
		return RoleCache, nil
	default:
		return "", fmt.Errorf("unknown role byte %d", b)
	}
}

func sectionFromByte(b byte) (RecordSection, error) {
	switch b {
	case 0:
		return SectionAnswer, nil
	case 1:
		return SectionAuthority, nil
	case 2:
		return SectionAdditional, nil
	case 3:
		return SectionCache, nil
	default:
		return "", fmt.Errorf("unknown section byte %d", b)
	}
}
