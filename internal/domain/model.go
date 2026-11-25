package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

type Boundary string

const (
	BoundaryLeftClosedRightOpen Boundary = "LEFT_CLOSED_RIGHT_OPEN"
)

type WindowType string

const (
	WindowTumbling WindowType = "tumbling"
	WindowSliding  WindowType = "sliding"
	WindowSession  WindowType = "session"
)

type Tenant struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Priority int    `json:"priority"`
}

type WindowSpec struct {
	ID           string     `json:"id"`
	Type         WindowType `json:"type"`
	WidthMS      int64      `json:"width_ms"`
	SlideMS      int64      `json:"slide_ms,omitempty"`
	SessionGapMS int64      `json:"session_gap_ms,omitempty"`
	Boundary     Boundary   `json:"boundary,omitempty"`
	MaxSessionMS int64      `json:"max_session_ms,omitempty"`
}

type Event struct {
	EventID     string  `json:"event_id"`
	TenantID    string  `json:"tenant_id"`
	GroupKey    string  `json:"group_key"`
	TimestampMS int64   `json:"timestamp_ms"`
	Seq         int64   `json:"seq"`
	Measure     Measure `json:"-"`
}

type Snapshot struct {
	SnapshotID  string       `json:"snapshot_id"`
	Tenants     []Tenant     `json:"tenants"`
	WindowSpecs []WindowSpec `json:"window_specs"`
	Events      []Event      `json:"events"`
	RawJSON     []byte       `json:"-"`
	ContentHash string       `json:"-"`
}

type snapshotInput struct {
	SnapshotID  string       `json:"snapshot_id"`
	Tenants     []Tenant     `json:"tenants"`
	WindowSpecs []WindowSpec `json:"window_specs"`
	Events      []eventInput `json:"events"`
}

type eventInput struct {
	EventID     string          `json:"event_id,omitempty"`
	TenantID    string          `json:"tenant_id"`
	GroupKey    string          `json:"group_key"`
	TimestampMS int64           `json:"timestamp_ms"`
	Seq         int64           `json:"seq"`
	Measure     json.RawMessage `json:"measure"`
}

type Window struct {
	SnapshotID string     `json:"snapshot_id"`
	WindowID   string     `json:"window_id"`
	TenantID   string     `json:"tenant_id"`
	SpecID     string     `json:"spec_id"`
	GroupKey   string     `json:"group_key"`
	StartMS    int64      `json:"window_start_ms"`
	EndMS      int64      `json:"window_end_ms"`
	Boundary   Boundary   `json:"boundary"`
	WindowType WindowType `json:"window_type"`
	EventCount int        `json:"event_count"`
	ZeroLength bool       `json:"zero_duration"`
}

type Aggregate struct {
	SnapshotID string     `json:"snapshot_id"`
	WindowID   string     `json:"window_id"`
	TenantID   string     `json:"tenant_id"`
	SpecID     string     `json:"spec_id"`
	GroupKey   string     `json:"group_key"`
	WindowType WindowType `json:"window_type"`
	StartMS    int64      `json:"window_start_ms"`
	EndMS      int64      `json:"window_end_ms"`
	Boundary   Boundary   `json:"boundary"`
	Count      int64      `json:"count"`
	SumScaled  int64      `json:"-"`
	MinScaled  *int64     `json:"-"`
	MaxScaled  *int64     `json:"-"`
	SampleSize int64      `json:"sample_size"`
	Rounding   string     `json:"rounding_mode"`
	Version    int64      `json:"version"`
}

type AggregateView struct {
	SnapshotID  string     `json:"snapshot_id"`
	WindowID    string     `json:"window_id"`
	TenantID    string     `json:"tenant_id"`
	SpecID      string     `json:"spec_id"`
	GroupKey    string     `json:"group_key"`
	WindowType  WindowType `json:"window_type"`
	StartMS     int64      `json:"window_start_ms"`
	EndMS       int64      `json:"window_end_ms"`
	Boundary    Boundary   `json:"boundary"`
	Inclusivity string     `json:"inclusivity"`
	Count       int64      `json:"count"`
	SumScaled   string     `json:"sum_scaled"`
	SumFixed    string     `json:"sum_fixed"`
	MinScaled   *string    `json:"min_scaled"`
	MinFixed    *string    `json:"min_fixed"`
	MaxScaled   *string    `json:"max_scaled"`
	MaxFixed    *string    `json:"max_fixed"`
	SampleSize  int64      `json:"sample_size"`
	Scale       int64      `json:"scale"`
	Rounding    string     `json:"rounding_mode"`
	Version     int64      `json:"version"`
	WindowState string     `json:"window_state"`
	Committed   bool       `json:"committed"`
}

func (aggregate Aggregate) View(windowState string) AggregateView {
	view := AggregateView{
		SnapshotID:  aggregate.SnapshotID,
		WindowID:    aggregate.WindowID,
		TenantID:    aggregate.TenantID,
		SpecID:      aggregate.SpecID,
		GroupKey:    aggregate.GroupKey,
		WindowType:  aggregate.WindowType,
		StartMS:     aggregate.StartMS,
		EndMS:       aggregate.EndMS,
		Boundary:    aggregate.Boundary,
		Inclusivity: "start inclusive; end exclusive",
		Count:       aggregate.Count,
		SumScaled:   strconv.FormatInt(aggregate.SumScaled, 10),
		SumFixed:    FormatFixed(aggregate.SumScaled),
		SampleSize:  aggregate.SampleSize,
		Scale:       Scale,
		Rounding:    aggregate.Rounding,
		Version:     aggregate.Version,
		WindowState: windowState,
		Committed:   windowState == "COMMITTED" || aggregate.Version > 0,
	}
	if aggregate.MinScaled != nil {
		value := strconv.FormatInt(*aggregate.MinScaled, 10)
		fixed := FormatFixed(*aggregate.MinScaled)
		view.MinScaled = &value
		view.MinFixed = &fixed
	}
	if aggregate.MaxScaled != nil {
		value := strconv.FormatInt(*aggregate.MaxScaled, 10)
		fixed := FormatFixed(*aggregate.MaxScaled)
		view.MaxScaled = &value
		view.MaxFixed = &fixed
	}
	return view
}

func ParseSnapshot(data []byte) (*Snapshot, *Error) {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var input snapshotInput
	if err := decoder.Decode(&input); err != nil {
		return nil, NewError(CodeSnapshotInvalid, "snapshot JSON cannot be decoded", map[string]any{
			"reason": err.Error(),
		})
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, NewError(CodeSnapshotInvalid, "snapshot JSON contains trailing data", nil)
	}

	input.SnapshotID = strings.TrimSpace(input.SnapshotID)
	if input.SnapshotID == "" {
		return nil, NewError(CodeSnapshotInvalid, "snapshot_id is required", map[string]any{"field": "snapshot_id"})
	}
	if len(input.Tenants) == 0 {
		return nil, NewError(CodeSnapshotInvalid, "at least one tenant is required", map[string]any{"field": "tenants"})
	}
	if len(input.WindowSpecs) == 0 {
		return nil, NewError(CodeSnapshotInvalid, "at least one window spec is required", map[string]any{"field": "window_specs"})
	}

	tenantIDs := make(map[string]struct{}, len(input.Tenants))
	tenants := make([]Tenant, 0, len(input.Tenants))
	for index, tenant := range input.Tenants {
		tenant.ID = strings.TrimSpace(tenant.ID)
		if tenant.ID == "" {
			return nil, NewError(CodeSnapshotInvalid, "tenant id is required", map[string]any{"field": "tenants.id", "index": index})
		}
		if _, exists := tenantIDs[tenant.ID]; exists {
			return nil, NewError(CodeSnapshotInvalid, "tenant id must be unique", map[string]any{
				"field": "tenants.id", "index": index, "tenant_id": tenant.ID,
			})
		}
		tenantIDs[tenant.ID] = struct{}{}
		if tenant.Name == "" {
			tenant.Name = tenant.ID
		}
		if tenant.Priority < 0 {
			return nil, NewError(CodeSnapshotInvalid, "tenant priority must be non-negative", map[string]any{
				"field": "tenants.priority", "tenant_id": tenant.ID,
			})
		}
		tenants = append(tenants, tenant)
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i].ID < tenants[j].ID })

	specIDs := make(map[string]struct{}, len(input.WindowSpecs))
	specs := make([]WindowSpec, 0, len(input.WindowSpecs))
	for index, spec := range input.WindowSpecs {
		spec.ID = strings.TrimSpace(spec.ID)
		if spec.ID == "" {
			return nil, NewError(CodeSnapshotInvalid, "window spec id is required", map[string]any{"field": "window_specs.id", "index": index})
		}
		if _, exists := specIDs[spec.ID]; exists {
			return nil, NewError(CodeSnapshotInvalid, "window spec id must be unique", map[string]any{
				"field": "window_specs.id", "index": index, "spec_id": spec.ID,
			})
		}
		specIDs[spec.ID] = struct{}{}
		spec.Type = WindowType(strings.ToLower(string(spec.Type)))
		if spec.Boundary == "" {
			spec.Boundary = BoundaryLeftClosedRightOpen
		}
		if spec.Boundary != BoundaryLeftClosedRightOpen {
			return nil, NewError(CodeSnapshotInvalid, "only LEFT_CLOSED_RIGHT_OPEN is supported", map[string]any{
				"field": "window_specs.boundary", "spec_id": spec.ID, "boundary": spec.Boundary,
			})
		}
		if spec.WidthMS <= 0 {
			return nil, NewError(CodeSnapshotInvalid, "window width_ms must be a positive integer", map[string]any{
				"field": "window_specs.width_ms", "spec_id": spec.ID,
			})
		}
		switch spec.Type {
		case WindowTumbling:
		case WindowSliding:
			if spec.SlideMS <= 0 {
				return nil, NewError(CodeSnapshotInvalid, "sliding window slide_ms must be a positive integer", map[string]any{
					"field": "window_specs.slide_ms", "spec_id": spec.ID,
				})
			}
		case WindowSession:
			if spec.SessionGapMS <= 0 {
				return nil, NewError(CodeSnapshotInvalid, "session window session_gap_ms must be a positive integer", map[string]any{
					"field": "window_specs.session_gap_ms", "spec_id": spec.ID,
				})
			}
		default:
			return nil, NewError(CodeSnapshotInvalid, "unsupported window type", map[string]any{
				"field": "window_specs.type", "spec_id": spec.ID, "type": spec.Type,
				"allowed": []string{string(WindowTumbling), string(WindowSliding), string(WindowSession)},
			})
		}
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].ID < specs[j].ID })

	eventIDs := make(map[string]struct{}, len(input.Events))
	events := make([]Event, 0, len(input.Events))
	for index, rawEvent := range input.Events {
		eventID := strings.TrimSpace(rawEvent.EventID)
		if eventID == "" {
			eventID = fmt.Sprintf("e_%06d", index+1)
		}
		if _, exists := eventIDs[eventID]; exists {
			return nil, NewError(CodeSnapshotInvalid, "event id must be unique", map[string]any{
				"field": "events.event_id", "index": index, "event_id": eventID,
			})
		}
		eventIDs[eventID] = struct{}{}
		tenantID := strings.TrimSpace(rawEvent.TenantID)
		if _, exists := tenantIDs[tenantID]; !exists {
			return nil, NewError(CodeSnapshotInvalid, "event references unknown tenant", map[string]any{
				"field": "events.tenant_id", "index": index, "tenant_id": tenantID,
			})
		}
		groupKey := strings.TrimSpace(rawEvent.GroupKey)
		if groupKey == "" {
			return nil, NewError(CodeSnapshotInvalid, "event group_key is required", map[string]any{
				"field": "events.group_key", "index": index, "event_id": eventID,
			})
		}
		if rawEvent.TimestampMS < 0 {
			return nil, NewError(CodeSnapshotInvalid, "event timestamp_ms must be non-negative", map[string]any{
				"field": "events.timestamp_ms", "index": index, "event_id": eventID,
			})
		}
		if rawEvent.Seq <= 0 {
			return nil, NewError(CodeSnapshotInvalid, "event seq must be positive", map[string]any{
				"field": "events.seq", "index": index, "event_id": eventID,
			})
		}
		measure, measureErr := ParseFixedPoint(rawEvent.Measure)
		if measureErr != nil {
			measureErr.Context["index"] = index
			measureErr.Context["event_id"] = eventID
			return nil, measureErr
		}
		events = append(events, Event{
			EventID: eventID, TenantID: tenantID, GroupKey: groupKey,
			TimestampMS: rawEvent.TimestampMS, Seq: rawEvent.Seq, Measure: measure,
		})
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].TenantID != events[j].TenantID {
			return events[i].TenantID < events[j].TenantID
		}
		if events[i].GroupKey != events[j].GroupKey {
			return events[i].GroupKey < events[j].GroupKey
		}
		if events[i].TimestampMS != events[j].TimestampMS {
			return events[i].TimestampMS < events[j].TimestampMS
		}
		if events[i].Seq != events[j].Seq {
			return events[i].Seq < events[j].Seq
		}
		return events[i].EventID < events[j].EventID
	})

	compact := make([]byte, 0, len(data))
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, data); err == nil {
		compact = append(compact, buffer.Bytes()...)
	} else {
		compact = append(compact, data...)
	}
	digest := sha256.Sum256(compact)
	return &Snapshot{
		SnapshotID: input.SnapshotID, Tenants: tenants, WindowSpecs: specs, Events: events,
		RawJSON: compact, ContentHash: hex.EncodeToString(digest[:]),
	}, nil
}

func WindowID(snapshotID, tenantID, specID, groupKey string, startMS, endMS int64, boundary Boundary) string {
	raw := strings.Join([]string{
		snapshotID, tenantID, specID, groupKey,
		strconv.FormatInt(startMS, 10), strconv.FormatInt(endMS, 10), string(boundary),
	}, "\x1f")
	digest := sha256.Sum256([]byte(raw))
	return "w_" + hex.EncodeToString(digest[:12])
}
