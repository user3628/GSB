package sample

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type tenantJSON struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}

type specJSON struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	WidthMS      int64  `json:"width_ms"`
	SlideMS      int64  `json:"slide_ms,omitempty"`
	SessionGapMS int64  `json:"session_gap_ms,omitempty"`
	Boundary     string `json:"boundary"`
}

type eventJSON struct {
	EventID     string `json:"event_id"`
	TenantID    string `json:"tenant_id"`
	GroupKey    string `json:"group_key"`
	TimestampMS int64  `json:"timestamp_ms"`
	Seq         int64  `json:"seq"`
	Measure     any    `json:"measure,omitempty"`
}

type snapshotJSON struct {
	SnapshotID  string       `json:"snapshot_id"`
	Tenants     []tenantJSON `json:"tenants"`
	WindowSpecs []specJSON   `json:"window_specs"`
	Events      []eventJSON  `json:"events"`
}

// Generate creates the reproducible multi-tenant sample used by the demo and
// tests. The event array is intentionally shuffled so ingestion order cannot
// influence planning or aggregation.
func Generate(seed int64) ([]byte, error) {
	random := rand.New(rand.NewSource(seed))
	base := time.Date(2023, 12, 31, 23, 0, 0, 0, time.UTC).UnixMilli()
	events := make([]eventJSON, 0, 72)
	eventNumber := 1
	appendEvent := func(tenantID, groupKey string, timestamp int64, sequence int64, measure any) {
		events = append(events, eventJSON{
			EventID: fmt.Sprintf("evt_%03d", eventNumber), TenantID: tenantID,
			GroupKey: groupKey, TimestampMS: timestamp, Seq: sequence, Measure: measure,
		})
		eventNumber++
	}

	groups := []struct {
		tenantID string
		groupKey string
		count    int
		windowMS int64
		offsetMS int64
	}{
		{"tenant-vip", "vip-group", 10, 8 * time.Minute.Milliseconds(), 55 * time.Minute.Milliseconds()},
		{"tenant-a", "alpha", 18, 7 * time.Minute.Milliseconds(), 5 * time.Minute.Milliseconds()},
		{"tenant-a", "beta", 14, 11 * time.Minute.Milliseconds(), 17 * time.Minute.Milliseconds()},
		{"tenant-b", "gamma", 12, 9 * time.Minute.Milliseconds(), 31 * time.Minute.Milliseconds()},
		{"tenant-b", "delta", 10, 13 * time.Minute.Milliseconds(), 43 * time.Minute.Milliseconds()},
	}
	sequence := int64(1)
	for _, group := range groups {
		for index := 0; index < group.count; index++ {
			jitter := int64(random.Intn(90_000))
			timestamp := base + group.offsetMS + int64(index)*group.windowMS + jitter
			var measure any = fmt.Sprintf("%d.%03d", random.Intn(900)-300, random.Intn(1000))
			if random.Intn(13) == 0 {
				measure = nil
			}
			appendEvent(group.tenantID, group.groupKey, timestamp, sequence, measure)
			sequence++
		}
	}

	// A duplicate sequence inside the same tenant/group/hour exercises the
	// deterministic winner rule. The later event wins because timestamp is
	// greater.
	appendEvent("tenant-a", "alpha", base+30*time.Minute.Milliseconds()+123, 3, "111.111111")
	// Exact epoch boundary: 2024-01-01T00:00:00Z. It belongs to the new day
	// and the new hour under LEFT_CLOSED_RIGHT_OPEN.
	appendEvent("tenant-b", "gamma", 1704067200000, 9001, "500.000000")
	// One millisecond before the boundary belongs to the previous day/hour.
	appendEvent("tenant-b", "gamma", 1704067199999, 9002, "-700.250000")
	// Session gap boundary: tenant-a/alpha has an isolated follow-up event
	// after more than 15 minutes, forcing a new session.
	appendEvent("tenant-a", "alpha", base+70*time.Minute.Milliseconds(), 9100, "42.000000")
	appendEvent("tenant-vip", "vip-group", base+31*time.Minute.Milliseconds(), 9200, "0.000001")
	// A null measure is valid and contributes to count but not sum/min/max.
	appendEvent("tenant-b", "delta", base+58*time.Minute.Milliseconds(), 9300, nil)

	random.Shuffle(len(events), func(i, j int) { events[i], events[j] = events[j], events[i] })
	snapshot := snapshotJSON{
		SnapshotID: fmt.Sprintf("det-sample-seed-%d", seed),
		Tenants: []tenantJSON{
			{ID: "tenant-vip", Name: "VIP Tenant", Priority: 1},
			{ID: "tenant-a", Name: "Analytic Tenant A", Priority: 10},
			{ID: "tenant-b", Name: "Analytic Tenant B", Priority: 20},
		},
		WindowSpecs: []specJSON{
			{ID: "tumble-1h", Type: "tumbling", WidthMS: 3_600_000, Boundary: "LEFT_CLOSED_RIGHT_OPEN"},
			{ID: "tumble-1d-cross-year", Type: "tumbling", WidthMS: 86_400_000, Boundary: "LEFT_CLOSED_RIGHT_OPEN"},
			{ID: "slide-30m-width-1h", Type: "sliding", WidthMS: 3_600_000, SlideMS: 1_800_000, Boundary: "LEFT_CLOSED_RIGHT_OPEN"},
			{ID: "session-15m", Type: "session", WidthMS: 7_200_000, SessionGapMS: 900_000, Boundary: "LEFT_CLOSED_RIGHT_OPEN"},
		},
		Events: events,
	}
	return json.MarshalIndent(snapshot, "", "  ")
}
