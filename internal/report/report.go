// Package report renders the deterministic JSON acceptance report.
//
// Determinism rules: every slice is explicitly sorted by stable keys, only
// integer durations and fixed sample markers are emitted (no wall timestamps),
// and encoding is indentation-normalized. Two runs with identical inputs
// therefore produce byte-identical bytes.
package report

import (
	"bytes"
	"encoding/json"
	"sort"

	"casengine/internal/engine"
	"casengine/internal/metrics"
	"casengine/internal/sample"
	"casengine/internal/store"
)

// Report is the top-level exported document.
type Report struct {
	Schema       string                      `json:"schema"`
	Generator    string                      `json:"generator"`
	SampleTag    string                      `json:"sampleTag"`
	Seed         int64                       `json:"seed"`
	Tenants      []TenantBlock               `json:"tenants"`
	Tickets      []TicketSummary             `json:"tickets"`
	Results      []engine.Result             `json:"results"`
	Distribution map[string]int              `json:"resultDistribution"`
	Latency      []metrics.Agg               `json:"latencyStats"`
	Conflicts    []store.Conflict            `json:"conflicts"`
	Snapshots    map[string][]store.Snapshot `json:"snapshotChains"`
	ChainReports []engine.ChainReport        `json:"chainReports"`
}

// TenantBlock captures generated tenant parameters (keys included for the
// local, offline demo only).
type TenantBlock struct {
	TenantID string        `json:"tenantId"`
	Name     string        `json:"name"`
	Secret   string        `json:"secret"`
	SPs      []sample.SP   `json:"sps"`
	Users    []sample.User `json:"users"`
}

// TicketSummary is the stable per-ticket view.
type TicketSummary struct {
	TicketID  string `json:"ticketId"`
	TenantID  string `json:"tenantId"`
	SPID      string `json:"spId"`
	UserID    string `json:"userId"`
	ExpiresAt int64  `json:"expiresAt"`
	Status    string `json:"status"`
	Redeemed  bool   `json:"redeemed"`
}

// Builder accumulates report pieces.
type Builder struct {
	params *sample.Params
}

// NewBuilder returns a report builder for the given parameters.
func NewBuilder(p *sample.Params) *Builder { return &Builder{params: p} }

// Build assembles the full report from live engine storage and in-memory data.
func (b *Builder) Build(
	results []engine.Result,
	latAgg []metrics.Agg,
	tickets []store.Ticket,
	redemptionsByTenant map[string][]store.Redemption,
	conflicts []store.Conflict,
	snapshotsByTenant map[string][]store.Snapshot,
	chainReports []engine.ChainReport,
) *Report {
	rep := &Report{
		Schema:       "casengine.report/v1",
		Generator:    "casengine-deterministic",
		SampleTag:    "fixed-sample-marker",
		Seed:         b.params.Seed,
		Distribution: map[string]int{},
		Snapshots:    map[string][]store.Snapshot{},
	}

	for _, t := range b.params.Tenants {
		rep.Tenants = append(rep.Tenants, TenantBlock{
			TenantID: t.ID, Name: t.Name, Secret: t.Secret,
			SPs:   append([]sample.SP(nil), t.SPs...),
			Users: append([]sample.User(nil), t.Users...),
		})
	}

	redSet := map[string]bool{}
	for _, rs := range redemptionsByTenant {
		for _, r := range rs {
			redSet[r.TicketID] = true
		}
	}
	for _, t := range tickets {
		rep.Tickets = append(rep.Tickets, TicketSummary{
			TicketID: t.ID, TenantID: t.TenantID, SPID: t.SPID, UserID: t.UserID,
			ExpiresAt: t.ExpiresAt, Status: t.Status, Redeemed: redSet[t.ID],
		})
	}
	sort.Slice(rep.Tickets, func(i, j int) bool { return rep.Tickets[i].TicketID < rep.Tickets[j].TicketID })

	rep.Results = append(rep.Results, results...)
	sort.Slice(rep.Results, func(i, j int) bool {
		if rep.Results[i].TicketID != rep.Results[j].TicketID {
			return rep.Results[i].TicketID < rep.Results[j].TicketID
		}
		return rep.Results[i].Seq < rep.Results[j].Seq
	})
	for _, r := range rep.Results {
		rep.Distribution[string(r.Code)]++
	}

	rep.Latency = append(rep.Latency, latAgg...)
	rep.Conflicts = append(rep.Conflicts, conflicts...)
	sort.Slice(rep.Conflicts, func(i, j int) bool {
		if rep.Conflicts[i].TicketID != rep.Conflicts[j].TicketID {
			return rep.Conflicts[i].TicketID < rep.Conflicts[j].TicketID
		}
		if rep.Conflicts[i].Seq != rep.Conflicts[j].Seq {
			return rep.Conflicts[i].Seq < rep.Conflicts[j].Seq
		}
		return rep.Conflicts[i].Code < rep.Conflicts[j].Code
	})

	tenantIDs := make([]string, 0, len(snapshotsByTenant))
	for id := range snapshotsByTenant {
		tenantIDs = append(tenantIDs, id)
	}
	sort.Strings(tenantIDs)
	for _, id := range tenantIDs {
		chains := append([]store.Snapshot(nil), snapshotsByTenant[id]...)
		sort.Slice(chains, func(i, j int) bool { return chains[i].Version < chains[j].Version })
		rep.Snapshots[id] = chains
	}

	rep.ChainReports = append(rep.ChainReports, chainReports...)
	sort.Slice(rep.ChainReports, func(i, j int) bool {
		return rep.ChainReports[i].TenantID < rep.ChainReports[j].TenantID
	})
	return rep
}

// Render serializes with stable indentation and a trailing newline.
func Render(rep *Report) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(rep); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
