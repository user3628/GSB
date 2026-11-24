package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"casengine/internal/clock"
	"casengine/internal/codes"
	"casengine/internal/engine"
	"casengine/internal/report"
	"casengine/internal/sample"
	"casengine/internal/store"
)

// runScenario replays one named scenario on a shared runner and returns its
// results. The shared runner keeps a monotonically increasing global ticket
// cursor so identifiers never collide across scenarios.
func runScenario(rt *Runtime, runner *engine.Runner, sc *clock.ScriptedClock, scn sample.Scenario, baseIdx *int, seqStart int64) ([]engine.Result, int64) {
	reqs, nextSeq := buildRequests(rt.App.Params, scn, baseIdx, seqStart)
	runner.ResetCursor(*baseIdx)
	items, nextIdx, err := runner.Plan(reqs, *baseIdx)
	must(err)
	*baseIdx = nextIdx
	return runner.Execute(items), nextSeq
}

// buildRequests materializes scenario requests; baseIdx tracks the global
// ticket allocation cursor so each scenario gets distinct, deterministic IDs.
// seqStart is the global request sequence cursor; the returned cursor
// continues after the last allocated request.
func buildRequests(p *sample.Params, scn sample.Scenario, baseIdx *int, seqStart int64) ([]sample.Request, int64) {
	var reqs []sample.Request
	seq := seqStart
	for _, g := range scn.Groups {
		for _, rel := range g.TicketIdx {
			global := *baseIdx + rel
			tenant, sp, user := p.TicketSpec(global)
			mkReq := func() sample.Request {
				return sample.Request{
					Seq:        seq,
					TicketKey:  fmt.Sprintf("scn-%06d", global),
					TenantID:   tenant.ID,
					SPID:       sp.ID,
					UserID:     user.ID,
					PresentSP:  sp.ID,
					TTLSeconds: 300,
				}
			}
			reqs = append(reqs, mkReq())
			seq++
			for d := 0; d < g.Duplicates; d++ {
				reqs = append(reqs, mkReq())
				seq++
			}
		}
		*baseIdx += maxTicketRef(g) + 1
	}
	return reqs, seq
}

func maxTicketRef(g sample.Group) int {
	m := 0
	for _, v := range g.TicketIdx {
		if v > m {
			m = v
		}
	}
	return m
}

type faultCheck struct {
	Name     string `json:"name"`
	WantCode string `json:"wantCode"`
	GotCode  string `json:"gotCode"`
	Pass     bool   `json:"pass"`
	Detail   string `json:"detail"`
}

// runFaultSuite executes the five input-driven injection cases with assertions.
// seqStart continues the global request sequence so every Seq is unique.
func runFaultSuite(rt *Runtime, runner *engine.Runner, startAt int, seqStart int64) ([]engine.Result, []faultCheck) {
	reqs := rt.App.Params.FaultRequests(seqStart)
	runner.ResetCursor(startAt)
	items, _, err := runner.Plan(reqs, startAt)
	must(err)
	results := runner.Execute(items)

	bySeq := map[int64]engine.Result{}
	for _, r := range results {
		bySeq[r.Seq] = r
	}
	var checks []faultCheck
	check := func(name string, seq int64, want codes.Code) {
		got := bySeq[seq]
		pass := got.Code == want
		detail := ""
		if !pass {
			detail = fmt.Sprintf("want %s got %s", want, got.Code)
		}
		checks = append(checks, faultCheck{
			Name: name, WantCode: string(want), GotCode: string(got.Code), Pass: pass, Detail: detail,
		})
	}
	s := seqStart
	check("signature-tamper", s, codes.Signature)
	check("audience-mismatch", s+1, codes.Audience)
	check("expired-ticket", s+2, codes.Expired)
	var okCount int
	for seq := s + 3; seq <= s+6; seq++ {
		if bySeq[seq].Code == codes.OK {
			okCount++
		}
	}
	racePass := okCount == 1
	raceDetail := ""
	if !racePass {
		raceDetail = fmt.Sprintf("expected 1 success, got %d", okCount)
	}
	checks = append(checks, faultCheck{
		Name: "concurrent-replay", WantCode: string(codes.OK) + "x1",
		GotCode: fmt.Sprintf("%sx%d", string(codes.OK), okCount), Pass: racePass, Detail: raceDetail,
	})
	check("healthy-control", s+7, codes.OK)
	return results, checks
}

// buildReport collects durable state and renders the deterministic report.
func buildReport(rt *Runtime, results []engine.Result, chainReports []engine.ChainReport) *report.Report {
	allTickets, err := rt.Store.ListAllTickets()
	must(err)
	redByTenant := map[string][]store.Redemption{}
	snapByTenant := map[string][]store.Snapshot{}
	for _, t := range rt.App.Params.Tenants {
		rs, err := rt.Store.ListRedemptions(t.ID)
		must(err)
		redByTenant[t.ID] = rs
		sn, err := rt.Store.ListSnapshots(t.ID)
		must(err)
		snapByTenant[t.ID] = sn
	}
	conflicts, err := rt.Store.ListConflicts()
	must(err)
	latAgg := rt.Metric.Aggregate()
	b := report.NewBuilder(rt.App.Params)
	return b.Build(results, latAgg, allTickets, redByTenant, conflicts, snapByTenant, chainReports)
}

func writeReport(rt *Runtime, name string, rep *report.Report) (string, error) {
	raw, err := report.Render(rep)
	if err != nil {
		return "", err
	}
	path := filepath.Join(rt.App.WorkDir, "reports", name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	return path, os.WriteFile(path, raw, 0o644)
}

// chainReportsFor verifies every tenant chain for inclusion in the report.
func chainReportsFor(rt *Runtime) []engine.ChainReport {
	var reps []engine.ChainReport
	ids := make([]string, 0, len(rt.App.Params.Tenants))
	for _, t := range rt.App.Params.Tenants {
		ids = append(ids, t.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		rep, err := rt.Engine.VerifyTenantChain(id)
		must(err)
		reps = append(reps, rep)
	}
	return reps
}
