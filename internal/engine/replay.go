package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"casengine/internal/clock"
	"casengine/internal/sample"
	"casengine/internal/token"
)

// ticketHolder is the single minted ticket shared by every request that maps
// to the same identity key.
type ticketHolder struct {
	Index  int
	Issued *IssuedTicket
}

// PlanItem binds one request descriptor to the (possibly shared) ticket it
// targets. Each request keeps its own Seq / tamper / audience fields.
type PlanItem struct {
	Req    sample.Request
	Holder *ticketHolder
}

// Runner drives deterministic replay against a scripted clock.
type Runner struct {
	engine *Engine
	clk    *clock.ScriptedClock
	params *sample.Params

	byKey map[string]*ticketHolder
	mu    sync.Mutex
	next  int
}

// NewRunner wires a replay runner to an engine and generated parameters.
func NewRunner(e *Engine, clk *clock.ScriptedClock, p *sample.Params) *Runner {
	return &Runner{engine: e, clk: clk, params: p, byKey: map[string]*ticketHolder{}}
}

// ResetCursor sets the global ticket allocation cursor used by Plan.
func (r *Runner) ResetCursor(startAt int) { r.next = startAt }

func keyOf(rq sample.Request) string {
	if rq.TicketKey != "" {
		return rq.TenantID + "|" + rq.TicketKey
	}
	return rq.TenantID + "|" + rq.SPID + "|" + rq.UserID
}

func (r *Runner) ticketID(idx int, tenantID string) string {
	return fmt.Sprintf("ST-%06d-%s", idx+1, tenantID)
}

// Plan allocates a global ticket index per distinct identity key and mints the
// corresponding ticket once. Repeated requests for the same key share the
// ticket holder (which is what makes replay/race semantics observable) while
// retaining their individual request descriptor.
func (r *Runner) Plan(reqs []sample.Request, startAt int) ([]*PlanItem, int, error) {
	items := make([]*PlanItem, len(reqs))
	for i, rq := range reqs {
		key := keyOf(rq)
		r.mu.Lock()
		holder, ok := r.byKey[key]
		if !ok {
			idx := r.next
			r.next++
			ttl := rq.TTLSeconds
			if ttl <= 0 {
				ttl = 300
			}
			issued, err := r.engine.Issue(IssueRequest{
				ID:         r.ticketID(idx, rq.TenantID),
				TenantID:   rq.TenantID,
				SPID:       rq.SPID,
				UserID:     rq.UserID,
				TTLSeconds: ttl,
				Seq:        int64(idx) + 1,
			})
			if err != nil {
				r.mu.Unlock()
				return nil, 0, err
			}
			holder = &ticketHolder{Index: idx, Issued: issued}
			r.byKey[key] = holder
		}
		items[i] = &PlanItem{Req: rq, Holder: holder}
		r.mu.Unlock()
	}
	return items, r.next, nil
}

// Execute runs the planned requests and returns sequence-sorted results.
//
// Requests are processed in a fixed global order (the minimum sequence of each
// ticket, then sequence within a ticket). The engine still exercises the real
// concurrency machinery on every request (resource semaphore, per-ticket mutex
// and immediate SQLite transaction); deterministic scheduling of the replay
// additionally makes the one-time winner and every snapshot version
// independent of goroutine timing, which guarantees byte-identical output.
func (r *Runner) Execute(items []*PlanItem) []Result {
	r.registerGates(items)
	results := make([]Result, len(items))
	byTicket := map[string][]int{}
	for i, item := range items {
		id := item.Holder.Issued.Record.ID
		byTicket[id] = append(byTicket[id], i)
	}
	var ticketOrder []string
	minSeq := map[string]int64{}
	for id, idxs := range byTicket {
		ticketOrder = append(ticketOrder, id)
		sort.Slice(byTicket[id], func(a, b int) bool {
			return items[byTicket[id][a]].Req.Seq < items[byTicket[id][b]].Req.Seq
		})
		minSeq[id] = items[idxs[0]].Req.Seq
	}
	sort.Slice(ticketOrder, func(a, b int) bool {
		if minSeq[ticketOrder[a]] != minSeq[ticketOrder[b]] {
			return minSeq[ticketOrder[a]] < minSeq[ticketOrder[b]]
		}
		return ticketOrder[a] < ticketOrder[b]
	})

	for _, id := range ticketOrder {
		for _, i := range byTicket[id] {
			results[i] = r.runOne(items[i])
		}
	}
	sort.Slice(results, func(a, b int) bool { return results[a].Seq < results[b].Seq })
	return results
}

// registerGates groups requests by their shared ticket and registers a
// deterministic winner for every ticket targeted by more than one request.
func (r *Runner) registerGates(items []*PlanItem) {
	seqsByTicket := map[string][]int64{}
	var ticketIDs []string
	for _, item := range items {
		id := item.Holder.Issued.Record.ID
		if _, ok := seqsByTicket[id]; !ok {
			ticketIDs = append(ticketIDs, id)
		}
		seqsByTicket[id] = append(seqsByTicket[id], item.Req.Seq)
	}
	for _, id := range ticketIDs {
		seqs := seqsByTicket[id]
		if len(seqs) > 1 {
			winner := seqs[0]
			for _, v := range seqs[1:] {
				if v < winner {
					winner = v
				}
			}
			r.engine.RegisterWinner(id, winner)
		}
	}
}

func (r *Runner) runOne(item *PlanItem) Result {
	rq := item.Req
	raw := item.Holder.Issued.Ticket
	if rq.Tampered {
		if t, err := token.Tamper(raw); err == nil {
			raw = t
		}
	}
	if rq.ExpireBefore {
		// Deterministically move the virtual clock beyond the ticket TTL.
		r.clk.Advance(time.Duration(rq.TTLSeconds+10) * time.Second)
	}
	return r.engine.Validate(context.Background(), ValidateRequest{
		Seq: rq.Seq, Ticket: raw, PresentSP: rq.PresentSP,
	})
}
