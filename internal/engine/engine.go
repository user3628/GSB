// Package engine implements the CAS ticket validation engine: issue, verify,
// one-time redemption under concurrency, versioned snapshots, background
// expiry cleanup and deterministic replay.
package engine

import (
	"context"
	"sync"
	"time"

	"casengine/internal/clock"
	"casengine/internal/codes"
	"casengine/internal/metrics"
	"casengine/internal/store"
	"casengine/internal/token"
)

// Config tunes runtime resource controls.
type Config struct {
	MaxConcurrency  int           // total shared budget (validation + cleanup)
	AcquireTimeout  time.Duration // wait threshold before E_TICKET_CONTENTION
	CleanupInterval time.Duration // background cleanup period; 0 disables
	CleanupQuota    int           // max semaphore slots one cleanup pass may take
	LatencyBuffer   int           // metrics channel buffer
}

// DefaultConfig returns sane production defaults.
func DefaultConfig() Config {
	return Config{
		MaxConcurrency:  16,
		AcquireTimeout:  200 * time.Millisecond,
		CleanupInterval: 5 * time.Second,
		CleanupQuota:    2,
		LatencyBuffer:   4096,
	}
}

// Assertion is the identity claim returned on a successful redemption.
type Assertion struct {
	TicketID string `json:"ticketId"`
	TenantID string `json:"tenantId"`
	SPID     string `json:"spId"`
	UserID   string `json:"userId"`
	Version  int64  `json:"snapshotVersion"`
}

// Result is the outcome of validating a single request.
type Result struct {
	Seq       int64      `json:"requestSeq"`
	TicketID  string     `json:"ticketId"`
	TenantID  string     `json:"tenantId"`
	PresentSP string     `json:"presentSp"`
	Code      codes.Code `json:"code"`
	LatencyNS int64      `json:"latencyNs"`
	SampleTag string     `json:"sampleTag"` // fixed marker, never a wall timestamp
	Assertion *Assertion `json:"assertion,omitempty"`
}

// Engine wires storage, clock, metrics and resource controls together.
type Engine struct {
	store *store.Store
	clk   clock.Clock
	mc    *metrics.Collector
	cfg   Config

	sem       chan struct{}
	ticketMu  sync.Map // ticketID -> *sync.Mutex
	winnerReg *winnerIndex

	tenantFault sync.Map // tenantID -> *codes.Error when chain is broken

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New constructs an engine and starts the background cleanup loop when enabled.
func New(st *store.Store, clk clock.Clock, mc *metrics.Collector, cfg Config) *Engine {
	if cfg.MaxConcurrency <= 0 {
		cfg = DefaultConfig()
	}
	if cfg.CleanupQuota <= 0 || cfg.CleanupQuota > cfg.MaxConcurrency {
		cfg.CleanupQuota = cfg.MaxConcurrency
	}
	e := &Engine{
		store:     st,
		clk:       clk,
		mc:        mc,
		cfg:       cfg,
		sem:       make(chan struct{}, cfg.MaxConcurrency),
		stopCh:    make(chan struct{}),
		winnerReg: newWinnerIndex(),
	}
	if cfg.CleanupInterval > 0 {
		e.wg.Add(1)
		go e.cleanupLoop()
	}
	return e
}

// Close signals background workers to stop.
func (e *Engine) Close() {
	close(e.stopCh)
	e.wg.Wait()
}

// acquire takes a resource slot within the configured wait threshold.
func (e *Engine) acquire(ctx context.Context) error {
	select {
	case e.sem <- struct{}{}:
		return nil
	case <-time.After(e.cfg.AcquireTimeout):
		return codes.New(codes.Contention, "concurrency wait threshold exceeded")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) release() { <-e.sem }

// lockTicket returns the dedicated mutex of a ticket.
func (e *Engine) lockTicket(id string) *sync.Mutex {
	m, _ := e.ticketMu.LoadOrStore(id, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// MarkTenantFault takes a tenant out of service until a rollback is performed.
func (e *Engine) MarkTenantFault(tenantID string, err *codes.Error) {
	e.tenantFault.Store(tenantID, err)
}

// ClearTenantFault restores service to a tenant after recovery.
func (e *Engine) ClearTenantFault(tenantID string) { e.tenantFault.Delete(tenantID) }

// TenantFault reports the active fault for a tenant, if any.
func (e *Engine) TenantFault(tenantID string) *codes.Error {
	if v, ok := e.tenantFault.Load(tenantID); ok {
		return v.(*codes.Error)
	}
	return nil
}

// ValidateRequest is the input to Validate.
type ValidateRequest struct {
	Seq       int64
	Ticket    string
	PresentSP string
}

// Validate runs the full validation pipeline and returns a structured result.
// Latency is sampled with a monotonic difference around the whole pipeline.
func (e *Engine) Validate(ctx context.Context, req ValidateRequest) Result {
	mark := e.clk.Mark(req.Seq)
	res := e.validate(ctx, req)
	res.LatencyNS = int64(e.clk.Elapsed(mark))
	res.Seq = req.Seq
	res.SampleTag = "sample#" + itoa(req.Seq)
	e.mc.Observe(metrics.Sample{
		Seq:       req.Seq,
		TenantID:  res.TenantID,
		Code:      string(res.Code),
		LatencyNS: res.LatencyNS,
	})
	return res
}

func (e *Engine) validate(ctx context.Context, req ValidateRequest) Result {
	res := Result{TicketID: "?", PresentSP: req.PresentSP}

	// Resolve payload (without trusting the signature yet) for tenant routing.
	payload, perr := token.Parse(req.Ticket)
	if perr != nil {
		res.Code = codes.Signature
		return res
	}
	res.TicketID = payload.ID
	res.TenantID = payload.TenantID

	if err := e.acquire(ctx); err != nil {
		res.Code = codes.From(err)
		_ = e.store.InsertConflict(store.Conflict{
			TenantID: payload.TenantID, TicketID: payload.ID,
			Seq: req.Seq, Code: string(res.Code), AtNS: e.clk.EventNS(req.Seq),
		})
		return res
	}
	defer e.release()

	if fault := e.TenantFault(payload.TenantID); fault != nil {
		res.Code = fault.Code
		return res
	}

	// 1. Signature integrity.
	secret, err := e.store.TenantSecret(payload.TenantID)
	if err != nil {
		res.Code = codes.Signature
		e.recordConflict(payload, req.Seq, res.Code)
		return res
	}
	verified, verr := token.Verify([]byte(secret), req.Ticket)
	if verr != nil {
		res.Code = codes.Signature
		e.recordConflict(payload, req.Seq, res.Code)
		return res
	}

	// 2. Audience match.
	if verified.SPID != req.PresentSP {
		res.Code = codes.Audience
		e.recordConflict(payload, req.Seq, res.Code)
		return res
	}

	// 3. Expiry (against the engine clock; no wall-clock dependency in replay).
	if verified.ExpiresAt <= e.clk.Now().Unix() {
		res.Code = codes.Expired
		e.recordConflict(payload, req.Seq, res.Code)
		return res
	}

	// Ticket-granularity mutual exclusion for the state transition.
	releaseTicket := e.acquireTicketLock(verified.ID)
	defer releaseTicket()
	isWinner := e.isDesignatedWinner(verified.ID, req.Seq)
	if !isWinner {
		// Deterministic replay: a registered non-winner must not redeem even
		// if it reaches the mutex first. The one-time ticket is consumed by
		// the designated winner, so this attempt is recorded as consumed.
		res.Code = codes.Consumed
		e.recordConflict(payload, req.Seq, res.Code)
		return res
	}

	t, err := e.store.GetTicket(verified.ID)
	if err != nil {
		res.Code = codes.Internal
		return res
	}
	if t == nil {
		res.Code = codes.UnknownTicket
		e.recordConflict(payload, req.Seq, res.Code)
		return res
	}
	if t.Status != store.StatusValid {
		res.Code = codes.Consumed
		e.recordConflict(payload, req.Seq, res.Code)
		return res
	}

	outcome, err := e.store.Redeem(t, req.Seq, e.clk.EventNS(req.Seq))
	if err != nil {
		res.Code = codes.Internal
		return res
	}
	if outcome.Version < 0 {
		// Lost the in-database race: transaction wrote nothing.
		res.Code = codes.Consumed
		e.recordConflict(payload, req.Seq, res.Code)
		return res
	}
	res.Code = codes.OK
	res.Assertion = &Assertion{
		TicketID: t.ID, TenantID: t.TenantID, SPID: t.SPID,
		UserID: t.UserID, Version: outcome.Version,
	}
	return res
}

func (e *Engine) recordConflict(p *token.Payload, seq int64, code codes.Code) {
	_ = e.store.InsertConflict(store.Conflict{
		TenantID: p.TenantID, TicketID: p.ID,
		Seq: seq, Code: string(code), AtNS: e.clk.EventNS(seq),
	})
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
