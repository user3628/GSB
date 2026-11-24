// Package scoring computes integer fixed-point privilege-escalation scores,
// records every truncation remainder in a draft error ledger, and renders a
// bounded score interval into a deterministic per-process verdict.
package scoring

import (
	"fmt"
	"sort"

	"privescworkbench/internal/domain"
	"privescworkbench/internal/fixedpoint"
)

// ThresholdMicro is the default suspicious threshold (score 60.000000).
const ThresholdMicro int64 = 55 * fixedpoint.Scale

// Event bit weights at micro scale (bit i contributes i+1 whole micro units).
const bitWeightStep int64 = fixedpoint.Scale

// Time buckets: 5_000_000 micro units (5 s); each full bucket takes 1/4
// micro of decay factor. All numbers stay integral.
const timeBucketMicros int64 = 5_000  // 5 ms per decay bucket
const decayStepMicros int64 = 250_000 // 0.25 micro per bucket
const factorBase int64 = fixedpoint.Scale
const factorFloor int64 = 100_000 // decay factor never drops below 0.1

var accountWeight = map[string]int64{
	"LocalSystem": 40_000_000,
	"Service":     30_000_000,
	"Admin":       20_000_000,
	"User":        10_000_000,
	"Anonymous":   2_000_000,
}

var serviceWriteWeight = [4]int64{0, 5_000_000, 15_000_000, 30_000_000}

var privOps = map[string]bool{
	"WRITE_DAC":      true,
	"WRITE_OWNER":    true,
	"PROCESS_INJECT": true,
	"SERVICE_WRITE":  true,
}

// Config holds tunable scoring thresholds and budgets.
type Config struct {
	ThresholdMicro int64
}

// DefaultConfig returns the standard scoring configuration.
func DefaultConfig() Config {
	return Config{ThresholdMicro: ThresholdMicro}
}

// MaskWeightMicro sums the per-bit weights. Bit i (0-based) weighs i+1 micro.
func MaskWeightMicro(mask uint64) int64 {
	var w int64
	for i := uint(0); i < 16; i++ {
		if mask&(1<<i) != 0 {
			w += int64(i+1) * bitWeightStep
		}
	}
	return w
}

// TimeFactor converts a micro offset to an integral decay factor and records
// the truncation remainder of the factor multiplication performed by callers.
func decayFactor(offsetMicro int64) int64 {
	buckets := offsetMicro / timeBucketMicros
	factor := factorBase - buckets*decayStepMicros
	if factor < factorFloor {
		factor = factorFloor
	}
	return factor
}

// Engine runs one scoring round over a normalized bundle.
type Engine struct {
	cfg    Config
	ledger *fixedpoint.Ledger
}

// NewEngine creates a scoring engine with a fresh draft ledger.
func NewEngine(cfg Config) *Engine {
	return &Engine{cfg: cfg, ledger: fixedpoint.NewLedger()}
}

// Score evaluates every process. On overflow the draft ledger is discarded
// and E_SCORE_OVERFLOW is returned; nothing is committed partially.
func (e *Engine) Score(b *domain.Bundle) ([]domain.ProcConclusion, []domain.LedgerItem, error) {
	eventsByProc := map[int64][]domain.Event{}
	for _, ev := range b.Events {
		eventsByProc[ev.SubjectPID] = append(eventsByProc[ev.SubjectPID], ev)
	}
	conclusions := make([]domain.ProcConclusion, 0, len(b.Processes))
	for _, proc := range b.Processes {
		concl, err := e.scoreProc(proc, eventsByProc[proc.PID])
		if err != nil {
			return nil, nil, err
		}
		conclusions = append(conclusions, concl)
	}
	if err := e.ledger.Commit(); err != nil {
		return nil, nil, err
	}
	// attach finalized ledger bounds
	for i := range conclusions {
		pid := conclusions[i].PID
		conclusions[i].Ledger = e.ledger.Proc(pid)
		conclusions[i].BoundMicro = conclusions[i].Ledger.BoundMicro
		conclusions[i].Range = domain.Range{
			Low:  subChecked(conclusions[i].ScoreMicro, conclusions[i].BoundMicro),
			High: addChecked(conclusions[i].ScoreMicro, conclusions[i].BoundMicro),
		}
		conclusions[i].Code = e.verdict(conclusions[i].Range)
	}
	sort.Slice(conclusions, func(i, j int) bool { return conclusions[i].PID < conclusions[j].PID })
	return conclusions, e.ledger.Items(), nil
}

func (e *Engine) scoreProc(proc domain.ProcessDecl, evs []domain.Event) (domain.ProcConclusion, error) {
	out := domain.ProcConclusion{PID: proc.PID, Reasons: make([]domain.Reason, 0, 4+len(evs))}
	score := int64(0)
	add := func(w int64, kind, detail string) error {
		s, err := fixedpoint.Add(score, w)
		if err != nil {
			return err
		}
		score = s
		out.Reasons = append(out.Reasons, domain.Reason{Kind: kind, Weight: w, Detail: detail})
		return nil
	}

	// 1. service binary writability grade
	svcW := serviceWriteWeight[proc.ServiceWrite]
	if err := add(svcW, "service_write",
		fmt.Sprintf("grade=%d weight=%d", proc.ServiceWrite, svcW)); err != nil {
		return domain.ProcConclusion{}, err
	}

	// 2. account tier weight
	accW := accountWeight[proc.Account]
	if err := add(accW, "account", "account="+proc.Account); err != nil {
		return domain.ProcConclusion{}, err
	}

	// 3. inherited handle contribution: mask weight * factor 0.5, remainder booked
	if proc.InheritedMask != 0 {
		mw := MaskWeightMicro(proc.InheritedMask)
		half, rem, err := fixedpoint.Mul(mw, 500_000)
		if err != nil {
			return domain.ProcConclusion{}, err
		}
		if err := e.ledger.Record(proc.PID, 0, "inherited_handle_mul", rem); err != nil {
			return domain.ProcConclusion{}, err
		}
		if err := add(half, "inherited_handle",
			fmt.Sprintf("mask=0x%x from_pid=%d", proc.InheritedMask, proc.InheritedFrom)); err != nil {
			return domain.ProcConclusion{}, err
		}
	}

	// 4. privilege events with time decay
	sort.Slice(evs, func(i, j int) bool { return evs[i].Seq < evs[j].Seq })
	for _, ev := range evs {
		offsetMicro, timeRem, err := fixedpoint.NanosToMicro(ev.OffsetNS)
		if err != nil {
			return domain.ProcConclusion{}, err
		}
		if err := e.ledger.Record(proc.PID, ev.Seq, "time_truncate", timeRem); err != nil {
			return domain.ProcConclusion{}, err
		}
		if !privOps[ev.Op] {
			out.Reasons = append(out.Reasons, domain.Reason{
				Kind: "event_ignored", Weight: 0,
				Detail: fmt.Sprintf("seq=%d op=%s", ev.Seq, ev.Op),
			})
			continue
		}
		mw := MaskWeightMicro(ev.Mask)
		factor := decayFactor(offsetMicro)
		decayed, mulRem, err := fixedpoint.Mul(mw, factor)
		if err != nil {
			return domain.ProcConclusion{}, err
		}
		if err := e.ledger.Record(proc.PID, ev.Seq, "event_decay_mul", mulRem); err != nil {
			return domain.ProcConclusion{}, err
		}
		if err := add(decayed, "priv_event",
			fmt.Sprintf("seq=%d op=%s mask=0x%x factor_micro=%d", ev.Seq, ev.Op, ev.Mask, factor)); err != nil {
			return domain.ProcConclusion{}, err
		}
	}

	out.ScoreMicro = score
	return out, nil
}

func (e *Engine) verdict(r domain.Range) domain.BizCode {
	if r.Low > e.cfg.ThresholdMicro {
		return domain.CodePrivEscalation
	}
	if r.Low <= e.cfg.ThresholdMicro && r.High > e.cfg.ThresholdMicro {
		return domain.CodeIndeterminateRange
	}
	return domain.CodeOK
}

func addChecked(a, b int64) int64 {
	v, err := fixedpoint.Add(a, b)
	if err != nil {
		return 1 << 62
	}
	return v
}

func subChecked(a, b int64) int64 {
	v, err := fixedpoint.Add(a, -b)
	if err != nil {
		return -(1 << 62)
	}
	return v
}
