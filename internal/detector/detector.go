// Package detector orchestrates one detection round: parse, fixed-point
// scoring with error bookkeeping, provenance graph construction, verdict
// rendering and transactional snapshot commit.
package detector

import (
	"time"

	"privescworkbench/internal/domain"
	"privescworkbench/internal/graph"
	"privescworkbench/internal/scoring"
	"privescworkbench/internal/snapshot"
)

// Options parameterizes a detection round.
type Options struct {
	SessionID      string
	ThresholdMicro int64
	CommitSnapshot bool
}

// Result bundles the round output with the new snapshot version (0 if none).
type Result struct {
	Round       *domain.RoundResult
	Ledger      []domain.LedgerItem
	Graph       *graph.Graph
	Version     int64
	Replaced    bool
	OrphanCodes map[int64]domain.BizCode
}

// Run executes one deterministic detection round over a parsed bundle.
func Run(b *domain.Bundle, store *snapshot.Store, opts Options) (*Result, error) {
	start := time.Now() // monotonic clock; elapsed is the only measured time

	cfg := scoring.DefaultConfig()
	if opts.ThresholdMicro != 0 {
		cfg.ThresholdMicro = opts.ThresholdMicro
	}
	engine := scoring.NewEngine(cfg)
	conclusions, ledger, err := engine.Score(b)
	if err != nil {
		return nil, err
	}

	g := graph.Build(b.Processes, b.Events)
	orphans := g.Orphans()
	orphanSet := map[int64]bool{}
	for _, pid := range orphans {
		orphanSet[pid] = true
	}
	orphanCodes := map[int64]domain.BizCode{}
	for i := range conclusions {
		c := &conclusions[i]
		c.Trace = g.Trace(c.PID)
		if orphanSet[c.PID] {
			orphanCodes[c.PID] = domain.CodeOrphanRef
			if c.Code == domain.CodeOK {
				c.Code = domain.CodeOrphanRef
			}
		}
	}

	round := &domain.RoundResult{
		SampleVersion: b.Version,
		SampleHash:    snapshot.HashBundle(b),
		Conclusions:   conclusions,
		Orphans:       orphans,
		SampledAt:     domain.FixedSampledAt,
		ElapsedMicros: time.Since(start).Microseconds(),
	}

	res := &Result{Round: round, Ledger: ledger, Graph: g, OrphanCodes: orphanCodes}

	if opts.CommitSnapshot && store != nil {
		if err := store.BeginProgress(opts.SessionID, b.Version, round.SampleHash); err != nil {
			return nil, err
		}
		version, err := store.Commit(round, ledger)
		if err != nil {
			return nil, err
		}
		replaced, _, _, err := store.ReplacementDetected()
		if err != nil {
			return nil, err
		}
		if err := store.ClearProgress(opts.SessionID); err != nil {
			return nil, err
		}
		res.Version = version
		res.Replaced = replaced
	}
	return res, nil
}
