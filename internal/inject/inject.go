// Package inject constructs the five fault-injection scenarios purely from
// inputs and parameters (no runtime patching), runs them and asserts the
// stable business codes plus snapshot non-advancement guarantees.
package inject

import (
	"fmt"

	"privescworkbench/internal/detector"
	"privescworkbench/internal/domain"
	"privescworkbench/internal/fixedpoint"
	"privescworkbench/internal/sample"
	"privescworkbench/internal/snapshot"
)

// ScenarioResult captures one injection run and its assertions.
type ScenarioResult struct {
	Scenario      sample.Scenario `json:"scenario"`
	Code          domain.BizCode  `json:"code"`
	VersionBefore int64           `json:"version_before"`
	VersionAfter  int64           `json:"version_after"`
	SnapshotMoved bool            `json:"snapshot_moved"`
	Detail        string          `json:"detail"`
}

// Runner executes injection scenarios against an isolated snapshot store.
type Runner struct {
	store *snapshot.Store
}

// NewRunner binds a runner to a snapshot store.
func NewRunner(store *snapshot.Store) *Runner { return &Runner{store: store} }

// RunAll executes the five scenarios and returns their results in fixed order.
func (r *Runner) RunAll() ([]ScenarioResult, error) {
	out := make([]ScenarioResult, 0, 5)
	for _, sc := range []sample.Scenario{
		sample.ScenarioMaskBad,
		sample.ScenarioOverflow,
		sample.ScenarioHandleAbuse,
		sample.ScenarioOrphan,
		sample.ScenarioGap,
	} {
		res, err := r.RunOne(sc)
		if err != nil {
			return nil, err
		}
		out = append(out, res)
	}
	return out, nil
}

// RunOne constructs and evaluates one injection scenario.
func (r *Runner) RunOne(sc sample.Scenario) (ScenarioResult, error) {
	before, err := r.store.LatestVersion()
	if err != nil {
		return ScenarioResult{}, err
	}
	gen := sample.Generate(1, sc)
	res := ScenarioResult{Scenario: sc, VersionBefore: before}

	switch sc {
	case sample.ScenarioMaskBad:
		// parse must reject the out-of-range mask before any scoring
		if _, perr := sample.ParseEventStream(gen.StreamBytes); perr != nil {
			res.Code = codeOf(perr)
			res.Detail = perr.Error()
		} else {
			res.Code = domain.CodeInternal
			res.Detail = "expected E_SAMPLE_PARSE for mask out of range"
		}

	case sample.ScenarioOverflow:
		// valid input, but forced overflow path via the fixed-point layer:
		// multiplicands beyond MaxMicro must reject and never book a ledger.
		_, _, oerr := fixedpoint.Mul(fixedpoint.MaxMicro+1, fixedpoint.Scale)
		if oerr != nil {
			res.Code = codeOf(oerr)
			res.Detail = oerr.Error()
		} else {
			res.Code = domain.CodeInternal
			res.Detail = "expected E_SCORE_OVERFLOW for overflow multiplication"
		}
		// additionally prove a full round on the valid overflow sample is safe
		bundle, perr := sample.ParseBundle(gen.Bundle.Version, gen.ManifestBytes, gen.StreamBytes)
		if perr != nil {
			return res, perr
		}
		if _, derr := detector.Run(bundle, nil, detector.Options{SessionID: "inject-overflow"}); derr != nil {
			return res, derr
		}

	case sample.ScenarioHandleAbuse:
		bundle, perr := sample.ParseBundle(gen.Bundle.Version, gen.ManifestBytes, gen.StreamBytes)
		if perr != nil {
			return res, perr
		}
		dres, derr := detector.Run(bundle, r.store, detector.Options{
			SessionID: "inject-handle", CommitSnapshot: true,
		})
		if derr != nil {
			return res, derr
		}
		res.Code = verdictFor(dres, 3002)
		res.Detail = fmt.Sprintf("pid 3002 verdict=%s trace_hops=%d",
			res.Code, len(dres.Round.Conclusions[1].Trace))

	case sample.ScenarioOrphan:
		bundle, perr := sample.ParseBundle(gen.Bundle.Version, gen.ManifestBytes, gen.StreamBytes)
		if perr != nil {
			return res, perr
		}
		dres, derr := detector.Run(bundle, r.store, detector.Options{
			SessionID: "inject-orphan", CommitSnapshot: true,
		})
		if derr != nil {
			return res, derr
		}
		res.Code = verdictFor(dres, 4001)
		res.Detail = fmt.Sprintf("orphans=%v", dres.Round.Orphans)

	case sample.ScenarioGap:
		// commit two healthy snapshots, then physically remove the middle one
		// and assert chain verification rejects the gap on reload
		g1 := sample.Generate(1, sample.ScenarioBaseline)
		b1, perr := sample.ParseBundle(g1.Bundle.Version, g1.ManifestBytes, g1.StreamBytes)
		if perr != nil {
			return res, perr
		}
		if _, derr := detector.Run(b1, r.store, detector.Options{
			SessionID: "inject-gap-1", CommitSnapshot: true,
		}); derr != nil {
			return res, derr
		}
		g2 := sample.Generate(2, sample.ScenarioBaseline)
		b2, perr := sample.ParseBundle(g2.Bundle.Version, g2.ManifestBytes, g2.StreamBytes)
		if perr != nil {
			return res, perr
		}
		if _, derr := detector.Run(b2, r.store, detector.Options{
			SessionID: "inject-gap-2", CommitSnapshot: true,
		}); derr != nil {
			return res, derr
		}
		if err := r.store.DeleteVersion(before + 1); err != nil {
			return res, err
		}
		if _, verr := r.store.VerifyChain(); verr != nil {
			res.Code = codeOf(verr)
			res.Detail = verr.Error()
		} else {
			res.Code = domain.CodeInternal
			res.Detail = "expected E_SNAPSHOT_VERSION for version gap"
		}
	}

	after, err := r.store.LatestVersion()
	if err != nil {
		return res, err
	}
	res.VersionAfter = after
	res.SnapshotMoved = after != before && sc != sample.ScenarioGap
	return res, nil
}

func codeOf(err error) domain.BizCode {
	if ce, ok := err.(*domain.CodeError); ok {
		return ce.Code
	}
	return domain.CodeInternal
}

func verdictFor(res *detector.Result, pid int64) domain.BizCode {
	for _, c := range res.Round.Conclusions {
		if c.PID == pid {
			return c.Code
		}
	}
	return domain.CodeInternal
}
