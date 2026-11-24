// Package demo replays deterministic sample workloads against the local engine.
package demo

import (
	"context"
	"fmt"

	"walgroup"
	walsample "walgroup/sample"
)

type ActionResult struct {
	Index        int                     `json:"index"`
	Type         string                  `json:"type"`
	Expect       string                  `json:"expect"`
	ObservedCode string                  `json:"observed_code"`
	OK           bool                    `json:"ok"`
	Message      string                  `json:"message,omitempty"`
	Group        *walgroup.GroupResult   `json:"group,omitempty"`
	Checkpoint   *walgroup.Checkpoint    `json:"checkpoint,omitempty"`
	Reclaim      *walgroup.ReclaimResult `json:"reclaim,omitempty"`
	Page         *walgroup.PageResult    `json:"page,omitempty"`
	Segments     []walgroup.SegmentInfo  `json:"segments,omitempty"`
}

type Report struct {
	Seed     int64                  `json:"seed"`
	AllOK    bool                   `json:"all_ok"`
	Actions  []ActionResult         `json:"actions"`
	Segments []walgroup.SegmentInfo `json:"segments"`
}

func toInputs(g *walsample.Group) []walgroup.TransactionInput {
	if g == nil {
		return nil
	}
	out := make([]walgroup.TransactionInput, 0, len(g.Transactions))
	for _, tx := range g.Transactions {
		updates := make([]walgroup.PageUpdate, 0, len(tx.Records))
		for _, r := range tx.Records {
			updates = append(updates, walgroup.PageUpdate{PageNo: r.PageNo, Value: r.Value})
		}
		out = append(out, walgroup.TransactionInput{ID: tx.ID, Records: updates})
	}
	return out
}

// Run executes every action and records the exact observed business code. A
// mismatch sets AllOK=false but does not hide later recovery-relevant actions.
func Run(ctx context.Context, e *walgroup.Engine, w walsample.Workload) (Report, error) {
	report := Report{Seed: w.Seed, AllOK: true}
	for i, a := range w.Actions {
		res := ActionResult{Index: i, Type: a.Type, Expect: a.Expect}
		res.ObservedCode = "OK"
		fail := func(err error, msg string) {
			if err != nil {
				res.ObservedCode = walgroup.CodeOf(err)
				if res.ObservedCode == "" {
					res.ObservedCode = "E_INTERNAL"
				}
				res.Message = err.Error()
			} else if msg != "" {
				res.Message = msg
			}
			res.OK = expectationMet(a.Expect, res.ObservedCode, a.Type, &res)
			if !res.OK {
				report.AllOK = false
			}
		}
		switch a.Type {
		case "commit_group":
			group, err := e.CommitGroup(ctx, toInputs(a.Group))
			if err == nil {
				res.Group = &group
			}
			fail(err, "")
			if err == nil && a.Expect == "OK_DUPLICATE" {
				allDup := group.FsyncCount == 0
				for _, tx := range group.Transactions {
					allDup = allDup && tx.Duplicate
				}
				res.OK = allDup
				if !allDup {
					res.Message = "expected all transactions to be idempotent duplicates"
					report.AllOK = false
				}
			}
		case "commit_conflict":
			inputs := toInputs(a.Group)
			if len(inputs) != 0 && len(inputs[0].Records) != 0 {
				inputs[0].Records[0].Value += "-conflict"
			}
			_, err := e.CommitGroup(ctx, inputs)
			fail(err, "")
		case "checkpoint":
			cp, err := e.AdvanceCheckpoint(ctx, walgroup.CheckpointRequest{
				Sequence: a.Checkpoint.Sequence, ReplayStartLSN: a.Checkpoint.ReplayStartLSN,
			})
			if err == nil {
				res.Checkpoint = &cp
			}
			fail(err, "")
		case "reclaim":
			var nos []uint64
			if a.Reclaim != nil {
				nos = a.Reclaim.SegmentNos
			}
			rr, err := e.ReclaimSegments(ctx, walgroup.ReclaimRequest{SegmentNos: nos})
			if err == nil {
				res.Reclaim = &rr
			}
			fail(err, "")
		case "reclaim_current":
			segs, err := e.Segments(ctx)
			if err != nil {
				return report, err
			}
			var open []uint64
			for _, s := range segs {
				if s.State == walgroup.SegmentOpen {
					open = append(open, s.SegmentNo)
				}
			}
			if len(open) == 0 {
				res.ObservedCode = "E_TEST_NO_OPEN_SEGMENT"
				res.OK = false
				report.AllOK = false
			} else {
				_, err = e.ReclaimSegments(ctx, walgroup.ReclaimRequest{SegmentNos: open[:1]})
				fail(err, "")
			}
		case "read":
			p, err := e.ReadPage(ctx, a.Read.PageNo, a.Read.LSN)
			if err == nil {
				res.Page = &p
			}
			fail(err, "")
		default:
			res.ObservedCode = "E_UNKNOWN_ACTION"
			res.Message = fmt.Sprintf("unknown action type %q", a.Type)
			res.OK = false
			report.AllOK = false
		}
		report.Actions = append(report.Actions, res)
	}
	segs, err := e.Segments(ctx)
	if err != nil {
		return report, err
	}
	report.Segments = segs
	if err := ctx.Err(); err != nil {
		return report, err
	}
	return report, nil
}

func expectationMet(expect, observed, typ string, res *ActionResult) bool {
	if expect == "" || expect == "OK" {
		return observed == "OK"
	}
	if expect == "OK_DUPLICATE" {
		return observed == "OK"
	}
	if typ == "reclaim_current" && observed == "E_RECLAIM_UNSAFE" {
		return true
	}
	return observed == expect
}
