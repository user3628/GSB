package writeback

import (
	"context"
	"fmt"
	"sort"

	"example.com/ntfsacl/internal/domain"
	"example.com/ntfsacl/internal/fixture"
	"example.com/ntfsacl/internal/store"
)

type RecoveryNode struct {
	NodePath string `json:"node_path"`
	Action   string `json:"action"`
	Detail   string `json:"detail"`
}

type RecoverySession struct {
	SessionID     string         `json:"session_id"`
	Status        string         `json:"status"`
	Code          string         `json:"code,omitempty"`
	Committed     []string       `json:"committed"`
	RolledBack    []string       `json:"rolled_back"`
	Requeue       []string       `json:"requeue"`
	Residual      []string       `json:"residual"`
	AffectedRange []string       `json:"affected_range"`
	Notes         []RecoveryNode `json:"notes"`
}

type RecoveryResult struct {
	Schema              string            `json:"schema"`
	Code                string            `json:"code,omitempty"`
	ExitReportCommitted bool              `json:"exit_report_committed"`
	Sessions            []RecoverySession `json:"sessions"`
}

func (r RecoveryResult) MarshalJSON() ([]byte, error) {
	type wireResult RecoveryResult
	v := wireResult(r)
	if v.Sessions == nil {
		v.Sessions = []RecoverySession{}
	}
	for i := range v.Sessions {
		s := &v.Sessions[i]
		if s.Committed == nil {
			s.Committed = []string{}
		}
		if s.RolledBack == nil {
			s.RolledBack = []string{}
		}
		if s.Requeue == nil {
			s.Requeue = []string{}
		}
		if s.Residual == nil {
			s.Residual = []string{}
		}
		if s.AffectedRange == nil {
			s.AffectedRange = []string{}
		}
		if s.Notes == nil {
			s.Notes = []RecoveryNode{}
		}
	}
	return domain.CanonicalJSON(v)
}

func (r *RecoveryResult) Marshal() ([]byte, error) { return r.MarshalJSON() }

// Recover is deliberately report-free: it returns recovery state only and
// never commits a later writeback_after report.
func Recover(ctx context.Context, workdir string) (RecoveryResult, error) {
	fs, err := fixture.Open(workdir)
	if err != nil {
		return RecoveryResult{}, err
	}
	db, err := store.Open(fs.DBPath())
	if err != nil {
		return RecoveryResult{}, err
	}
	defer db.Close()
	out := RecoveryResult{Schema: "ntfsacl/recovery/v1", ExitReportCommitted: false, Sessions: []RecoverySession{}}
	sessions, err := db.UnfinishedSessions(ctx)
	if err != nil {
		return out, err
	}
	for _, sess := range sessions {
		rs := RecoverySession{SessionID: sess.ID, Status: store.SessionRecovered, Committed: []string{}, RolledBack: []string{}, Requeue: []string{}, Residual: []string{}, AffectedRange: []string{}, Notes: []RecoveryNode{}}
		ops, err := db.Ops(ctx, sess.ID)
		if err != nil {
			return out, err
		}
		for _, op := range ops {
			switch op.Phase {
			case store.OpCommitted:
				rs.Committed = append(rs.Committed, op.NodePath)
			case store.OpPending:
				rs.AffectedRange = append(rs.AffectedRange, op.NodePath)
				current, readErr := fs.LoadDescriptorBytes(op.NodePath)
				if readErr != nil {
					rs.Residual = append(rs.Residual, op.NodePath)
					rs.Notes = append(rs.Notes, RecoveryNode{NodePath: op.NodePath, Action: "RESIDUAL", Detail: "descriptor unreadable: " + readErr.Error()})
					continue
				}
				curHash := domain.HashBytes(current)
				matchesBefore := curHash == op.BeforeSHA256
				matchesAfter := op.AfterSHA256 != "" && curHash == op.AfterSHA256
				if !matchesBefore && !matchesAfter {
					rs.Residual = append(rs.Residual, op.NodePath)
					rs.Notes = append(rs.Notes, RecoveryNode{NodePath: op.NodePath, Action: "RESIDUAL", Detail: "descriptor matches neither the before nor after writeback revision"})
					continue
				}
				if matchesAfter {
					if err := fs.RestoreDescriptor(op.NodePath, op.BeforePayload); err != nil {
						rs.Residual = append(rs.Residual, op.NodePath)
						rs.Notes = append(rs.Notes, RecoveryNode{NodePath: op.NodePath, Action: "RESIDUAL", Detail: "restore failed: " + err.Error()})
						continue
					}
					ver, err := fs.LoadDescriptorBytes(op.NodePath)
					if err != nil || domain.HashBytes(ver) != op.BeforeSHA256 {
						rs.Residual = append(rs.Residual, op.NodePath)
						rs.Notes = append(rs.Notes, RecoveryNode{NodePath: op.NodePath, Action: "RESIDUAL", Detail: "restored descriptor failed hash verification"})
						continue
					}
				}
				if err := db.MarkNodeRolledBack(ctx, sess.ID, op.NodePath, op.Seq, "recovery discarded uncommitted node write"); err != nil {
					return out, err
				}
				rs.RolledBack = append(rs.RolledBack, op.NodePath)
				rs.Requeue = append(rs.Requeue, op.NodePath)
				rs.Notes = append(rs.Notes, RecoveryNode{NodePath: op.NodePath, Action: "REQUEUED", Detail: "uncommitted writeback discarded and added back to the repair set"})
			case store.OpRolledBack:
				rs.Requeue = append(rs.Requeue, op.NodePath)
			}
		}
		if len(rs.Residual) > 0 {
			rs.Status = store.SessionIncomplete
			rs.Code = CodeRecoveryIncomplete
			out.Code = CodeRecoveryIncomplete
		}
		sort.Strings(rs.Committed)
		sort.Strings(rs.RolledBack)
		sort.Strings(rs.Requeue)
		sort.Strings(rs.Residual)
		sort.Strings(rs.AffectedRange)
		sort.Slice(rs.Notes, func(i, j int) bool {
			if rs.Notes[i].NodePath != rs.Notes[j].NodePath {
				return rs.Notes[i].NodePath < rs.Notes[j].NodePath
			}
			return rs.Notes[i].Action < rs.Notes[j].Action
		})
		if err := db.SetSessionStatus(ctx, sess.ID, rs.Status, rs.Code, 0, 0); err != nil {
			return out, err
		}
		out.Sessions = append(out.Sessions, rs)
		if err := db.ReleaseLocksForOwner(ctx, sess.ID); err != nil {
			return out, fmt.Errorf("release recovery locks for %s: %w", sess.ID, err)
		}
	}
	sort.Slice(out.Sessions, func(i, j int) bool { return out.Sessions[i].SessionID < out.Sessions[j].SessionID })
	return out, nil
}
