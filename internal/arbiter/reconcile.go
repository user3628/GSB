package arbiter

import (
	"context"

	"leasearbiter/internal/store"
)

// ReconcileRequest re-runs every full-path check for an edit without applying
// it. It powers requirement 8: when a fast precheck said editable but the full
// commit fails, the caller receives an itemized, independently reproducible
// diff over the same snapshot and lease version.
type ReconcileRequest struct {
	RowID          string `json:"row_id"`
	Holder         string `json:"holder"`
	LeaseVersion   int64  `json:"lease_version"`
	BaseRowVersion int64  `json:"base_row_version"`
	SnapshotVer    int64  `json:"snapshot_ver"`
}

// ReconcileVerdict is the field-by-field full-path revalidation result.
type ReconcileVerdict struct {
	RowID       string   `json:"row_id"`
	Pass        bool     `json:"pass"`
	FailureCode string   `json:"failure_code,omitempty"`
	Reasons     []string `json:"reasons"`
	Diff        Diff     `json:"diff"`
}

// Reconcile performs read-only checks under the row gate so the verdict is
// computed against one consistent observation of lease and latest row version.
func (e *Engine) Reconcile(ctx context.Context, req ReconcileRequest) (*ReconcileVerdict, error) {
	if req.RowID == "" {
		return nil, apiError(CodeBadRequest, "row_id is required", nil)
	}
	g := e.gates.get(req.RowID)
	g.mu.Lock()
	defer g.mu.Unlock()

	e.expireIfDueLocked(req.RowID)
	verdict := &ReconcileVerdict{RowID: req.RowID}
	lease, err := e.leaseOrNil(req.RowID)
	if err != nil {
		return nil, err
	}
	latest, err := e.latestCommitted(req.RowID)
	if err != nil {
		return nil, err
	}
	nowMs := store.Milli(e.now())
	snapshot := req.SnapshotVer
	if snapshot == 0 {
		snapshot = latest.CommitVer
	}
	verdict.Diff = Diff{
		SnapshotVer:        snapshot,
		ExpectedRowVersion: req.BaseRowVersion,
		ActualRowVersion:   latest.RowVersion,
		PresentedHolder:    req.Holder,
		PresentedLeaseVer:  req.LeaseVersion,
	}
	if lease != nil {
		verdict.Diff.ActualHolder = lease.Holder
		verdict.Diff.ActualLeaseVer = lease.LeaseVersion
		verdict.Diff.LeaseState = string(lease.State)
		verdict.Diff.ExpiresAtMs = lease.ExpiresAtMs
	}
	if lease == nil {
		verdict.Pass = false
		verdict.FailureCode = string(CodeLeaseLost)
		verdict.Reasons = append(verdict.Reasons, "no durable lease exists")
	} else {
		if lease.State != store.LeaseGranted {
			verdict.Reasons = append(verdict.Reasons, "lease state is "+string(lease.State))
		}
		if lease.Holder != req.Holder {
			verdict.Reasons = append(verdict.Reasons, "actual holder "+lease.Holder+" differs from presented "+req.Holder)
		}
		if lease.LeaseVersion != req.LeaseVersion {
			verdict.Reasons = append(verdict.Reasons, "lease was preempted to version "+itoa(lease.LeaseVersion))
		}
		if lease.ExpiresAtMs <= nowMs {
			verdict.Reasons = append(verdict.Reasons, "lease reached expiry without renewal")
		}
		if len(verdict.Reasons) > 0 {
			verdict.FailureCode = string(CodeLeaseLost)
		}
	}
	if latest.RowVersion != req.BaseRowVersion {
		verdict.Pass = false
		if verdict.FailureCode == "" {
			verdict.FailureCode = string(CodeEditStale)
		}
		verdict.Reasons = append(verdict.Reasons, "actual row version "+itoa(latest.RowVersion)+
			" differs from base "+itoa(req.BaseRowVersion))
	}
	if len(verdict.Reasons) == 0 {
		verdict.Pass = true
	}
	return verdict, nil
}
