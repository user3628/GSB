package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type probeTask struct {
	instance  RuntimeInstance
	admission int
}

type jobResult struct {
	task    probeTask
	outcome ProbeOutcome
}

// RunProbeRound executes real probes under global, per-service and wall-clock
// budgets. The writer gate is held for the whole round, but stateMu is not, so
// readers continue to see the last complete derived revision.
func (e *Engine) RunProbeRound(ctx context.Context, opts RoundOptions) (RoundResult, *Error) {
	var out RoundResult
	hash := roundHash(opts)
	body, opErr := e.idempotent(ctx, opts.RequestID, "probe-round", hash, func() ([]byte, *Error) {
		e.writeMu.Lock()
		defer e.writeMu.Unlock()
		return e.doProbeRound(ctx, opts)
	})
	if opErr != nil {
		return out, opErr
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, newError(ErrInternal, "decode probe round response", "detail", err.Error())
	}
	return out, nil
}

func (e *Engine) doProbeRound(ctx context.Context, opts RoundOptions) ([]byte, *Error) {
	if opts.GlobalConcurrency < 1 || opts.PerServiceConcurrency < 1 || opts.BudgetMS < 1 || opts.WaitQueueCapacity < 0 {
		return nil, validationError(fmt.Errorf("globalConcurrency, perServiceConcurrency and budgetMs must be positive; waitQueueCapacity must be non-negative"))
	}
	roundStarted := time.Now()
	hash := roundHash(opts)
	e.stateMu.RLock()
	meta, instances, loadErr := e.loadState(ctx)
	e.stateMu.RUnlock()
	if loadErr != nil {
		return nil, loadErr
	}

	tasks := make([]probeTask, 0, len(instances))
	for _, in := range instances {
		if in.State == StateDraining {
			continue
		}
		tasks = append(tasks, probeTask{instance: in, admission: len(tasks)})
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].instance.ServiceName != tasks[j].instance.ServiceName {
			return tasks[i].instance.ServiceName < tasks[j].instance.ServiceName
		}
		return tasks[i].instance.InstanceID < tasks[j].instance.InstanceID
	})

	serviceCounts := make(map[string]int)
	for _, task := range tasks {
		serviceCounts[task.instance.ServiceName]++
	}
	maxActive := 0
	for _, count := range serviceCounts {
		if count < opts.PerServiceConcurrency {
			maxActive += count
		} else {
			maxActive += opts.PerServiceConcurrency
		}
	}
	if maxActive > opts.GlobalConcurrency {
		maxActive = opts.GlobalConcurrency
	}
	pendingAtAdmission := len(tasks) - maxActive
	if pendingAtAdmission > opts.WaitQueueCapacity {
		return nil, newError(ErrProbeQueueFull, "bounded probe wait queue is full",
			"taskCount", len(tasks), "maxActive", maxActive, "waitQueueCapacity", opts.WaitQueueCapacity,
			"globalConcurrency", opts.GlobalConcurrency, "perServiceConcurrency", opts.PerServiceConcurrency)
	}

	roundID := newID()
	intentID := newID()
	deadline := roundStarted.Add(time.Duration(opts.BudgetMS) * time.Millisecond)
	prospectiveRevision := meta.DerivedRevision + 1

	err := writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		if err := insertIntent(ctx, tx, intentID, "probe", opts.RequestID, hash, meta.Version, prospectiveRevision, roundID,
			map[string]any{"taskCount": len(tasks), "budgetMs": opts.BudgetMS}); err != nil {
			return err
		}
		if opts.RequestID != "" {
			if err := putRequestRunning(ctx, tx, opts.RequestID, "probe-round", hash); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO probe_rounds(round_id,snapshot_version,base_derived_revision,status,budget_ms,
			global_limit,service_limit,queue_capacity,created_at) VALUES(?,?,?,?,?,?,?,?,?)`,
			roundID, meta.Version, meta.DerivedRevision, "RUNNING", opts.BudgetMS, opts.GlobalConcurrency,
			opts.PerServiceConcurrency, opts.WaitQueueCapacity, nowText()); err != nil {
			return err
		}
		for _, task := range tasks {
			if _, err := tx.ExecContext(ctx, `INSERT INTO probe_results(round_id,service_name,instance_id,kind,status)
				VALUES(?,?,?,?,?)`, roundID, task.instance.ServiceName, task.instance.InstanceID,
				string(task.instance.Probe.Kind), "PENDING"); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, newError(ErrInternal, "stage probe round failed", "detail", err.Error())
	}
	e.crash("after_probe_intent")

	roundCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	results := make(chan jobResult, len(tasks)+1)
	pending := make([]probeTask, len(tasks))
	copy(pending, tasks)
	serviceRunning := make(map[string]int)
	running := 0
	var outcomes []ProbeOutcome
	deferredReason := ""
	persistedFirst := false

	drainRunning := func() {
		for running > 0 {
			res := <-results
			running--
			serviceRunning[res.task.instance.ServiceName]--
			outcomes = append(outcomes, res.outcome)
			_ = e.persistProbeResult(ctx, roundID, res)
			if !persistedFirst {
				persistedFirst = true
				e.crash("after_probe_result")
			}
		}
	}

	for {
		now := time.Now()
		if !now.Before(deadline) {
			deferredReason = "round_budget_exhausted"
			break
		}
		startedThisPass := 0
		for i := 0; i < len(pending); {
			if running >= opts.GlobalConcurrency || !time.Now().Before(deadline) {
				break
			}
			task := pending[i]
			if serviceRunning[task.instance.ServiceName] >= opts.PerServiceConcurrency {
				i++
				continue
			}
			running++
			startedThisPass++
			serviceRunning[task.instance.ServiceName]++
			pending = append(pending[:i], pending[i+1:]...)
			go func(task probeTask) {
				outcome := executeProbe(roundCtx, deadline, task.instance)
				results <- jobResult{task: task, outcome: outcome}
			}(task)
		}
		if len(pending) == 0 && running == 0 {
			break
		}
		if running == 0 {
			if startedThisPass == 0 {
				deferredReason = "round_budget_exhausted"
			}
			break
		}
		wait := time.Until(deadline)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case res := <-results:
			if !timer.Stop() {
				<-timer.C
			}
			running--
			serviceRunning[res.task.instance.ServiceName]--
			outcomes = append(outcomes, res.outcome)
			_ = e.persistProbeResult(ctx, roundID, res)
			if !persistedFirst {
				persistedFirst = true
				e.crash("after_probe_result")
			}
		case <-timer.C:
			deferredReason = "round_budget_exhausted"
			break
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			deferredReason = "round_context_canceled"
			break
		}
		if deferredReason != "" {
			break
		}
	}
	if deferredReason != "" {
		drainRunning()
	} else if running > 0 {
		drainRunning()
	}
	cancel()

	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].ServiceName != outcomes[j].ServiceName {
			return outcomes[i].ServiceName < outcomes[j].ServiceName
		}
		return outcomes[i].InstanceID < outcomes[j].InstanceID
	})
	elapsed := time.Since(roundStarted).Milliseconds()
	consumed := elapsed
	if consumed > opts.BudgetMS {
		consumed = opts.BudgetMS
	}
	deferred := buildDeferred(pending, consumed, opts.BudgetMS, deferredReason)

	byKey := runtimeMap(instances)
	newRevision := meta.DerivedRevision
	var updated []RuntimeInstance
	for _, outcome := range outcomes {
		key := outcome.ServiceName + "\x00" + outcome.InstanceID
		in, ok := byKey[key]
		if !ok {
			continue
		}
		if outcome.OK {
			in.ConsecutiveSuccess++
			in.ConsecutiveFailure = 0
		} else {
			in.ConsecutiveFailure++
			in.ConsecutiveSuccess = 0
		}
		next := nextProbeState(in.State, outcome.OK, in.ConsecutiveSuccess, in.ConsecutiveFailure,
			in.SuccessThreshold, in.FailureThreshold)
		if !CanTransition(in.State, next) {
			return nil, newError(ErrInvalidTransition, "probe result produced an illegal state transition",
				"serviceName", in.ServiceName, "instanceId", in.InstanceID, "from", in.State, "to", next)
		}
		in.State = next
		in.LastProbeAt = outcome.ObservedAt
		ok = outcome.OK
		in.LastProbeOK = &ok
		in.LastProbeCode = outcome.Code
		in.UpdatedRevision = prospectiveRevision
		in.Probe.Kind = outcome.Kind
		byKey[key] = in
		updated = append(updated, in)
	}
	var view DerivedView
	if len(updated) > 0 {
		newRevision = prospectiveRevision
		all := sortedRuntimeMap(byKey)
		view = buildDerived(newRevision, all, time.Now().UnixMilli())
		view.SnapshotVersion = meta.Version
	}
	status := "COMPLETE"
	if deferredReason != "" && len(outcomes) == 0 {
		status = "BUDGET_DEFERRED"
	} else if len(deferred) > 0 {
		status = "PARTIAL"
	}
	result := RoundResult{
		RoundID: roundID, SnapshotVersion: meta.Version, DerivedRevision: newRevision, Status: status,
		Started: len(outcomes), Completed: len(outcomes), Deferred: deferred, Outcomes: outcomes,
		BudgetMS: opts.BudgetMS, ElapsedMS: elapsed,
	}
	body, _ := json.Marshal(result)

	if len(updated) > 0 {
		err = writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
			if err := stageDerived(ctx, tx, view, meta.Version, "probe"); err != nil {
				return err
			}
			e.crash("mid_cascade")
			return nil
		})
		if err != nil {
			e.abortRound(ctx, intentID, roundID, prospectiveRevision, "E_INTERNAL")
			return nil, newError(ErrInternal, "stage probe derived view failed", "detail", err.Error())
		}
		e.crash("after_stage")
	}

	err = writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		for _, in := range updated {
			if err := upsertRuntimeInstance(ctx, tx, in); err != nil {
				return err
			}
			if err := upsertLease(ctx, tx, in, meta.Version); err != nil {
				return err
			}
		}
		if len(updated) > 0 {
			if err := activateDerived(ctx, tx, view, -1); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE probe_results SET status='DEFERRED',committed=1
			WHERE round_id=? AND status='PENDING'`, roundID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE probe_results SET status='COMMITTED',committed=1
			WHERE round_id=? AND status='COMPLETED_UNCOMMITTED'`, roundID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE probe_rounds SET status=?,completed_at=?,result_json=?
			WHERE round_id=?`, status, nowText(), string(body), roundID); err != nil {
			return err
		}
		if err := setMeta(ctx, tx, "latest_round_id", roundID); err != nil {
			return err
		}
		if err := completeIntent(ctx, tx, intentID, "COMPLETED"); err != nil {
			return err
		}
		if opts.RequestID != "" {
			return finishRequest(ctx, tx, opts.RequestID, "COMPLETED", body, 200, meta.Version, "")
		}
		return nil
	})
	if err != nil {
		e.abortRound(ctx, intentID, roundID, prospectiveRevision, "E_INTERNAL")
		return nil, newError(ErrInternal, "activate probe round failed", "detail", err.Error())
	}
	e.crash("after_activation")
	return body, nil
}

func (e *Engine) persistProbeResult(ctx context.Context, roundID string, res jobResult) error {
	ok := 0
	if res.outcome.OK {
		ok = 1
	}
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE probe_results SET status='COMPLETED_UNCOMMITTED',ok=?,code=?,latency_ms=?,
			observed_at=?,detail=?,committed=0 WHERE round_id=? AND service_name=? AND instance_id=?`,
			ok, res.outcome.Code, res.outcome.LatencyMS, res.outcome.ObservedAt, res.outcome.Detail,
			roundID, res.task.instance.ServiceName, res.task.instance.InstanceID)
		return err
	})
}

func buildDeferred(pending []probeTask, consumed, budget int64, reason string) []DeferredTask {
	if reason == "" {
		reason = "round_budget_exhausted"
	}
	sorted := append([]probeTask(nil), pending...)
	sort.Slice(sorted, func(i, j int) bool {
		pi, pj := sorted[i].instance.Priority, sorted[j].instance.Priority
		if pi != pj {
			return pi > pj
		}
		if sorted[i].instance.ServiceName != sorted[j].instance.ServiceName {
			return sorted[i].instance.ServiceName < sorted[j].instance.ServiceName
		}
		return sorted[i].instance.InstanceID < sorted[j].instance.InstanceID
	})
	out := make([]DeferredTask, 0, len(sorted))
	for i, task := range sorted {
		remaining := budget - consumed
		if remaining < 0 {
			remaining = 0
		}
		out = append(out, DeferredTask{
			Rank: i + 1, ServiceName: task.instance.ServiceName, InstanceID: task.instance.InstanceID,
			Kind: task.instance.Probe.Kind, Priority: task.instance.Priority, AdmissionIndex: task.admission,
			Reason: reason, ConsumedBudgetMS: consumed, RemainingBudgetMS: remaining,
		})
	}
	return out
}

func (e *Engine) abortRound(ctx context.Context, intentID, roundID string, revision int64, code string) {
	_ = writeTx(context.Background(), e.db.sql, func(tx *sql.Tx) error {
		_, _ = tx.ExecContext(ctx, `DELETE FROM health_summary WHERE revision=?`, revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM weight_allocations WHERE revision=?`, revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM probe_plans WHERE revision=?`, revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM derived_revisions WHERE revision=? AND status='STAGING'`, revision)
		_, _ = tx.ExecContext(ctx, `UPDATE probe_rounds SET status='ABORTED',completed_at=?,error_code=? WHERE round_id=?`, nowText(), code, roundID)
		_, _ = tx.ExecContext(ctx, `UPDATE intents SET status='ABORTED',updated_at=? WHERE id=?`, nowText(), intentID)
		return nil
	})
}

// GetRound returns a durable round verdict by id.
func (e *Engine) GetRound(ctx context.Context, roundID string) (RoundResult, *Error) {
	var out RoundResult
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		var raw, status string
		if err := tx.QueryRowContext(ctx, `SELECT status,result_json FROM probe_rounds WHERE round_id=?`, roundID).Scan(&status, &raw); err != nil {
			return err
		}
		if raw == "" {
			out = RoundResult{RoundID: roundID, Status: status, Deferred: []DeferredTask{}, Outcomes: []ProbeOutcome{}}
			return nil
		}
		return json.Unmarshal([]byte(raw), &out)
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return out, newError(ErrNotFound, "probe round not found", "roundId", roundID)
		}
		return out, newError(ErrInternal, "read probe round failed", "detail", err.Error())
	}
	return out, nil
}
