package pool

import (
	"context"
	"fmt"
	"time"

	"local/sqlitepool/internal/dbconn"
	"local/sqlitepool/internal/domain"
)

func (p *Pool) post(command any) bool {
	select {
	case <-p.done:
		return false
	default:
	}
	select {
	case p.cmdCh <- command:
		return true
	case <-p.done:
		return false
	}
}

func (p *Pool) persistContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func (p *Pool) newWaiter(state *poolState, ctx context.Context, opts AcquireOptions, reply chan acquireOutcome) *waiter {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = state.cfg.AcquireTimeout()
	}
	now := time.Now()
	w := &waiter{
		id:        p.nextWaiterID(),
		requestID: opts.RequestID,
		deadline:  now.Add(timeout),
		arrived:   now,
		done:      make(chan struct{}),
		reply:     reply,
	}
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			_ = p.post(waiterExpiredCmd{waiter: w, reason: "context_cancelled"})
		case <-timer.C:
			_ = p.post(waiterExpiredCmd{waiter: w, reason: "acquire_deadline"})
		case <-w.done:
		}
	}()
	return w
}

func (p *Pool) finishWaiter(state *poolState, w *waiter, outcome acquireOutcome) bool {
	if w == nil || w.finished {
		return false
	}
	w.finished = true
	if w.queued {
		removeWaiter(state, w)
		w.queued = false
	}
	close(w.done)
	w.reply <- outcome
	return true
}

func removeWaiter(state *poolState, target *waiter) bool {
	for i, w := range state.waiters {
		if w == target {
			state.waiters = append(state.waiters[:i], state.waiters[i+1:]...)
			return true
		}
	}
	return false
}

func (p *Pool) handleAcquire(state *poolState, cmd acquireCmd) {
	now := time.Now()
	p.cleanupExpiredIdle(state, now)

	// Existing waiters always win over newly arriving requests.
	if len(state.waiters) > 0 && len(state.idle) > 0 {
		mc := popIdle(state)
		if !p.dispatchIdleToHead(state, mc, now) {
			state.idle = append([]*managedConn{mc}, state.idle...)
		}
	}

	if len(state.waiters) == 0 && len(state.idle) > 0 {
		mc := popIdle(state)
		w := p.newWaiter(state, cmd.ctx, cmd.opts, cmd.reply)
		if err := p.leaseConnection(state, mc, w, now, false); err != nil {
			state.idle = append([]*managedConn{mc}, state.idle...)
			p.finishWaiter(state, w, acquireOutcome{err: err})
		}
		p.publish(state)
		p.ensureDials(state)
		return
	}

	if len(state.waiters) == 0 && p.total(state) < state.cfg.MaxOpen {
		w := p.newWaiter(state, cmd.ctx, cmd.opts, cmd.reply)
		p.startDial(state, w, cmd.opts.ForceDialFailure)
		p.publish(state)
		return
	}

	if len(state.waiters) >= state.cfg.MaxWaiters {
		state.metrics.QueueOverflows++
		ctx := map[string]any{
			"waiting":    len(state.waiters),
			"capacity":   state.cfg.MaxWaiters,
			"active":     len(state.inUse),
			"idle":       len(state.idle),
			"max_open":   state.cfg.MaxOpen,
			"request_id": cmd.opts.RequestID,
		}
		p.event(state, "queue_overflow", "", "", ctx)
		p.publish(state)
		cmd.reply <- acquireOutcome{err: domain.NewError(domain.CodePoolExhausted,
			"waiting queue is full", ctx)}
		return
	}

	w := p.newWaiter(state, cmd.ctx, cmd.opts, cmd.reply)
	w.queued = true
	state.waiters = append(state.waiters, w)
	p.event(state, "acquire_wait", "", w.id, map[string]any{
		"position":   len(state.waiters),
		"request_id": w.requestID,
	})
	p.publish(state)
	p.ensureDials(state)
}

func (p *Pool) dispatchIdleToHead(state *poolState, mc *managedConn, now time.Time) bool {
	for len(state.waiters) > 0 {
		head := state.waiters[0]
		if head.finished {
			removeWaiter(state, head)
			continue
		}
		if now.After(head.deadline) {
			p.expireQueuedWaiter(state, head, now, "acquire_deadline")
			continue
		}
		if err := p.leaseConnection(state, mc, head, now, true); err != nil {
			p.finishWaiter(state, head, acquireOutcome{err: err})
			return false
		}
		return true
	}
	return false
}

func (p *Pool) expireQueuedWaiter(state *poolState, w *waiter, now time.Time, reason string) {
	if w.finished {
		return
	}
	removeWaiter(state, w)
	w.queued = false
	state.metrics.AcquireTimeouts++
	ctx := map[string]any{
		"waiter_id":  w.id,
		"request_id": w.requestID,
		"reason":     reason,
		"wait_ms":    now.Sub(w.arrived).Milliseconds(),
	}
	p.event(state, "acquire_timeout", "", w.id, ctx)
	p.finishWaiter(state, w, acquireOutcome{err: domain.NewError(domain.CodeAcquireTimeout,
		"acquire timed out while waiting", ctx)})
}

func (p *Pool) handleWaiterExpired(state *poolState, cmd waiterExpiredCmd) {
	w := cmd.waiter
	if w == nil || w.finished {
		return
	}
	now := time.Now()
	if w.queued {
		p.expireQueuedWaiter(state, w, now, cmd.reason)
	} else {
		state.metrics.AcquireTimeouts++
		ctx := map[string]any{
			"waiter_id":  w.id,
			"request_id": w.requestID,
			"reason":     cmd.reason,
			"wait_ms":    now.Sub(w.arrived).Milliseconds(),
		}
		p.event(state, "acquire_timeout", "", w.id, ctx)
		p.finishWaiter(state, w, acquireOutcome{err: domain.NewError(domain.CodeAcquireTimeout,
			"acquire timed out before a connection became available", ctx)})
	}
	p.publish(state)
	p.ensureDials(state)
}

func (p *Pool) leaseConnection(state *poolState, mc *managedConn, w *waiter, now time.Time, wake bool) error {
	if mc == nil {
		return domain.NewError(domain.CodeInternal, "lease target is missing", nil)
	}
	ctx, cancel := p.persistContext()
	defer cancel()
	intentID, rec, err := p.store.PrepareAcquire(ctx, mc.rec.ConnectionID, w.requestID)
	if err != nil {
		return domain.WrapError(domain.CodeInternal, "failed to persist acquisition intent", map[string]any{
			"connection_id": mc.rec.ConnectionID,
			"request_id":    w.requestID,
		}, err)
	}
	if err := p.store.MarkIntentCommitted(ctx, intentID); err != nil {
		rollbackIntent, rollbackErr := p.store.PrepareRelease(ctx, mc.rec.ConnectionID, w.requestID,
			domain.StateAvailable, "acquire_commit_failed")
		if rollbackErr == nil {
			_ = p.store.MarkIntentCommitted(ctx, rollbackIntent)
		}
		return domain.WrapError(domain.CodeInternal, "failed to commit acquisition intent", map[string]any{
			"connection_id": mc.rec.ConnectionID,
			"intent_id":     intentID,
		}, err)
	}
	mc.rec = rec
	mc.idleSince = time.Time{}
	state.inUse[rec.ConnectionID] = mc
	cp := rec
	state.known[rec.ConnectionID] = &cp
	state.metrics.AcquireSuccesses++
	eventType := "acquire_success"
	detail := map[string]any{
		"use_count":  rec.UseCount,
		"request_id": w.requestID,
		"wait_ms":    now.Sub(w.arrived).Milliseconds(),
	}
	if wake {
		eventType = "acquire_wake"
		state.metrics.ReleaseHandoffs++
		detail["woken_from_fifo"] = true
	}
	p.event(state, eventType, rec.ConnectionID, w.id, detail)
	p.finishWaiter(state, w, acquireOutcome{lease: domain.Lease{
		ConnectionID:   rec.ConnectionID,
		CreatedAt:      rec.CreatedAt,
		LastUsedAt:     rec.LastUsedAt,
		AcquiredAt:     rec.LastUsedAt,
		UseCount:       rec.UseCount,
		WaitDurationMS: maxInt64(0, now.Sub(w.arrived).Milliseconds()),
		WaiterID:       w.id,
	}})
	return nil
}

func (p *Pool) handleRelease(state *poolState, cmd releaseCmd) {
	now := time.Now()
	mc, ok := state.inUse[cmd.connID]
	if !ok {
		if known, exists := state.known[cmd.connID]; exists {
			if known.State == domain.StateInvalid || known.State == domain.StateClosed {
				cmd.reply <- releaseOutcome{result: domain.ReleaseResult{
					ConnectionID: cmd.connID,
					State:        known.State,
					Disposition:  dispositionForState(known.State),
					CreatedAt:    known.CreatedAt,
					ReleasedAt:   now,
					UseCount:     known.UseCount,
					CloseReason:  known.CloseReason,
				}}
				return
			}
		}
		cmd.reply <- releaseOutcome{err: domain.NewError(domain.CodeConnNotLeased,
			"connection is not currently leased", map[string]any{"connection_id": cmd.connID})}
		return
	}

	prior := mc.rec
	expiryReason := p.releaseExpired(state, mc, now)
	targetState := domain.StateAvailable
	if expiryReason != "" {
		targetState = domain.StateClosed
	}
	ctx, cancel := p.persistContext()
	defer cancel()
	intentID, err := p.store.PrepareRelease(ctx, cmd.connID, cmd.requestID, targetState, expiryReason)
	if err != nil {
		cmd.reply <- releaseOutcome{err: domain.WrapError(domain.CodeInternal,
			"failed to persist release intent", map[string]any{"connection_id": cmd.connID}, err)}
		return
	}
	if err := p.store.MarkIntentCommitted(ctx, intentID); err != nil {
		cmd.reply <- releaseOutcome{err: domain.WrapError(domain.CodeInternal,
			"failed to commit release intent", map[string]any{"connection_id": cmd.connID}, err)}
		return
	}
	delete(state.inUse, cmd.connID)

	result := domain.ReleaseResult{
		ConnectionID: cmd.connID,
		State:        targetState,
		CreatedAt:    prior.CreatedAt,
		ReleasedAt:   now,
		UseCount:     prior.UseCount,
	}
	if targetState == domain.StateClosed {
		p.closeManagedRaw(mc, expiryReason)
		mc.rec.State = domain.StateClosed
		mc.rec.CloseReason = expiryReason
		mc.rec.LastUsedAt = now
		cp := mc.rec
		state.known[cmd.connID] = &cp
		state.metrics.ExpiredClosed++
		result.Disposition = domain.DispositionClosed
		result.CloseReason = expiryReason
		p.event(state, "release_closed", cmd.connID, "", map[string]any{"reason": expiryReason})
		p.publish(state)
		p.ensureDials(state)
		cmd.reply <- releaseOutcome{result: result}
		return
	}

	mc.rec.State = domain.StateAvailable
	mc.rec.LastUsedAt = now
	mc.rec.CloseReason = ""
	handedOff := false
	if len(state.waiters) > 0 {
		headID := state.waiters[0].id
		if p.dispatchIdleToHead(state, mc, now) {
			handedOff = true
			result.HandoffTo = headID
		}
	}
	if handedOff {
		result.State = domain.StateAvailable
		result.Disposition = domain.DispositionHandoff
		p.event(state, "release_handoff", cmd.connID, result.HandoffTo, map[string]any{
			"next_owner_waiter": result.HandoffTo,
		})
	} else {
		mc.idleSince = now
		state.idle = append(state.idle, mc)
		cp := mc.rec
		state.known[cmd.connID] = &cp
		result.Disposition = domain.DispositionIdle
		p.event(state, "release_idle", cmd.connID, "", map[string]any{"idle": len(state.idle) + 1})
		p.trimIdle(state, now)
	}
	p.publish(state)
	p.ensureDials(state)
	cmd.reply <- releaseOutcome{result: result}
}

func (state *poolState) waitersSafeHead() string {
	if len(state.waiters) == 0 {
		return ""
	}
	return state.waiters[0].id
}

func dispositionForState(state string) string {
	switch state {
	case domain.StateClosed:
		return domain.DispositionClosed
	case domain.StateInvalid:
		return domain.DispositionInvalid
	default:
		return state
	}
}

func (p *Pool) cleanupExpiredIdle(state *poolState, now time.Time) bool {
	changed := false
	for i := 0; i < len(state.idle); {
		mc := state.idle[i]
		reason := p.idleExpired(state, mc, now)
		if reason == "" {
			i++
			continue
		}
		state.idle = append(state.idle[:i], state.idle[i+1:]...)
		p.closeIdleConnection(state, mc, reason)
		changed = true
	}
	if changed {
		p.publish(state)
	}
	return changed
}

func (p *Pool) closeIdleConnection(state *poolState, mc *managedConn, reason string) {
	ctx, cancel := p.persistContext()
	defer cancel()
	intentID, err := p.store.PrepareRelease(ctx, mc.rec.ConnectionID, "", domain.StateClosed, reason)
	if err == nil {
		_ = p.store.MarkIntentCommitted(ctx, intentID)
	}
	p.closeManagedRaw(mc, reason)
	mc.rec.State = domain.StateClosed
	mc.rec.CloseReason = reason
	mc.rec.LastUsedAt = time.Now()
	cp := mc.rec
	state.known[mc.rec.ConnectionID] = &cp
	state.metrics.ExpiredClosed++
	if reason == "max_idle_exceeded" {
		state.metrics.IdleClosed++
	}
	p.event(state, "idle_connection_closed", mc.rec.ConnectionID, "", map[string]any{"reason": reason})
}

func (p *Pool) trimIdle(state *poolState, now time.Time) {
	for len(state.idle) > state.cfg.MaxIdle {
		mc := state.idle[0]
		state.idle = state.idle[1:]
		p.closeIdleConnection(state, mc, "max_idle_exceeded")
	}
}

func popIdle(state *poolState) *managedConn {
	if len(state.idle) == 0 {
		return nil
	}
	mc := state.idle[0]
	state.idle = state.idle[1:]
	return mc
}

func (p *Pool) closeManagedRaw(mc *managedConn, reason string) {
	if mc == nil {
		return
	}
	if mc.closed.CompareAndSwap(false, true) {
		if err := mc.raw.Close(); err != nil {
			_ = err
		}
	}
}

func (p *Pool) startDial(state *poolState, target *waiter, forceFail bool) *dialCall {
	call := &dialCall{
		id:        fmt.Sprintf("dial-%d-%d", time.Now().UnixNano(), len(state.dials)+1),
		waiter:    target,
		forceFail: forceFail,
		started:   time.Now(),
	}
	if len(state.replace) > 0 {
		candidate := state.replace[0]
		state.replace = state.replace[1:]
		call.replacement = &candidate
	}
	state.dials[call.id] = call
	p.event(state, "dial_started", "", target.id, map[string]any{
		"force_failure": forceFail,
		"replacement_of": func() string {
			if call.replacement == nil {
				return ""
			}
			return call.replacement.oldID
		}(),
	})
	dialCtx, cancel := context.WithTimeout(context.Background(), maxDuration(state.cfg.AcquireTimeout(), 3*time.Second))
	go func() {
		defer cancel()
		if forceFail {
			_ = p.post(dialResultCmd{call: call, err: dbconn.ErrInjectedDial})
			return
		}
		raw, err := p.opener.Open(dialCtx)
		if err != nil {
			_ = p.post(dialResultCmd{call: call, err: err})
			return
		}
		if !p.post(dialResultCmd{call: call, conn: &managedConn{raw: raw}}) {
			_ = raw.Close()
		}
	}()
	return call
}

func (p *Pool) ensureDials(state *poolState) {
	for len(state.waiters) > 0 && len(state.dials) == 0 && p.total(state) < state.cfg.MaxOpen {
		head := state.waiters[0]
		if head.finished {
			removeWaiter(state, head)
			continue
		}
		if time.Now().After(head.deadline) {
			p.expireQueuedWaiter(state, head, time.Now(), "acquire_deadline")
			continue
		}
		p.startDial(state, head, false)
		p.publish(state)
		return
	}
}

func (p *Pool) handleDialResult(state *poolState, cmd dialResultCmd) {
	call := cmd.call
	if call == nil {
		return
	}
	delete(state.dials, call.id)
	now := time.Now()
	if cmd.err != nil {
		state.metrics.DialFailures++
		if call.replacement != nil {
			state.replace = append([]replacementCandidate{*call.replacement}, state.replace...)
		}
		if call.waiter != nil && !call.waiter.finished {
			p.finishWaiter(state, call.waiter, acquireOutcome{err: domain.WrapError(
				domain.CodeDialFailed, "connection establishment failed", map[string]any{
					"waiter_id":  call.waiter.id,
					"request_id": call.waiter.requestID,
					"dial_id":    call.id,
					"error":      cmd.err.Error(),
				}, cmd.err)})
		}
		p.event(state, "dial_failed", "", waiterID(call.waiter), map[string]any{"dial_id": call.id})
		p.publish(state)
		p.ensureDials(state)
		return
	}
	if cmd.conn == nil || cmd.conn.raw == nil {
		p.event(state, "dial_failed", "", waiterID(call.waiter), map[string]any{"dial_id": call.id, "reason": "nil connection"})
		p.publish(state)
		p.ensureDials(state)
		return
	}

	connID := p.nextConnID()
	rec := domain.ConnectionRecord{
		ConnectionID: connID,
		State:        domain.StateAvailable,
		CreatedAt:    now,
		LastUsedAt:   now,
		UseCount:     0,
	}
	if call.replacement != nil {
		rec.ReplacedConnID = call.replacement.oldID
		rec.ReplacementReason = call.replacement.reason
	}
	ctx, cancel := p.persistContext()
	insertErr := p.store.InsertConnection(ctx, rec)
	var replacementErr error
	if insertErr == nil && call.replacement != nil {
		replacementErr = p.store.MarkReplacement(ctx, call.replacement.oldID, connID, call.replacement.reason)
	}
	cancel()
	if insertErr != nil {
		p.closeManagedRaw(cmd.conn, "dial_persist_failed")
		if call.replacement != nil {
			state.replace = append([]replacementCandidate{*call.replacement}, state.replace...)
		}
		if call.waiter != nil && !call.waiter.finished {
			p.finishWaiter(state, call.waiter, acquireOutcome{err: domain.WrapError(
				domain.CodeInternal, "failed to persist newly established connection", map[string]any{"dial_id": call.id}, insertErr)})
		}
		p.publish(state)
		p.ensureDials(state)
		return
	}
	if call.replacement != nil && replacementErr == nil {
		if old, ok := state.known[call.replacement.oldID]; ok {
			old.ReplacedBy = connID
			old.ReplacementReason = call.replacement.reason
		}
	} else if replacementErr != nil {
		p.event(state, "replacement_link_failed", call.replacement.oldID, "", map[string]any{
			"new_connection_id": connID,
			"error":             replacementErr.Error(),
		})
	}
	mc := cmd.conn
	mc.rec = rec
	mc.idleSince = now
	cp := rec
	state.known[connID] = &cp
	p.event(state, "dial_succeeded", connID, waiterID(call.waiter), map[string]any{
		"dial_id":            call.id,
		"replacement_reason": rec.ReplacementReason,
	})
	if call.replacement != nil {
		p.event(state, "connection_replaced", call.replacement.oldID, "", map[string]any{
			"new_connection_id": connID,
			"reason":            call.replacement.reason,
		})
	}

	leased := false
	if call.waiter != nil && !call.waiter.finished {
		if now.After(call.waiter.deadline) {
			if call.waiter.queued {
				p.expireQueuedWaiter(state, call.waiter, now, "acquire_deadline")
			} else {
				state.metrics.AcquireTimeouts++
				p.event(state, "acquire_timeout", "", call.waiter.id, map[string]any{"request_id": call.waiter.requestID})
				p.finishWaiter(state, call.waiter, acquireOutcome{err: domain.NewError(domain.CodeAcquireTimeout,
					"acquire deadline elapsed while dialing", map[string]any{"waiter_id": call.waiter.id})})
			}
		} else {
			if err := p.leaseConnection(state, mc, call.waiter, now, call.waiter.queued); err != nil {
				p.finishWaiter(state, call.waiter, acquireOutcome{err: err})
			} else {
				leased = true
			}
		}
	}
	if !leased && len(state.waiters) > 0 {
		head := state.waiters[0]
		if !head.finished {
			if now.After(head.deadline) {
				p.expireQueuedWaiter(state, head, now, "acquire_deadline")
			} else if err := p.leaseConnection(state, mc, head, now, true); err == nil {
				leased = true
			} else {
				p.finishWaiter(state, head, acquireOutcome{err: err})
			}
		}
	}
	if !leased {
		mc.idleSince = now
		state.idle = append(state.idle, mc)
		p.trimIdle(state, now)
	}
	p.publish(state)
	p.ensureDials(state)
}

func waiterID(w *waiter) string {
	if w == nil {
		return ""
	}
	return w.id
}

func (p *Pool) handleGetHandle(state *poolState, cmd getHandleCmd) {
	mc, ok := state.inUse[cmd.connID]
	if !ok || mc.closed.Load() {
		cmd.reply <- handleOutcome{err: domain.NewError(domain.CodeConnReset,
			"connection is not available to the caller", map[string]any{"connection_id": cmd.connID})}
		return
	}
	cmd.reply <- handleOutcome{conn: mc, rec: mc.rec}
}

func (p *Pool) handleTouch(state *poolState, cmd touchCmd) {
	mc, ok := state.inUse[cmd.connID]
	if !ok || mc.closed.Load() {
		cmd.reply <- domain.NewError(domain.CodeConnReset, "connection is no longer leased",
			map[string]any{"connection_id": cmd.connID})
		return
	}
	now := time.Now()
	ctx, cancel := p.persistContext()
	err := p.store.TouchConnection(ctx, cmd.connID, now)
	cancel()
	if err != nil {
		cmd.reply <- err
		return
	}
	mc.rec.LastUsedAt = now
	cp := mc.rec
	state.known[cmd.connID] = &cp
	p.publish(state)
	cmd.reply <- nil
}

func (p *Pool) handleForceClose(state *poolState, cmd forceCloseCmd) {
	now := time.Now()
	mc, inUse := state.inUse[cmd.connID]
	if !inUse {
		for i, candidate := range state.idle {
			if candidate.rec.ConnectionID == cmd.connID {
				mc = candidate
				state.idle = append(state.idle[:i], state.idle[i+1:]...)
				break
			}
		}
	}
	if mc == nil {
		if known, ok := state.known[cmd.connID]; ok && known.State == domain.StateInvalid {
			cmd.reply <- forceCloseOutcome{result: ForceCloseResult{
				ConnectionID: cmd.connID,
				Reason:       known.CloseReason,
				ClosedAt:     now,
				WasInUse:     false,
			}}
			return
		}
		cmd.reply <- forceCloseOutcome{err: domain.NewError(domain.CodeConnReset,
			"connection not found for forced close", map[string]any{"connection_id": cmd.connID})}
		return
	}
	ctx, cancel := p.persistContext()
	intentID, err := p.store.PrepareInvalidate(ctx, cmd.connID, "", cmd.reason)
	if err == nil {
		_ = p.store.MarkIntentCommitted(ctx, intentID)
	}
	cancel()
	if inUse {
		delete(state.inUse, cmd.connID)
	}
	p.closeManagedRaw(mc, cmd.reason)
	mc.rec.State = domain.StateInvalid
	mc.rec.CloseReason = cmd.reason
	mc.rec.LastUsedAt = now
	cp := mc.rec
	state.known[cmd.connID] = &cp
	state.replace = append(state.replace, replacementCandidate{oldID: cmd.connID, reason: cmd.reason})
	state.metrics.ConnResets++
	p.event(state, "force_close", cmd.connID, "", map[string]any{
		"reason":     cmd.reason,
		"was_in_use": inUse,
	})
	p.publish(state)
	p.ensureDials(state)
	cmd.reply <- forceCloseOutcome{result: ForceCloseResult{
		ConnectionID: cmd.connID,
		Reason:       cmd.reason,
		ClosedAt:     now,
		WasInUse:     inUse,
	}}
}

func (p *Pool) handleReconfigure(state *poolState, cmd reconfigureCmd) {
	if cmd.cfg.Version <= state.cfg.Version {
		cmd.reply <- reconfigureOutcome{err: domain.NewError(domain.CodeStaleConfig,
			"configuration version must increase", map[string]any{
				"current_version":   state.cfg.Version,
				"requested_version": cmd.cfg.Version,
			})}
		return
	}
	ctx, cancel := p.persistContext()
	intentID, err := p.store.BeginConfigIntent(ctx, "", cmd.cfg, state.cfg.Version)
	if err != nil {
		cancel()
		cmd.reply <- reconfigureOutcome{err: domain.WrapError(domain.CodeInternal,
			"failed to persist config intent", map[string]any{"requested_version": cmd.cfg.Version}, err)}
		return
	}
	applied, err := p.store.CommitConfig(ctx, intentID, cmd.cfg, state.cfg.Version)
	cancel()
	if err != nil {
		cmd.reply <- reconfigureOutcome{err: domain.WrapError(domain.CodeInternal,
			"failed to commit config change", map[string]any{"intent_id": intentID}, err)}
		return
	}
	if !applied {
		cmd.reply <- reconfigureOutcome{err: domain.NewError(domain.CodeStaleConfig,
			"configuration version is stale", map[string]any{
				"current_version":   state.cfg.Version,
				"requested_version": cmd.cfg.Version,
			})}
		return
	}
	old := state.cfg
	state.cfg = cmd.cfg
	now := time.Now()
	p.trimIdle(state, now)
	for p.total(state) > state.cfg.MaxOpen && len(state.idle) > 0 {
		mc := popIdle(state)
		p.closeIdleConnection(state, mc, "max_open_reduced")
	}
	p.event(state, "config_change", "", "", map[string]any{
		"old_version": old.Version,
		"new_version": state.cfg.Version,
	})
	p.publish(state)
	p.ensureDials(state)
	cmd.reply <- reconfigureOutcome{cfg: state.cfg}
}

func (p *Pool) handleShutdown(state *poolState) {
	now := time.Now()
	for len(state.waiters) > 0 {
		w := state.waiters[0]
		removeWaiter(state, w)
		w.queued = false
		p.finishWaiter(state, w, acquireOutcome{err: domain.NewError(domain.CodeShutdown,
			"pool is shutting down", nil)})
	}
	ctx, cancel := p.persistContext()
	for _, mc := range state.idle {
		intentID, err := p.store.PrepareRelease(ctx, mc.rec.ConnectionID, "", domain.StateClosed, "pool_shutdown")
		if err == nil {
			_ = p.store.MarkIntentCommitted(ctx, intentID)
		}
		p.closeManagedRaw(mc, "pool_shutdown")
		mc.rec.State = domain.StateClosed
		mc.rec.CloseReason = "pool_shutdown"
		mc.rec.LastUsedAt = now
		cp := mc.rec
		state.known[mc.rec.ConnectionID] = &cp
	}
	for _, mc := range state.inUse {
		intentID, err := p.store.PrepareRelease(ctx, mc.rec.ConnectionID, "", domain.StateClosed, "pool_shutdown_with_lease")
		if err == nil {
			_ = p.store.MarkIntentCommitted(ctx, intentID)
		}
		p.closeManagedRaw(mc, "pool_shutdown_with_lease")
		mc.rec.State = domain.StateClosed
		mc.rec.CloseReason = "pool_shutdown_with_lease"
		mc.rec.LastUsedAt = now
		cp := mc.rec
		state.known[mc.rec.ConnectionID] = &cp
	}
	cancel()
	state.idle = nil
	state.inUse = make(map[string]*managedConn)
	p.event(state, "shutdown", "", "", nil)
	p.publish(state)
	close(p.done)
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
