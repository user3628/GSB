package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	CoordinatorCommit   = "commit"
	CoordinatorRollback = "rollback"
	CoordinatorRefuse   = "refuse"
	CoordinatorHang     = "hang"
	CoordinatorConflict = "conflict"
)

func validateCoordinatorMode(mode string) error {
	switch mode {
	case "", CoordinatorCommit, CoordinatorRollback, CoordinatorRefuse, CoordinatorHang, CoordinatorConflict:
		return nil
	default:
		return newErrDetails(CodeInvalidArgument, "unsupported coordinator injection mode",
			map[string]any{"mode": mode})
	}
}

type coordinatorDecision struct {
	TxnID    string `json:"txn_id"`
	Decision string `json:"decision"`
	Mode     string `json:"mode"`
	QueryKey string `json:"query_key,omitempty"`
}

func (s *Service) queryCoordinator(ctx context.Context, txnID, mode, injectionID string,
	timeout time.Duration) (coordinatorDecision, error) {
	if timeout <= 0 {
		timeout = 150 * time.Millisecond
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	u, err := url.Parse(s.coordinatorURL + "/test/txn/" + url.PathEscape(txnID))
	if err != nil {
		return coordinatorDecision{}, newErr(CodeCoordInvalid, "invalid coordinator URL")
	}
	q := u.Query()
	if mode != "" {
		q.Set("mode", mode)
	}
	if injectionID != "" {
		q.Set("injection_id", injectionID)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(queryCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return coordinatorDecision{}, newErr(CodeCoordInvalid, err.Error())
	}
	req.Close = true
	req.Header.Set("Accept", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		if errors.Is(queryCtx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return coordinatorDecision{}, newErrDetails(CodeTxnTimeout,
				"transaction coordinator query timed out; connection was closed",
				map[string]any{"timeout_ms": timeout.Milliseconds(), "txn_id": txnID})
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return coordinatorDecision{}, newErrDetails(CodeTxnTimeout,
				"transaction coordinator query timed out; connection was closed",
				map[string]any{"timeout_ms": timeout.Milliseconds(), "txn_id": txnID})
		}
		return coordinatorDecision{}, newErrDetails(CodeCoordRefused,
			"transaction coordinator connection was refused or closed",
			map[string]any{"txn_id": txnID, "transport_error": err.Error()})
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return coordinatorDecision{}, newErrDetails(CodeCoordInvalid,
			"could not read coordinator response", map[string]any{"error": readErr.Error()})
	}
	if resp.StatusCode != http.StatusOK {
		return coordinatorDecision{}, newErrDetails(CodeCoordInvalid,
			"coordinator returned a non-200 response",
			map[string]any{"status": resp.StatusCode, "body": string(body)})
	}
	var out coordinatorDecision
	if err := json.Unmarshal(body, &out); err != nil {
		return coordinatorDecision{}, newErrDetails(CodeCoordInvalid,
			"coordinator returned invalid JSON", map[string]any{"body": string(body)})
	}
	out.Decision = strings.ToUpper(strings.TrimSpace(out.Decision))
	if out.Decision != TxnDecisionCommit && out.Decision != TxnDecisionRollback {
		return coordinatorDecision{}, newErrDetails(CodeTxnUnknown,
			"coordinator returned an unknown conclusion",
			map[string]any{"decision": out.Decision})
	}
	return out, nil
}

func (s *Service) ResolveTransaction(ctx context.Context, txnID string, req ResolveRequest) (ResolveResult, error) {
	var zero ResolveResult
	if err := validateCoordinatorMode(req.InjectionMode); err != nil {
		return zero, err
	}
	if req.QueryTimeoutMS <= 0 {
		req.QueryTimeoutMS = 150
	}
	if req.QueryTimeoutMS > 10000 {
		req.QueryTimeoutMS = 10000
	}
	fingerprintPayload := req
	fingerprintPayload.RequestID = ""
	var injection *InjectionRequest
	if req.InjectionMode != "" {
		active := false
		injection = &InjectionRequest{Kind: "txn", Target: txnID,
			Mode: req.InjectionMode, Active: &active}
	}
	decision, err := s.prepareOperation(ctx, req.RequestID, "txn_resolve", txnID,
		fingerprintPayload, injection)
	if err != nil {
		return zero, err
	}
	if decision.InProgress {
		decision, err = s.awaitOperation(ctx, decision.RequestID)
		if err != nil {
			return zero, err
		}
	}
	if decision.StoredErr != nil {
		return zero, decision.StoredErr
	}
	if decision.Response != nil {
		var out ResolveResult
		if err := json.Unmarshal(decision.Response, &out); err != nil {
			return zero, err
		}
		return out, nil
	}

	lock := s.txnLock(txnID)
	lock.Lock()
	defer lock.Unlock()

	txn, err := s.GetTransaction(ctx, txnID)
	if err != nil {
		we := asWorkbenchError(err)
		_ = s.markOperationFailed(ctx, decision.Op, we)
		return zero, we
	}
	if txn.Status != TxnPending && !req.ForceRecheck {
		raw, err := s.commitOperation(ctx, decision.Op, func(tx *sql.Tx) (any, error) {
			current, err := s.loadTxnTx(ctx, tx, txnID)
			if err != nil {
				return nil, err
			}
			msg, err := s.loadMessageTx(ctx, tx, current.MessageID)
			if err != nil {
				return nil, err
			}
			ps, err := s.loadPartitionTx(ctx, tx, current.Topic, current.Partition)
			if err != nil {
				return nil, err
			}
			view, err := s.partitionViewTx(ctx, tx, ps)
			if err != nil {
				return nil, err
			}
			return ResolveResult{Transaction: current, Message: msg, Partition: view,
				Idempotent: true, RequestID: decision.RequestID}, nil
		})
		if err != nil {
			return zero, err
		}
		var out ResolveResult
		if err := json.Unmarshal(raw, &out); err != nil {
			return zero, err
		}
		return out, nil
	}

	coord, coordErr := s.queryCoordinator(ctx, txnID, req.InjectionMode,
		decision.Op.InjectionID, time.Duration(req.QueryTimeoutMS)*time.Millisecond)
	var observation *Observation
	if decision.Op.InjectionID != "" {
		code := ""
		detail := "coordinator_decision=" + coord.Decision
		if coordErr != nil {
			code = asWorkbenchError(coordErr).Code
			detail = coordErr.Error()
		}
		observation, _ = s.recordObservation(ctx, decision.Op.InjectionID, "txn",
			req.InjectionMode, txnID, "client_resolve", code, detail, decision.RequestID)
	}
	if coordErr != nil {
		we := asWorkbenchError(coordErr)
		_ = s.recordCoordinatorFailureAttempt(ctx, txnID, we.Code)
		if failErr := s.markOperationFailed(ctx, decision.Op, we); failErr != nil {
			return zero, failErr
		}
		return zero, we
	}

	raw, err := s.commitOperation(ctx, decision.Op, func(tx *sql.Tx) (any, error) {
		current, err := s.loadTxnTx(ctx, tx, txnID)
		if err != nil {
			return nil, err
		}
		msg, err := s.loadMessageTx(ctx, tx, current.MessageID)
		if err != nil {
			return nil, err
		}
		ps, err := s.loadPartitionTx(ctx, tx, current.Topic, current.Partition)
		if err != nil {
			return nil, err
		}
		now := nowMS(s.now())
		if current.Status != TxnPending {
			existing := TxnDecisionCommit
			if current.Status == TxnRolledBack {
				existing = TxnDecisionRollback
			}
			if coord.Decision != existing {
				_, _ = s.recordObservationLocked(ctx, tx, decision.Op.InjectionID, "txn",
					req.InjectionMode, txnID, "conflict_detected", CodeTxnConflict,
					"persisted="+existing+" coordinator="+coord.Decision, decision.RequestID)
				return nil, newErrDetails(CodeTxnConflict,
					"coordinator returned a conclusion that contradicts persisted state",
					map[string]any{"persisted": existing, "coordinator": coord.Decision})
			}
			view, err := s.partitionViewTx(ctx, tx, ps)
			if err != nil {
				return nil, err
			}
			return ResolveResult{Transaction: current, Message: msg, Partition: view,
				Idempotent: true, RequestID: decision.RequestID}, nil
		}
		if coord.Decision == TxnDecisionCommit {
			if err := s.transitionMessageTx(ctx, tx, current.MessageID,
				MsgPendingInvisible, MsgVisible, "txn_commit"); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE txn_visibility SET status=?, decision=?,
				query_count=query_count+1, coordinator_result=?, queried_at_ms=?, decided_at_ms=?
				WHERE txn_id=? AND status=?`, TxnCommitted, TxnDecisionCommit,
				coord.Decision, now, now, txnID, TxnPending); err != nil {
				return nil, err
			}
			if err := s.recordTransitionTx(ctx, tx, "transaction", txnID,
				TxnPending, TxnCommitted, "coordinator_commit"); err != nil {
				return nil, err
			}
			if err := s.advanceVisibilityFrontierTx(ctx, tx, ps, false); err != nil {
				return nil, err
			}
		} else {
			if err := s.transitionMessageTx(ctx, tx, current.MessageID,
				MsgPendingInvisible, MsgRolledBack, "txn_rollback"); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE txn_visibility SET status=?, decision=?,
				query_count=query_count+1, coordinator_result=?, queried_at_ms=?, decided_at_ms=?
				WHERE txn_id=? AND status=?`, TxnRolledBack, TxnDecisionRollback,
				coord.Decision, now, now, txnID, TxnPending); err != nil {
				return nil, err
			}
			if err := s.recordTransitionTx(ctx, tx, "transaction", txnID,
				TxnPending, TxnRolledBack, "coordinator_rollback"); err != nil {
				return nil, err
			}
			if err := s.advanceVisibilityFrontierTx(ctx, tx, ps, false); err != nil {
				return nil, err
			}
		}
		if err := s.savePartitionTx(ctx, tx, ps); err != nil {
			return nil, err
		}
		current, err = s.loadTxnTx(ctx, tx, txnID)
		if err != nil {
			return nil, err
		}
		msg, err = s.loadMessageTx(ctx, tx, current.MessageID)
		if err != nil {
			return nil, err
		}
		view, err := s.partitionViewTx(ctx, tx, ps)
		if err != nil {
			return nil, err
		}
		return ResolveResult{Transaction: current, Message: msg, Partition: view,
			Observation: observation, RequestID: decision.RequestID}, nil
	})
	if err != nil {
		return zero, err
	}
	var out ResolveResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, err
	}
	return out, nil
}

func (s *Service) recordCoordinatorFailureAttempt(ctx context.Context, txnID, code string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE txn_visibility
		SET query_count=query_count+1, coordinator_result=?, queried_at_ms=?
		WHERE txn_id=? AND status=?`, code, nowMS(s.now()), txnID, TxnPending)
	return err
}

func (s *Service) GetTransaction(ctx context.Context, txnID string) (Transaction, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Transaction{}, err
	}
	defer tx.Rollback()
	return s.loadTxnTx(ctx, tx, txnID)
}

func (s *Service) ensureInjection(ctx context.Context, kind, target, mode string, delayMS int) (string, error) {
	id := newID("inj")
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `INSERT INTO injections
		(id, kind, target, mode, delay_ms, payload, active, created_at_ms)
		VALUES (?,?,?,?,?,?,?,?)`, id, kind, target, mode, delayMS, "", 0, nowMS(s.now()))
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *Service) coordinatorDecisionFor(ctx context.Context, txnID, mode, injectionID string,
	delayMS int) (string, string, error) {
	if mode == "" {
		var injMode string
		var id string
		err := s.db.QueryRowContext(ctx, `SELECT id, mode FROM injections
			WHERE kind='txn' AND target IN (?, '*') AND active=1
			ORDER BY CASE WHEN target=? THEN 0 ELSE 1 END, created_at_ms DESC LIMIT 1`,
			txnID, txnID).Scan(&id, &injMode)
		if err == nil {
			mode = injMode
			injectionID = id
		} else if !errors.Is(err, sql.ErrNoRows) {
			return "", "", err
		}
	}
	if mode == "" {
		mode = CoordinatorCommit
	}
	if injectionID == "" {
		id, err := s.ensureInjection(ctx, "txn", txnID, mode, delayMS)
		if err != nil {
			return "", "", err
		}
		injectionID = id
	}
	switch mode {
	case CoordinatorCommit:
		return TxnDecisionCommit, injectionID, nil
	case CoordinatorRollback:
		return TxnDecisionRollback, injectionID, nil
	case CoordinatorConflict:
		var status, decision string
		err := s.db.QueryRowContext(ctx, `SELECT status, COALESCE(decision,'') FROM txn_visibility
			WHERE txn_id=?`, txnID).Scan(&status, &decision)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", "", err
		}
		if status == TxnCommitted || decision == TxnDecisionCommit {
			return TxnDecisionRollback, injectionID, nil
		}
		return TxnDecisionCommit, injectionID, nil
	case CoordinatorHang:
		if delayMS <= 0 {
			delayMS = 2000
		}
		select {
		case <-ctx.Done():
			return "", injectionID, newErr(CodeTxnTimeout, "coordinator client disconnected")
		case <-time.After(time.Duration(delayMS) * time.Millisecond):
			return TxnDecisionCommit, injectionID, nil
		}
	case CoordinatorRefuse:
		return "", injectionID, newErr(CodeCoordRefused, "injected connection refusal")
	default:
		return "", injectionID, newErr(CodeCoordInvalid, "unknown coordinator mode "+mode)
	}
}

func (s *Service) writeCoordinatorHTTPError(w http.ResponseWriter, err error) {
	we := asWorkbenchError(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(we.HTTP)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": we})
}

// CoordinatorHandler is the local transaction-coordinator test endpoint. The
// production component reaches it over a real TCP HTTP connection so injected
// hangs and refusals exercise connection closing semantics.
func (s *Service) CoordinatorHandler(w http.ResponseWriter, r *http.Request) {
	txnID := r.PathValue("txnID")
	if txnID == "" {
		s.writeCoordinatorHTTPError(w, newErr(CodeInvalidArgument, "txn_id is required"))
		return
	}
	mode := r.URL.Query().Get("mode")
	injectionID := r.URL.Query().Get("injection_id")
	delayMS, _ := strconv.Atoi(r.URL.Query().Get("delay_ms"))
	decision, injectionID, err := s.coordinatorDecisionFor(r.Context(), txnID, mode, injectionID, delayMS)
	if err != nil {
		we := asWorkbenchError(err)
		if mode == CoordinatorRefuse {
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, hijackErr := hj.Hijack()
				if hijackErr == nil {
					_, _ = s.recordObservation(r.Context(), injectionID, "txn", mode, txnID,
						"coordinator_query", we.Code, "connection_closed_without_response", "")
					_ = conn.Close()
					return
				}
			}
		}
		_, _ = s.recordObservation(r.Context(), injectionID, "txn", mode, txnID,
			"coordinator_query", we.Code, err.Error(), "")
		s.writeCoordinatorHTTPError(w, we)
		return
	}
	_, _ = s.recordObservation(r.Context(), injectionID, "txn", mode, txnID,
		"coordinator_query", "", "decision="+decision, "")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(coordinatorDecision{TxnID: txnID, Decision: decision,
		Mode: mode, QueryKey: digestString(txnID + ":" + decision)})
}

func (s *Service) ListInjections(ctx context.Context, kind string) ([]Injection, error) {
	query := `SELECT id, kind, target, mode, delay_ms, payload, active, created_at_ms FROM injections`
	args := []any{}
	if kind != "" {
		query += ` WHERE kind=?`
		args = append(args, kind)
	}
	query += ` ORDER BY created_at_ms DESC, id DESC LIMIT 200`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Injection{}
	for rows.Next() {
		var inj Injection
		var active int
		if err := rows.Scan(&inj.ID, &inj.Kind, &inj.Target, &inj.Mode, &inj.DelayMS,
			&inj.Payload, &active, &inj.CreatedAtMS); err != nil {
			return nil, err
		}
		inj.Active = active != 0
		out = append(out, inj)
	}
	return out, rows.Err()
}

func (s *Service) CreateInjection(ctx context.Context, req InjectionRequest) (Injection, error) {
	if req.Kind != "txn" && req.Kind != "lease" {
		return Injection{}, newErr(CodeInvalidArgument, "injection kind must be txn or lease")
	}
	if req.Target == "" {
		return Injection{}, newErr(CodeInvalidArgument, "injection target is required")
	}
	if req.Kind == "txn" {
		if err := validateCoordinatorMode(req.Mode); err != nil || req.Mode == "" {
			return Injection{}, newErr(CodeInvalidArgument, "invalid txn injection mode")
		}
	} else if err := validateLeaseInjection(req.Mode, true); err != nil {
		return Injection{}, err
	}
	active := true
	if req.Active != nil {
		active = *req.Active
	}
	id := newID("inj")
	now := nowMS(s.now())
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Injection{}, err
	}
	defer tx.Rollback()
	if active {
		if _, err := tx.ExecContext(ctx, `UPDATE injections SET active=0
			WHERE kind=? AND target=? AND active=1`, req.Kind, req.Target); err != nil {
			return Injection{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO injections
		(id, kind, target, mode, delay_ms, payload, active, created_at_ms)
		VALUES (?,?,?,?,?,?,?,?)`, id, req.Kind, req.Target, req.Mode, req.DelayMS,
		req.Payload, boolInt(active), now); err != nil {
		return Injection{}, err
	}
	if err := tx.Commit(); err != nil {
		return Injection{}, err
	}
	return Injection{ID: id, Kind: req.Kind, Target: req.Target, Mode: req.Mode,
		DelayMS: req.DelayMS, Payload: req.Payload, Active: active, CreatedAtMS: now}, nil
}

func (s *Service) ListObservations(ctx context.Context, limit int) ([]Observation, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, injection_id, kind, mode, target, phase,
		observed_code, detail, request_id, observed_at_ms FROM injection_observations
		ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Observation{}
	for rows.Next() {
		var o Observation
		if err := rows.Scan(&o.ID, &o.InjectionID, &o.Kind, &o.Mode, &o.Target, &o.Phase,
			&o.ObservedCode, &o.Detail, &o.RequestID, &o.ObservedAtMS); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func formatCoordinatorError(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%v", err)
}
