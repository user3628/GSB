package blockpool

import (
	"context"
	"database/sql"
	"errors"
	"math"
)

func (p *Pool) Reclaim(ctx context.Context, req ReclaimRequest) (ReclaimResult, *Error) {
	if req.Digest != "" && req.CapacityBytes != nil {
		return ReclaimResult{}, Errorf(CodeSpecInvalid, "specify either digest or capacity_bytes, not both")
	}
	if req.Digest == "" && req.CapacityBytes == nil {
		return ReclaimResult{}, Errorf(CodeSpecInvalid, "digest or capacity_bytes is required")
	}
	if req.Digest != "" {
		if err := validateDigest(req.Digest); err != nil {
			return ReclaimResult{}, err
		}
		payload := struct {
			Digest string `json:"digest"`
		}{req.Digest}
		return runMutation(p, ctx, mutationSpec{
			RequestID:    req.RequestID,
			Operation:    "reclaim",
			AggregateKey: req.Digest,
			Payload:      payload,
			LockKeys:     []string{digestKey(req.Digest)},
		}, func(ctx context.Context, tx *sql.Tx, now int64) (ReclaimResult, *Error) {
			item, err := reclaimOneTx(ctx, tx, req.Digest, now, "manual", req.RequestID)
			if err != nil {
				return ReclaimResult{}, err
			}
			live, err := liveBytes(ctx, tx)
			if err != nil {
				return ReclaimResult{}, err
			}
			return ReclaimResult{
				Reclaimed:      []ReclaimedBlock{item},
				ReleasedBytes:  item.SizeBytes,
				LiveBytesAfter: live,
			}, nil
		})
	}

	if *req.CapacityBytes < 0 {
		return ReclaimResult{}, Errorf(CodeSpecInvalid, "capacity_bytes must be a non-negative integer")
	}
	budget := *req.CapacityBytes
	lockKeys, err := p.liveDigestLockKeys(ctx)
	if err != nil {
		return ReclaimResult{}, err
	}
	payload := struct {
		CapacityBytes int64 `json:"capacity_bytes"`
	}{budget}
	return runMutation(p, ctx, mutationSpec{
		RequestID:    req.RequestID,
		Operation:    "reclaim_capacity",
		AggregateKey: "capacity",
		Payload:      payload,
		LockKeys:     lockKeys,
	}, func(ctx context.Context, tx *sql.Tx, now int64) (ReclaimResult, *Error) {
		return reclaimCapacityTx(ctx, tx, budget, now, req.RequestID)
	})
}

func (p *Pool) liveDigestLockKeys(ctx context.Context) ([]string, *Error) {
	rows, err := p.db.QueryContext(ctx, `SELECT digest FROM blocks WHERE state='live' ORDER BY digest`)
	if err != nil {
		return nil, Errorf(CodeInternal, "list live blocks for reclaim", "cause", err.Error())
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var digest string
		if err := rows.Scan(&digest); err != nil {
			return nil, Errorf(CodeInternal, "scan live block digest", "cause", err.Error())
		}
		keys = append(keys, digestKey(digest))
	}
	if err := rows.Err(); err != nil {
		return nil, Errorf(CodeInternal, "iterate live block digests", "cause", err.Error())
	}
	return keys, nil
}

type reclaimCandidate struct {
	digest      string
	size        int64
	state       string
	storedAt    int64
	retentionMS int64
	refCount    int64
}

func reclaimCapacityTx(ctx context.Context, tx *sql.Tx, budget, now int64, requestID string) (ReclaimResult, *Error) {
	rows, err := tx.QueryContext(ctx, `SELECT digest, size_bytes, state, stored_at_ms, retention_ms, ref_count
		FROM blocks ORDER BY size_bytes DESC, digest ASC`)
	if err != nil {
		return ReclaimResult{}, Errorf(CodeInternal, "read reclaim candidates", "cause", err.Error())
	}
	var candidates []reclaimCandidate
	for rows.Next() {
		var c reclaimCandidate
		if err := rows.Scan(&c.digest, &c.size, &c.state, &c.storedAt, &c.retentionMS, &c.refCount); err != nil {
			rows.Close()
			return ReclaimResult{}, Errorf(CodeInternal, "scan reclaim candidate", "cause", err.Error())
		}
		candidates = append(candidates, c)
	}
	rows.Close()

	var total int64
	for _, c := range candidates {
		if c.state != BlockLive {
			continue
		}
		total += c.size
		// Expiry is materialized while all live digest locks are held.
		if err := expireRefsForDigest(ctx, tx, c.digest, now); err != nil {
			return ReclaimResult{}, Errorf(CodeInternal, "expire refs during reclaim", "digest", c.digest, "cause", err.Error())
		}
		if err := assertRefCount(ctx, tx, c.digest); err != nil {
			return ReclaimResult{}, err
		}
	}
	overflow := total - budget
	if overflow < 0 {
		overflow = 0
	}

	result := ReclaimResult{BudgetBytes: &budget, LiveBytesAfter: total, Reclaimed: make([]ReclaimedBlock, 0)}
	var released int64
	for _, c := range candidates {
		if released >= overflow {
			break
		}
		if c.state != BlockLive {
			continue
		}
		safe, blocked, checkErr := reclaimEligibility(ctx, tx, c.digest, c.state, c.storedAt, c.retentionMS, c.refCount, now)
		if checkErr != nil {
			return ReclaimResult{}, checkErr
		}
		if !safe {
			result.Blocked = append(result.Blocked, blocked)
			continue
		}
		item, reclaimErr := reclaimOneTx(ctx, tx, c.digest, now, "capacity", requestID)
		if reclaimErr != nil {
			return ReclaimResult{}, reclaimErr
		}
		result.Reclaimed = append(result.Reclaimed, item)
		released += item.SizeBytes
	}
	result.ReleasedBytes = released
	result.LiveBytesAfter = total - released
	if result.LiveBytesAfter > budget {
		result.OverBudgetBytes = result.LiveBytesAfter - budget
	} else {
		result.OverBudgetBytes = 0
	}
	if total > budget && released == 0 {
		return ReclaimResult{}, Errorf(CodeReclaimUnsafe,
			"capacity is over budget but no eligible block can be reclaimed",
			"budget_bytes", budget, "live_bytes", total, "over_budget_bytes", total-budget,
			"blocked", result.Blocked)
	}
	return result, nil
}

func reclaimOneTx(ctx context.Context, tx *sql.Tx, digest string, now int64, reason, requestID string) (ReclaimedBlock, *Error) {
	var c reclaimCandidate
	err := tx.QueryRowContext(ctx, `SELECT digest, size_bytes, state, stored_at_ms, retention_ms, ref_count
		FROM blocks WHERE digest=?`, digest).
		Scan(&c.digest, &c.size, &c.state, &c.storedAt, &c.retentionMS, &c.refCount)
	if errors.Is(err, sql.ErrNoRows) {
		return ReclaimedBlock{}, Errorf(CodeBlockNotFound, "block is not present", "digest", digest)
	}
	if err != nil {
		return ReclaimedBlock{}, Errorf(CodeInternal, "read block for reclaim", "cause", err.Error())
	}
	if err := expireRefsForDigest(ctx, tx, digest, now); err != nil {
		return ReclaimedBlock{}, Errorf(CodeInternal, "expire refs before reclaim", "cause", err.Error())
	}
	if err := assertRefCount(ctx, tx, digest); err != nil {
		return ReclaimedBlock{}, err
	}
	// Re-read the count after expiry so every persisted fact is current.
	if err := tx.QueryRowContext(ctx, `SELECT ref_count FROM blocks WHERE digest=?`, digest).Scan(&c.refCount); err != nil {
		return ReclaimedBlock{}, Errorf(CodeInternal, "read ref count for reclaim", "cause", err.Error())
	}
	safe, blocked, eligibilityErr := reclaimEligibility(ctx, tx, digest, c.state, c.storedAt, c.retentionMS, c.refCount, now)
	if eligibilityErr != nil {
		return ReclaimedBlock{}, eligibilityErr
	}
	if !safe {
		return ReclaimedBlock{}, Errorf(CodeReclaimUnsafe, "block is not safe to reclaim",
			"digest", digest, "ref_count", blocked.RefCount,
			"effective_lease_count", blocked.EffectiveLeaseCount,
			"remaining_retention_ms", blocked.RemainingRetentionMS,
			"blockers", blocked.Reasons, "state", c.state)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE blocks SET state='pending_reclaim' WHERE digest=?`, digest); err != nil {
		return ReclaimedBlock{}, Errorf(CodeInternal, "mark pending reclaim", "cause", err.Error())
	}
	if _, err := tx.ExecContext(ctx, `UPDATE blocks SET state='reclaimed', content=NULL WHERE digest=?`, digest); err != nil {
		return ReclaimedBlock{}, Errorf(CodeInternal, "remove block content", "cause", err.Error())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO reclaim_records
		(digest, size_bytes, reclaimed_at_ms, reason, request_id) VALUES (?, ?, ?, ?, ?)`,
		digest, c.size, now, reason, requestID); err != nil {
		return ReclaimedBlock{}, Errorf(CodeInternal, "record reclaim", "cause", err.Error())
	}
	return ReclaimedBlock{Digest: digest, SizeBytes: c.size, ReclaimedAt: now}, nil
}

func reclaimEligibility(ctx context.Context, tx *sql.Tx, digest, state string, storedAt, retentionMS, refCount, now int64) (bool, ReclaimBlocked, *Error) {
	blocked := ReclaimBlocked{Digest: digest, RefCount: refCount}
	if state == BlockReclaimed {
		blocked.Reasons = append(blocked.Reasons, "already_reclaimed")
		return false, blocked, nil
	}
	if state != BlockLive {
		blocked.Reasons = append(blocked.Reasons, "not_live")
		return false, blocked, nil
	}
	if refCount != 0 {
		blocked.Reasons = append(blocked.Reasons, "ref_count_nonzero")
	}
	var activeLeases int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM refs WHERE digest=? AND expires_at_ms > ?`,
		digest, now).Scan(&activeLeases); err != nil {
		return false, blocked, Errorf(CodeInternal, "count active leases for reclaim", "cause", err.Error())
	}
	blocked.EffectiveLeaseCount = activeLeases
	if activeLeases != 0 {
		blocked.Reasons = append(blocked.Reasons, "active_leases")
	}
	end := storedAt + retentionMS
	if retentionMS > math.MaxInt64-storedAt {
		end = math.MaxInt64
	}
	remaining := end - now
	if remaining < 0 {
		remaining = 0
	}
	blocked.RemainingRetentionMS = remaining
	if remaining > 0 {
		blocked.Reasons = append(blocked.Reasons, "retention")
	}
	return len(blocked.Reasons) == 0, blocked, nil
}

func liveBytes(ctx context.Context, tx *sql.Tx) (int64, *Error) {
	var total int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(size_bytes), 0) FROM blocks WHERE state='live'`).Scan(&total); err != nil {
		return 0, Errorf(CodeInternal, "sum live bytes", "cause", err.Error())
	}
	return total, nil
}
