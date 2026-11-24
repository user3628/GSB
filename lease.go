package blockpool

import (
	"context"
	"database/sql"
	"errors"
	"math"
)

func (p *Pool) Mount(ctx context.Context, req MountRequest) (Lease, *Error) {
	if err := validateNode(req.Namespace, req.Path); err != nil {
		return Lease{}, err
	}
	if err := validateDigest(req.Digest); err != nil {
		return Lease{}, err
	}
	ttl, err := p.leaseTTL(req.LeaseTTLMS)
	if err != nil {
		return Lease{}, err
	}
	startedAt := p.nowMS()
	startEpoch := p.leaseIssuedEpoch.Load()
	if ttl > math.MaxInt64-startedAt {
		return Lease{}, Errorf(CodeSpecInvalid, "lease duration overflows persisted timestamp")
	}
	payload := struct {
		Namespace  string `json:"namespace"`
		Path       string `json:"path"`
		Digest     string `json:"digest"`
		LeaseTTLMS int64  `json:"lease_ms"`
	}{req.Namespace, req.Path, req.Digest, ttl}
	return runMutation(p, ctx, mutationSpec{
		RequestID:    req.RequestID,
		Operation:    "mount",
		AggregateKey: specNodeDigest(req.Namespace, req.Path, req.Digest),
		Payload:      payload,
		LockKeys:     []string{nodeKey(req.Namespace, req.Path), digestKey(req.Digest)},
	}, func(ctx context.Context, tx *sql.Tx, now int64) (Lease, *Error) {
		return p.mountTx(ctx, tx, req, ttl, startEpoch, startedAt, now)
	})
}

func (p *Pool) mountTx(ctx context.Context, tx *sql.Tx, req MountRequest, ttl int64, startEpoch uint64, startedAt, now int64) (Lease, *Error) {
	declared, dbErr := namespaceDeclared(ctx, tx, req.Namespace)
	if dbErr != nil {
		return Lease{}, Errorf(CodeInternal, "check namespace", "cause", dbErr.Error())
	}
	if !declared {
		return Lease{}, Errorf(CodeSpecInvalid, "namespace is not declared", "namespace", req.Namespace)
	}
	if _, dbErr := tx.ExecContext(ctx, `INSERT INTO nodes(namespace, path, created_at_ms)
		VALUES (?, ?, ?) ON CONFLICT(namespace, path) DO NOTHING`, req.Namespace, req.Path, now); dbErr != nil {
		return Lease{}, Errorf(CodeInternal, "auto-register mount node", "cause", dbErr.Error())
	}
	if _, err := getLiveContentTx(ctx, tx, req.Digest); err != nil {
		return Lease{}, err
	}
	if err := expireRefsForDigest(ctx, tx, req.Digest, now); err != nil {
		return Lease{}, Errorf(CodeInternal, "expire old leases", "cause", err.Error())
	}
	existing, err := leaseByRef(ctx, tx, req.Namespace, req.Path, req.Digest)
	if err != nil {
		return Lease{}, err
	}
	if existing != nil {
		concurrent := false
		issueKey := specNodeDigest(req.Namespace, req.Path, req.Digest)
		if issued, ok := p.leaseIssues.Load(issueKey); ok {
			concurrent = issued.(uint64) > startEpoch
		} else {
			// A lease from another Pool instance has no in-process sequence.
			// Its persisted millisecond timestamp is the only available fact.
			concurrent = existing.MountedAtMS >= startedAt
		}
		if concurrent {
			return Lease{}, Errorf(CodeLeaseHeld,
				"another mount issued the lease while this request was in flight",
				"version", existing.Version, "expires_at_ms", existing.ExpiresAtMS,
				"mounted_at_ms", existing.MountedAtMS)
		}
		return *existing, nil
	}
	version, err := nextLeaseVersion(ctx, tx, req.Namespace, req.Path, req.Digest)
	if err != nil {
		return Lease{}, err
	}
	lease, err := insertLease(ctx, tx, req.Namespace, req.Path, req.Digest, version, now, now+ttl)
	if err != nil {
		return Lease{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE blocks SET ref_count=ref_count+1 WHERE digest=?`, req.Digest); err != nil {
		return Lease{}, Errorf(CodeInternal, "increment ref count", "cause", err.Error())
	}
	if err := assertRefCount(ctx, tx, req.Digest); err != nil {
		return Lease{}, err
	}
	p.recordLeaseIssued(req.Namespace, req.Path, req.Digest)
	return lease, nil
}

func (p *Pool) recordLeaseIssued(namespace, path, digest string) {
	epoch := p.leaseIssuedEpoch.Add(1)
	p.leaseIssues.Store(specNodeDigest(namespace, path, digest), epoch)
}

func nextLeaseVersion(ctx context.Context, tx *sql.Tx, namespace, path, digest string) (int64, *Error) {
	var current int64
	err := tx.QueryRowContext(ctx, `SELECT next_version FROM lease_counters
		WHERE namespace=? AND path=? AND digest=?`, namespace, path, digest).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM leases
			WHERE namespace=? AND path=? AND digest=?`, namespace, path, digest).Scan(&current); err != nil {
			return 0, Errorf(CodeInternal, "read max lease version", "cause", err.Error())
		}
		version := current + 1
		if _, err := tx.ExecContext(ctx, `INSERT INTO lease_counters
			(namespace, path, digest, next_version) VALUES (?, ?, ?, ?)`, namespace, path, digest, version); err != nil {
			return 0, Errorf(CodeInternal, "create lease counter", "cause", err.Error())
		}
		return version, nil
	}
	if err != nil {
		return 0, Errorf(CodeInternal, "read lease counter", "cause", err.Error())
	}
	version := current + 1
	if version <= 0 {
		return 0, Errorf(CodeInternal, "lease version overflow", "current_version", current)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE lease_counters SET next_version=? WHERE namespace=? AND path=? AND digest=?`,
		version, namespace, path, digest); err != nil {
		return 0, Errorf(CodeInternal, "advance lease counter", "cause", err.Error())
	}
	return version, nil
}

func insertLease(ctx context.Context, tx *sql.Tx, namespace, path, digest string, version, mountedAt, expiresAt int64) (Lease, *Error) {
	lease := Lease{
		Namespace: namespace, Path: path, Digest: digest, Version: version,
		MountedAtMS: mountedAt, ExpiresAtMS: expiresAt, State: LeaseActive,
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO leases
		(namespace, path, digest, version, mounted_at_ms, expires_at_ms, last_renewed_at_ms, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'active')`, namespace, path, digest, version, mountedAt, expiresAt, mountedAt)
	if err != nil {
		return Lease{}, Errorf(CodeInternal, "insert lease", "cause", err.Error())
	}
	leaseID, err := res.LastInsertId()
	if err != nil {
		return Lease{}, Errorf(CodeInternal, "read lease id", "cause", err.Error())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO refs
		(namespace, path, digest, lease_id, version, mounted_at_ms, expires_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, namespace, path, digest, leaseID, version, mountedAt, expiresAt); err != nil {
		return Lease{}, Errorf(CodeInternal, "insert reference", "cause", err.Error())
	}
	return lease, nil
}

func (p *Pool) Renew(ctx context.Context, req RenewRequest) (Lease, *Error) {
	if err := validateNode(req.Namespace, req.Path); err != nil {
		return Lease{}, err
	}
	if err := validateDigest(req.Digest); err != nil {
		return Lease{}, err
	}
	if req.Version <= 0 {
		return Lease{}, Errorf(CodeSpecInvalid, "version must be a positive integer")
	}
	ttl, err := p.leaseTTL(req.LeaseTTLMS)
	if err != nil {
		return Lease{}, err
	}
	startedAt := p.nowMS()
	if ttl > math.MaxInt64-startedAt {
		return Lease{}, Errorf(CodeSpecInvalid, "lease duration overflows persisted timestamp")
	}
	payload := struct {
		Namespace  string `json:"namespace"`
		Path       string `json:"path"`
		Digest     string `json:"digest"`
		Version    int64  `json:"version"`
		LeaseTTLMS int64  `json:"lease_ms"`
	}{req.Namespace, req.Path, req.Digest, req.Version, ttl}
	return runMutation(p, ctx, mutationSpec{
		RequestID:    req.RequestID,
		Operation:    "renew",
		AggregateKey: specNodeDigest(req.Namespace, req.Path, req.Digest),
		Payload:      payload,
		LockKeys:     []string{nodeKey(req.Namespace, req.Path), digestKey(req.Digest)},
	}, func(ctx context.Context, tx *sql.Tx, now int64) (Lease, *Error) {
		exists, dbErr := nodeExists(ctx, tx, req.Namespace, req.Path)
		if dbErr != nil {
			return Lease{}, Errorf(CodeInternal, "check node", "cause", dbErr.Error())
		}
		if !exists {
			return Lease{}, Errorf(CodeNodeUnknown, "logical node is not registered",
				"namespace", req.Namespace, "path", req.Path)
		}
		if err := expireRefsForDigest(ctx, tx, req.Digest, now); err != nil {
			return Lease{}, Errorf(CodeInternal, "expire old leases", "cause", err.Error())
		}
		active, err := leaseByRef(ctx, tx, req.Namespace, req.Path, req.Digest)
		if err != nil {
			return Lease{}, err
		}
		if active == nil || active.Version != req.Version {
			current, dbErr := latestLeaseVersion(ctx, tx, req.Namespace, req.Path, req.Digest)
			if dbErr != nil {
				return Lease{}, dbErr
			}
			return Lease{}, Errorf(CodeLeaseStale, "lease version is no longer current",
				"requested_version", req.Version, "current_version", current)
		}
		expiresAt := now + ttl
		res, dbErr := tx.ExecContext(ctx, `UPDATE leases SET expires_at_ms=?, last_renewed_at_ms=?
			WHERE namespace=? AND path=? AND digest=? AND version=? AND state='active'`,
			expiresAt, now, req.Namespace, req.Path, req.Digest, req.Version)
		if dbErr != nil {
			return Lease{}, Errorf(CodeInternal, "renew lease", "cause", dbErr.Error())
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return Lease{}, Errorf(CodeLeaseStale, "lease changed during renewal",
				"requested_version", req.Version)
		}
		if _, dbErr := tx.ExecContext(ctx, `UPDATE refs SET expires_at_ms=?
			WHERE namespace=? AND path=? AND digest=? AND version=?`,
			expiresAt, req.Namespace, req.Path, req.Digest, req.Version); dbErr != nil {
			return Lease{}, Errorf(CodeInternal, "renew reference", "cause", dbErr.Error())
		}
		active.ExpiresAtMS = expiresAt
		return *active, nil
	})
}

func latestLeaseVersion(ctx context.Context, tx *sql.Tx, namespace, path, digest string) (int64, *Error) {
	var version int64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM leases
		WHERE namespace=? AND path=? AND digest=?`, namespace, path, digest).Scan(&version)
	if err != nil {
		return 0, Errorf(CodeInternal, "read current lease version", "cause", err.Error())
	}
	return version, nil
}

func (p *Pool) Unmount(ctx context.Context, req UnmountRequest) (UnmountResult, *Error) {
	if err := validateNode(req.Namespace, req.Path); err != nil {
		return UnmountResult{}, err
	}
	if err := validateDigest(req.Digest); err != nil {
		return UnmountResult{}, err
	}
	payload := struct {
		Namespace string `json:"namespace"`
		Path      string `json:"path"`
		Digest    string `json:"digest"`
	}{req.Namespace, req.Path, req.Digest}
	return runMutation(p, ctx, mutationSpec{
		RequestID:    req.RequestID,
		Operation:    "unmount",
		AggregateKey: specNodeDigest(req.Namespace, req.Path, req.Digest),
		Payload:      payload,
		LockKeys:     []string{nodeKey(req.Namespace, req.Path), digestKey(req.Digest)},
	}, func(ctx context.Context, tx *sql.Tx, now int64) (UnmountResult, *Error) {
		exists, dbErr := nodeExists(ctx, tx, req.Namespace, req.Path)
		if dbErr != nil {
			return UnmountResult{}, Errorf(CodeInternal, "check node", "cause", dbErr.Error())
		}
		if !exists {
			return UnmountResult{}, Errorf(CodeNodeUnknown, "logical node is not registered",
				"namespace", req.Namespace, "path", req.Path)
		}
		before, err := activeRefVersion(ctx, tx, req.Namespace, req.Path, req.Digest)
		if err != nil {
			return UnmountResult{}, err
		}
		if err := expireRefsForDigest(ctx, tx, req.Digest, now); err != nil {
			return UnmountResult{}, Errorf(CodeInternal, "expire old leases", "cause", err.Error())
		}
		after, err := activeRefVersion(ctx, tx, req.Namespace, req.Path, req.Digest)
		if err != nil {
			return UnmountResult{}, err
		}
		result := UnmountResult{Namespace: req.Namespace, Path: req.Path, Digest: req.Digest}
		if before > 0 && after == 0 {
			// A lease existed at request time and was removed by expiry in
			// this same transaction; report it as expired rather than manually
			// unmounted.
			if err := setLeaseState(ctx, tx, req.Namespace, req.Path, req.Digest, LeaseExpired); err != nil {
				return UnmountResult{}, err
			}
			result.Expired = true
		} else if after > 0 {
			lease, err := leaseByRef(ctx, tx, req.Namespace, req.Path, req.Digest)
			if err != nil {
				return UnmountResult{}, err
			}
			if lease == nil {
				return UnmountResult{}, Errorf(CodeInternal, "active ref has no active lease")
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM refs WHERE namespace=? AND path=? AND digest=?`,
				req.Namespace, req.Path, req.Digest); err != nil {
				return UnmountResult{}, Errorf(CodeInternal, "delete reference", "cause", err.Error())
			}
			if _, err := tx.ExecContext(ctx, `UPDATE blocks SET ref_count=ref_count-1 WHERE digest=? AND ref_count > 0`,
				req.Digest); err != nil {
				return UnmountResult{}, Errorf(CodeInternal, "decrement ref count", "cause", err.Error())
			}
			if err := setLeaseState(ctx, tx, req.Namespace, req.Path, req.Digest, LeaseUnmounted); err != nil {
				return UnmountResult{}, err
			}
			result.Removed = true
		}
		if err := assertRefCount(ctx, tx, req.Digest); err != nil {
			return UnmountResult{}, err
		}
		if err := tx.QueryRowContext(ctx, `SELECT ref_count FROM blocks WHERE digest=?`, req.Digest).Scan(&result.RefCount); err != nil {
			return UnmountResult{}, Errorf(CodeInternal, "read ref count after unmount", "cause", err.Error())
		}
		return result, nil
	})
}

func activeRefVersion(ctx context.Context, tx *sql.Tx, namespace, path, digest string) (int64, *Error) {
	var version int64
	err := tx.QueryRowContext(ctx, `SELECT version FROM refs WHERE namespace=? AND path=? AND digest=?`,
		namespace, path, digest).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, Errorf(CodeInternal, "read active reference", "cause", err.Error())
	}
	return version, nil
}

func setLeaseState(ctx context.Context, tx *sql.Tx, namespace, path, digest, state string) *Error {
	if _, err := tx.ExecContext(ctx, `UPDATE leases SET state=?
		WHERE namespace=? AND path=? AND digest=? AND state='active'`, state, namespace, path, digest); err != nil {
		return Errorf(CodeInternal, "update lease state", "cause", err.Error())
	}
	return nil
}

func specNodeDigest(namespace, path, digest string) string {
	return namespace + "\x00" + path + "\x00" + digest
}
