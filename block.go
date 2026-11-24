package blockpool

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
)

func (p *Pool) Ingest(ctx context.Context, req IngestRequest) (Block, *Error) {
	if req.Data == nil {
		req.Data = []byte{}
	}
	if err := validateDigest(req.Digest); err != nil {
		return Block{}, err
	}
	if req.SizeBytes != nil {
		if err := validateNonNegative("size_bytes", *req.SizeBytes); err != nil {
			return Block{}, err
		}
		if *req.SizeBytes != int64(len(req.Data)) {
			return Block{}, Errorf(CodeContentCorrupt, "declared size does not match content bytes",
				"declared_size", *req.SizeBytes, "actual_size", len(req.Data))
		}
	}
	if err := validateNonNegative("retention_ms", req.RetentionMS); err != nil {
		return Block{}, err
	}
	actual := sha256.Sum256(req.Data)
	actualDigest := hex.EncodeToString(actual[:])
	if actualDigest != req.Digest {
		return Block{}, Errorf(CodeContentCorrupt, "content SHA-256 does not match digest",
			"declared_digest", req.Digest, "actual_digest", actualDigest)
	}

	payload := struct {
		Digest      string `json:"digest"`
		Data        []byte `json:"data"`
		SizeBytes   *int64 `json:"size_bytes,omitempty"`
		RetentionMS int64  `json:"retention_ms"`
	}{req.Digest, req.Data, req.SizeBytes, req.RetentionMS}
	return runMutation(p, ctx, mutationSpec{
		RequestID:    req.RequestID,
		Operation:    "ingest",
		AggregateKey: req.Digest,
		Payload:      payload,
		LockKeys:     []string{digestKey(req.Digest)},
	}, func(ctx context.Context, tx *sql.Tx, now int64) (Block, *Error) {
		if err := expireRefsForDigest(ctx, tx, req.Digest, now); err != nil {
			return Block{}, Errorf(CodeInternal, "expire refs before ingest", "cause", err.Error())
		}
		block, err := ingestTx(ctx, tx, req.Digest, req.Data, req.RetentionMS, now)
		if err != nil {
			return Block{}, err
		}
		if err := assertRefCount(ctx, tx, req.Digest); err != nil {
			return Block{}, err
		}
		return block, nil
	})
}

func ingestTx(ctx context.Context, tx *sql.Tx, digest string, data []byte, retentionMS, now int64) (Block, *Error) {
	var state string
	var storedAt, existingRetention, refCount int64
	var storedData []byte
	err := tx.QueryRowContext(ctx, `SELECT state, stored_at_ms, retention_ms, ref_count, content
		FROM blocks WHERE digest=?`, digest).Scan(&state, &storedAt, &existingRetention, &refCount, &storedData)
	if err == nil {
		if state != BlockReclaimed {
			if err := verifyContent(digest, storedData); err != nil {
				return Block{}, err
			}
			return Block{Digest: digest, SizeBytes: int64(len(storedData)), StoredAtMS: storedAt,
				RetentionMS: existingRetention, State: state, RefCount: refCount}, nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE blocks SET size_bytes=?, stored_at_ms=?, retention_ms=?,
			state='live', ref_count=0, content=? WHERE digest=?`, len(data), now, retentionMS, data, digest); err != nil {
			return Block{}, Errorf(CodeInternal, "re-ingest reclaimed block", "cause", err.Error())
		}
		return Block{Digest: digest, SizeBytes: int64(len(data)), StoredAtMS: now,
			RetentionMS: retentionMS, State: BlockLive, RefCount: 0}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Block{}, Errorf(CodeInternal, "read existing block", "cause", err.Error())
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO blocks
		(digest, size_bytes, stored_at_ms, retention_ms, state, ref_count, content)
		VALUES (?, ?, ?, ?, 'live', 0, ?)`, digest, len(data), now, retentionMS, data); err != nil {
		return Block{}, Errorf(CodeInternal, "insert block", "cause", err.Error())
	}
	return Block{Digest: digest, SizeBytes: int64(len(data)), StoredAtMS: now,
		RetentionMS: retentionMS, State: BlockLive, RefCount: 0}, nil
}

func verifyContent(digest string, data []byte) *Error {
	sum := sha256.Sum256(data)
	actual := hex.EncodeToString(sum[:])
	if actual != digest {
		return Errorf(CodeContentCorrupt, "stored content no longer matches digest",
			"digest", digest, "actual_digest", actual)
	}
	return nil
}

func (p *Pool) GetBlock(ctx context.Context, digest string) (BlockDetails, *Error) {
	if err := validateDigest(digest); err != nil {
		return BlockDetails{}, err
	}
	if err := p.expireBlock(ctx, digest, p.nowMS()); err != nil {
		return BlockDetails{}, err
	}
	details, _, err := p.readBlockSnapshot(ctx, digest, false)
	return details, err
}

func (p *Pool) readBlockSnapshot(ctx context.Context, digest string, includeHistory bool) (BlockDetails, []Lease, *Error) {
	tx, err := p.beginRead(ctx)
	if err != nil {
		return BlockDetails{}, nil, Errorf(CodeInternal, "begin block read", "cause", err.Error())
	}
	defer rollback(tx)
	block, err := scanBlock(tx.QueryRowContext(ctx, `SELECT `+blockColumns+` FROM blocks WHERE digest=?`, digest))
	if errors.Is(err, sql.ErrNoRows) {
		return BlockDetails{}, nil, Errorf(CodeBlockNotFound, "block is not present", "digest", digest)
	}
	if err != nil {
		return BlockDetails{}, nil, Errorf(CodeInternal, "read block", "cause", err.Error())
	}
	leases, active, err := activeLeases(ctx, tx, digest)
	if err != nil {
		return BlockDetails{}, nil, Errorf(CodeInternal, "read block leases", "cause", err.Error())
	}
	if int64(len(leases)) != block.RefCount || active != block.RefCount {
		return BlockDetails{}, nil, Errorf(CodeInternal, "reference count invariant violated",
			"digest", digest, "ref_count", block.RefCount, "effective_refs", len(leases))
	}
	details := BlockDetails{Block: block, ActiveLeaseCount: active, Leases: leases}
	if !includeHistory {
		return details, leases, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+leaseColumns+` FROM leases
		WHERE digest=? ORDER BY mounted_at_ms, namespace, path, version`, digest)
	if err != nil {
		return BlockDetails{}, nil, Errorf(CodeInternal, "read lease history", "cause", err.Error())
	}
	defer rows.Close()
	history := make([]Lease, 0)
	for rows.Next() {
		lease, scanErr := scanLease(rows)
		if scanErr != nil {
			return BlockDetails{}, nil, Errorf(CodeInternal, "scan lease history", "cause", scanErr.Error())
		}
		history = append(history, lease)
	}
	if err := rows.Err(); err != nil {
		return BlockDetails{}, nil, Errorf(CodeInternal, "iterate lease history", "cause", err.Error())
	}
	return details, history, nil
}

func activeLeases(ctx context.Context, tx *sql.Tx, digest string) ([]Lease, int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+leaseColumns+` FROM leases
		WHERE digest=? AND state='active' ORDER BY namespace, path, version`, digest)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	leases := make([]Lease, 0)
	for rows.Next() {
		l, err := scanLease(rows)
		if err != nil {
			return nil, 0, err
		}
		leases = append(leases, l)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return leases, int64(len(leases)), nil
}

func (p *Pool) LeaseDetails(ctx context.Context, digest string, includeHistory bool) (BlockDetails, []Lease, *Error) {
	if err := validateDigest(digest); err != nil {
		return BlockDetails{}, nil, err
	}
	if err := p.expireBlock(ctx, digest, p.nowMS()); err != nil {
		return BlockDetails{}, nil, err
	}
	return p.readBlockSnapshot(ctx, digest, includeHistory)
}

// GetContent returns the one physical copy of a live block after verifying it
// still hashes to the requested digest. Reclaimed content is not readable.
func (p *Pool) GetContent(ctx context.Context, digest string) ([]byte, *Error) {
	if err := validateDigest(digest); err != nil {
		return nil, err
	}
	if err := p.expireBlock(ctx, digest, p.nowMS()); err != nil {
		return nil, err
	}
	tx, err := p.beginRead(ctx)
	if err != nil {
		return nil, Errorf(CodeInternal, "begin content read", "cause", err.Error())
	}
	defer rollback(tx)
	var state string
	var content []byte
	if err := tx.QueryRowContext(ctx, `SELECT state, content FROM blocks WHERE digest=?`, digest).
		Scan(&state, &content); errors.Is(err, sql.ErrNoRows) {
		return nil, Errorf(CodeBlockNotFound, "block is not present", "digest", digest)
	} else if err != nil {
		return nil, Errorf(CodeInternal, "read block content", "cause", err.Error())
	}
	if state != BlockLive {
		return nil, Errorf(CodeBlockNotFound, "block content has been removed", "digest", digest, "state", state)
	}
	if err := verifyContent(digest, content); err != nil {
		return nil, err
	}
	result := make([]byte, len(content))
	copy(result, content)
	return result, nil
}

// expireBlock materializes persisted lease expiry before a read. This is the
// only mutation allowed on a read path, and it runs in its own transaction so
// the subsequent reader still observes one complete commit.
func (p *Pool) expireBlock(ctx context.Context, digest string, now int64) *Error {
	unlock, err := p.locks.Lock(ctx, []string{digestKey(digest)})
	if err != nil {
		return err
	}
	defer unlock()
	tx, dbErr := p.beginWrite(ctx)
	if dbErr != nil {
		return Errorf(CodeInternal, "begin expiry transaction", "cause", dbErr.Error())
	}
	defer rollback(tx)
	if err := expireRefsForDigest(ctx, tx, digest, now); err != nil {
		return Errorf(CodeInternal, "expire references", "cause", err.Error())
	}
	if err := assertRefCount(ctx, tx, digest); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return Errorf(CodeInternal, "commit expiry", "cause", err.Error())
	}
	return nil
}

func expireRefsForDigest(ctx context.Context, tx *sql.Tx, digest string, now int64) error {
	type expiredRef struct {
		namespace, path string
		leaseID         int64
	}
	rows, err := tx.QueryContext(ctx, `SELECT namespace, path, lease_id FROM refs
		WHERE digest=? AND expires_at_ms <= ?`, digest, now)
	if err != nil {
		return err
	}
	var refs []expiredRef
	for rows.Next() {
		var r expiredRef
		if err := rows.Scan(&r.namespace, &r.path, &r.leaseID); err != nil {
			rows.Close()
			return err
		}
		refs = append(refs, r)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, ref := range refs {
		res, err := tx.ExecContext(ctx, `DELETE FROM refs WHERE namespace=? AND path=? AND digest=?`,
			ref.namespace, ref.path, digest)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 1 {
			if _, err := tx.ExecContext(ctx, `UPDATE blocks SET ref_count=ref_count-1
				WHERE digest=? AND ref_count > 0`, digest); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE leases SET state='expired'
			WHERE id=? AND state='active'`, ref.leaseID); err != nil {
			return err
		}
	}
	return nil
}

func assertRefCount(ctx context.Context, tx *sql.Tx, digest string) *Error {
	var count, persisted int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM refs WHERE digest=?`, digest).Scan(&count); err != nil {
		return Errorf(CodeInternal, "count refs", "cause", err.Error())
	}
	if err := tx.QueryRowContext(ctx, `SELECT ref_count FROM blocks WHERE digest=?`, digest).Scan(&persisted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Errorf(CodeBlockNotFound, "block is not present", "digest", digest)
		}
		return Errorf(CodeInternal, "read persisted ref count", "cause", err.Error())
	}
	if count != persisted {
		return Errorf(CodeInternal, "reference count does not match committed refs",
			"digest", digest, "persisted", persisted, "actual", count)
	}
	return nil
}

func getLiveContentTx(ctx context.Context, tx *sql.Tx, digest string) ([]byte, *Error) {
	var state string
	var content []byte
	err := tx.QueryRowContext(ctx, `SELECT state, content FROM blocks WHERE digest=?`, digest).Scan(&state, &content)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, Errorf(CodeBlockNotFound, "block is not present", "digest", digest)
	}
	if err != nil {
		return nil, Errorf(CodeInternal, "read block content", "cause", err.Error())
	}
	if state != BlockLive {
		return nil, Errorf(CodeBlockNotFound, "block is not live", "digest", digest, "state", state)
	}
	if err := verifyContent(digest, content); err != nil {
		return nil, err
	}
	return content, nil
}

func leaseByRef(ctx context.Context, tx *sql.Tx, namespace, path, digest string) (*Lease, *Error) {
	lease, err := scanLease(tx.QueryRowContext(ctx, `SELECT `+leaseColumns+` FROM leases
		WHERE namespace=? AND path=? AND digest=? AND state='active'`, namespace, path, digest))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, Errorf(CodeInternal, "read active lease", "cause", err.Error())
	}
	return &lease, nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
