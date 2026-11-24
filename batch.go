package blockpool

import (
	"context"
	"database/sql"
	"math"
	"sort"
)

func (p *Pool) ReplaceNodeRefs(ctx context.Context, req ReplaceRequest) (ReplaceResult, *Error) {
	if err := validateNode(req.Namespace, req.Path); err != nil {
		return ReplaceResult{}, err
	}
	ttl, err := p.leaseTTL(req.LeaseTTLMS)
	if err != nil {
		return ReplaceResult{}, err
	}
	startedAt := p.nowMS()
	if ttl > math.MaxInt64-startedAt {
		return ReplaceResult{}, Errorf(CodeSpecInvalid, "lease duration overflows persisted timestamp")
	}
	digests := make([]string, 0, len(req.Blocks))
	seen := make(map[string]struct{}, len(req.Blocks))
	lockKeys := []string{nodeKey(req.Namespace, req.Path)}
	for _, item := range req.Blocks {
		if err := validateDigest(item.Digest); err != nil {
			return ReplaceResult{}, err
		}
		if _, exists := seen[item.Digest]; exists {
			return ReplaceResult{}, Errorf(CodeSpecInvalid, "duplicate digest in replacement set", "digest", item.Digest)
		}
		seen[item.Digest] = struct{}{}
		if item.SizeBytes != nil {
			if err := validateNonNegative("size_bytes", *item.SizeBytes); err != nil {
				return ReplaceResult{}, err
			}
			if item.Data != nil && *item.SizeBytes != int64(len(item.Data)) {
				return ReplaceResult{}, Errorf(CodeContentCorrupt, "declared size does not match content bytes",
					"digest", item.Digest, "declared_size", *item.SizeBytes, "actual_size", len(item.Data))
			}
		}
		if err := validateNonNegative("retention_ms", item.RetentionMS); err != nil {
			return ReplaceResult{}, err
		}
		if item.Data != nil {
			actual := digestOf(item.Data)
			if actual != item.Digest {
				return ReplaceResult{}, Errorf(CodeContentCorrupt, "content SHA-256 does not match digest",
					"declared_digest", item.Digest, "actual_digest", actual)
			}
		}
		digests = append(digests, item.Digest)
		lockKeys = append(lockKeys, digestKey(item.Digest))
	}
	sortStrings(digests)
	type canonicalBlock struct {
		Digest      string `json:"digest"`
		Data        []byte `json:"data,omitempty"`
		SizeBytes   *int64 `json:"size_bytes,omitempty"`
		RetentionMS int64  `json:"retention_ms"`
	}
	canonicalBlocks := make([]canonicalBlock, 0, len(req.Blocks))
	for _, item := range req.Blocks {
		canonicalBlocks = append(canonicalBlocks, canonicalBlock{
			Digest: item.Digest, Data: item.Data, SizeBytes: item.SizeBytes, RetentionMS: item.RetentionMS,
		})
	}
	sort.Slice(canonicalBlocks, func(i, j int) bool { return canonicalBlocks[i].Digest < canonicalBlocks[j].Digest })
	payload := struct {
		Namespace  string           `json:"namespace"`
		Path       string           `json:"path"`
		Blocks     []canonicalBlock `json:"blocks"`
		LeaseTTLMS int64            `json:"lease_ms"`
	}{req.Namespace, req.Path, canonicalBlocks, ttl}

	return runMutation(p, ctx, mutationSpec{
		RequestID:    req.RequestID,
		Operation:    "replace_refs",
		AggregateKey: nodeKey(req.Namespace, req.Path),
		Payload:      payload,
		LockKeys:     lockKeys,
		BusyKey:      batchKey(req.Namespace, req.Path),
	}, func(ctx context.Context, tx *sql.Tx, now int64) (ReplaceResult, *Error) {
		return p.replaceRefsTx(ctx, tx, req, ttl, now)
	})
}

func (p *Pool) replaceRefsTx(ctx context.Context, tx *sql.Tx, req ReplaceRequest, ttl, now int64) (ReplaceResult, *Error) {
	exists, err := nodeExists(ctx, tx, req.Namespace, req.Path)
	if err != nil {
		return ReplaceResult{}, Errorf(CodeInternal, "check node", "cause", err.Error())
	}
	if !exists {
		return ReplaceResult{}, Errorf(CodeNodeUnknown, "logical node is not registered",
			"namespace", req.Namespace, "path", req.Path)
	}

	desired := make(map[string]BatchBlock, len(req.Blocks))
	touched := make(map[string]struct{}, len(req.Blocks))
	for _, item := range req.Blocks {
		desired[item.Digest] = item
		touched[item.Digest] = struct{}{}
	}

	rows, dbErr := tx.QueryContext(ctx, `SELECT digest FROM refs WHERE namespace=? AND path=?`,
		req.Namespace, req.Path)
	if dbErr != nil {
		return ReplaceResult{}, Errorf(CodeInternal, "read current node refs", "cause", dbErr.Error())
	}
	current := make(map[string]int64)
	for rows.Next() {
		var digest string
		if scanErr := rows.Scan(&digest); scanErr != nil {
			rows.Close()
			return ReplaceResult{}, Errorf(CodeInternal, "scan current node ref", "cause", scanErr.Error())
		}
		current[digest] = 0
		touched[digest] = struct{}{}
	}
	if closeErr := rows.Close(); closeErr != nil {
		return ReplaceResult{}, Errorf(CodeInternal, "close current node refs", "cause", closeErr.Error())
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return ReplaceResult{}, Errorf(CodeInternal, "iterate current node refs", "cause", rowsErr.Error())
	}

	touchedList := make([]string, 0, len(touched))
	for digest := range touched {
		touchedList = append(touchedList, digest)
	}
	sortStrings(touchedList)
	for _, digest := range touchedList {
		if err := expireRefsForDigest(ctx, tx, digest, now); err != nil {
			return ReplaceResult{}, Errorf(CodeInternal, "expire block refs", "digest", digest, "cause", err.Error())
		}
	}

	// Reload after expiry. Expired desired blocks must be added afresh.
	current = make(map[string]int64)
	rows, dbErr = tx.QueryContext(ctx, `SELECT digest, version FROM refs WHERE namespace=? AND path=?`,
		req.Namespace, req.Path)
	if dbErr != nil {
		return ReplaceResult{}, Errorf(CodeInternal, "reload current node refs", "cause", dbErr.Error())
	}
	for rows.Next() {
		var digest string
		var version int64
		if scanErr := rows.Scan(&digest, &version); scanErr != nil {
			rows.Close()
			return ReplaceResult{}, Errorf(CodeInternal, "scan current node ref", "cause", scanErr.Error())
		}
		current[digest] = version
	}
	rows.Close()

	result := ReplaceResult{
		Namespace: req.Namespace, Path: req.Path,
		Added: make([]string, 0), Removed: make([]string, 0), Unchanged: make([]string, 0),
		Leases: make([]Lease, 0),
	}
	leases := make([]Lease, 0)
	for _, item := range req.Blocks {
		if _, ok := current[item.Digest]; ok {
			if _, err := getLiveContentTx(ctx, tx, item.Digest); err != nil {
				return ReplaceResult{}, err
			}
			lease, err := leaseByRef(ctx, tx, req.Namespace, req.Path, item.Digest)
			if err != nil {
				return ReplaceResult{}, err
			}
			if lease == nil {
				return ReplaceResult{}, Errorf(CodeInternal, "active reference has no active lease", "digest", item.Digest)
			}
			result.Unchanged = append(result.Unchanged, item.Digest)
			leases = append(leases, *lease)
			continue
		}

		if item.Data != nil {
			if _, err := ingestTx(ctx, tx, item.Digest, item.Data, item.RetentionMS, now); err != nil {
				return ReplaceResult{}, err
			}
		} else {
			if _, err := getLiveContentTx(ctx, tx, item.Digest); err != nil {
				if err.Code == CodeBlockNotFound {
					return ReplaceResult{}, Errorf(CodeContentCorrupt,
						"block content is unavailable and no replacement bytes were supplied", "digest", item.Digest)
				}
				return ReplaceResult{}, err
			}
		}
		version, err := nextLeaseVersion(ctx, tx, req.Namespace, req.Path, item.Digest)
		if err != nil {
			return ReplaceResult{}, err
		}
		lease, err := insertLease(ctx, tx, req.Namespace, req.Path, item.Digest, version, now, now+ttl)
		if err != nil {
			return ReplaceResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE blocks SET ref_count=ref_count+1 WHERE digest=?`, item.Digest); err != nil {
			return ReplaceResult{}, Errorf(CodeInternal, "increment ref count", "cause", err.Error())
		}
		p.recordLeaseIssued(req.Namespace, req.Path, item.Digest)
		result.Added = append(result.Added, item.Digest)
		leases = append(leases, lease)
	}

	for _, digest := range touchedList {
		if _, keep := desired[digest]; keep {
			continue
		}
		if _, present := current[digest]; !present {
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM refs WHERE namespace=? AND path=? AND digest=?`,
			req.Namespace, req.Path, digest); err != nil {
			return ReplaceResult{}, Errorf(CodeInternal, "remove reference", "cause", err.Error())
		}
		if _, err := tx.ExecContext(ctx, `UPDATE blocks SET ref_count=ref_count-1 WHERE digest=? AND ref_count > 0`, digest); err != nil {
			return ReplaceResult{}, Errorf(CodeInternal, "decrement ref count", "cause", err.Error())
		}
		if err := setLeaseState(ctx, tx, req.Namespace, req.Path, digest, LeaseUnmounted); err != nil {
			return ReplaceResult{}, err
		}
		result.Removed = append(result.Removed, digest)
	}
	sortStrings(result.Added)
	sortStrings(result.Removed)
	sortStrings(result.Unchanged)

	for _, digest := range touchedList {
		if err := assertRefCount(ctx, tx, digest); err != nil {
			return ReplaceResult{}, err
		}
	}
	// Also assert blocks removed from the old set but not in touchedList can
	// never happen: old digests were merged into touchedList above.
	result.Leases = leases
	return result, nil
}
