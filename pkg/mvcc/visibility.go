package mvcc

import "fmt"

type View struct {
	Txns     map[int64]Txn
	Versions map[int64]RowVersion
	Heads    map[string]int64
}

func NewView() View {
	return View{
		Txns:     make(map[int64]Txn),
		Versions: make(map[int64]RowVersion),
		Heads:    make(map[string]int64),
	}
}

func Evaluate(view View, reader Txn, rowKey string) ReadResult {
	result := ReadResult{
		RowKey:       rowKey,
		ReaderTxnSeq: reader.Seq,
		SnapshotSeq:  reader.SnapshotSeq,
		UpperBound:   reader.UpperBound,
		Basis:        []string{},
		Candidates:   []Candidate{},
	}

	headSeq, ok := view.Heads[rowKey]
	if !ok {
		result.Outcome = "NOT_FOUND"
		result.Reason = ReasonNotFound
		result.Basis = append(result.Basis, fmt.Sprintf("no head pointer exists for row %q", rowKey))
		return result
	}

	visited := make(map[int64]bool)
	currentSeq := &headSeq
	for currentSeq != nil {
		seq := *currentSeq
		if visited[seq] {
			result.Reason = string(CodeChainBroken)
			result.Outcome = "CHAIN_BROKEN"
			result.Basis = append(result.Basis, fmt.Sprintf("cycle detected at version %d", seq))
			return result
		}
		visited[seq] = true

		version, ok := view.Versions[seq]
		if !ok {
			result.Basis = append(result.Basis, fmt.Sprintf("version %d is absent; stop at persisted chain boundary", seq))
			result.Reason = ReasonNoVisibleVersion
			break
		}

		txn, txnOK := view.Txns[version.CreateTxnSeq]
		candidate := Candidate{
			VersionSeq:     version.VersionSeq,
			PrevVersionSeq: version.PrevVersionSeq,
			CreateTxnSeq:   version.CreateTxnSeq,
			CommitState:    string(version.CommitState),
			IsDelete:       version.IsDelete,
		}

		switch {
		case version.CommitState == VersionAborted || (txnOK && txn.Status == TxnAborted):
			candidate.Eligible = false
			candidate.Reason = ReasonRolledBack
			candidate.Detail = "creating transaction or version was rolled back"
		case version.CommitState == VersionUncommitted || !txnOK || txn.Status == TxnActive:
			if version.CreateTxnSeq == reader.Seq {
				candidate.Reason = ReasonSelfUncommitted
				candidate.Detail = "the reading transaction has created this version but has not committed it"
			} else {
				candidate.Reason = "OTHER_UNCOMMITTED"
				candidate.Detail = "another active transaction has not committed this version"
			}
			candidate.Eligible = false
		case txn.Status == TxnCommitted && version.CommitState == VersionCommitted:
			if version.CreateTxnSeq > reader.UpperBound {
				candidate.Eligible = false
				candidate.Reason = ReasonSnapshotOutsideCommit
				candidate.Detail = fmt.Sprintf("txn %d is COMMITTED but %d > snapshot upper bound %d", version.CreateTxnSeq, version.CreateTxnSeq, reader.UpperBound)
			} else {
				candidate.Eligible = true
				candidate.Reason = ReasonVisible
				candidate.Detail = fmt.Sprintf("txn %d is COMMITTED and %d <= upper_bound %d", version.CreateTxnSeq, version.CreateTxnSeq, reader.UpperBound)
			}
		default:
			candidate.Eligible = false
			candidate.Reason = "INCONSISTENT_COMMIT_FACT"
			candidate.Detail = "version and transaction commit states do not agree"
		}

		result.Candidates = append(result.Candidates, candidate)
		result.Basis = append(result.Basis, candidate.Detail)

		if candidate.Eligible {
			if version.IsDelete {
				result.Visible = false
				result.Outcome = "DELETED"
				result.Reason = ReasonDeletedInSnapshot
				shadow := version.VersionSeq
				result.ShadowVersionSeq = &shadow
				result.Basis = append(result.Basis, fmt.Sprintf("v%d is a committed delete inside the snapshot and shadows older versions", version.VersionSeq))
				return result
			}

			result.Visible = true
			result.Outcome = "VISIBLE"
			result.Reason = ReasonVisible
			hit := version.VersionSeq
			result.HitVersionSeq = &hit
			result.HitDigest = version.Digest
			result.HitContent = version.Content
			result.Basis = append(result.Basis, fmt.Sprintf("v%d is the newest eligible non-delete version", version.VersionSeq))
			return result
		}

		currentSeq = version.PrevVersionSeq
	}

	result.Visible = false
	if result.Reason == "" {
		result.Outcome = "NO_VISIBLE_VERSION"
		result.Reason = ReasonNoVisibleVersion
	} else if result.Outcome == "" {
		result.Outcome = "NO_VISIBLE_VERSION"
		if len(result.Candidates) > 0 {
			result.Reason = result.Candidates[0].Reason
		} else {
			result.Reason = ReasonNoVisibleVersion
		}
	}
	return result
}

func (s Sample) Validate() error {
	if len(s.Transactions) == 0 {
		return NewError(CodeTxnInvalid, "sample must contain transactions", map[string]any{"name": s.Name})
	}

	seenTxn := make(map[int64]bool, len(s.Transactions))
	for i, txn := range s.Transactions {
		expected := int64(i + 1)
		if txn.TxnSeq != expected {
			return NewError(CodeTxnInvalid, "transaction sequences must start at 1 and be strictly increasing without gaps", map[string]any{
				"index":             i,
				"expected_txn_seq":  expected,
				"actual_txn_seq":    txn.TxnSeq,
			})
		}
		if seenTxn[txn.TxnSeq] {
			return NewError(CodeTxnInvalid, "duplicate transaction sequence", map[string]any{"txn_seq": txn.TxnSeq, "index": i})
		}
		seenTxn[txn.TxnSeq] = true
		switch txn.Status {
		case TxnActive, TxnCommitted, TxnAborted:
		default:
			return NewError(CodeTxnInvalid, "invalid transaction status", map[string]any{"txn_seq": txn.TxnSeq, "status": string(txn.Status)})
		}
		if txn.StartedAtMS < 0 {
			return NewError(CodeTxnInvalid, "started_at_ms must not be negative", map[string]any{"txn_seq": txn.TxnSeq})
		}
	}

	seenVersion := make(map[int64]bool, len(s.Versions))
	for i, version := range s.Versions {
		if version.RowKey == "" {
			return NewError(CodeVersionInvalid, "row_key must not be empty", map[string]any{"index": i, "version_seq": version.VersionSeq})
		}
		if version.VersionSeq <= 0 {
			return NewError(CodeVersionInvalid, "version_seq must be positive", map[string]any{"index": i, "row_key": version.RowKey})
		}
		if seenVersion[version.VersionSeq] {
			return NewError(CodeVersionInvalid, "duplicate version_seq", map[string]any{"index": i, "version_seq": version.VersionSeq})
		}
		seenVersion[version.VersionSeq] = true
		if !ValidDigest(version.Digest) {
			return NewError(CodeVersionInvalid, "digest must be 64-character fixed-length hexadecimal", map[string]any{
				"index":       i,
				"row_key":     version.RowKey,
				"version_seq": version.VersionSeq,
				"digest":      version.Digest,
			})
		}
		if !seenTxn[version.CreateTxnSeq] {
			return NewError(CodeVersionInvalid, "create_txn_seq does not reference a sample transaction", map[string]any{
				"index":          i,
				"version_seq":    version.VersionSeq,
				"create_txn_seq": version.CreateTxnSeq,
			})
		}
		switch version.CommitState {
		case VersionUncommitted, VersionCommitted, VersionAborted:
		default:
			return NewError(CodeVersionInvalid, "invalid version commit_state", map[string]any{"version_seq": version.VersionSeq, "commit_state": string(version.CommitState)})
		}
		if version.PrevVersionSeq != nil && *version.PrevVersionSeq >= version.VersionSeq {
			return NewError(CodeVersionInvalid, "previous version must be older than current version", map[string]any{
				"version_seq":      version.VersionSeq,
				"prev_version_seq": *version.PrevVersionSeq,
			})
		}
	}
	return nil
}
