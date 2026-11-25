package engine

import (
	"fmt"
	"sort"

	"mvccengine/pkg/mvcc"
)

type ChainReport struct {
	OK                bool     `json:"ok"`
	RowKey            string   `json:"row_key"`
	HeadVersionSeq    *int64   `json:"head_version_seq,omitempty"`
	TipVersionSeqs    []int64  `json:"tip_version_seqs"`
	ForkVersionSeq    *int64   `json:"fork_version_seq,omitempty"`
	MissingVersionSeq *int64   `json:"missing_version_seq,omitempty"`
	Issues            []string `json:"issues"`
}

type RepairChainRequest struct {
	RequestID string `json:"request_id,omitempty"`
	RowKey    string `json:"row_key"`
}

type RepairChainResult struct {
	RowKey               string  `json:"row_key"`
	IntentID             string  `json:"intent_id"`
	OldHeadVersionSeq    *int64  `json:"old_head_version_seq,omitempty"`
	ResetHeadVersionSeq  int64   `json:"reset_head_version_seq"`
	ForkVersionSeq       *int64  `json:"fork_version_seq,omitempty"`
	DetachedVersionSeqs  []int64 `json:"detached_version_seqs"`
	RequestID            string  `json:"request_id"`
}

func (e *Engine) ValidateChain(rowKey string) (*ChainReport, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	report, err := e.validateChainLocked(rowKey)
	if err != nil {
		return report, err
	}
	return report, nil
}

func (e *Engine) validateChainLocked(rowKey string) (*ChainReport, *mvcc.Error) {
	report := &ChainReport{RowKey: rowKey, TipVersionSeqs: []int64{}, Issues: []string{}}

	headSeq, hasHead := e.heads[rowKey]
	if hasHead {
		h := headSeq
		report.HeadVersionSeq = &h
	}

	rowVersions := map[int64]mvcc.RowVersion{}
	for seq, version := range e.versions {
		if version.RowKey == rowKey && !version.Detached {
			rowVersions[seq] = version
		}
	}
	if len(rowVersions) == 0 {
		return report, mvcc.NewError(mvcc.CodeNotFound, "row has no versions", map[string]any{"row_key": rowKey})
	}

	referenced := map[int64]bool{}
	for seq, version := range rowVersions {
		if version.PrevVersionSeq == nil {
			continue
		}
		prevSeq := *version.PrevVersionSeq
		referenced[prevSeq] = true
		prev, ok := rowVersions[prevSeq]
		if !ok {
			if e.isReclaimedVersion(prevSeq) {
				report.Issues = append(report.Issues, fmt.Sprintf("prev version %d was safely reclaimed", prevSeq))
				continue
			}
			missing := prevSeq
			report.MissingVersionSeq = &missing
			return report, mvcc.NewError(mvcc.CodeChainBroken, "previous version reference does not exist", map[string]any{
				"row_key":             rowKey,
				"version_seq":         seq,
				"missing_version_seq": prevSeq,
			})
		}
		if prev.RowKey != rowKey {
			return report, mvcc.NewError(mvcc.CodeChainBroken, "previous version belongs to another row", map[string]any{
				"row_key":      rowKey,
				"version_seq":  seq,
				"prev_row_key": prev.RowKey,
			})
		}
		if prevSeq >= seq {
			return report, mvcc.NewError(mvcc.CodeChainBroken, "version sequence falls back or repeats", map[string]any{
				"row_key":     rowKey,
				"version_seq": seq,
				"prev_seq":    prevSeq,
			})
		}
	}

	for seq := range rowVersions {
		if !referenced[seq] && rowVersions[seq].CommitState != mvcc.VersionAborted {
			report.TipVersionSeqs = append(report.TipVersionSeqs, seq)
		}
	}
	sort.Slice(report.TipVersionSeqs, func(i, j int) bool { return report.TipVersionSeqs[i] < report.TipVersionSeqs[j] })

	if hasHead {
		head, ok := rowVersions[headSeq]
		if !ok || head.CommitState == mvcc.VersionAborted {
			return report, mvcc.NewError(mvcc.CodeChainBroken, "latest pointer does not reference a usable version", map[string]any{
				"row_key":          rowKey,
				"head_version_seq": headSeq,
			})
		}
		if !containsInt64(report.TipVersionSeqs, headSeq) {
			return report, mvcc.NewError(mvcc.CodeChainBroken, "latest pointer is not a chain tip", map[string]any{
				"row_key":          rowKey,
				"head_version_seq": headSeq,
				"tip_version_seqs": report.TipVersionSeqs,
			})
		}
	}

	if len(report.TipVersionSeqs) > 1 {
		fork := commonForkPoint(report.TipVersionSeqs, rowVersions)
		report.ForkVersionSeq = fork
		context := map[string]any{
			"row_key":           rowKey,
			"head_version_seqs": report.TipVersionSeqs,
		}
		if fork != nil {
			context["fork_version_seq"] = *fork
		}
		return report, mvcc.NewError(mvcc.CodeChainBroken, "two latest version pointers form a fork", context)
	}

	if cycleSeq, ok := detectChainCycle(headSeq, rowVersions); ok {
		return report, mvcc.NewError(mvcc.CodeChainBroken, "version reference cycle detected", map[string]any{
			"row_key":     rowKey,
			"version_seq": cycleSeq,
		})
	}

	report.OK = true
	return report, nil
}

func (e *Engine) isReclaimedVersion(seq int64) bool {
	var count int
	if err := e.db.QueryRow(`SELECT COUNT(1) FROM reclaimed_versions WHERE version_seq=?`, seq).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func commonForkPoint(tips []int64, versions map[int64]mvcc.RowVersion) *int64 {
	if len(tips) < 2 {
		return nil
	}
	ancestorSets := make([]map[int64]bool, 0, len(tips))
	paths := make([][]int64, 0, len(tips))
	for _, tip := range tips {
		set := map[int64]bool{}
		path := []int64{}
		visited := map[int64]bool{}
		current := &tip
		for current != nil {
			seq := *current
			if visited[seq] {
				break
			}
			visited[seq] = true
			set[seq] = true
			path = append(path, seq)
			v, ok := versions[seq]
			if !ok {
				break
			}
			current = v.PrevVersionSeq
		}
		ancestorSets = append(ancestorSets, set)
		paths = append(paths, path)
	}

	for _, seq := range paths[0] {
		common := true
		for _, set := range ancestorSets[1:] {
			if !set[seq] {
				common = false
				break
			}
		}
		if common {
			return &seq
		}
	}
	return nil
}

func detectChainCycle(head int64, versions map[int64]mvcc.RowVersion) (int64, bool) {
	visited := map[int64]bool{}
	current := &head
	for current != nil {
		seq := *current
		if visited[seq] {
			return seq, true
		}
		visited[seq] = true
		v, ok := versions[seq]
		if !ok {
			return 0, false
		}
		current = v.PrevVersionSeq
	}
	return 0, false
}

func containsInt64(values []int64, target int64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (e *Engine) RepairChain(req RepairChainRequest) (*RepairChainResult, error) {
	return executeAs(e, req.RequestID, "repair_chain", req, func(run *RequestRun) (*RepairChainResult, error) {
		if req.RowKey == "" {
			return nil, mvcc.NewError(mvcc.CodeVersionInvalid, "row_key must not be empty", nil)
		}
		e.mu.Lock()
		defer e.mu.Unlock()

		report, validationErr := e.validateChainLocked(req.RowKey)
		if validationErr == nil && report.OK {
			head := e.heads[req.RowKey]
			return &RepairChainResult{RowKey: req.RowKey, ResetHeadVersionSeq: head, DetachedVersionSeqs: []int64{}, RequestID: run.ID}, nil
		}
		if validationErr != nil && validationErr.Code != mvcc.CodeChainBroken {
			return nil, validationErr
		}

		start := report.ForkVersionSeq
		if start == nil && report.HeadVersionSeq != nil {
			start = report.HeadVersionSeq
		}
		if start == nil {
			return nil, mvcc.NewError(mvcc.CodeChainBroken, "cannot repair chain without a known rollback point", map[string]any{"row_key": req.RowKey})
		}

		target := e.lastCompleteCommittedPrefixLocked(req.RowKey, *start)
		if target == nil {
			return nil, mvcc.NewError(mvcc.CodeChainBroken, "no complete committed chain prefix is available; refusing to fabricate versions", map[string]any{
				"row_key": req.RowKey,
			})
		}

		oldHead := report.HeadVersionSeq
		detached := []int64{}
		for seq, version := range e.versions {
			if version.RowKey == req.RowKey && !version.Detached && seq > *target {
				detached = append(detached, seq)
			}
		}
		sort.Slice(detached, func(i, j int) bool { return detached[i] < detached[j] })

		result := &RepairChainResult{
			RowKey:              req.RowKey,
			OldHeadVersionSeq:   oldHead,
			ResetHeadVersionSeq: *target,
			ForkVersionSeq:      report.ForkVersionSeq,
			DetachedVersionSeqs: detached,
			RequestID:           run.ID,
		}
		intentID, err := e.createIntent(run, "repair_chain", result)
		if err != nil {
			return nil, err
		}
		result.IntentID = intentID

		dbTx, err := e.db.Begin()
		if err != nil {
			return nil, err
		}
		defer func() { _ = dbTx.Rollback() }()

		if len(detached) > 0 {
			if _, err := dbTx.Exec(`UPDATE versions SET detached=1, updated_at_ms=? WHERE row_key=? AND version_seq > ?`, e.now(), req.RowKey, *target); err != nil {
				return nil, err
			}
		}
		if _, err := dbTx.Exec(`INSERT INTO row_heads(row_key,version_seq) VALUES(?,?)
			ON CONFLICT(row_key) DO UPDATE SET version_seq=excluded.version_seq`, req.RowKey, *target); err != nil {
			return nil, err
		}
		if err := completeIntentTx(dbTx, intentID, result); err != nil {
			return nil, err
		}
		if err := run.CompleteTx(dbTx, result, intentID); err != nil {
			return nil, err
		}
		if err := dbTx.Commit(); err != nil {
			return nil, err
		}

		for _, seq := range detached {
			v := e.versions[seq]
			v.Detached = true
			e.versions[seq] = v
		}
		e.heads[req.RowKey] = *target
		return result, nil
	})
}

func (e *Engine) lastCompleteCommittedPrefixLocked(rowKey string, start int64) *int64 {
	candidate := &start
	for candidate != nil {
		seq := *candidate
		ok := true
		visited := map[int64]bool{}
		current := &seq
		for current != nil {
			curSeq := *current
			if visited[curSeq] {
				ok = false
				break
			}
			visited[curSeq] = true
			v, exists := e.versions[curSeq]
			if !exists || v.RowKey != rowKey || v.Detached || v.CommitState != mvcc.VersionCommitted {
				ok = false
				break
			}
			current = v.PrevVersionSeq
		}
		if ok {
			return &seq
		}
		v, exists := e.versions[seq]
		if !exists {
			return nil
		}
		candidate = v.PrevVersionSeq
	}
	return nil
}
