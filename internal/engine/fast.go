package engine

import (
	"context"
	"database/sql"
	"math"

	"nonceguard/internal/canonical"
	"nonceguard/internal/model"
)

type FastOptions struct {
	ConfigVersion int64 `json:"config_version,omitempty"`
	ReferenceMs   int64 `json:"reference_ms,omitempty"`
	PrecheckLimit int   `json:"precheck_limit"`
}

type FastDetail struct {
	Selected         bool     `json:"selected"`
	SignatureChecked bool     `json:"signature_checked"`
	SignatureValid   *bool    `json:"signature_valid,omitempty"`
	NonceChecked     bool     `json:"nonce_checked"`
	NonceState       string   `json:"nonce_state,omitempty"`
	CheckedFields    []string `json:"checked_fields,omitempty"`
	UncheckedFields  []string `json:"unchecked_fields,omitempty"`
	CachedDecision   bool     `json:"cached_decision,omitempty"`
	ConfidenceNote   string   `json:"confidence_note"`
}

type SamplingReport struct {
	PrecheckLimit     int     `json:"precheck_limit"`
	CheckedCount      int     `json:"checked_count"`
	Total             int     `json:"total"`
	SampleRate        float64 `json:"sample_rate"`
	Method            string  `json:"method"`
	Selection         []int   `json:"selection"`
	SignatureCoverage float64 `json:"signature_coverage"`
	NonceCoverage     float64 `json:"nonce_coverage"`
	Confidence        string  `json:"confidence"`
	Description       string  `json:"description"`
}

type ComparisonRow struct {
	Index      int    `json:"index"`
	RequestID  string `json:"request_id"`
	Selected   bool   `json:"selected"`
	Comparable bool   `json:"comparable"`
	Matches    bool   `json:"matches"`
	State      string `json:"state"`
	FastCode   string `json:"fast_code"`
	FullCode   string `json:"full_code"`
	Note       string `json:"note"`
}

type ComparisonReport struct {
	ConfigVersion    int64           `json:"config_version"`
	Total            int             `json:"total"`
	Selected         int             `json:"selected"`
	Comparable       int             `json:"comparable"`
	Matching         int             `json:"matching"`
	Differences      int             `json:"differences"`
	NotPrechecked    int             `json:"not_prechecked"`
	AllSelectedMatch bool            `json:"all_selected_match"`
	Rows             []ComparisonRow `json:"rows"`
	Interpretation   string          `json:"interpretation"`
}

func (e *Engine) FastCheck(ctx context.Context, batch model.Batch, opts FastOptions) (*BatchResult, *Failure) {
	cfg, ferr := e.loadConfigForWrite(ctx, opts.ConfigVersion)
	if ferr != nil {
		return nil, ferr
	}
	high, err := e.Store.HighWaterMs(ctx)
	if err != nil {
		return nil, NewFailure(model.CodeInternal, "cannot read monotonic reference", map[string]any{"error": err.Error()})
	}
	base := e.Clock()
	if opts.ReferenceMs > 0 && e.AllowRefOverride {
		base = opts.ReferenceMs
	} else if batch.ReferenceMs > 0 && e.AllowRefOverride {
		base = batch.ReferenceMs
	}
	ref := base
	protected := false
	if high > ref {
		ref = high
		protected = true
	}

	limit := opts.PrecheckLimit
	if limit < 0 {
		limit = 0
	}
	if limit > len(batch.Requests) {
		limit = len(batch.Requests)
	}
	selection := make([]int, 0, limit)
	selectedSet := map[int]bool{}
	for i := 0; i < limit; i++ {
		selection = append(selection, i)
		selectedSet[i] = true
	}
	rate := 0.0
	if len(batch.Requests) > 0 {
		rate = float64(limit) / float64(len(batch.Requests))
	}
	confidence := "none"
	confidenceNote := "no field was prechecked; this is not a pass verdict"
	switch {
	case limit == 0:
	case limit >= len(batch.Requests):
		confidence = "full-precheck"
		confidenceNote = "all records were checked in shadow mode; conclusion is high coverage but remains non-committing until full adjudication"
	case rate >= 0.5:
		confidence = "partial-high-coverage"
		confidenceNote = "a declared prefix sample was checked; unselected signature/nonce fields remain explicitly not prechecked"
	default:
		confidence = "sample-based-low-confidence"
		confidenceNote = "only a small prefix sample was checked; absence of a sampled failure must not be interpreted as whole-batch success"
	}

	out := &BatchResult{
		BatchID:       batch.BatchID,
		Mode:          model.ModeFast,
		ConfigVersion: cfg.Version,
		Reference: referenceResolution{
			RequestedMs:       firstNonZero(opts.ReferenceMs, batch.ReferenceMs),
			HighWaterMs:       high,
			ReferenceMs:       ref,
			RollbackProtected: protected,
		},
		Total:          len(batch.Requests),
		BoundaryPolicy: model.BoundaryDescription(),
		Results:        make([]ItemResult, 0, len(batch.Requests)),
		Sampling: &SamplingReport{
			PrecheckLimit:     opts.PrecheckLimit,
			CheckedCount:      limit,
			Total:             len(batch.Requests),
			SampleRate:        math.Round(rate*10000) / 10000,
			Method:            "deterministic-prefix-cap-v1",
			Selection:         selection,
			SignatureCoverage: rate,
			NonceCoverage:     rate,
			Confidence:        confidence,
			Description:       "Checks at most precheck_limit records in fixed batch order using the same snapshot digest fields and nonce window; no fast result writes occupancy or decisions.",
		},
	}

	type shadowNonce struct {
		firstSeen int64
		index     int
		requestID string
	}
	shadowNonces := map[string]shadowNonce{}
	shadowRequests := map[string]priorCall{}

	for i, req := range batch.Requests {
		result := ItemResult{
			Index: i, RequestID: req.RequestID, BatchID: batch.BatchID,
			ConfigVersion: cfg.Version, ReferenceMs: ref, ContentHash: req.ContentHash(),
			Fast: &FastDetail{UncheckedFields: []string{"signature", "timestamp_offset", "nonce"}},
		}
		if !selectedSet[i] {
			result.Code = model.CodeNotPrechecked
			result.Status = model.StatusNotChecked
			result.Message = "outside declared precheck upper bound; not adjudicated and not passed"
			result.Fast.ConfidenceNote = "field status is NOT_PRECHECKED, never PASS"
			out.Results = append(out.Results, result)
			continue
		}

		result.Fast.Selected = true
		result.Fast.CheckedFields = []string{"signature", "timestamp_offset", "nonce"}
		result.Fast.UncheckedFields = []string{}
		result.Fast.ConfidenceNote = confidenceNote

		if prior, ok := shadowRequests[req.RequestID]; ok {
			if prior.hash != result.ContentHash {
				result.Code = model.CodeConflictingRequest
				result.Status = model.StatusRejected
				result.Message = "same request_id has different content in batch"
			} else {
				r := prior.result
				r.Index = i
				r.Fast = result.Fast
				r.IdempotentReuse = true
				r.Message = "fast shadow reused identical request_id result"
				result = r
			}
			out.Results = append(out.Results, result)
			continue
		}

		var storedHash string
		err := e.Store.DB.QueryRowContext(ctx, `SELECT content_hash FROM request_registry WHERE request_id=?`, req.RequestID).Scan(&storedHash)
		if err == nil {
			if storedHash != result.ContentHash {
				result.Code = model.CodeConflictingRequest
				result.Status = model.StatusRejected
				result.Message = "request_id already exists with different committed content"
				out.Results = append(out.Results, result)
				continue
			}
			cached, derr := scanDecision(ctx, e.Store.DB, `SELECT `+decisionColumns+` FROM decisions WHERE request_id=?`, req.RequestID)
			if derr == nil {
				cached.Index = i
				cached.Fast = result.Fast
				cached.IdempotentReuse = true
				cached.Fast.CachedDecision = true
				cached.Message = "fast comparison reused the committed idempotent decision for this request_id"
				shadowRequests[req.RequestID] = priorCall{hash: result.ContentHash, result: cached}
				out.Results = append(out.Results, cached)
				continue
			}
		} else if err != sql.ErrNoRows {
			return nil, NewFailure(model.CodeInternal, "fast request registry query failed", map[string]any{"error": err.Error()})
		}

		valid, verr := canonical.VerifyRequest(req, req.SignatureDigest, cfg.SignatureFields)
		result.Fast.SignatureChecked = true
		result.Fast.SignatureValid = &valid
		if verr != nil || !valid {
			result.Code = model.CodeSignatureMismatch
			result.Status = model.StatusRejected
			result.Message = "sampled signature mismatch"
			if verr != nil {
				result.Message = verr.Error()
			}
			shadowRequests[req.RequestID] = priorCall{hash: result.ContentHash, result: result}
			out.Results = append(out.Results, result)
			continue
		}

		offset := req.TimestampMs - ref
		result.OffsetMs = &offset
		if offset < cfg.MinOffsetMs {
			b := cfg.MinOffsetMs
			result.BoundaryMs = &b
			result.Code = model.CodeTimestampEarly
			result.Status = model.StatusRejected
			result.Message = "sampled timestamp is early"
			shadowRequests[req.RequestID] = priorCall{hash: result.ContentHash, result: result}
			out.Results = append(out.Results, result)
			continue
		}
		if offset > cfg.MaxOffsetMs {
			b := cfg.MaxOffsetMs
			result.BoundaryMs = &b
			result.Code = model.CodeTimestampLate
			result.Status = model.StatusRejected
			result.Message = "sampled timestamp is late"
			shadowRequests[req.RequestID] = priorCall{hash: result.ContentHash, result: result}
			out.Results = append(out.Results, result)
			continue
		}

		result.Fast.NonceChecked = true
		key := req.ClientID + "\x00" + req.Nonce
		if sh, ok := shadowNonces[key]; ok {
			expiry := sh.firstSeen + cfg.NonceWindowMs
			if ref < expiry {
				remaining := expiry - ref
				first := sh.firstSeen
				result.FirstSeenMs = &first
				result.ExpiresAtMs = &expiry
				result.RemainingWindowMs = &remaining
				result.Code = model.CodeNonceReplayed
				result.Status = model.StatusRejected
				result.Fast.NonceState = "shadow_active_replay"
				result.Message = "sampled nonce repeats an earlier prechecked record in the same batch"
				shadowRequests[req.RequestID] = priorCall{hash: result.ContentHash, result: result}
				out.Results = append(out.Results, result)
				continue
			}
		}

		var firstSeen int64
		var occVersion int64
		err = e.Store.DB.QueryRowContext(ctx, `SELECT config_version,first_seen_ms FROM nonce_occupancy WHERE client_id=? AND nonce=?`, req.ClientID, req.Nonce).Scan(&occVersion, &firstSeen)
		if err == nil {
			expiry := firstSeen + cfg.NonceWindowMs
			if ref < expiry {
				remaining := expiry - ref
				result.OccupancyConfigVersion = occVersion
				first := firstSeen
				result.FirstSeenMs = &first
				result.ExpiresAtMs = &expiry
				result.RemainingWindowMs = &remaining
				result.Code = model.CodeNonceReplayed
				result.Status = model.StatusRejected
				result.Fast.NonceState = "committed_active_replay"
				result.Message = "sampled nonce matches committed occupancy"
				shadowRequests[req.RequestID] = priorCall{hash: result.ContentHash, result: result}
				out.Results = append(out.Results, result)
				continue
			}
			result.Fast.NonceState = "committed_expired_would_recycle"
		} else if err != sql.ErrNoRows {
			ferr = NewFailure(model.CodeInternal, "fast nonce query failed", map[string]any{"error": err.Error()})
			return nil, ferr
		} else {
			result.Fast.NonceState = "no_active_occupancy_shadow_claim"
		}

		shadowNonces[key] = shadowNonce{firstSeen: ref, index: i, requestID: req.RequestID}
		result.Code = model.CodeOK
		result.Status = "precheck_pass_provisional"
		result.Accepted = true
		result.Provisional = true
		result.Message = "sampled signature, offset, and nonce checks passed provisionally; full mode remains authoritative"
		shadowRequests[req.RequestID] = priorCall{hash: result.ContentHash, result: result}
		out.Accepted++
		out.Results = append(out.Results, result)
	}
	return out, nil
}

func CompareResults(fast *BatchResult, full *BatchResult) *ComparisonReport {
	rep := &ComparisonReport{ConfigVersion: full.ConfigVersion, Total: len(full.Results), Rows: []ComparisonRow{}}
	fastByIndex := map[int]ItemResult{}
	if fast != nil {
		for _, r := range fast.Results {
			fastByIndex[r.Index] = r
		}
	}
	for _, fr := range full.Results {
		qr := fastByIndex[fr.Index]
		row := ComparisonRow{Index: fr.Index, RequestID: fr.RequestID, FullCode: fr.Code, FastCode: qr.Code}
		if qr.Fast != nil && qr.Fast.Selected {
			row.Selected = true
			row.Comparable = true
			rep.Selected++
			rep.Comparable++
			if qr.Code == fr.Code {
				row.Matches = true
				row.State = "match"
				rep.Matching++
				row.Note = "selected fast verdict matches full verdict for the same snapshot version"
			} else {
				row.State = "difference"
				rep.Differences++
				row.Note = "selected verdict differs; inspect state/config and idempotency context"
			}
		} else {
			row.State = "not_prechecked"
			rep.NotPrechecked++
			row.Note = "fast mode did not precheck this record; E_FAST_NOT_PRECHECKED is not a pass"
		}
		rep.Rows = append(rep.Rows, row)
	}
	rep.AllSelectedMatch = rep.Differences == 0
	rep.Interpretation = "Selected rows are directly comparable under one config version. Unselected rows explain coverage gaps and must be verified by full mode."
	return rep
}
