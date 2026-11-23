package mfa

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type preparedFactor struct {
	factor       SnapshotFactor
	challenge    Challenge
	expected     string
	response     string
	code         Code
	reason       string
	recoveryIdx  *int
	timeStep     int64
	stepInWindow bool
}

func (e *Engine) reserveAuthBudget(key string) error {
	e.budgetMu.Lock()
	defer e.budgetMu.Unlock()
	if e.pending[key] >= e.limits.MaxPendingPerUser {
		return bizError(CodeAuthBudget, "per-user pending authentication limit reached")
	}
	e.pending[key]++
	return nil
}

func (e *Engine) releaseAuthBudget(key string) {
	e.budgetMu.Lock()
	defer e.budgetMu.Unlock()
	if e.pending[key] <= 1 {
		delete(e.pending, key)
		return
	}
	e.pending[key]--
}

func (e *Engine) PendingAuth(tenantID, userID string) int {
	e.budgetMu.Lock()
	defer e.budgetMu.Unlock()
	return e.pending[userKey(tenantID, userID)]
}

func (e *Engine) Authenticate(ctx context.Context, request AuthRequest) (AuthResult, error) {
	result := AuthResult{
		SessionID:  request.SessionID,
		AttemptID:  request.AttemptID,
		TenantID:   request.TenantID,
		UserID:     request.UserID,
		FactorIDs:  make([]string, 0),
		Challenges: make([]Challenge, 0),
	}
	if request.TenantID == "" || request.UserID == "" || request.AttemptID == "" {
		result.Code = CodePolicyMalformed
		return result, policyError("auth", "tenant_id, user_id and attempt_id are required")
	}
	if request.SessionID == "" {
		request.SessionID = request.AttemptID
		result.SessionID = request.SessionID
	}
	switch request.AnswerMode {
	case "", "valid", "invalid":
	default:
		result.Code = CodePolicyMalformed
		return result, policyError("auth.answer_mode", "supported values are valid and invalid")
	}
	if request.HoldMillis < 0 {
		result.Code = CodePolicyMalformed
		return result, policyError("auth.hold_millis", "must not be negative")
	}

	key := userKey(request.TenantID, request.UserID)
	if err := e.reserveAuthBudget(key); err != nil {
		result.Code = CodeAuthBudget
		result.SnapshotVersion = e.SnapshotStatus(ctx).LatestValid
		return result, err
	}
	defer e.releaseAuthBudget(key)

	select {
	case e.sem <- struct{}{}:
		defer func() { <-e.sem }()
	case <-ctx.Done():
		result.Code = CodeInternal
		return result, ctx.Err()
	}
	if request.HoldMillis > 0 {
		timer := time.NewTimer(time.Duration(request.HoldMillis) * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			result.Code = CodeInternal
			return result, ctx.Err()
		}
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		result.Code = CodeInternal
		return result, err
	}
	defer tx.Rollback()

	var userExists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE id = ? AND tenant_id = ?`, request.UserID, request.TenantID).Scan(&userExists); err != nil {
		return result, err
	}
	if userExists == 0 {
		current, _ := currentSnapshotVersion(ctx, tx)
		result.Code = CodeNoViableFactor
		result.SnapshotVersion = current
		return result, bizError(CodeNoViableFactor, "user does not resolve within the tenant")
	}
	policy, err := loadPolicy(ctx, tx, request.TenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			current, _ := currentSnapshotVersion(ctx, tx)
			result.Code = CodeNoViableFactor
			result.SnapshotVersion = current
			return result, bizError(CodeNoViableFactor, "tenant policy is missing")
		}
		return result, err
	}
	factors, err := listUserFactors(ctx, tx, request.TenantID, request.UserID)
	if err != nil {
		return result, err
	}

	selected, selectCode, selectReason := selectFactors(policy, factors, request.FactorHints, e.ClockUnix())
	if selectCode != CodeOK {
		current, _ := currentSnapshotVersion(ctx, tx)
		result.Code = selectCode
		result.Message = selectReason
		result.SnapshotVersion = current
		return result, bizError(selectCode, selectReason)
	}

	prepared := make([]preparedFactor, 0, len(selected))
	for _, factor := range selected {
		item, err := e.prepareChallenge(request, factor)
		if err != nil {
			return result, err
		}
		prepared = append(prepared, item)
		result.FactorIDs = append(result.FactorIDs, item.factor.ID)
		result.Challenges = append(result.Challenges, item.challenge)
	}

	finalCode := CodeOK
	for _, item := range prepared {
		switch item.code {
		case CodeConsumed:
			finalCode = CodeConsumed
		case CodeChallengeFailed:
			if finalCode == CodeOK {
				finalCode = CodeChallengeFailed
			}
		}
	}

	for index := range prepared {
		item := &prepared[index]
		if item.code == CodeOK && finalCode == CodeOK && item.factor.Type == FactorRecovery && item.recoveryIdx != nil {
			if _, err := tx.ExecContext(ctx, `
UPDATE recovery_codes SET consumed = 1, consumed_attempt = ? WHERE factor_id = ? AND code_index = ?
`, request.AttemptID, item.factor.ID, *item.recoveryIdx); err != nil {
				return result, err
			}
		}

		if item.code == CodeOK {
			if _, err := tx.ExecContext(ctx, `
UPDATE factors SET success_count = success_count + 1, consecutive_failures = 0 WHERE factor_id = ?
`, item.factor.ID); err != nil {
				return result, err
			}
			continue
		}

		if _, err := tx.ExecContext(ctx, `
UPDATE factors
SET failure_count = failure_count + 1, consecutive_failures = consecutive_failures + 1
WHERE factor_id = ?
`, item.factor.ID); err != nil {
			return result, err
		}
		var status, degradedReason string
		var consecutive int
		if err := tx.QueryRowContext(ctx, `
SELECT status, degraded_reason, consecutive_failures FROM factors WHERE factor_id = ?
`, item.factor.ID).Scan(&status, &degradedReason, &consecutive); err != nil {
			return result, err
		}
		if FactorStatus(status) == StatusActive && consecutive > e.limits.FailureThreshold {
			degradedReason = fmt.Sprintf("consecutive_failures %d exceeded threshold %d", consecutive, e.limits.FailureThreshold)
			if _, err := tx.ExecContext(ctx, `
UPDATE factors SET status = ?, degraded_reason = ? WHERE factor_id = ?
`, string(StatusDegraded), degradedReason, item.factor.ID); err != nil {
				return result, err
			}
			sequence, err := nextSequence(ctx, tx, "risk_seq")
			if err != nil {
				return result, err
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO risk_traces(trace_id, sequence, tenant_id, user_id, factor_id, attempt_id, kind, reason, consecutive_failures, snapshot_version)
VALUES(?, ?, ?, ?, ?, ?, 'degraded', ?, ?, 0)
`, stableID("risk", sequence), sequence, item.factor.TenantID, item.factor.UserID, item.factor.ID, request.AttemptID, degradedReason, consecutive); err != nil {
				return result, err
			}
		}
	}

	reason := "auth_success"
	if finalCode != CodeOK {
		reason = "auth_failure"
	}
	version, err := writeSnapshot(ctx, tx, reason, request.AttemptID)
	if err != nil {
		return result, err
	}
	for _, item := range prepared {
		sequence, err := nextSequence(ctx, tx, "usage_seq")
		if err != nil {
			return result, err
		}
		outcome := "success"
		if item.code != CodeOK {
			outcome = "failure"
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO usage_traces(trace_id, sequence, tenant_id, user_id, factor_id, attempt_id, factor_type, outcome, code, challenge_prompt, response_sha256, snapshot_version)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, stableID("usage", sequence), sequence, item.factor.TenantID, item.factor.UserID, item.factor.ID,
			request.AttemptID, string(item.factor.Type), outcome, string(item.code), item.challenge.Prompt,
			item.challenge.ResponseSHA, version); err != nil {
			return result, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE risk_traces SET snapshot_version = ? WHERE attempt_id = ? AND snapshot_version = 0`, version, request.AttemptID); err != nil {
		return result, err
	}
	if err := commitWithHooks(tx, "authenticate", e.options); err != nil {
		return result, err
	}

	result.Code = finalCode
	result.SnapshotVersion = version
	if finalCode == CodeOK {
		return result, nil
	}
	message := "one or more factor responses failed"
	if finalCode == CodeConsumed {
		message = "recovery code was already consumed"
	}
	result.Message = message
	return result, bizError(finalCode, message)
}

func currentSnapshotVersion(ctx context.Context, tx *sql.Tx) (int64, error) {
	var version int64
	err := tx.QueryRowContext(ctx, `SELECT head_version FROM snapshot_meta WHERE singleton = 1`).Scan(&version)
	return version, err
}

func loadPolicy(ctx context.Context, tx *sql.Tx, tenantID string) (Policy, error) {
	var policy Policy
	var requiredJSON string
	err := tx.QueryRowContext(ctx, `
SELECT tenant_id, min_strength, required_json FROM policies WHERE tenant_id = ?
`, tenantID).Scan(&policy.TenantID, &policy.MinStrength, &requiredJSON)
	if err != nil {
		return policy, err
	}
	if err := decodeRequiredFactors(requiredJSON, &policy.RequiredFactors); err != nil {
		return policy, err
	}
	sort.Slice(policy.RequiredFactors, func(i, j int) bool {
		return policy.RequiredFactors[i].Type < policy.RequiredFactors[j].Type
	})
	return policy, nil
}

func listUserFactors(ctx context.Context, tx *sql.Tx, tenantID, userID string) ([]SnapshotFactor, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT factor_id, tenant_id, user_id, factor_type, sensitivity, strength, status,
       rebindable, last_activated, expires_at_unix, secret_seed, recovery_code_count,
       success_count, failure_count, consecutive_failures, degraded_reason
FROM factors
WHERE tenant_id = ? AND user_id = ?
ORDER BY tenant_id, user_id, factor_id
`, tenantID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	factors := make([]SnapshotFactor, 0)
	for rows.Next() {
		factor, err := scanFactor(rows)
		if err != nil {
			return nil, err
		}
		factors = append(factors, *factor)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for index := range factors {
		codes, err := loadRecoveryCodes(ctx, tx, factors[index].ID)
		if err != nil {
			return nil, err
		}
		factors[index].RecoveryCodes = codes
	}
	return factors, nil
}

func loadRecoveryCodes(ctx context.Context, tx *sql.Tx, factorID string) ([]RecoveryCode, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT factor_id, code_index, code_hash, consumed FROM recovery_codes WHERE factor_id = ? ORDER BY code_index
`, factorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	codes := make([]RecoveryCode, 0)
	for rows.Next() {
		var code RecoveryCode
		var consumed int
		if err := rows.Scan(&code.FactorID, &code.Index, &code.CodeHash, &consumed); err != nil {
			return nil, err
		}
		code.Consumed = consumed != 0
		codes = append(codes, code)
	}
	return codes, rows.Err()
}

func selectFactors(policy Policy, factors []SnapshotFactor, hints []string, clockUnix int64) ([]SnapshotFactor, Code, string) {
	viable := make(map[string]SnapshotFactor)
	for _, factor := range factors {
		if factor.Status != StatusActive || !factor.LastActivated {
			continue
		}
		if factor.ExpiresAtUnix > 0 && factor.ExpiresAtUnix <= clockUnix {
			continue
		}
		viable[factor.ID] = factor
	}
	hintSet := make(map[string]struct{}, len(hints))
	for _, hint := range hints {
		hint = strings.TrimSpace(hint)
		if hint == "" {
			return nil, CodeNoViableFactor, "factor hint is empty"
		}
		if _, exists := viable[hint]; !exists {
			return nil, CodeNoViableFactor, "requested factor identifier is missing or not viable"
		}
		hintSet[hint] = struct{}{}
	}

	selected := make([]SnapshotFactor, 0, len(policy.RequiredFactors))
	used := make(map[string]struct{})
	for _, requirement := range policy.RequiredFactors {
		minStrength := int(math.Max(float64(policy.MinStrength), float64(requirement.MinStrength)))
		candidates := make([]SnapshotFactor, 0)
		for _, factor := range viable {
			if factor.Type != requirement.Type || factor.Strength < minStrength {
				continue
			}
			if len(hintSet) > 0 {
				if _, exists := hintSet[factor.ID]; !exists {
					continue
				}
			}
			if _, exists := used[factor.ID]; exists {
				continue
			}
			candidates = append(candidates, factor)
		}
		if len(candidates) == 0 {
			return nil, CodeNoViableFactor, fmt.Sprintf("no viable factor satisfies type %s at strength %d", requirement.Type, minStrength)
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Strength == candidates[j].Strength {
				return candidates[i].ID < candidates[j].ID
			}
			return candidates[i].Strength > candidates[j].Strength
		})
		selected = append(selected, candidates[0])
		used[candidates[0].ID] = struct{}{}
	}
	if len(hintSet) > 0 && len(hintSet) != len(used) {
		return nil, CodeNoViableFactor, "one or more factor hints do not participate in the policy"
	}
	return selected, CodeOK, ""
}

func (e *Engine) prepareChallenge(request AuthRequest, factor SnapshotFactor) (preparedFactor, error) {
	item := preparedFactor{
		factor:       factor,
		code:         CodeOK,
		stepInWindow: true,
	}
	switch factor.Type {
	case FactorTOTP:
		baseStep := e.ClockUnix() / int64(e.limits.TOTPStepSeconds)
		step := baseStep + int64(request.TOTPStepOffset)
		item.timeStep = step
		item.stepInWindow = absInt(request.TOTPStepOffset) <= e.limits.TOTPWindow
		item.expected = totpValue(factor.SecretSeed, step)
		item.challenge = Challenge{
			FactorID:   factor.ID,
			FactorType: factor.Type,
			Kind:       "totp_time_step",
			Prompt:     fmt.Sprintf("totp_step:%d", step),
			TimeStep:   step,
		}
	case FactorRecovery:
		index, code, reason := chooseRecoveryCode(factor, request.RecoveryCodeIndex)
		item.recoveryIdx = index
		if index == nil {
			item.code = code
			item.reason = reason
			item.challenge = Challenge{
				FactorID:   factor.ID,
				FactorType: factor.Type,
				Kind:       "recovery_code",
				Prompt:     "recovery_code:unavailable",
			}
			break
		}
		item.expected = recoveryCodeValue(factor.SecretSeed, *index)
		item.challenge = Challenge{
			FactorID:   factor.ID,
			FactorType: factor.Type,
			Kind:       "recovery_code",
			Prompt:     fmt.Sprintf("recovery_code:%d", *index),
			CodeIndex:  index,
		}
	case FactorWebAuthn:
		nonce := sha256Hex([]byte(factor.SecretSeed + "\x00" + request.AttemptID + "\x00" + factor.ID))
		item.expected = webAuthnValue(factor.SecretSeed, request.AttemptID, factor.ID, nonce)
		item.challenge = Challenge{
			FactorID:   factor.ID,
			FactorType: factor.Type,
			Kind:       "webauthn_assertion",
			Prompt:     "webauthn_nonce:" + nonce,
			Nonce:      nonce,
		}
	default:
		return item, policyError("factor.type", "unsupported factor type")
	}

	if item.code == CodeOK && factor.Type == FactorTOTP && !item.stepInWindow {
		item.code = CodeChallengeFailed
		item.reason = "TOTP time step is outside the configured acceptance window"
	}
	if item.code == CodeOK {
		item.response = item.expected
		if request.AnswerMode == "invalid" {
			item.response = wrongResponse(item.expected)
		}
		if request.Answer != "" {
			item.response = request.Answer
		}
		if item.response != item.expected {
			item.code = CodeChallengeFailed
			item.reason = "deterministic pseudo response did not match the challenge"
		}
	}
	item.challenge.ResponseSHA = responseHash(item.response)
	return item, nil
}

func chooseRecoveryCode(factor SnapshotFactor, requested *int) (*int, Code, string) {
	if requested != nil {
		index := *requested
		for _, code := range factor.RecoveryCodes {
			if code.Index != index {
				continue
			}
			if code.Consumed {
				return nil, CodeConsumed, "recovery code has already been consumed"
			}
			value := index
			return &value, CodeOK, ""
		}
		return nil, CodeChallengeFailed, "recovery code index is outside the registered range"
	}
	for _, code := range factor.RecoveryCodes {
		if !code.Consumed {
			index := code.Index
			return &index, CodeOK, ""
		}
	}
	return nil, CodeConsumed, "all recovery codes have been consumed"
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
