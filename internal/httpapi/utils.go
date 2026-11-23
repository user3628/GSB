package httpapi

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"math/big"
	"net/http"
	"strings"

	"blocksnap/internal/apperr"
	"blocksnap/internal/fixed"
	"blocksnap/internal/store"
)

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func parseDecisionInput(body decisionBody) (store.DecisionInput, error) {
	if body.MarginFloor == "" || body.CostBudgetBytes == "" || body.RelativeTolerance == "" {
		return store.DecisionInput{}, apperr.Invalid("margin_floor, cost_budget_bytes and relative_tolerance are required")
	}
	margin, err := fixed.ParseDecimal(body.MarginFloor)
	if err != nil {
		return store.DecisionInput{}, apperr.WithContext(apperr.Invalid("margin_floor must be a fixed-point decimal"), "detail", err.Error())
	}
	if margin.Sign() < 0 {
		return store.DecisionInput{}, apperr.Invalid("margin_floor must be non-negative")
	}
	budget, ok := new(big.Int).SetString(strings.TrimSpace(body.CostBudgetBytes), 10)
	if !ok || budget.Sign() < 0 {
		return store.DecisionInput{}, apperr.Invalid("cost_budget_bytes must be a non-negative integer string")
	}
	tolerance, err := fixed.ParseDecimal(body.RelativeTolerance)
	if err != nil {
		return store.DecisionInput{}, apperr.WithContext(apperr.Invalid("relative_tolerance must be a fixed-point decimal"), "detail", err.Error())
	}
	if tolerance.Sign() < 0 || tolerance.Cmp(fixed.ScaleFactor()) > 0 {
		return store.DecisionInput{}, apperr.Invalid("relative_tolerance must be between 0 and 1")
	}
	return store.DecisionInput{MarginFloor: margin, CostBudgetBytes: budget, RelativeTolerance: tolerance}, nil
}

func decisionFromQuery(r *http.Request) (store.DecisionInput, bool, error) {
	q := r.URL.Query()
	body := decisionBody{
		MarginFloor: q.Get("margin_floor"), CostBudgetBytes: q.Get("cost_budget_bytes"), RelativeTolerance: q.Get("relative_tolerance"),
	}
	if body.MarginFloor == "" && body.CostBudgetBytes == "" && body.RelativeTolerance == "" {
		return store.DecisionInput{}, false, nil
	}
	input, err := parseDecisionInput(body)
	if err != nil {
		return store.DecisionInput{}, false, err
	}
	return input, true, nil
}
