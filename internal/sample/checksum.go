package sample

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"

	"alarmengine/internal/model"
)

// ComputeChecksum produces a deterministic integrity digest over every
// semantic field of a policy snapshot. The checksum field itself is excluded.
// Canonical encoding is hand-built (sorted keys) so the digest is independent
// of Go map iteration order.
func ComputeChecksum(p model.PolicySnapshot) string {
	var b strings.Builder

	b.WriteString("v=")
	b.WriteString(strconv.FormatInt(p.Version, 10))

	metrics := make([]string, 0, len(p.Thresholds))
	for m := range p.Thresholds {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)
	b.WriteString("|thr={")
	for i, m := range metrics {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(m)
		b.WriteString("=")
		b.WriteString(strconv.FormatInt(p.Thresholds[m], 10))
	}
	b.WriteString("}")

	b.WriteString("|fp=")
	b.WriteString(strings.Join(p.FingerprintFields, ","))
	b.WriteString("|supwin=")
	b.WriteString(strconv.FormatInt(p.SuppressionWindowMS, 10))
	b.WriteString("|tol=")
	b.WriteString(strconv.FormatInt(p.RootCauseTolerance, 10))
	b.WriteString("|conc=")
	b.WriteString(strconv.Itoa(p.ConcurrencyLimit))
	b.WriteString("|budget=")
	b.WriteString(strconv.FormatInt(p.RoundBudgetMS, 10))

	b.WriteString("|rules=[")
	rules := append([]model.SuppressionRule(nil), p.SuppressionRules...)
	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
	for i, r := range rules {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(r.ID)
		b.WriteString(":svc=")
		b.WriteString(r.Service)
		b.WriteString(":zone=")
		b.WriteString(r.Zone)
		b.WriteString(":hop=")
		b.WriteString(strconv.Itoa(r.Hop))
	}
	b.WriteString("]")

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
