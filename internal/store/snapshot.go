package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"injectctl/internal/model"
	"injectctl/internal/session"
)

// NewSnapshot builds a versioned snapshot whose content hash covers the full
// adjudication payload and whose summary hash chains to the previous
// snapshot, so a wholesale content replacement is detected even if the
// version number itself is preserved.
func NewSnapshot(version int, batchID, contextVersion string,
	verdicts []model.Verdict, corrections []session.Correction, prevSummaryHash string) Snapshot {

	sorted := append([]model.Verdict(nil), verdicts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })

	snap := Snapshot{
		Version:        version,
		BatchID:        batchID,
		RuleSetVersion: model.RuleSetVersion,
		ContextVersion: contextVersion,
		Verdicts:       sorted,
		Corrections:    corrections,
		SummaryHash:    prevSummaryHash,
	}

	// Deterministic content body: verdicts, corrections and rule/context
	// versions, excluding the hash fields themselves.
	body := struct {
		RuleSetVersion string               `json:"rule_set_version"`
		ContextVersion string               `json:"context_version"`
		BatchID        string               `json:"batch_id"`
		Verdicts       []model.Verdict      `json:"verdicts"`
		Corrections    []session.Correction `json:"corrections"`
	}{snap.RuleSetVersion, snap.ContextVersion, snap.BatchID, sorted, corrections}
	raw, _ := json.Marshal(body)
	sum := sha256.Sum256(raw)
	snap.ContentHash = hex.EncodeToString(sum[:])
	return snap
}

// ChainSummaryHash folds the current snapshot content into the running chain.
func ChainSummaryHash(prevSummaryHash, contentHash string) string {
	sum := sha256.Sum256([]byte(prevSummaryHash + "|" + contentHash))
	return hex.EncodeToString(sum[:])
}
