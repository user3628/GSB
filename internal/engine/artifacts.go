package engine

import (
	"encoding/json"
	"os"
	"path/filepath"

	"ticketpipeline/internal/bayes"
	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
	"ticketpipeline/internal/normalize"
	"ticketpipeline/internal/priority"
	"ticketpipeline/internal/validate"
)

// writeCanonical writes v as canonical JSON with a trailing newline, which is
// the single artifact format used by every stage.
func writeCanonical(path string, v any) error {
	b, err := determ.MarshalJSON(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// decodeArtifact loads one persisted artifact into the snapshot.
func decodeArtifact(stage string, raw []byte, snap *Snapshot) error {
	switch stage {
	case model.StageNormalize:
		var v normalize.Result
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		snap.Normalize[v.TicketID] = &v
	case model.StageIntent:
		var v bayes.Result
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		snap.Intent[v.TicketID] = &v
	case model.StagePriority:
		var v priority.Result
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		snap.Priority[v.TicketID] = &v
	case model.StageValidate:
		var v validate.Report
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		snap.Report = &v
	}
	return nil
}

// recordArtifact mirrors a freshly produced artifact into the in-memory
// snapshot so later stages within the same run see it immediately.
func recordArtifact(stage, ticketID string, artifact any, snap *Snapshot) {
	if snap == nil {
		return
	}
	switch stage {
	case model.StageNormalize:
		if v, ok := artifact.(*normalize.Result); ok {
			snap.Normalize[ticketID] = v
		}
	case model.StageIntent:
		if v, ok := artifact.(*bayes.Result); ok {
			snap.Intent[ticketID] = v
		}
	case model.StagePriority:
		if v, ok := artifact.(*priority.Result); ok {
			snap.Priority[ticketID] = v
		}
	case model.StageValidate:
		if v, ok := artifact.(*validate.Report); ok {
			snap.Report = v
		}
	}
}

// recordBiz mirrors a business error into the matching snapshot error map.
func recordBiz(stage, ticketID string, be *model.BizError, snap *Snapshot) {
	if snap == nil {
		return
	}
	switch stage {
	case model.StageNormalize:
		snap.NormErr[ticketID] = be
	case model.StageIntent:
		snap.IntentErr[ticketID] = be
	case model.StagePriority:
		snap.PrioErr[ticketID] = be
	}
}

func resTokens(r *normalize.Result) int {
	if r == nil {
		return 0
	}
	return len(r.Tokens)
}

func safeTop(r *bayes.Result) string {
	if r == nil {
		return ""
	}
	return r.TopIntent
}
