package engine

import (
	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/store"
)

// stageCheckpoint is the minimal ordered projection used for the stage digest.
type stageCheckpoint struct {
	TicketID   string `json:"ticket_id"`
	ArtifactFP string `json:"artifact_fp"`
	Status     string `json:"status"`
	ErrorCode  string `json:"error_code"`
}

// stageDigest fingerprints the committed checkpoint set of a stage in
// stage_seq order. It is independent of wall-clock time and map iteration.
func (e *Engine) stageDigest(stage string) (string, error) {
	cps, err := e.store.ListCheckpoints(stage)
	if err != nil {
		return "", err
	}
	rows := make([]stageCheckpoint, 0, len(cps))
	for _, cp := range cps {
		rows = append(rows, stageCheckpoint{
			TicketID:   cp.TicketID,
			ArtifactFP: cp.ArtifactFP, Status: cp.Status, ErrorCode: cp.ErrorCode,
		})
	}
	fp, err := determ.Fingerprint(rows)
	if err != nil {
		return "", err
	}
	_ = store.StatusOK
	return fp, nil
}
