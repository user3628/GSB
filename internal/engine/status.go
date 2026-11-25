package engine

import (
	"ticketpipeline/internal/model"
	"ticketpipeline/internal/store"
)

// StageStatus is the verification-facing view of one stage's persisted state.
type StageStatus struct {
	Stage         string `json:"stage"`
	Checkpoints   int    `json:"checkpoints"`
	OK            int    `json:"ok"`
	BizErrors     int    `json:"biz_errors"`
	LatestGen     int    `json:"latest_generation"`
	ArtifactFP    string `json:"artifact_fp"`
	Contiguous    bool   `json:"contiguous"`
	PolicyVersion int    `json:"policy_version"`
}

// Status reads stage facts directly from SQLite without re-running work.
func Status(st *store.Store) ([]StageStatus, error) {
	out := make([]StageStatus, 0, len(model.Stages))
	for _, stage := range model.Stages {
		cps, err := st.ListCheckpoints(stage)
		if err != nil {
			return nil, err
		}
		stat := StageStatus{Stage: stage, Checkpoints: len(cps), Contiguous: true}
		maxVersion := 0
		for _, cp := range cps {
			if cp.Status == store.StatusOK {
				stat.OK++
			} else {
				stat.BizErrors++
			}
			if cp.PolicyVersion > maxVersion {
				maxVersion = cp.PolicyVersion
			}
		}
		stat.PolicyVersion = maxVersion
		if err := st.AssertContiguous(stage); err != nil {
			stat.Contiguous = false
		}
		gen, err := st.LatestGeneration(stage)
		if err != nil {
			return nil, err
		}
		stat.LatestGen = gen
		fp, err := st.StageArtifactFP(stage)
		if err != nil {
			return nil, err
		}
		stat.ArtifactFP = fp
		out = append(out, stat)
	}
	return out, nil
}
