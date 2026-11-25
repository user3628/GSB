package engine

import (
	"ticketpipeline/internal/model"
	"ticketpipeline/internal/store"
)

func verifyStore(st *store.Store) error {
	for _, stage := range model.Stages {
		if err := st.AssertContiguous(stage); err != nil {
			return err
		}
		cps, err := st.ListCheckpoints(stage)
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		for _, cp := range cps {
			if seen[cp.TicketID] {
				return model.NewBizError(model.CodeStaleCheckpoint, stage, cp.TicketID,
					"duplicate effective checkpoint for ticket", nil)
			}
			seen[cp.TicketID] = true
		}
	}
	return nil
}
