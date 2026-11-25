package engine

import "ticketpipeline/internal/model"

// expectedCounts returns the number of effective checkpoints each stage must
// contain for the given inputs. The count is a pure function of the tickets
// and policy so crash recovery can distinguish a complete stage from a
// partially committed one without relying on persisted bookkeeping.
func (e *Engine) expectedCounts(tickets []model.RawTicket, policy model.Policy) map[string]int {
	counts := map[string]int{
		model.StageNormalize: len(tickets),
		model.StageIntent:    0,
		model.StagePriority:  0,
		model.StageValidate:  1,
	}
	for _, t := range tickets {
		// Mirror the stage-1 emptiness rule: punctuation/empty text never
		// produces a token artifact and therefore never reaches later stages.
		if normalizeTicketReachesScoring(t, policy.HashDim) {
			counts[model.StageIntent]++
			counts[model.StagePriority]++
		}
	}
	return counts
}
