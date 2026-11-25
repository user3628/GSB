package engine

import (
	"ticketpipeline/internal/model"
	"ticketpipeline/internal/normalize"
)

// normalizeTicketReachesScoring reports whether a ticket yields any token and
// therefore flows into the intent and priority stages.
func normalizeTicketReachesScoring(t model.RawTicket, hashDim int) bool {
	n := normalize.NormalizeText(t.Text)
	if n == "" {
		return false
	}
	tokens, be := normalize.Tokenize(n, t.Channel, hashDim)
	if be != nil {
		return false
	}
	return len(tokens) > 0
}
