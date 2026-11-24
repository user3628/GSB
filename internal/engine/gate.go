package engine

import "sync"

// winnerIndex records, for deterministic replay, the single request sequence
// designated to win each ticket's one-time redemption. It is a passive lookup
// table: it never blocks and therefore cannot participate in a deadlock. The
// actual ordering is provided solely by the batch orderGate plus the per-ticket
// mutex. With no entry (the live HTTP service), first-come semantics apply.
type winnerIndex struct {
	mu      sync.Mutex
	winners map[string]int64
}

func newWinnerIndex() *winnerIndex {
	return &winnerIndex{winners: map[string]int64{}}
}

// RegisterWinner records the sequence allowed to redeem a ticket first.
func (e *Engine) RegisterWinner(ticketID string, winnerSeq int64) {
	e.winnerReg.mu.Lock()
	defer e.winnerReg.mu.Unlock()
	e.winnerReg.winners[ticketID] = winnerSeq
}

// isDesignatedWinner reports whether seq is the registered winner of a ticket.
// Tickets without a registration treat every caller as a winner (live mode).
func (e *Engine) isDesignatedWinner(ticketID string, seq int64) bool {
	e.winnerReg.mu.Lock()
	defer e.winnerReg.mu.Unlock()
	winner, ok := e.winnerReg.winners[ticketID]
	if !ok {
		return true
	}
	return winner == seq
}

// acquireTicketLock returns the per-ticket mutex unlock function.
func (e *Engine) acquireTicketLock(ticketID string) func() {
	lock := e.lockTicket(ticketID)
	lock.Lock()
	return lock.Unlock
}
