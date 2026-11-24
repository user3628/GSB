package engine

import (
	"time"

	"casengine/internal/codes"
	"casengine/internal/store"
)

// CleanupReport summarizes one cleanup pass.
type CleanupReport struct {
	Expired []string `json:"expiredTicketIds"`
}

func (e *Engine) cleanupLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, _ = e.CleanupOnce()
		case <-e.stopCh:
			return
		}
	}
}

// CleanupOnce expires every ticket past its expiry time.
//
// Resource discipline: the pass shares the validation semaphore and never holds
// more than CleanupQuota slots at once (batches of that size). Each candidate is
// additionally protected by its ticket mutex and re-checked under that lock, so
// a ticket currently being validated can never be expired out from under it.
func (e *Engine) CleanupOnce() (CleanupReport, error) {
	rep := CleanupReport{}
	all, err := e.store.ListAllTickets()
	if err != nil {
		return rep, err
	}
	nowUnix := e.clk.Now().Unix()
	var candidates []string
	for _, t := range all {
		if t.Status == store.StatusValid && t.ExpiresAt <= nowUnix {
			candidates = append(candidates, t.ID)
		}
	}
	for start := 0; start < len(candidates); start += e.cfg.CleanupQuota {
		end := start + e.cfg.CleanupQuota
		if end > len(candidates) {
			end = len(candidates)
		}
		batch := candidates[start:end]
		acquired := 0
		for range batch {
			select {
			case e.sem <- struct{}{}:
				acquired++
			case <-time.After(e.cfg.AcquireTimeout):
				// Budget exhausted by validation traffic: stop this pass.
			case <-e.stopCh:
				for i := 0; i < acquired; i++ {
					e.release()
				}
				return rep, nil
			}
		}
		for _, id := range batch {
			if acquired == 0 {
				break
			}
			e.expireOne(id, &rep)
		}
		for i := 0; i < acquired; i++ {
			e.release()
		}
	}
	return rep, nil
}

func (e *Engine) expireOne(id string, rep *CleanupReport) {
	mu := e.lockTicket(id)
	mu.Lock()
	defer mu.Unlock()

	t, err := e.store.GetTicket(id)
	if err != nil || t == nil || t.Status != store.StatusValid {
		return
	}
	// Re-check expiry under the lock to avoid racing a late validation.
	if t.ExpiresAt > e.clk.Now().Unix() {
		return
	}
	if _, err := e.store.Expire(t, e.clk.EventNS(700001)); err != nil {
		// A storage failure leaves the ticket untouched; retry next pass.
		return
	}
	rep.Expired = append(rep.Expired, id)
	_ = e.store.InsertConflict(store.Conflict{
		TenantID: t.TenantID, TicketID: t.ID, Seq: -1,
		Code: string(codes.Expired), AtNS: e.clk.EventNS(700001),
	})
}
