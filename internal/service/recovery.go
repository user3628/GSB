package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// RecoveryReport contains only facts found on disk at startup.
type RecoveryReport struct {
	RecoveredAt        string       `json:"recovered_at"`
	PendingIntents     []IntentFact `json:"pending_intents"`
	RunningRounds      []int64      `json:"running_rounds"`
	AbortedBatches     []string     `json:"aborted_batches"`
	RolledBackRequests []string     `json:"rolled_back_to_admitted"`
	AbortedIntents     int64        `json:"aborted_intents"`
	Notes              []string     `json:"notes"`
}

// IntentFact is a durable intent row observed during recovery.
type IntentFact struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	RoundID   int64  `json:"round_id"`
	RequestID string `json:"request_id"`
	BatchID   string `json:"batch_id"`
	CreatedAt string `json:"created_at"`
}

// Recover runs at process start. SQLite guarantees that every committed
// transaction is durable and every uncommitted one is rolled back automatically.
// The remaining work is bookkeeping: reconcile intent/round markers that never
// reached a terminal state, returning non-terminal batches and their requests
// to the last complete version (ADMITTED) and emitting a fact-based report.
func (a *App) Recover(ctx context.Context) (*RecoveryReport, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	report := &RecoveryReport{
		RecoveredAt:        time.Now().UTC().Format(time.RFC3339Nano),
		PendingIntents:     []IntentFact{},
		RunningRounds:      []int64{},
		AbortedBatches:     []string{},
		RolledBackRequests: []string{},
		Notes:              []string{},
	}

	pending, err := a.store.ScanPendingIntents(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range pending {
		report.PendingIntents = append(report.PendingIntents, IntentFact{
			ID: p.ID, Type: p.Type, RoundID: p.RoundID,
			RequestID: p.RequestID, BatchID: p.BatchID, CreatedAt: p.CreatedAt,
		})
	}

	rounds, err := a.store.ScanRunningRounds(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rounds {
		report.RunningRounds = append(report.RunningRounds, r.ID)
	}

	err = a.store.Tx(ctx, func(tx *sql.Tx) error {
		// 1. Reconcile every round that never finished.
		for _, r := range rounds {
			batches, berr := a.store.ListBatchesByRoundTx(tx, r.ID)
			if berr != nil {
				return berr
			}
			for _, b := range batches {
				if b.Status == domain.BatchCompleted || b.Status == domain.BatchAborted {
					continue
				}
				members, merr := store.MembersOfBatchTx(tx, b.ID)
				if merr != nil {
					return merr
				}
				// Batched/Running/Queued work returns to ADMITTED (last complete state).
				for _, rid := range members {
					req, gerr := a.store.GetRequestTx(tx, rid)
					if gerr != nil {
						return gerr
					}
					if req.State == domain.StateBatched || req.State == domain.StateRunning {
						if _, uerr := store.UpdateRequestState(tx, rid, req.State, domain.StateAdmitted); uerr != nil {
							return uerr
						}
						report.RolledBackRequests = append(report.RolledBackRequests, rid)
					}
				}
				if err := store.UpdateBatchStatusTx(tx, b.ID, domain.BatchAborted); err != nil {
					return err
				}
				report.AbortedBatches = append(report.AbortedBatches, b.ID)
			}

			abortedIntents, ierr := store.AbortIntentsOfRoundTx(tx, r.ID)
			if ierr != nil {
				return ierr
			}
			report.AbortedIntents += abortedIntents

			roundReport, _ := json.Marshal(map[string]any{
				"recovered":       true,
				"aborted_batches": report.AbortedBatches,
			})
			if err := a.store.MarkRoundRecoveredTx(tx, r.ID,
				time.Now().UTC().Format(time.RFC3339Nano), string(roundReport)); err != nil {
				return err
			}
		}

		// 2. Pending intents without a round (admission/preemption/transition) are
		// markers of transactions that never committed; mark them aborted. Their
		// actual data changes were already discarded by SQLite rollback, so this
		// only records the durable fact.
		leftover, err := store.AbortAllPendingIntentsTx(tx)
		if err != nil {
			return err
		}
		report.AbortedIntents += leftover
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(report.AbortedBatches)
	sort.Strings(report.RolledBackRequests)
	report.Notes = append(report.Notes,
		"committed transactions are durable; uncommitted changes were rolled back by SQLite WAL",
		"returned requests are ADMITTED and retain their admission reservation",
	)
	return report, nil
}
