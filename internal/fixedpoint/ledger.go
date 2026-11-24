package fixedpoint

import (
	"privescworkbench/internal/domain"
)

// Ledger is a draft error book. All writes are buffered; Commit promotes the
// draft only after a whole scoring round succeeds, guaranteeing that an
// overflow never leaves a partially written ledger behind.
type Ledger struct {
	items     []domain.LedgerItem
	byProc    map[int64]*domain.ProcLedger
	committed bool
}

// NewLedger creates an empty draft ledger.
func NewLedger() *Ledger {
	return &Ledger{byProc: map[int64]*domain.ProcLedger{}}
}

// Record appends one truncation remainder for a process/event.
func (l *Ledger) Record(pid, seq int64, kind string, remainder int64) error {
	if l.committed {
		return domain.NewError(domain.CodeInternal, "write to committed ledger")
	}
	if remainder < 0 {
		return domain.NewError(domain.CodeScoreOverflow, "negative remainder")
	}
	l.items = append(l.items, domain.LedgerItem{
		ProcPID: pid, EventSeq: seq, Kind: kind, Remainder: remainder,
	})
	agg, ok := l.byProc[pid]
	if !ok {
		agg = &domain.ProcLedger{PID: pid}
		l.byProc[pid] = agg
	}
	sum, err := Add(agg.RemainderSum, remainder)
	if err != nil {
		return err
	}
	agg.RemainderSum = sum
	agg.ItemCount++
	return nil
}

// Commit finalizes the draft and computes per-process micro error bounds.
func (l *Ledger) Commit() error {
	for pid, agg := range l.byProc {
		bound, err := BoundMicro(agg.RemainderSum)
		if err != nil {
			return err
		}
		agg.BoundMicro = bound
		_ = pid
	}
	l.committed = true
	return nil
}

// Proc returns the finalized aggregate ledger for a process.
func (l *Ledger) Proc(pid int64) domain.ProcLedger {
	if agg, ok := l.byProc[pid]; ok {
		return *agg
	}
	return domain.ProcLedger{PID: pid}
}

// Items returns all recorded ledger items in insertion order.
func (l *Ledger) Items() []domain.LedgerItem {
	return l.items
}
