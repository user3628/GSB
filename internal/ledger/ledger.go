// Package ledger implements the append-only truncation error ledger.
package ledger

import "sync"

// Entry is a single truncation-error record. Amount is expressed in
// fixed.ErrScale (1e-12) units and is always >= 0.
type Entry struct {
	Seq        int64
	SnapshotID string
	RuleIndex  int
	Field      string
	Amount     int64
}

// Ledger is an append-only, session-isolated error ledger.
type Ledger struct {
	mu      sync.Mutex
	entries []Entry
}

func New() *Ledger { return &Ledger{} }

// Add appends one entry. Entries are only visible after Add returns;
// callers stage per-operation entries and apply them atomically so a
// failed operation never leaves partial writes behind.
func (l *Ledger) Add(snapshotID string, ruleIndex int, field string, amount int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, Entry{
		Seq:        int64(len(l.entries)) + 1,
		SnapshotID: snapshotID,
		RuleIndex:  ruleIndex,
		Field:      field,
		Amount:     amount,
	})
}

// Apply atomically appends a staged batch of entries.
func (l *Ledger) Apply(batch []Entry) {
	if len(batch) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range batch {
		e.Seq = int64(len(l.entries)) + 1
		l.entries = append(l.entries, e)
	}
}

func (l *Ledger) Entries() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, len(l.entries))
	copy(out, l.entries)
	return out
}

// BoundUnits returns the aggregated error upper bound in ErrScale units.
func (l *Ledger) BoundUnits() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var sum int64
	for _, e := range l.entries {
		sum += e.Amount
	}
	return sum
}

// BoundFixed converts an ErrScale-unit bound into a scale-1e-6 fixed
// bound, rounding up so the bound stays a valid upper bound.
func BoundFixed(units int64) int64 {
	if units <= 0 {
		return 0
	}
	return (units + 999_999) / 1_000_000
}
