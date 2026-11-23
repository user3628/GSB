package ledger

const (
	TerminalCommitted  = "committed"
	TerminalPartial    = "partial"
	TerminalRolledBack = "rolled_back"
)

type Entry struct {
	Sequence         int    `json:"sequence"`
	ItemID           string `json:"item_id"`
	View             string `json:"view"`
	Stage            string `json:"stage"`
	Operation        string `json:"operation"`
	Input            string `json:"input,omitempty"`
	From             string `json:"from,omitempty"`
	To               string `json:"to,omitempty"`
	Factor           int64  `json:"factor,omitempty"`
	Quotient         int64  `json:"quotient"`
	Remainder        int64  `json:"remainder,omitempty"`
	ErrorUpperScaled int64  `json:"error_upper_scaled,omitempty"`
	Note             string `json:"note,omitempty"`
}

type Ledger struct {
	Entries  []Entry `json:"entries"`
	Terminal string  `json:"terminal"`
}

func New() Ledger {
	return Ledger{Entries: []Entry{}, Terminal: TerminalRolledBack}
}

func (l *Ledger) Checkpoint() int { return len(l.Entries) }

func (l *Ledger) Rollback(checkpoint int) {
	if checkpoint < 0 {
		checkpoint = 0
	}
	if checkpoint > len(l.Entries) {
		checkpoint = len(l.Entries)
	}
	l.Entries = l.Entries[:checkpoint]
}

func (l *Ledger) Add(entry Entry) {
	entry.Sequence = len(l.Entries) + 1
	l.Entries = append(l.Entries, entry)
}

func (l Ledger) Clone() Ledger {
	entries := make([]Entry, len(l.Entries))
	copy(entries, l.Entries)
	return Ledger{Entries: entries, Terminal: l.Terminal}
}
