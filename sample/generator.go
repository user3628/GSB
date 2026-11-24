// Package sample generates deterministic local WAL workloads. It uses only
// the standard library and never performs I/O or network access itself.
package sample

import (
	"encoding/json"
	"fmt"
	"math/rand"
)

const FormatVersion = 1

type PageUpdate struct {
	PageNo int64  `json:"page_no"`
	Value  string `json:"value"`
}

type Transaction struct {
	ID      string       `json:"id"`
	Records []PageUpdate `json:"records"`
}

type Group struct {
	Name         string        `json:"name"`
	Transactions []Transaction `json:"transactions"`
}

type Checkpoint struct {
	Sequence       uint64 `json:"sequence"`
	ReplayStartLSN uint64 `json:"replay_start_lsn"`
}

type Reclaim struct {
	SegmentNos []uint64 `json:"segment_nos"`
}

type Read struct {
	PageNo int64  `json:"page_no"`
	LSN    uint64 `json:"lsn"`
}

// Action is one deterministic workload step. Expect is "OK" for success or a
// stable business error code that must be observed.
type Action struct {
	Type       string      `json:"type"` // commit_group, checkpoint, reclaim, read, reclaim_current
	Group      *Group      `json:"group,omitempty"`
	Checkpoint *Checkpoint `json:"checkpoint,omitempty"`
	Reclaim    *Reclaim    `json:"reclaim,omitempty"`
	Read       *Read       `json:"read,omitempty"`
	Expect     string      `json:"expect"`
}

type Workload struct {
	FormatVersion     int      `json:"format_version"`
	Seed              int64    `json:"seed"`
	SegmentMaxBytes   int64    `json:"segment_max_bytes"`
	MaxTxnsPerSegment int      `json:"max_txns_per_segment"`
	Actions           []Action `json:"actions"`
}

// Generate returns a repeatable workload for any seed. The shape intentionally
// crosses several segment boundaries and exercises both successful and failing
// checkpoint/reclaim/read adjudications.
func Generate(seed int64) Workload {
	rng := rand.New(rand.NewSource(seed))
	w := Workload{
		FormatVersion:     FormatVersion,
		Seed:              seed,
		SegmentMaxBytes:   900,
		MaxTxnsPerSegment: 3,
	}
	pageFor := func() int64 { return int64(1 + rng.Intn(6)) }
	valueFor := func() string {
		const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
		b := make([]byte, 96+int(rng.Intn(32)))
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(b)
	}
	group := func(number int, forcePage1 bool) Group {
		g := Group{Name: fmt.Sprintf("group-%02d", number)}
		for t := 0; t < 2; t++ {
			page := pageFor()
			if forcePage1 && t == 0 {
				page = 1
			}
			g.Transactions = append(g.Transactions, Transaction{
				ID: fmt.Sprintf("seed%d-g%02d-t%02d", seed, number, t+1),
				Records: []PageUpdate{{
					PageNo: page,
					Value:  valueFor(),
				}},
			})
		}
		return g
	}
	generated := make(map[int]Group)
	addGroup := func(n int, forcePage1 bool) {
		g := group(n, forcePage1)
		generated[n] = g
		w.Actions = append(w.Actions, Action{Type: "commit_group", Group: &g, Expect: "OK"})
	}
	for i := 1; i <= 6; i++ {
		addGroup(i, false)
	}
	w.Actions = append(w.Actions,
		Action{Type: "checkpoint", Checkpoint: &Checkpoint{Sequence: 1, ReplayStartLSN: 12}, Expect: "OK"},
		Action{Type: "read", Read: &Read{PageNo: 1, LSN: 12}, Expect: "OK"},
	)
	addGroup(7, true)
	addGroup(8, true)
	w.Actions = append(w.Actions,
		Action{Type: "read", Read: &Read{PageNo: 1, LSN: 12}, Expect: "E_CHECKPOINT_LAG"},
		Action{Type: "reclaim", Reclaim: &Reclaim{}, Expect: "OK"},
		Action{Type: "commit_group", Group: ptrGroup(generated[7]), Expect: "OK_DUPLICATE"},
		Action{Type: "commit_conflict", Group: ptrGroup(generated[7]), Expect: "E_CONFLICTING_TXN"},
		Action{Type: "checkpoint", Checkpoint: &Checkpoint{Sequence: 2, ReplayStartLSN: 16}, Expect: "OK"},
		Action{Type: "read", Read: &Read{PageNo: 1, LSN: 16}, Expect: "OK"},
		Action{Type: "read", Read: &Read{PageNo: 1, LSN: 4}, Expect: "E_SNAPSHOT_RETIRED"},
		Action{Type: "reclaim", Reclaim: &Reclaim{}, Expect: "OK"},
		Action{Type: "reclaim_current", Expect: "E_RECLAIM_UNSAFE"},
	)
	addGroup(9, true)
	w.Actions = append(w.Actions,
		Action{Type: "reclaim", Reclaim: &Reclaim{}, Expect: "OK"},
		Action{Type: "reclaim_current", Expect: "E_RECLAIM_UNSAFE"},
	)
	return w
}

func ptrGroup(g Group) *Group { return &g }

func (w Workload) JSON() ([]byte, error) { return json.MarshalIndent(w, "", "  ") }
