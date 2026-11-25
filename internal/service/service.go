package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// CrashHook lets tests / the debug API force a process kill at a precise
// durability point. Hooks receive the transaction kind and may terminate the
// process (via the injected function) to simulate SIGKILL mid-commit.
type CrashHook func(point string)

// App is the single-writer / multi-reader arbitration service.
//
// writeMu serializes every mutating arbitration (admission, batching,
// preemption, state transitions). Readers take no write lock and rely on SQLite
// WAL snapshot isolation, so they always observe a fully committed version and
// never an intermediate state (e.g. ADMITTED without reservation).
type App struct {
	store *store.Store

	writeMu sync.Mutex

	// dedup collapses concurrent identical arbitrations onto one execution.
	dedupMu sync.Mutex
	infly   map[string]*inflightCall

	crashHook CrashHook
}

type inflightCall struct {
	done    chan struct{}
	payload any
	resp    any
	err     error
}

func New(st *store.Store) *App {
	return &App{store: st, infly: map[string]*inflightCall{}}
}

// SetCrashHook installs (or clears, with nil) the forced-termination hook.
func (a *App) SetCrashHook(h CrashHook) { a.crashHook = h }

func (a *App) hook(point string) {
	if a.crashHook != nil {
		a.crashHook(point)
	}
}

// doOnce guarantees that concurrent arbitrations sharing the same key run once;
// waiters reuse the result. A conflicting payload yields E_CONFLICTING_REQUEST.
func (a *App) doOnce(key string, payload any, fn func() (any, error)) (any, error) {
	a.dedupMu.Lock()
	if call, ok := a.infly[key]; ok {
		a.dedupMu.Unlock()
		<-call.done
		if !jsonEqual(call.payload, payload) {
			return nil, domain.NewError(domain.EConflictingRequest,
				"in-flight arbitration with the same id but different content",
				map[string]any{"key": key})
		}
		return call.resp, call.err
	}
	call := &inflightCall{done: make(chan struct{}), payload: payload}
	a.infly[key] = call
	a.dedupMu.Unlock()

	call.resp, call.err = fn()
	close(call.done)

	a.dedupMu.Lock()
	delete(a.infly, key)
	a.dedupMu.Unlock()
	return call.resp, call.err
}

// Load atomically persists a normalized set: either every row is stored or none.
func (a *App) Load(ctx context.Context, set *domain.NormalizedSet) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	return a.store.Tx(ctx, func(tx *sql.Tx) error {
		intentID, err := store.InsertIntent(tx, "LOAD_SPEC", 0, "", "",
			`{"groups":`+itoa(len(set.Groups))+`,"requests":`+itoa(len(set.Requests))+`}`)
		if err != nil {
			return err
		}
		for i := range set.Groups {
			if err := a.store.UpsertGroupTx(tx, set.Groups[i]); err != nil {
				return err
			}
		}
		for i := range set.Nodes {
			if err := a.store.UpsertNodeTx(tx, set.Nodes[i]); err != nil {
				return err
			}
		}
		seq, err := store.NextRequestSeqTx(tx)
		if err != nil {
			return err
		}
		for i := range set.Requests {
			r := set.Requests[i]
			if existing, gerr := a.store.GetRequestTx(tx, r.ID); gerr == nil {
				return domain.NewError(domain.EConflictingRequest,
					"request id already exists", map[string]any{"request_id": existing.ID})
			}
			r.State = domain.StatePending
			r.Seq = seq
			seq++
			if err := a.store.InsertRequestTx(tx, r); err != nil {
				return err
			}
		}
		for _, snap := range set.Snapshots {
			if err := a.store.InsertSnapshotTx(tx, snap); err != nil {
				return err
			}
		}
		for _, f := range set.Fragments {
			if err := a.store.InsertFragmentTx(tx, f); err != nil {
				return err
			}
		}
		return store.MarkIntent(tx, intentID, domain.IntentCommitted)
	})
}

func jsonEqual(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
