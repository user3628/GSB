package engine

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"recoveryengine/internal/model"
)

type ReservationView struct {
	ID            string `json:"id"`
	DrillID       string `json:"drill_id"`
	ComponentID   string `json:"component_id"`
	CPUMillicores int64  `json:"cpu_millicores"`
	MemoryBytes   int64  `json:"memory_bytes"`
	AcquiredAt    string `json:"acquired_at"`
}

type reservationTracker struct {
	mu       sync.Mutex
	next     uint64
	active   map[string]ReservationView
	released uint64
}

func newReservationTracker() *reservationTracker {
	return &reservationTracker{active: map[string]ReservationView{}}
}

func (t *reservationTracker) Acquire(drillID, componentID string, resources model.Resource) ReservationView {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next++
	reservation := ReservationView{
		ID:            fmt.Sprintf("reservation-%d", t.next),
		DrillID:       drillID,
		ComponentID:   componentID,
		CPUMillicores: resources.CPUMillicores,
		MemoryBytes:   resources.MemoryBytes,
		AcquiredAt:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	t.active[reservation.ID] = reservation
	return reservation
}

func (t *reservationTracker) Release(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.active[id]; ok {
		delete(t.active, id)
		t.released++
	}
}

func (t *reservationTracker) Snapshot() []ReservationView {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]ReservationView, 0, len(t.active))
	for _, reservation := range t.active {
		out = append(out, reservation)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DrillID == out[j].DrillID {
			return out[i].ComponentID < out[j].ComponentID
		}
		return out[i].DrillID < out[j].DrillID
	})
	return out
}

func (t *reservationTracker) ReleasedTotal() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.released
}
