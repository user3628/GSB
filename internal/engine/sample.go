package engine

import (
	"fmt"
	"math/rand"
)

// Injection names accepted via CLI / options.
const (
	InjectDupEventID           = "dup-event-id"
	InjectOrphanRef            = "orphan-ref"
	InjectTinyWindowBudget     = "tiny-window-budget"
	InjectGlobalBudgetShortage = "global-budget-shortage"
	InjectConflictMerge        = "conflict-merge"
	InjectSnapshotVersionSkip  = "snapshot-version-skip"
)

func eventID(windowIdx, eventIdx int) string {
	return fmt.Sprintf("E-%03d-%02d", windowIdx, eventIdx)
}

// GenerateSample produces a fully deterministic local sample. The same seed,
// window count and injection list always yield byte-identical windows.
func GenerateSample(seed int64, numWindows int, inject []string) []Window {
	inj := map[string]bool{}
	for _, name := range inject {
		inj[name] = true
	}

	ws := make([]Window, 0, numWindows+8)
	for i := 0; i < numWindows; i++ {
		r := rand.New(rand.NewSource(seed*7919 + int64(i)*104729))
		subjects := []string{fmt.Sprintf("S-%03d-a", i), fmt.Sprintf("S-%03d-b", i)}
		objects := []string{fmt.Sprintf("O-%03d-x", i), fmt.Sprintf("O-%03d-y", i)}
		nEvents := 5 + r.Intn(5)
		events := make([]Event, 0, nEvents)
		events = append(events, Event{ID: eventID(i, 0), Subject: subjects[0], Object: objects[0], Op: OpCreate, TimeOffset: 0, Weight: 1 + r.Intn(10)})
		events = append(events, Event{ID: eventID(i, 1), Subject: subjects[1], Object: objects[1], Op: OpCreate, TimeOffset: 1, Weight: 1 + r.Intn(10)})
		ops := []Op{OpRead, OpUpdate, OpDelete, OpGrant, OpRevoke}
		for j := 2; j < nEvents; j++ {
			events = append(events, Event{
				ID:         eventID(i, j),
				Subject:    subjects[r.Intn(len(subjects))],
				Object:     objects[r.Intn(len(objects))],
				Op:         ops[r.Intn(len(ops))],
				TimeOffset: int64(j),
				Weight:     1 + r.Intn(10),
			})
		}
		r.Shuffle(len(events), func(a, b int) { events[a], events[b] = events[b], events[a] })
		ws = append(ws, Window{
			ID:     fmt.Sprintf("W-%03d", i),
			Events: events,
			Claim:  ResourceClaim{Depth: 2 + r.Intn(5), Memory: int64(100 * (1 + r.Intn(4)))},
		})
	}

	if inj[InjectDupEventID] {
		ws = append(ws, Window{
			ID: "W-INJ-DUP",
			Events: []Event{
				{ID: "E-DUP-0", Subject: "S-DUP-a", Object: "O-DUP-x", Op: OpCreate, TimeOffset: 0, Weight: 5},
				{ID: "E-DUP-0", Subject: "S-DUP-a", Object: "O-DUP-x", Op: OpRead, TimeOffset: 1, Weight: 4},
			},
			Claim: ResourceClaim{Depth: 2, Memory: 100},
		})
	}

	if inj[InjectOrphanRef] {
		ws = append(ws, Window{
			ID: "W-INJ-ORPHAN",
			Events: []Event{
				{ID: "E-ORP-0", Subject: "S-ORP-a", Object: "O-ORP-x", Op: OpCreate, TimeOffset: 0, Weight: 10},
				{ID: "E-ORP-1", Subject: "S-ORP-a", Object: "O-ORP-x", Op: OpUpdate, TimeOffset: 1, Weight: 10},
				{ID: "E-ORP-2", Subject: "S-ORP-a", Object: "GHOST-OBJECT", Op: OpRead, TimeOffset: 2, Weight: 10},
				{ID: "E-ORP-3", Subject: "GHOST-SUBJECT", Object: "O-ORP-x", Op: OpGrant, TimeOffset: 3, Weight: 10},
			},
			Claim: ResourceClaim{Depth: 3, Memory: 120},
		})
	}

	if inj[InjectTinyWindowBudget] {
		ws = append(ws, Window{
			ID: "W-INJ-DEEP",
			Events: []Event{
				{ID: "E-DEEP-0", Subject: "S-DEEP-a", Object: "O-DEEP-x", Op: OpCreate, TimeOffset: 0, Weight: 7},
				{ID: "E-DEEP-1", Subject: "S-DEEP-a", Object: "O-DEEP-x", Op: OpUpdate, TimeOffset: 1, Weight: 6},
				{ID: "E-DEEP-2", Subject: "S-DEEP-a", Object: "O-DEEP-x", Op: OpRead, TimeOffset: 2, Weight: 5},
			},
			Claim: ResourceClaim{Depth: 100000, Memory: 100},
		})
	}

	if inj[InjectGlobalBudgetShortage] {
		for k := 0; k < 3; k++ {
			name := fmt.Sprintf("W-INJ-HOG-%d", k)
			subj := fmt.Sprintf("S-HOG-%d", k)
			obj := fmt.Sprintf("O-HOG-%d", k)
			ws = append(ws, Window{
				ID: name,
				Events: []Event{
					{ID: fmt.Sprintf("E-HOG-%d-0", k), Subject: subj, Object: obj, Op: OpCreate, TimeOffset: 0, Weight: 10},
					{ID: fmt.Sprintf("E-HOG-%d-1", k), Subject: subj, Object: obj, Op: OpUpdate, TimeOffset: 1, Weight: 9},
				},
				Claim: ResourceClaim{Depth: 3, Memory: 1500},
			})
		}
	}

	if inj[InjectConflictMerge] {
		ws = append(ws,
			Window{
				ID: "W-INJ-CONF-A",
				Events: []Event{
					{ID: "E-CFA-0", Subject: "S-CFA-a", Object: "SHARED-1", Op: OpCreate, TimeOffset: 0, Weight: 10},
					{ID: "E-CFA-1", Subject: "S-CFA-a", Object: "SHARED-1", Op: OpGrant, TimeOffset: 1, Weight: 10},
					{ID: "E-CFA-2", Subject: "S-CFA-a", Object: "SHARED-2", Op: OpCreate, TimeOffset: 2, Weight: 10},
				},
				Claim: ResourceClaim{Depth: 2, Memory: 100},
			},
			Window{
				ID: "W-INJ-CONF-B",
				Events: []Event{
					{ID: "E-CFB-0", Subject: "S-CFB-b", Object: "SHARED-1", Op: OpCreate, TimeOffset: 0, Weight: 10},
					{ID: "E-CFB-1", Subject: "S-CFB-b", Object: "SHARED-1", Op: OpRevoke, TimeOffset: 5, Weight: 10},
					{ID: "E-CFB-2", Subject: "S-CFB-b", Object: "SHARED-2", Op: OpCreate, TimeOffset: 3, Weight: 10},
					{ID: "E-CFB-3", Subject: "S-CFB-b", Object: "SHARED-2", Op: OpDelete, TimeOffset: 6, Weight: 10},
				},
				Claim: ResourceClaim{Depth: 2, Memory: 100},
			},
		)
	}

	return ws
}
