package engine

import "sort"

type ConflictItem struct {
	Entity     string   `json:"entity"`
	Windows    []string `json:"windows"`
	Offsets    []int64  `json:"offsets"`
	FinalState Op       `json:"final_state"`
}

// DetectConflicts merges state-changing dispositions (create/delete,
// grant/revoke) of the same subject or object across windows. Only windows
// that actually produced a correlation conclusion (full or degraded)
// participate. The merge key is the logical time offset, so the result never
// depends on window processing order.
func DetectConflicts(ws []Window, verdictOf map[string]Verdict) []ConflictItem {
	type disposition struct {
		window string
		event  string
		op     Op
		offset int64
	}
	byEntity := map[string][]disposition{}
	for _, w := range ws {
		v := verdictOf[w.ID]
		if v != VerdictFull && v != VerdictDegraded {
			continue
		}
		for _, e := range w.Events {
			switch e.Op {
			case OpCreate, OpDelete, OpGrant, OpRevoke:
				byEntity[e.Subject] = append(byEntity[e.Subject], disposition{w.ID, e.ID, e.Op, e.TimeOffset})
				byEntity[e.Object] = append(byEntity[e.Object], disposition{w.ID, e.ID, e.Op, e.TimeOffset})
			}
		}
	}

	items := make([]ConflictItem, 0)
	for entity, ds := range byEntity {
		windows := map[string]bool{}
		ops := map[Op]bool{}
		for _, d := range ds {
			windows[d.window] = true
			ops[d.op] = true
		}
		if len(windows) < 2 {
			continue
		}
		conflicting := (ops[OpCreate] && ops[OpDelete]) || (ops[OpGrant] && ops[OpRevoke])
		if !conflicting {
			continue
		}

		best := ds[0]
		for _, d := range ds[1:] {
			if d.offset > best.offset ||
				(d.offset == best.offset && d.window > best.window) ||
				(d.offset == best.offset && d.window == best.window && d.event > best.event) {
				best = d
			}
		}

		wlist := make([]string, 0, len(windows))
		for id := range windows {
			wlist = append(wlist, id)
		}
		sort.Strings(wlist)

		offsetSet := map[int64]bool{}
		for _, d := range ds {
			offsetSet[d.offset] = true
		}
		offsets := make([]int64, 0, len(offsetSet))
		for off := range offsetSet {
			offsets = append(offsets, off)
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

		items = append(items, ConflictItem{
			Entity:     entity,
			Windows:    wlist,
			Offsets:    offsets,
			FinalState: best.op,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Entity < items[j].Entity })
	return items
}
