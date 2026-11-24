package ledger

import (
	"sort"
)

type scheduledSnapshot struct {
	Fact            FactSnapshot
	EffectiveMinute int64
}

type SnapshotSchedule struct {
	items []scheduledSnapshot
}

func NewSnapshotSchedule(facts []FactSnapshot) *SnapshotSchedule {
	items := make([]scheduledSnapshot, 0, len(facts))
	for _, f := range facts {
		items = append(items, scheduledSnapshot{
			Fact:            f,
			EffectiveMinute: MinuteOf(f.Snapshot.GeneratedAtMS),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].EffectiveMinute != items[j].EffectiveMinute {
			return items[i].EffectiveMinute < items[j].EffectiveMinute
		}
		return items[i].Fact.Snapshot.Version < items[j].Fact.Snapshot.Version
	})
	return &SnapshotSchedule{items: items}
}

func (s *SnapshotSchedule) At(minute int64) (*FactSnapshot, bool) {
	for i := len(s.items) - 1; i >= 0; i-- {
		if s.items[i].EffectiveMinute <= minute {
			return &s.items[i].Fact, true
		}
	}
	return nil, false
}

func (s *SnapshotSchedule) LatestCommitted() (*FactSnapshot, bool) {
	if len(s.items) == 0 {
		return nil, false
	}
	return &s.items[len(s.items)-1].Fact, true
}

func (s *SnapshotSchedule) Count() int {
	return len(s.items)
}
