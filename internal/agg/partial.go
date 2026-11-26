package agg

import (
	"sort"

	"logagg/internal/errs"
)

type Accumulator struct {
	Count int64 `json:"count"`
	Sum   int64 `json:"sum"`
	Min   int64 `json:"min"`
	Max   int64 `json:"max"`
	Has   bool  `json:"has"`
}

type SegmentSum struct {
	SessionID      string                 `json:"session_id"`
	ShardID        string                 `json:"shard_id"`
	BaseVersion    int64                  `json:"base_version"`
	Partition      int                    `json:"partition"`
	BlockNo        int                    `json:"block_no"`
	SegmentStart   int                    `json:"segment_start"`
	SegmentEnd     int                    `json:"segment_end"`
	DefinitionHash string                 `json:"definition_hash"`
	Groups         map[int64]*Accumulator `json:"groups"`
}

func NewSegmentSum(sessionID, shardID string, version int64, partition, blockNo, start, end int, definitionHash string) *SegmentSum {
	return &SegmentSum{
		SessionID:      sessionID,
		ShardID:        shardID,
		BaseVersion:    version,
		Partition:      partition,
		BlockNo:        blockNo,
		SegmentStart:   start,
		SegmentEnd:     end,
		DefinitionHash: definitionHash,
		Groups:         make(map[int64]*Accumulator),
	}
}

func (s *SegmentSum) Window() string {
	return segmentWindow(s.ShardID, s.BaseVersion, s.BlockNo, s.SegmentStart, s.SegmentEnd)
}

func segmentWindow(shardID string, version int64, blockNo, start, end int) string {
	return shardID + ":v" + intToString(version) + ":b" + intToString(int64(blockNo)) + ":r" +
		intToString(int64(start)) + "-" + intToString(int64(end))
}

func (s *SegmentSum) AddRows(groups, metrics []int64) error {
	if len(groups) != len(metrics) {
		return errs.New(errs.EInvalidRequest, "group/metric column length mismatch: %d != %d", len(groups), len(metrics))
	}
	if len(groups) != s.SegmentEnd-s.SegmentStart {
		return errs.New(errs.EInvalidRequest, "decoded row count %d does not match segment interval [%d,%d)", len(groups), s.SegmentStart, s.SegmentEnd)
	}
	for i := range groups {
		key := groups[i]
		acc := s.Groups[key]
		if acc == nil {
			acc = &Accumulator{}
			s.Groups[key] = acc
		}
		if err := acc.Add(metrics[i], s.Window(), key); err != nil {
			return err
		}
	}
	return nil
}

func (a *Accumulator) Add(value int64, window string, group int64) error {
	newCount, ok := addInt64(a.Count, 1)
	if !ok {
		return errs.NumericOverflow(window, group, "count overflow")
	}
	newSum, ok := addInt64(a.Sum, value)
	if !ok {
		return errs.NumericOverflow(window, group, "sum overflow while adding fixed-point value %d", value)
	}
	a.Count = newCount
	a.Sum = newSum
	if !a.Has {
		a.Min = value
		a.Max = value
		a.Has = true
	} else {
		if value < a.Min {
			a.Min = value
		}
		if value > a.Max {
			a.Max = value
		}
	}
	return nil
}

func (a *Accumulator) Merge(other *Accumulator, window string, group int64) error {
	if other == nil || !other.Has {
		return nil
	}
	count, ok := addInt64(a.Count, other.Count)
	if !ok {
		return errs.NumericOverflow(window, group, "merged count overflow")
	}
	sum, ok := addInt64(a.Sum, other.Sum)
	if !ok {
		return errs.NumericOverflow(window, group, "merged sum overflow")
	}
	if !a.Has {
		*a = *other
		a.Count = count
		a.Sum = sum
		return nil
	}
	a.Count = count
	a.Sum = sum
	if other.Min < a.Min {
		a.Min = other.Min
	}
	if other.Max > a.Max {
		a.Max = other.Max
	}
	return nil
}

func (s *SegmentSum) Clone() *SegmentSum {
	if s == nil {
		return nil
	}
	out := *s
	out.Groups = make(map[int64]*Accumulator, len(s.Groups))
	for key, acc := range s.Groups {
		copyAcc := *acc
		out.Groups[key] = &copyAcc
	}
	return &out
}

type TotalSum struct {
	Groups map[int64]*Accumulator `json:"groups"`
}

func NewTotalSum() *TotalSum {
	return &TotalSum{Groups: make(map[int64]*Accumulator)}
}

func (t *TotalSum) MergeSegment(segment *SegmentSum) error {
	if segment == nil {
		return nil
	}
	keys := make([]int64, 0, len(segment.Groups))
	for key := range segment.Groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		acc := t.Groups[key]
		if acc == nil {
			acc = &Accumulator{}
			t.Groups[key] = acc
		}
		if err := acc.Merge(segment.Groups[key], "session-total:"+segment.Window(), key); err != nil {
			return err
		}
	}
	return nil
}

func SortedKeys(groups map[int64]*Accumulator) []int64 {
	keys := make([]int64, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
