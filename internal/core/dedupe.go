package core

import (
	"fmt"
	"sort"
)

// MergeStoredObservations implements the idempotent delivery semantics. The
// canonical record is the earliest delivery by server arrival time, then client
// timestamp, digest and type. Those fields belong to the records themselves, so
// even the conflicting-duplicate "first record" choice does not depend on slice
// or batch order. A repeated digest increments DuplicateCount; a different
// digest is a hard mismatch and leaves the deterministic first record untouched.
func MergeStoredObservations(existing []StoredObservation, incoming []ObservationRecord) ([]StoredObservation, *BizError) {
	type group struct {
		candidates []ObservationRecord
		total      int
	}
	groups := make(map[string]*group, len(existing)+len(incoming))
	for _, stored := range existing {
		key := observationKey(stored.HolderID, stored.ReportSeq)
		item := groups[key]
		if item == nil {
			item = &group{}
			groups[key] = item
		}
		item.candidates = append(item.candidates, stored.ObservationRecord)
		item.total += 1 + stored.DuplicateCount
	}
	for _, record := range incoming {
		key := observationKey(record.HolderID, record.ReportSeq)
		item := groups[key]
		if item == nil {
			item = &group{}
			groups[key] = item
		}
		item.candidates = append(item.candidates, record)
		item.total++
	}

	merged := make([]StoredObservation, 0, len(groups))
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var mismatch *BizError
	for _, key := range keys {
		item := groups[key]
		canonical := item.candidates[0]
		for _, candidate := range item.candidates[1:] {
			if lessObservation(candidate, canonical) {
				canonical = candidate
			}
		}
		if mismatch == nil {
			for _, candidate := range item.candidates {
				if candidate.Digest != canonical.Digest {
					mismatch = &BizError{
						Code:      CodeReportMismatch,
						Message:   fmt.Sprintf("report_seq %d has conflicting digest; deterministic first digest %s was retained", canonical.ReportSeq, canonical.Digest),
						HolderID:  canonical.HolderID,
						ReportSeq: canonical.ReportSeq,
					}
					break
				}
			}
		}
		merged = append(merged, StoredObservation{
			ObservationRecord: canonical,
			DuplicateCount:    item.total - 1,
		})
	}
	return merged, mismatch
}

func lessObservation(a, b ObservationRecord) bool {
	if a.ServerArrivalMS != b.ServerArrivalMS {
		return a.ServerArrivalMS < b.ServerArrivalMS
	}
	if a.ClientTimestampMS != b.ClientTimestampMS {
		return a.ClientTimestampMS < b.ClientTimestampMS
	}
	if a.Digest != b.Digest {
		return a.Digest < b.Digest
	}
	return a.Type < b.Type
}

func observationKey(holderID string, reportSeq int64) string {
	return holderID + "\x00" + fmt.Sprintf("%020d", reportSeq)
}

func DeduplicateDataset(dataset Dataset) ([]StoredObservation, *BizError) {
	return MergeStoredObservations(nil, dataset.Observations)
}

func ObservationCount(stored []StoredObservation) (unique int, duplicates int) {
	for _, record := range stored {
		unique++
		duplicates += record.DuplicateCount
	}
	return unique, duplicates
}
