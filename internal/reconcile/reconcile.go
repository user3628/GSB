package reconcile

import (
	"fmt"

	"maskrevision/internal/model"
)

const (
	KindUnchanged   = "unchanged"
	KindAppend      = "append"
	KindReplacement = "replacement"
	KindRollback    = "rollback"
	KindFork        = "fork"
)

type Result struct {
	Kind                     string   `json:"kind"`
	ShardID                  string   `json:"shard_id"`
	OldInputFP               string   `json:"old_input_fp"`
	NewInputFP               string   `json:"new_input_fp"`
	OldLogFP                 string   `json:"old_log_fp"`
	NewLogFP                 string   `json:"new_log_fp"`
	CommonPrefixRevisions    int      `json:"common_prefix_revisions"`
	EarliestAffectedRevision int      `json:"earliest_affected_revision"`
	ReusableRevision         int      `json:"reusable_revision"`
	Reasons                  []string `json:"reasons"`
	OldRevisionFPs           []string `json:"old_revision_fps"`
	NewRevisionFPs           []string `json:"new_revision_fps"`
}

type RevisionHistoryEntry struct {
	Revision       int    `json:"revision"`
	ParentRevision int    `json:"parent_revision"`
	Action         string `json:"action"`
	RevisionFP     string `json:"revision_fp"`
}

// ClassifyFingerprints is used when the old full log is no longer available
// and SQLite checkpoint fingerprints are the authoritative committed history.
func ClassifyFingerprints(old []RevisionHistoryEntry, oldShardID, oldInputFP string, newLog model.Log, newInputFP string) Result {
	newFPs := revisionFPs(newLog.Revisions)
	oldFPs := make([]string, len(old))
	for i := range old {
		oldFPs[i] = old[i].RevisionFP
	}
	res := Result{
		Kind: KindUnchanged, ShardID: newLog.ShardID, OldInputFP: oldInputFP, NewInputFP: newInputFP,
		NewLogFP: model.LogPrefixDigest(newLog.Revisions), OldRevisionFPs: oldFPs, NewRevisionFPs: newFPs,
	}
	if oldInputFP != "" && oldInputFP != newInputFP {
		res.Kind = KindReplacement
		res.EarliestAffectedRevision = firstRevision(newLog, 1)
		res.Reasons = append(res.Reasons, "initial shard content fingerprint changed (retransmission or mutation)")
		return res
	}
	if oldShardID != "" && newLog.ShardID != oldShardID {
		res.Kind = KindReplacement
		res.EarliestAffectedRevision = firstRevision(newLog, 1)
		res.Reasons = append(res.Reasons, "shard id changed")
		return res
	}
	common := 0
	for common < len(oldFPs) && common < len(newFPs) && oldFPs[common] == newFPs[common] {
		common++
	}
	res.CommonPrefixRevisions = common
	if common == len(oldFPs) && common == len(newFPs) {
		res.Reasons = append(res.Reasons, "checkpoint and incoming revision fingerprints are byte-identical")
		return res
	}
	if common == len(oldFPs) && common < len(newFPs) {
		next := newLog.Revisions[common]
		oldHead := 0
		if len(old) > 0 {
			oldHead = old[len(old)-1].Revision
		}
		if next.ParentRevision != 0 && next.ParentRevision != oldHead {
			res.Kind = KindFork
			res.EarliestAffectedRevision = next.Revision
			res.ReusableRevision = oldHead
			res.Reasons = append(res.Reasons, fmt.Sprintf("appended revision %d declares parent %d, but checkpoint head is %d", next.Revision, next.ParentRevision, oldHead))
			return res
		}
		res.Kind = KindAppend
		res.EarliestAffectedRevision = next.Revision
		res.ReusableRevision = oldHead
		res.Reasons = append(res.Reasons, "checkpointed revision fingerprints are a strict prefix of the incoming log")
		return res
	}
	if common == len(newFPs) && common < len(oldFPs) {
		res.Kind = KindRollback
		if common < len(old) {
			res.EarliestAffectedRevision = old[common].Revision
		} else {
			res.EarliestAffectedRevision = firstRevision(newLog, 1)
		}
		res.ReusableRevision = revisionAt(old, common)
		res.Reasons = append(res.Reasons, "incoming log truncates the checkpointed revision sequence")
		return res
	}
	next := newLog.Revisions[common]
	oldNext := old[common]
	forkHint := newLog.ForkOf != "" || newLog.Branch != "" ||
		(next.ParentRevision != 0 && next.ParentRevision != oldNext.ParentRevision) ||
		(next.ParentRevision != 0 && next.ParentRevision < next.Revision-1)
	if forkHint {
		res.Kind = KindFork
		res.EarliestAffectedRevision = next.Revision
		res.ReusableRevision = revisionAt(old, common)
		res.Reasons = append(res.Reasons, "incoming branch/parent metadata diverges after the common checkpoint prefix")
		return res
	}
	res.Kind = KindReplacement
	res.EarliestAffectedRevision = next.Revision
	res.ReusableRevision = revisionAt(old, common)
	if common == 0 {
		res.Reasons = append(res.Reasons, "revision 1 differs, so the entire replay is replaced")
	} else {
		res.Reasons = append(res.Reasons, fmt.Sprintf("revision %d action content was edited; earlier %d revisions remain reusable", next.Revision, common))
	}
	return res
}

func Classify(oldLog, newLog model.Log, oldInputFP, newInputFP string) Result {
	oldFPs := revisionFPs(oldLog.Revisions)
	newFPs := revisionFPs(newLog.Revisions)
	res := Result{
		Kind: KindUnchanged, ShardID: newLog.ShardID, OldInputFP: oldInputFP, NewInputFP: newInputFP,
		OldLogFP: model.LogPrefixDigest(oldLog.Revisions), NewLogFP: model.LogPrefixDigest(newLog.Revisions),
		OldRevisionFPs: oldFPs, NewRevisionFPs: newFPs,
	}
	if oldInputFP != newInputFP {
		res.Kind = KindReplacement
		res.EarliestAffectedRevision = firstRevision(newLog, 1)
		res.Reasons = append(res.Reasons, "initial shard content fingerprint changed (retransmission or mutation)")
		return res
	}
	if oldLog.ShardID != "" && newLog.ShardID != oldLog.ShardID {
		res.Kind = KindReplacement
		res.EarliestAffectedRevision = firstRevision(newLog, 1)
		res.Reasons = append(res.Reasons, "shard id changed")
		return res
	}
	common := 0
	for common < len(oldFPs) && common < len(newFPs) && oldFPs[common] == newFPs[common] {
		common++
	}
	res.CommonPrefixRevisions = common
	if common == len(oldFPs) && common == len(newFPs) {
		res.Reasons = append(res.Reasons, "revision fingerprints are byte-identical")
		return res
	}
	if common == len(oldFPs) && common < len(newFPs) {
		next := newLog.Revisions[common]
		oldHead := 0
		if len(oldLog.Revisions) > 0 {
			oldHead = oldLog.Revisions[len(oldLog.Revisions)-1].Revision
		}
		if next.ParentRevision != 0 && next.ParentRevision != oldHead {
			res.Kind = KindFork
			res.EarliestAffectedRevision = next.Revision
			res.Reasons = append(res.Reasons,
				fmt.Sprintf("appended revision %d declares parent %d, but old head is %d", next.Revision, next.ParentRevision, oldHead),
				"undo followed by an insertion on an earlier revision is an explicit branch")
			res.ReusableRevision = commonRevision(oldLog, common)
			return res
		}
		res.Kind = KindAppend
		res.EarliestAffectedRevision = next.Revision
		res.ReusableRevision = oldHead
		res.Reasons = append(res.Reasons, "old revision fingerprint sequence is a strict prefix of the new log")
		return res
	}
	if common == len(newFPs) && common < len(oldFPs) {
		res.Kind = KindRollback
		res.EarliestAffectedRevision = firstRevisionAfterPrefix(oldLog, newLog, common)
		res.ReusableRevision = commonRevision(newLog, common)
		res.Reasons = append(res.Reasons, "new log is a truncation of the checkpointed history")
		return res
	}
	// Both logs continue after the common prefix but differ there. Explicit
	// parent/branch metadata wins; otherwise this is a content replacement of
	// the first changed revision and all descendants.
	next := newLog.Revisions[common]
	oldNext := oldLog.Revisions[common]
	forkHint := newLog.ForkOf != "" || newLog.Branch != "" || (next.ParentRevision != 0 && next.ParentRevision != oldNext.ParentRevision) ||
		(next.ParentRevision != 0 && next.ParentRevision < next.Revision-1)
	if forkHint {
		res.Kind = KindFork
		res.EarliestAffectedRevision = next.Revision
		res.ReusableRevision = commonRevision(oldLog, common)
		res.Reasons = append(res.Reasons, "explicit branch/parent metadata diverges after the common prefix")
		return res
	}
	res.Kind = KindReplacement
	res.EarliestAffectedRevision = next.Revision
	res.ReusableRevision = commonRevision(oldLog, common)
	if common == 0 {
		res.Reasons = append(res.Reasons, "revision 1 differs, so the entire replay is replaced")
	} else {
		res.Reasons = append(res.Reasons, fmt.Sprintf("revision %d action content was edited; earlier %d revisions remain reusable", next.Revision, common))
	}
	return res
}

func revisionFPs(revs []model.Revision) []string {
	out := make([]string, len(revs))
	for i, rev := range revs {
		out[i] = model.RevisionDigest(rev)
	}
	return out
}

func commonRevision(log model.Log, common int) int {
	if common <= 0 || len(log.Revisions) == 0 {
		return 0
	}
	if common > len(log.Revisions) {
		common = len(log.Revisions)
	}
	return log.Revisions[common-1].Revision
}

func firstRevision(log model.Log, fallback int) int {
	if len(log.Revisions) > 0 {
		return log.Revisions[0].Revision
	}
	return fallback
}

func firstRevisionAfterPrefix(old, new model.Log, common int) int {
	if common < len(old.Revisions) {
		return old.Revisions[common].Revision
	}
	return firstRevision(new, 1)
}

func revisionAt(old []RevisionHistoryEntry, common int) int {
	if common <= 0 || len(old) == 0 {
		return 0
	}
	if common > len(old) {
		common = len(old)
	}
	return old[common-1].Revision
}
