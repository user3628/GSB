package pipeline

import (
	"headprune/internal/model"
)

// Lineage relations used across replacement detection.
const (
	RelationInitial  = "initial"
	RelationAppend   = "append"
	RelationReplace  = "replace"
	RelationRollback = "rollback"
	RelationFork     = "fork"
)

const (
	KindPolicy    = "policy"
	KindHeads     = "heads"
	KindSentences = "sentences"
)

// LineageVerdict is the result of classifying a new input snapshot.
type LineageVerdict struct {
	Kind             string         `json:"kind"`
	ContentHash      string         `json:"content_hash"`
	Relation         string         `json:"relation"`
	AffectedHeads    []model.HeadID `json:"affected_heads,omitempty"`
	FullInvalidation bool           `json:"full_invalidation"`
	ReusableFromVer  int64          `json:"reusable_from_version,omitempty"`
}

// ClassifyHeads distinguishes head append from whole-shard replacement. A new
// shard for an existing head means replacement; only truly new head IDs are
// treated as append, which invalidates just the new heads' downstream rows.
func ClassifyHeads(oldDS, newDS model.Dataset, prior []model.HeadID) (LineageVerdict, error) {
	hash, err := model.Digest(newDS.Heads)
	if err != nil {
		return LineageVerdict{}, err
	}
	oldByID := map[model.HeadID]model.Head{}
	for _, h := range oldDS.Heads {
		oldByID[h.ID()] = h
	}
	var affected []model.HeadID
	appendedOnly := len(oldDS.Heads) > 0
	for _, h := range newDS.Heads {
		old, existed := oldByID[h.ID()]
		if !existed {
			affected = append(affected, h.ID())
			continue
		}
		oldHash, _ := model.Digest(old)
		newHash, _ := model.Digest(h)
		if oldHash != newHash {
			appendedOnly = false
			affected = append(affected, h.ID())
		}
	}
	v := LineageVerdict{Kind: KindHeads, ContentHash: hash, AffectedHeads: affected}
	if len(oldDS.Heads) == 0 {
		v.Relation = RelationInitial
		v.FullInvalidation = true
		return v, nil
	}
	if appendedOnly && len(affected) > 0 {
		v.Relation = RelationAppend
		return v, nil
	}
	v.Relation = RelationReplace
	v.FullInvalidation = true
	return v, nil
}

// ClassifySentences marks sentence-set replacement; additive-only changes let
// unaffected head error tables stay reusable.
func ClassifySentences(oldDS, newDS model.Dataset) (LineageVerdict, error) {
	hash, err := model.Digest(newDS.Sentences)
	if err != nil {
		return LineageVerdict{}, err
	}
	v := LineageVerdict{Kind: KindSentences, ContentHash: hash}
	if len(oldDS.Sentences) == 0 {
		v.Relation = RelationInitial
		v.FullInvalidation = true
		return v, nil
	}
	oldByID := map[string]model.Sentence{}
	for _, s := range oldDS.Sentences {
		oldByID[s.ID] = s
	}
	appendOnly := true
	for _, s := range newDS.Sentences {
		old, existed := oldByID[s.ID]
		if !existed {
			continue
		}
		oh, _ := model.Digest(old)
		nh, _ := model.Digest(s)
		if oh != nh {
			appendOnly = false
		}
	}
	if len(newDS.Sentences) < len(oldDS.Sentences) {
		appendOnly = false
	}
	if appendOnly && len(newDS.Sentences) >= len(oldDS.Sentences) {
		v.Relation = RelationAppend
		return v, nil
	}
	v.Relation = RelationReplace
	v.FullInvalidation = true
	return v, nil
}

// ClassifyPolicy uses content hash plus the monotonic policy sequence to
// distinguish append, replacement, rollback and fork.
func ClassifyPolicy(oldDS, newDS model.Dataset, knownHashes map[string]int64) (LineageVerdict, error) {
	hash, err := model.Digest(newDS.Policy)
	if err != nil {
		return LineageVerdict{}, err
	}
	v := LineageVerdict{Kind: KindPolicy, ContentHash: hash, FullInvalidation: true}
	if oldDS.Policy.Version == 0 {
		v.Relation = RelationInitial
		return v, nil
	}
	if seq, seen := knownHashes[hash]; seen {
		if seq > newDS.Policy.Version {
			v.Relation = RelationRollback
		} else {
			v.Relation = RelationFork
		}
		return v, nil
	}
	switch {
	case newDS.Policy.Version > oldDS.Policy.Version:
		v.Relation = RelationAppend
	case newDS.Policy.Version < oldDS.Policy.Version:
		v.Relation = RelationRollback
	default:
		v.Relation = RelationReplace
	}
	return v, nil
}

// ReusePlan states which per-head error rows can be reused and from where.
type ReusePlan struct {
	Reusable    map[string]int64 `json:"reusable"`
	Invalidated []model.HeadID   `json:"invalidated"`
}

// BuildReusePlan combines verdicts: any full invalidation discards the whole
// cache; head/sentence appends invalidate only the affected heads.
func BuildReusePlan(headVerdict, sentenceVerdict LineageVerdict, allHeads []model.HeadID, cachedSource map[model.HeadID]int64) ReusePlan {
	plan := ReusePlan{Reusable: map[string]int64{}}
	if headVerdict.FullInvalidation || sentenceVerdict.FullInvalidation {
		plan.Invalidated = append(plan.Invalidated, allHeads...)
		return plan
	}
	affected := map[model.HeadID]bool{}
	for _, id := range headVerdict.AffectedHeads {
		affected[id] = true
	}
	for _, id := range allHeads {
		if affected[id] {
			plan.Invalidated = append(plan.Invalidated, id)
			continue
		}
		if ver, ok := cachedSource[id]; ok {
			plan.Reusable[id.String()] = ver
		} else {
			plan.Invalidated = append(plan.Invalidated, id)
		}
	}
	return plan
}
