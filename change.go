package rewardpack

import (
	"context"
)

// Change kinds are stable labels in invalidation records.
const (
	ChangeInitialBuild   = "initial_build"
	ChangeAppend         = "append"
	ChangeAnnotationEdit = "annotation_edit"
	ChangeSourceReplace  = "source_replace"
	ChangeRollback       = "rollback"
	ChangeConfig         = "config_change"
	ChangeNoop           = "noop"
)

func AnalyzeChange(previous *Manifest, current Manifest, cfg Config, pending []PendingChange) (Invalidation, bool, error) {
	return analyzeChange(previous, current, cfg, pending, false)
}

func analyzeChange(previous *Manifest, current Manifest, cfg Config, pending []PendingChange, historical bool) (Invalidation, bool, error) {
	if previous == nil {
		allPairs := sortedManifestPairIDs(current)
		inv := Invalidation{
			ChangeKind:            ChangeInitialBuild,
			AffectedSources:       sortedManifestSources(current),
			EarliestStage:         StageNormalize,
			InvalidatedStages:     append([]string(nil), StageOrder...),
			AffectedPairs:         allPairs,
			AllPairs:              true,
			CurrentManifestDigest: current.Digest(),
			CurrentConfigVersion:  cfg.Version,
			Reason:                "no committed build manifest exists",
		}
		inv.Fingerprint = digestJSON(inv)
		return inv, true, nil
	}
	if cfg.Version < previous.ConfigVersion {
		inv := Invalidation{
			ChangeKind:             ChangeRollback,
			AffectedSources:        sortedManifestSources(current),
			EarliestStage:          StageNormalize,
			InvalidatedStages:      append([]string(nil), StageOrder...),
			AllPairs:               true,
			PreviousManifestDigest: previous.Digest(),
			CurrentManifestDigest:  current.Digest(),
			PreviousConfigVersion:  previous.ConfigVersion,
			CurrentConfigVersion:   cfg.Version,
			Reason:                 "configuration snapshot version regressed",
		}
		inv.Fingerprint = digestJSON(inv)
		return inv, true, &BusinessError{Code: CodeVersionRollback, Expected: previous.ConfigVersion, Actual: cfg.Version, Context: "build rejected because config version is older than committed manifest", Message: "config version rollback"}
	}
	if previous.Digest() == current.Digest() {
		return Invalidation{ChangeKind: ChangeNoop}, false, nil
	}

	changedPairs := changedPairs(previous, &current)
	changedSources := changedSources(previous, &current)
	hasReplace, hasEdit, hasAppend, hasConfig := false, false, false, false
	for _, change := range pending {
		switch change.Kind {
		case "source_replace":
			hasReplace = true
		case "edit":
			hasEdit = true
		case "append":
			hasAppend = true
		case "config_change":
			hasConfig = true
		}
	}
	for _, pairID := range changedPairs {
		_, existed := previous.PairDigests[pairID]
		_, exists := current.PairDigests[pairID]
		if existed && exists {
			hasEdit = true
		}
		if !existed && exists {
			hasAppend = true
		}
	}
	configStage := EarliestChangedStageFromManifest(*previous, current)
	if configStage != "" {
		hasConfig = true
	}
	kind := ChangeConfig
	switch {
	case historical:
		kind = ChangeRollback
	case hasReplace:
		kind = ChangeSourceReplace
	case hasEdit:
		kind = ChangeAnnotationEdit
	case hasAppend:
		kind = ChangeAppend
	case hasConfig:
		kind = ChangeConfig
	}
	earliest := configStage
	if earliest == "" {
		earliest = StageNormalize
	}
	if len(changedPairs) == 0 && configStage == "" && hasConfig {
		earliest = StageReport
	}
	invalidated := invalidatedFrom(earliest)
	allPairs := false
	affectedPairs := append([]string(nil), changedPairs...)
	if configStage != "" {
		allPairs = true
		affectedPairs = nil
	}
	affectedSources := changedSources
	if allPairs {
		affectedSources = unionStrings(sortedManifestSources(*previous), sortedManifestSources(current))
	}
	firstDiff := ""
	if len(changedPairs) > 0 {
		firstDiff = changedPairs[0]
	}
	inv := Invalidation{
		ChangeKind:             kind,
		FirstDiffPairID:        firstDiff,
		AffectedSources:        affectedSources,
		EarliestStage:          earliest,
		InvalidatedStages:      invalidated,
		AffectedPairs:          affectedPairs,
		AllPairs:               allPairs,
		PreviousManifestDigest: previous.Digest(),
		CurrentManifestDigest:  current.Digest(),
		PreviousConfigVersion:  previous.ConfigVersion,
		CurrentConfigVersion:   cfg.Version,
		Reason:                 changeReason(kind, earliest, changedPairs, changedSources),
	}
	inv.Fingerprint = digestJSON(inv)
	return inv, true, nil
}

func (s *Store) RecordInvalidation(ctx context.Context, inv Invalidation) error {
	if inv.ChangeKind == ChangeNoop || inv.ChangeKind == "" {
		return nil
	}
	return s.recordInvalidation(ctx, inv)
}

func changeReason(kind, earliest string, pairs, sources []string) string {
	if len(pairs) > 0 {
		return kind + ": " + earliest + " first differs at " + pairs[0]
	}
	if len(sources) > 0 {
		return kind + ": " + earliest + " affects source " + sources[0]
	}
	return kind + ": " + earliest
}

func EarliestChangedStageFromManifest(old, next Manifest) string {
	switch {
	case old.NormalizationDigest != next.NormalizationDigest:
		return StageNormalize
	case old.ScoringDigest != next.ScoringDigest:
		return StageScore
	case old.FeaturesDigest != next.FeaturesDigest:
		return StageFeatures
	case old.BudgetDigest != next.BudgetDigest:
		return StageBudget
	case old.ConfigDigest != next.ConfigDigest || old.ConfigVersion != next.ConfigVersion:
		return StageReport
	default:
		return ""
	}
}

func invalidatedFrom(stage string) []string {
	index := StageIndex(stage)
	if index < 0 {
		return append([]string(nil), StageOrder...)
	}
	return append([]string(nil), StageOrder[index:]...)
}

func changedPairs(previous, current *Manifest) []string {
	set := map[string]bool{}
	for pairID, digest := range previous.PairDigests {
		if currentDigest, ok := current.PairDigests[pairID]; !ok || currentDigest != digest || previous.PairSources[pairID] != current.PairSources[pairID] {
			set[pairID] = true
		}
	}
	for pairID := range current.PairDigests {
		if _, ok := previous.PairDigests[pairID]; !ok {
			set[pairID] = true
		}
	}
	return sortedStringKeys(set)
}

func changedSources(previous, current *Manifest) []string {
	set := map[string]bool{}
	for source, digest := range previous.SourceDigests {
		if currentDigest, ok := current.SourceDigests[source]; !ok || currentDigest != digest {
			set[source] = true
		}
	}
	for source := range current.SourceDigests {
		if _, ok := previous.SourceDigests[source]; !ok {
			set[source] = true
		}
	}
	for source, pairs := range previous.SourcePairs {
		if CanonicalString(pairs) != CanonicalString(current.SourcePairs[source]) {
			set[source] = true
		}
	}
	for source := range current.SourcePairs {
		if _, ok := previous.SourcePairs[source]; !ok {
			set[source] = true
		}
	}
	return sortedStringKeys(set)
}

func sortedManifestPairIDs(m Manifest) []string { return sortedStringKeys(m.PairDigests) }

func sortedManifestSources(m Manifest) []string {
	set := map[string]bool{}
	for source := range m.SourceDigests {
		set[source] = true
	}
	for _, source := range m.PairSources {
		set[source] = true
	}
	return sortedStringKeys(set)
}

func unionStrings(a, b []string) []string {
	set := map[string]bool{}
	for _, value := range a {
		set[value] = true
	}
	for _, value := range b {
		set[value] = true
	}
	return sortedStringKeys(set)
}
