package rewardpack

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
)

// VerifyResult contains the two bundles when an incremental build is proven
// byte-identical to a from-scratch build.
type VerifyResult struct {
	Incremental ArtifactBundle `json:"incremental"`
	Full        ArtifactBundle `json:"full"`
}

// VerifyIncremental rebuilds the current repository incrementally, then builds
// the same config and annotations from an empty database. Scores, feature
// summaries, retention decisions and the report must match byte-for-byte.
func (e *Engine) VerifyIncremental(ctx context.Context) (*VerifyResult, error) {
	incremental, err := e.Build(ctx, BuildOptions{})
	if err != nil {
		return nil, err
	}
	cfg, err := e.Store.ActiveConfig(ctx)
	if err != nil {
		return nil, err
	}
	annotations, err := e.Store.LoadAnnotations(ctx)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "rewardpack-full-build-*")
	if err != nil {
		return nil, errorWith(CodeInternal, err)
	}
	defer os.RemoveAll(dir)
	fullStore, err := OpenStoreContext(ctx, dir+string(os.PathSeparator)+"full.sqlite")
	if err != nil {
		return nil, err
	}
	if err := fullStore.ApplyConfig(ctx, cfg); err != nil {
		_ = fullStore.Close()
		return nil, err
	}
	bySource := map[string][]Annotation{}
	for _, annotation := range annotations {
		bySource[annotation.Source] = append(bySource[annotation.Source], annotation)
	}
	sourceRecords, err := e.Store.LoadSources(ctx)
	if err != nil {
		_ = fullStore.Close()
		return nil, err
	}
	sourceNames := map[string]bool{}
	for _, record := range sourceRecords {
		sourceNames[record.Source] = true
	}
	for _, source := range sortedStringKeys(bySource) {
		sourceNames[source] = true
	}
	for _, source := range sortedStringKeys(sourceNames) {
		if err := fullStore.ReplaceSource(ctx, source, bySource[source]); err != nil {
			_ = fullStore.Close()
			return nil, err
		}
	}
	full, err := NewEngine(fullStore).Build(ctx, BuildOptions{})
	closeErr := fullStore.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, errorWith(CodeInternal, closeErr)
	}
	if err := CompareArtifactBundles(incremental.Bundle, full.Bundle); err != nil {
		return nil, err
	}
	return &VerifyResult{Incremental: incremental.Bundle, Full: full.Bundle}, nil
}

// CompareArtifactBundles performs the byte-level consistency check and returns
// E_INCREMENTAL_MISMATCH with the first differing pair and both values.
func CompareArtifactBundles(incremental, full ArtifactBundle) error {
	if mismatch := comparePairArray("normalize", normalizedMap(incremental.Normalized), normalizedMap(full.Normalized)); mismatch != nil {
		return mismatch
	}
	if mismatch := comparePairArray("score", scoreMap(incremental.Scores), scoreMap(full.Scores)); mismatch != nil {
		return mismatch
	}
	if mismatch := comparePairArray("features", featureMap(incremental.Features), featureMap(full.Features)); mismatch != nil {
		return mismatch
	}
	if !bytes.Equal(canonicalMust(incremental.Budget), canonicalMust(full.Budget)) {
		return compareBudget(incremental.Budget, full.Budget, incremental.Scores, full.Scores)
	}
	if !bytes.Equal(canonicalMust(incremental.Report), canonicalMust(full.Report)) {
		return compareReport(incremental.Report, full.Report)
	}
	return nil
}

type pairRecord struct {
	id     string
	actual string
	expect string
}

func comparePairArray(section string, actual, expected map[string]string) error {
	set := map[string]bool{}
	for id := range actual {
		set[id] = true
	}
	for id := range expected {
		set[id] = true
	}
	ids := sortedStringKeys(set)
	for _, id := range ids {
		a, aOK := actual[id]
		e, eOK := expected[id]
		if aOK && eOK && a == e {
			continue
		}
		if !aOK {
			a = "<missing>"
		}
		if !eOK {
			e = "<missing>"
		}
		return &BusinessError{
			Code: CodeIncrementalMismatch, PairID: id, Stage: section,
			Expected: truncateValue(e), Actual: truncateValue(a),
			Context: section + " artifact differs between incremental and full build",
			Message: section + " mismatch at first differing pair",
		}
	}
	return nil
}

func normalizedMap(values []NormalizedPair) map[string]string {
	out := make(map[string]string, len(values))
	for _, value := range values {
		out[value.PairID] = string(canonicalMust(value))
	}
	return out
}

func scoreMap(values []ScoreResult) map[string]string {
	out := make(map[string]string, len(values))
	for _, value := range values {
		out[value.PairID] = string(canonicalMust(value))
	}
	return out
}

func featureMap(values []FeatureResult) map[string]string {
	out := make(map[string]string, len(values))
	for _, value := range values {
		out[value.PairID] = string(canonicalMust(value))
	}
	return out
}

func compareBudget(actual, expected BudgetResult, actualScores, expectedScores []ScoreResult) error {
	actualRetained := map[string]RetainedPair{}
	for _, pair := range actual.Retained {
		actualRetained[pair.PairID] = pair
	}
	expectedRetained := map[string]RetainedPair{}
	for _, pair := range expected.Retained {
		expectedRetained[pair.PairID] = pair
	}
	actualEliminated := map[string]EliminatedPair{}
	for _, pair := range actual.Eliminated {
		actualEliminated[pair.PairID] = pair
	}
	expectedEliminated := map[string]EliminatedPair{}
	for _, pair := range expected.Eliminated {
		expectedEliminated[pair.PairID] = pair
	}
	set := map[string]bool{}
	for id := range actualRetained {
		set[id] = true
	}
	for id := range expectedRetained {
		set[id] = true
	}
	for id := range actualEliminated {
		set[id] = true
	}
	for id := range expectedEliminated {
		set[id] = true
	}
	ids := sortedStringKeys(set)
	for _, id := range ids {
		actualValue := decisionValue(id, actualRetained, actualEliminated)
		expectedValue := decisionValue(id, expectedRetained, expectedEliminated)
		if actualValue == expectedValue {
			continue
		}
		return &BusinessError{
			Code: CodeIncrementalMismatch, PairID: id, Stage: StageBudget,
			Expected: expectedValue, Actual: actualValue,
			Context: "retention decision differs between incremental and full build",
			Message: "budget mismatch at first differing pair",
		}
	}
	firstID := firstScoreID(actualScores, expectedScores)
	return &BusinessError{
		Code: CodeIncrementalMismatch, PairID: firstID, Stage: StageBudget,
		Expected: truncateValue(string(canonicalMust(expected))), Actual: truncateValue(string(canonicalMust(actual))),
		Context: "global budget artifact differs without a per-pair decision difference",
		Message: "budget global metadata mismatch",
	}
}

func decisionValue(id string, retained map[string]RetainedPair, eliminated map[string]EliminatedPair) string {
	if pair, ok := retained[id]; ok {
		return string(canonicalMust(pair))
	}
	if pair, ok := eliminated[id]; ok {
		return string(canonicalMust(pair))
	}
	return "<absent>"
}

func compareReport(actual, expected Report) error {
	actualPairs := map[string]string{}
	for _, pair := range actual.Pairs {
		actualPairs[pair.PairID] = string(canonicalMust(pair))
	}
	expectedPairs := map[string]string{}
	for _, pair := range expected.Pairs {
		expectedPairs[pair.PairID] = string(canonicalMust(pair))
	}
	if mismatch := comparePairArray(StageReport, actualPairs, expectedPairs); mismatch != nil {
		return mismatch
	}
	firstID := ""
	if len(actual.Pairs) > 0 {
		firstID = actual.Pairs[0].PairID
	} else if len(expected.Pairs) > 0 {
		firstID = expected.Pairs[0].PairID
	}
	return &BusinessError{
		Code: CodeIncrementalMismatch, PairID: firstID, Stage: StageReport,
		Expected: truncateValue(string(canonicalMust(expected))), Actual: truncateValue(string(canonicalMust(actual))),
		Context: "report metadata differs without a per-pair difference",
		Message: "report mismatch",
	}
}

func firstScoreID(a, b []ScoreResult) string {
	set := map[string]bool{}
	for _, value := range a {
		set[value.PairID] = true
	}
	for _, value := range b {
		set[value.PairID] = true
	}
	ids := sortedStringKeys(set)
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func canonicalMust(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{\"marshal_error\":true}")
	}
	return b
}

func truncateValue(value string) string {
	const limit = 4096
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "...<truncated>"
}
