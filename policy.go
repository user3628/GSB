package prefixcache

import (
	"math"
	"strconv"
	"strings"
)

const (
	normalizationVersion = "nfkc-width-fold-lower-collapse-v1"
	blockFormatVersion   = "sha256-length-prefixed-block-v1"
	slotAlgorithmVersion = "direct-map-length-then-age-v1"
	hitAlgorithmVersion  = "leading-block-similarity-v1"
)

func (p Policy) Validate() error {
	if p.Version < 1 {
		return newError(CodeInvalidPolicy, "version must be >= 1")
	}
	if p.Granularity < 1 {
		return newError(CodeInvalidPolicy, "granularity must be >= 1")
	}
	if p.MinPrefixLen < 1 {
		return newError(CodeInvalidPolicy, "min_prefix_len must be >= 1")
	}
	if math.IsNaN(p.MergeThreshold) || math.IsInf(p.MergeThreshold, 0) || p.MergeThreshold < 0 || p.MergeThreshold > 1 {
		return newError(CodeInvalidPolicy, "merge_threshold must be within [0,1]")
	}
	if p.MaxPrefixesPerRequest < 1 {
		return newError(CodeInvalidPolicy, "max_prefixes_per_request must be >= 1")
	}
	if p.SlotCount < 0 {
		return newError(CodeInvalidPolicy, "slot_count must be >= 0")
	}
	if p.OccupancyBudget < 0 {
		return newError(CodeInvalidPolicy, "occupancy_budget must be >= 0")
	}
	return nil
}

func (p Policy) withVersion(version int64) Policy {
	p.Version = version
	return p
}

func (p PolicyPatch) ChangedField() (string, error) {
	fields := make([]string, 0, 4)
	if p.Granularity != nil {
		fields = append(fields, "granularity")
	}
	if p.MinPrefixLen != nil {
		fields = append(fields, "min_prefix_len")
	}
	if p.MergeThreshold != nil {
		fields = append(fields, "merge_threshold")
	}
	if p.MaxPrefixesPerRequest != nil {
		fields = append(fields, "max_prefixes_per_request")
	}
	if len(fields) != 1 {
		return "", newError(CodeInvalidPolicy, "a policy patch must change exactly one field")
	}
	return fields[0], nil
}

func (p PolicyPatch) Apply(base Policy) (Policy, string, error) {
	field, err := p.ChangedField()
	if err != nil {
		return Policy{}, "", err
	}
	next := base.withVersion(base.Version + 1)
	changed := false
	switch field {
	case "granularity":
		changed = *p.Granularity != base.Granularity
		next.Granularity = *p.Granularity
	case "min_prefix_len":
		changed = *p.MinPrefixLen != base.MinPrefixLen
		next.MinPrefixLen = *p.MinPrefixLen
	case "merge_threshold":
		changed = *p.MergeThreshold != base.MergeThreshold
		next.MergeThreshold = *p.MergeThreshold
	case "max_prefixes_per_request":
		changed = *p.MaxPrefixesPerRequest != base.MaxPrefixesPerRequest
		next.MaxPrefixesPerRequest = *p.MaxPrefixesPerRequest
	}
	if !changed {
		return Policy{}, field, newError(CodeInvalidPolicy, "patch does not change the selected field")
	}
	if err := next.Validate(); err != nil {
		return Policy{}, field, err
	}
	return next, field, nil
}

func policyContentString(p Policy) string {
	parts := []string{
		"policy-v1",
		strconv.Itoa(p.Granularity),
		strconv.Itoa(p.MinPrefixLen),
		strconv.FormatFloat(p.MergeThreshold, 'g', -1, 64),
		strconv.Itoa(p.MaxPrefixesPerRequest),
		strconv.Itoa(p.SlotCount),
		strconv.Itoa(p.OccupancyBudget),
	}
	return strings.Join(parts, "|")
}

// PolicyContentDigest is independent of Version so content equality can be
// checked while versions still advance monotonically.
func PolicyContentDigest(p Policy) string {
	return digestParts("prefixcache-policy-digest-v1", policyContentString(p))
}

// StageDigestsFor derives the stage DAG. A field only changes its own stage and
// therefore naturally causes all downstream digests to change.
func StageDigestsFor(p Policy) StageDigests {
	normalization := digestParts("prefixcache-stage-normalization-v1", normalizationVersion)
	fingerprint := digestParts("prefixcache-stage-fingerprint-v1", normalization, blockFormatVersion, strconv.Itoa(p.Granularity))
	slotting := digestParts(
		"prefixcache-stage-slotting-v1",
		fingerprint,
		slotAlgorithmVersion,
		strconv.Itoa(p.MinPrefixLen),
		strconv.Itoa(p.MaxPrefixesPerRequest),
		strconv.Itoa(p.SlotCount),
		strconv.Itoa(p.OccupancyBudget),
	)
	hit := digestParts(
		"prefixcache-stage-hit-v1",
		slotting,
		hitAlgorithmVersion,
		strconv.FormatFloat(p.MergeThreshold, 'g', -1, 64),
	)
	version := digestParts(
		"prefixcache-version-v1",
		strconv.FormatInt(p.Version, 10),
		PolicyContentDigest(p),
		normalization,
		fingerprint,
		slotting,
		hit,
	)
	return StageDigests{
		Normalization: normalization,
		Fingerprint:   fingerprint,
		Slotting:      slotting,
		Hit:           hit,
		Version:       version,
	}
}

func earliestAffectedStage(previous, next Policy, replacement bool) (Stage, string, string, string) {
	if replacement {
		return StageNormalization, "*", "", ""
	}
	previousDigests := StageDigestsFor(previous)
	nextDigests := StageDigestsFor(next)
	stage := Stage("")
	switch {
	case previousDigests.Normalization != nextDigests.Normalization:
		stage = StageNormalization
	case previousDigests.Fingerprint != nextDigests.Fingerprint:
		stage = StageFingerprint
	case previousDigests.Slotting != nextDigests.Slotting:
		stage = StageSlotting
	case previousDigests.Hit != nextDigests.Hit:
		stage = StageHit
	}
	if previous.Granularity != next.Granularity {
		return stage, "granularity", strconv.Itoa(previous.Granularity), strconv.Itoa(next.Granularity)
	}
	if previous.MinPrefixLen != next.MinPrefixLen {
		return stage, "min_prefix_len", strconv.Itoa(previous.MinPrefixLen), strconv.Itoa(next.MinPrefixLen)
	}
	if previous.MaxPrefixesPerRequest != next.MaxPrefixesPerRequest {
		return stage, "max_prefixes_per_request", strconv.Itoa(previous.MaxPrefixesPerRequest), strconv.Itoa(next.MaxPrefixesPerRequest)
	}
	if previous.SlotCount != next.SlotCount {
		return stage, "slot_count", strconv.Itoa(previous.SlotCount), strconv.Itoa(next.SlotCount)
	}
	if previous.OccupancyBudget != next.OccupancyBudget {
		return stage, "occupancy_budget", strconv.Itoa(previous.OccupancyBudget), strconv.Itoa(next.OccupancyBudget)
	}
	if previous.MergeThreshold != next.MergeThreshold {
		return stage, "merge_threshold", strconv.FormatFloat(previous.MergeThreshold, 'g', -1, 64), strconv.FormatFloat(next.MergeThreshold, 'g', -1, 64)
	}
	return stage, "", "", ""
}

func stageInvalidatesSlotting(stage Stage) bool {
	return stage == StageNormalization || stage == StageFingerprint || stage == StageSlotting
}
