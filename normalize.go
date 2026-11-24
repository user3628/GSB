package ledger

import (
	"strings"
)

// NormalizeEntryBatch validates and canonicalizes a complete batch. A non-nil
// business error means no entry from the batch may be written.
func NormalizeEntryBatch(batch *EntryBatch) ([]LedgerEntry, *Error) {
	if batch == nil {
		return nil, NewError(CodeLedgerInvalid, "missing entry batch")
	}
	issues := make([]ValidationIssue, 0)
	normalized := make([]LedgerEntry, 0, len(batch.Entries))

	for i := range batch.Entries {
		entry := batch.Entries[i]
		idx := i
		weights := entry.CandidateWeights
		if len(weights) == 0 {
			weights = entry.Weights
		}
		if weights == nil {
			weights = map[string]int{}
		}

		entry.RouteKey = strings.TrimSpace(entry.RouteKey)
		entry.ClientID = strings.TrimSpace(entry.ClientID)
		entry.HitVersion = strings.TrimSpace(entry.HitVersion)
		if entry.TimestampMS < 0 {
			issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "timestamp_ms must be non-negative"})
		}
		if entry.RouteKey == "" {
			issues = append(issues, ValidationIssue{Index: &idx, Code: CodeLedgerInvalid, Message: "route_key is required"})
		}
		if entry.ClientID == "" {
			issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "client_id is required"})
		}
		if entry.HitVersion == "" {
			issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "hit_version is required"})
		}
		if entry.RequestWeight <= 0 {
			issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "request_weight must be a positive integer"})
		}

		candidateSet := map[string]struct{}{}
		for _, candidate := range entry.Candidates {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			candidateSet[candidate] = struct{}{}
		}
		if len(candidateSet) == 0 && len(weights) > 0 {
			for version := range weights {
				candidateSet[strings.TrimSpace(version)] = struct{}{}
			}
		}
		if len(candidateSet) == 0 {
			issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "candidates must not be empty"})
		}

		cleanWeights := make(map[string]int, len(candidateSet))
		sum := 0
		for version := range candidateSet {
			weight, ok := weights[version]
			if !ok {
				issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "missing permille weight for candidate " + version})
				continue
			}
			if weight < 0 || weight > 1000 {
				issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "weight for " + version + " must be in [0,1000]"})
			}
			cleanWeights[version] = weight
			sum += weight
		}
		for version, weight := range weights {
			version = strings.TrimSpace(version)
			if _, ok := candidateSet[version]; !ok {
				issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "weight provided for non-candidate version " + version})
			}
			_ = weight
		}
		if sum != 1000 {
			issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "candidate weights must sum to 1000, got " + itoa(sum)})
		}
		if entry.HitVersion != "" {
			if _, ok := candidateSet[entry.HitVersion]; !ok {
				issues = append(issues, ValidationIssue{Index: &idx, RouteKey: entry.RouteKey, Code: CodeLedgerInvalid, Message: "hit_version " + entry.HitVersion + " is not in candidate set"})
			}
		}

		candidates := sortedKeys(cleanWeights)
		entry.Candidates = candidates
		entry.Weights = cleanWeights
		entry.CandidateWeights = cleanWeights
		normalized = append(normalized, entry)
	}

	if len(issues) > 0 {
		return nil, NewError(CodeLedgerInvalid, "ledger batch validation failed").
			WithContext("issues", issues).
			WithContext("issue_count", len(issues)).
			WithContext("batch_size", len(batch.Entries))
	}
	return normalized, nil
}

func NormalizeSnapshot(snapshot *WeightSnapshot) *Error {
	if snapshot == nil {
		return NewError(CodeSnapshotInvalid, "missing snapshot")
	}
	if snapshot.SnapshotVersion <= 0 {
		return NewError(CodeSnapshotInvalid, "snapshot_version must be positive").
			WithContext("snapshot_version", snapshot.SnapshotVersion)
	}
	if snapshot.GeneratedAtMS < 0 {
		return NewError(CodeSnapshotInvalid, "generated_at_ms must be non-negative")
	}
	if len(snapshot.Weights) == 0 {
		return NewError(CodeSnapshotInvalid, "weights must not be empty")
	}
	originalWeights := snapshot.Weights
	snapshot.Weights = make(map[string]map[string]int, len(originalWeights))
	for _, routeKey := range sortedKeys(originalWeights) {
		routeWeights := originalWeights[routeKey]
		if strings.TrimSpace(routeKey) == "" {
			return NewError(CodeSnapshotInvalid, "route_key must not be empty")
		}
		if len(routeWeights) == 0 {
			return NewError(CodeSnapshotInvalid, "route weights must not be empty").WithContext("route_key", routeKey)
		}
		sum := 0
		clean := make(map[string]int, len(routeWeights))
		for _, version := range sortedKeys(routeWeights) {
			name := strings.TrimSpace(version)
			if name == "" {
				return NewError(CodeSnapshotInvalid, "version must not be empty").WithContext("route_key", routeKey)
			}
			weight := routeWeights[version]
			if weight < 0 || weight > 1000 {
				return NewError(CodeSnapshotInvalid, "weight must be in [0,1000]").
					WithContextMap(map[string]any{"route_key": routeKey, "version": name, "weight": weight})
			}
			clean[name] = weight
			sum += weight
		}
		if sum != 1000 {
			return NewError(CodeLedgerInvalid, "snapshot candidate weights must sum to 1000").
				WithContextMap(map[string]any{"route_key": routeKey, "sum_permille": sum})
		}
		snapshot.Weights[routeKey] = clean
	}
	expectedDigest := SnapshotDigest(*snapshot)
	if snapshot.Digest != "" && snapshot.Digest != expectedDigest {
		return NewError(CodeDigestMismatch, "snapshot digest does not match canonical payload").
			WithContextMap(map[string]any{"expected_digest": expectedDigest, "provided_digest": snapshot.Digest})
	}
	snapshot.Digest = expectedDigest
	return nil
}

func NormalizeMigration(fragment *MigrationFragment) *Error {
	if fragment == nil {
		return NewError(CodeLedgerInvalid, "missing migration fragment")
	}
	fragment.MigrationID = strings.TrimSpace(fragment.MigrationID)
	fragment.RouteKey = strings.TrimSpace(fragment.RouteKey)
	fragment.FromVersion = strings.TrimSpace(fragment.FromVersion)
	fragment.ToVersion = strings.TrimSpace(fragment.ToVersion)
	if fragment.MigrationID == "" {
		return NewError(CodeMigrationRatioInvalid, "migration_id is required")
	}
	if fragment.RouteKey == "" || fragment.FromVersion == "" || fragment.ToVersion == "" {
		return NewError(CodeLedgerInvalid, "route_key, from_version and to_version are required").
			WithContext("migration_id", fragment.MigrationID)
	}
	if fragment.FromVersion == fragment.ToVersion {
		return NewError(CodeMigrationRatioInvalid, "from_version and to_version must differ").
			WithContext("migration_id", fragment.MigrationID)
	}
	if fragment.RatioPermille < 0 || fragment.RatioPermille > 1000 {
		return NewError(CodeMigrationRatioInvalid, "ratio_permille must be in [0,1000]").
			WithContextMap(map[string]any{"migration_id": fragment.MigrationID, "ratio_permille": fragment.RatioPermille})
	}
	if fragment.StartAtMS < 0 {
		return NewError(CodeLedgerInvalid, "start_at_ms must be non-negative").
			WithContext("migration_id", fragment.MigrationID)
	}
	if fragment.TransitionMS <= 0 {
		return NewError(CodeMigrationRatioInvalid, "transition_ms must be positive").
			WithContextMap(map[string]any{"migration_id": fragment.MigrationID, "transition_ms": fragment.TransitionMS})
	}
	return nil
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
