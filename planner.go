package prefixcache

import (
	"fmt"
	"strings"
)

// ValidateRequest checks structural request requirements. Empty message text is
// valid and is represented explicitly in the plan as an uncacheable block.
func ValidateRequest(request Request) error {
	if strings.TrimSpace(request.ID) == "" {
		return invalidRequestError(request.ID, "request id must not be empty")
	}
	if strings.TrimSpace(request.TemplateID) == "" {
		return invalidRequestError(request.ID, "template_id must not be empty")
	}
	for i, message := range request.Messages {
		if strings.TrimSpace(message.Role) == "" {
			return invalidRequestError(request.ID, fmt.Sprintf("message %d role must not be empty", i))
		}
		if strings.TrimSpace(message.Language) == "" {
			return invalidRequestError(request.ID, fmt.Sprintf("message %d language must not be empty", i))
		}
	}
	return nil
}

// BuildRequestPlan normalizes and fingerprints one request under an immutable
// policy. It includes zero-width blocks for empty messages and marks partial
// blocks as uncacheable rather than silently dropping them.
func BuildRequestPlan(request Request, policy Policy) (RequestPlan, error) {
	if err := ValidateRequest(request); err != nil {
		return RequestPlan{}, err
	}
	if err := policy.Validate(); err != nil {
		return RequestPlan{}, err
	}

	digests := StageDigestsFor(policy)
	plan := RequestPlan{
		Version:      policy.Version,
		RequestID:    request.ID,
		TemplateID:   request.TemplateID,
		StageDigests: digests,
	}

	blocks := make([]Block, 0)
	globalChars := 0
	for messageIndex, message := range request.Messages {
		role := strings.ToLower(strings.TrimSpace(message.Role))
		language := strings.ToLower(strings.TrimSpace(message.Language))
		normalized := NormalizeText(message.Text)

		if normalized == "" {
			fingerprint := blockFingerprint(messageIndex, 0, message, role, language, "")
			blocks = append(blocks, Block{
				Index:              len(blocks),
				SourceMessageIndex: messageIndex,
				Turn:               message.Turn,
				Role:               role,
				Language:           language,
				ChunkIndex:         0,
				Text:               "",
				StartChar:          0,
				EndChar:            0,
				CharSpan:           0,
				PrefixStartChar:    globalChars,
				PrefixEndChar:      globalChars,
				Fingerprint:        fingerprint,
				Cacheable:          false,
				InvalidReason:      "empty_message",
			})
			continue
		}

		runes := []rune(normalized)
		for start, chunkIndex := 0, 0; start < len(runes); start, chunkIndex = start+policy.Granularity, chunkIndex+1 {
			end := start + policy.Granularity
			if end > len(runes) {
				end = len(runes)
			}
			text := string(runes[start:end])
			span := end - start
			fingerprint := blockFingerprint(messageIndex, chunkIndex, message, role, language, text)
			cacheable := span == policy.Granularity
			reason := ""
			if !cacheable {
				reason = "short_block"
			}
			block := Block{
				Index:              len(blocks),
				SourceMessageIndex: messageIndex,
				Turn:               message.Turn,
				Role:               role,
				Language:           language,
				ChunkIndex:         chunkIndex,
				Text:               text,
				StartChar:          start,
				EndChar:            end,
				CharSpan:           span,
				PrefixStartChar:    globalChars,
				PrefixEndChar:      globalChars + span,
				Fingerprint:        fingerprint,
				Cacheable:          cacheable,
				InvalidReason:      reason,
			}
			blocks = append(blocks, block)
			globalChars += span
		}
	}

	prefixes := make([]PrefixCandidate, 0, len(blocks))
	fingerprints := make([]string, 0, len(blocks))
	prefixChars := 0
	badPrefix := false
	for _, block := range blocks {
		fingerprints = append(fingerprints, block.Fingerprint)
		prefixChars += block.CharSpan
		if block.InvalidReason != "" {
			badPrefix = true
		}
		copyFingerprints := append([]string(nil), fingerprints...)
		candidate := PrefixCandidate{
			BlockCount:   len(fingerprints),
			Fingerprints: copyFingerprints,
			PrefixKey:    prefixKey(copyFingerprints),
			PrefixChars:  prefixChars,
			Cacheable:    !badPrefix,
		}
		if block.InvalidReason != "" {
			candidate.RejectReason = "contains_" + block.InvalidReason
		} else if badPrefix {
			candidate.RejectReason = "upstream_uncacheable_block"
		}
		prefixes = append(prefixes, candidate)
	}

	plan.Blocks = blocks
	plan.Prefixes = prefixes
	plan.PlanDigest = planDigest(plan)
	return plan, nil
}

func blockFingerprints(plan RequestPlan, blockCount int) []string {
	if blockCount <= 0 {
		return nil
	}
	if blockCount > len(plan.Blocks) {
		blockCount = len(plan.Blocks)
	}
	out := make([]string, 0, blockCount)
	for i := 0; i < blockCount; i++ {
		out = append(out, plan.Blocks[i].Fingerprint)
	}
	return out
}
