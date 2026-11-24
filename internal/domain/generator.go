package domain

import (
	"fmt"
	"strings"
)

type GenerateSpec struct {
	Count          int
	Seed           int64
	Namespace      string
	DuplicateCount int
}

func DefaultGenerateSpec() GenerateSpec {
	return GenerateSpec{Count: 96, Seed: DefaultPolicy().Seed, Namespace: "", DuplicateCount: 2}
}

func GenerateRawInput(spec GenerateSpec) string {
	if spec.Count <= 0 {
		spec.Count = 64
	}
	if spec.Seed == 0 {
		spec.Seed = DefaultPolicy().Seed
	}
	if spec.Namespace == "" {
		spec.Namespace = "fixed"
	}
	rng := NewStageRNG(spec.Seed, "raw-generator", DigestString(spec.Namespace, "v1"))
	labels := []string{"alpha", "beta", "gamma"}
	keys := []string{"north", "south", "east", "west", "node-05", "node-06", "node-07", "node-08"}
	baseTS := DefaultPolicy().Filter.MinTimestampMS + 60_000
	lines := make([]string, 0, spec.Count+spec.DuplicateCount)
	for i := 0; i < spec.Count; i++ {
		ts := baseTS + int64(i)*180_000 + int64(rng.Float64()*30_000)
		id := fmt.Sprintf("%s-sample-%06d", safeNamespace(spec.Namespace), i)
		length := 4 + int(rng.Float64()*120)
		switch i % 17 {
		case 0:
			length = 0
		case 3:
			length = 257 + int(rng.Float64()*128)
		}
		label := labels[int(rng.Float64()*float64(len(labels)))%len(labels)]
		key := keys[int(rng.Float64()*float64(len(keys)))%len(keys)]
		lines = append(lines, fmt.Sprintf("%d|%s|%d|%s|%s", ts, id, length, label, key))
	}
	for d := 0; d < spec.DuplicateCount && len(lines) > 0; d++ {
		sourceIndex := d % min(3, len(lines))
		lines = append(lines, lines[sourceIndex])
	}
	return strings.Join(lines, "\n") + "\n"
}

func safeNamespace(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return "batch"
	}
	return b.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func InjectInvalidLabel(raw string) string {
	lines := splitNonEmptyLines(raw)
	if len(lines) == 0 {
		return raw
	}
	idx := min(2, len(lines)-1)
	parts := strings.Split(lines[idx], "|")
	if len(parts) == 5 {
		parts[3] = "label_not_declared"
		lines[idx] = strings.Join(parts, "|")
	}
	return strings.Join(lines, "\n") + "\n"
}

func InjectMissingShardKey(raw string) string {
	lines := splitNonEmptyLines(raw)
	if len(lines) == 0 {
		return raw
	}
	idx := min(4, len(lines)-1)
	parts := strings.Split(lines[idx], "|")
	if len(parts) == 5 {
		parts[4] = ""
		lines[idx] = strings.Join(parts, "|")
	}
	return strings.Join(lines, "\n") + "\n"
}

func InjectMalformedLine(raw string) string {
	return raw + "this-is-not-a-valid-record\n"
}

func splitNonEmptyLines(raw string) []string {
	var result []string
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) != "" {
			result = append(result, line)
		}
	}
	return result
}
