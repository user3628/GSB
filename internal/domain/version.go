package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var reportedVersionRE = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+){0,7})(?:[-+][0-9A-Za-z][0-9A-Za-z.-]*)?$`)

// Version is a parsed replica-reported version. Numeric segments drive ordering;
// the optional suffix is retained for round-trip fidelity.
type Version struct {
	Raw      string `json:"raw"`
	Segments []int  `json:"segments"`
	Suffix   string `json:"suffix,omitempty"`
}

func ParseVersion(raw string) (Version, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return Version{}, fmt.Errorf("reported version is empty")
	}
	if !reportedVersionRE.MatchString(text) {
		return Version{}, fmt.Errorf("reported version %q has invalid segments", text)
	}
	main := text
	suffix := ""
	if idx := strings.IndexAny(main, "-+"); idx >= 0 {
		suffix = main[idx:]
		main = main[:idx]
	}
	parts := strings.Split(main, ".")
	segments := make([]int, len(parts))
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return Version{}, fmt.Errorf("reported version segment %q is invalid", part)
		}
		segments[i] = value
	}
	return Version{Raw: text, Segments: segments, Suffix: suffix}, nil
}

func (v Version) Valid() bool {
	return len(v.Segments) > 0
}

func (v Version) String() string {
	if v.Raw != "" {
		return v.Raw
	}
	parts := make([]string, len(v.Segments))
	for i, segment := range v.Segments {
		parts[i] = strconv.Itoa(segment)
	}
	return strings.Join(parts, ".") + v.Suffix
}

// Compare returns -1, 0 or 1. Missing trailing numeric segments are zero.
func (v Version) Compare(other Version) int {
	n := len(v.Segments)
	if len(other.Segments) > n {
		n = len(other.Segments)
	}
	for i := 0; i < n; i++ {
		var a, b int
		if i < len(v.Segments) {
			a = v.Segments[i]
		}
		if i < len(other.Segments) {
			b = other.Segments[i]
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
	}
	return 0
}

func (v Version) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.String())
}

func (v *Version) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := ParseVersion(raw)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}
