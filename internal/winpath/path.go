package winpath

import (
	"strings"

	"svcworkbench/internal/domain"
)

var executableExtensions = []string{".exe", ".com", ".bat", ".cmd", ".scr", ".ps1", ".vbs"}

func Assess(raw string, declaredWritable bool) domain.PathAssessment {
	assessment := domain.PathAssessment{
		Original:         raw,
		DeclaredWritable: declaredWritable,
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return malformed(assessment, "empty image path")
	}

	imagePart := trimmed
	arguments := ""
	quoted := false
	if strings.HasPrefix(trimmed, "\"") {
		quoted = true
		closing := strings.IndexByte(trimmed[1:], '"')
		if closing < 0 {
			return malformed(assessment, "unterminated quoted image path")
		}
		closing++
		imagePart = trimmed[1:closing]
		rest := trimmed[closing+1:]
		if rest != "" && rest[0] != ' ' && rest[0] != '\t' {
			return malformed(assessment, "characters after closing quote without separator")
		}
		arguments = strings.TrimSpace(rest)
		if strings.Contains(arguments, "\"") {
			if strings.Count(arguments, "\"")%2 != 0 {
				return malformed(assessment, "unterminated quoted argument")
			}
		}
	} else {
		imagePart, arguments = splitUnquotedCommand(trimmed)
		if strings.Contains(imagePart, "\"") {
			return malformed(assessment, "quote encountered in unquoted image path")
		}
		if strings.Count(arguments, "\"")%2 != 0 {
			return malformed(assessment, "unterminated quoted argument")
		}
	}

	imagePart = strings.TrimSpace(imagePart)
	normalized, ok, reason := normalizeImagePath(imagePart)
	assessment.Quoted = quoted
	assessment.Normalized = normalized
	assessment.Arguments = arguments
	assessment.HasSpace = strings.Contains(imagePart, " ")
	assessment.Unquoted = !quoted && assessment.HasSpace
	assessment.ImagePathKey = normalized
	if arguments != "" {
		assessment.ImagePathKey += "|" + collapseSpaces(arguments)
	}
	if !ok {
		assessment.Malformed = true
		assessment.MalformedReason = reason
		return assessment
	}

	if assessment.Unquoted {
		assessment.ExecutableCandidates = candidatePrefixes(imagePart)
	}
	assessment.WritableDirectory, assessment.WritableReasons = writableDirectory(normalized, declaredWritable)
	return assessment
}

func malformed(assessment domain.PathAssessment, reason string) domain.PathAssessment {
	assessment.Malformed = true
	assessment.MalformedReason = reason
	assessment.Normalized = ""
	assessment.ImagePathKey = ""
	return assessment
}

func splitUnquotedCommand(value string) (string, string) {
	if hasExecutableExtension(value) {
		return value, ""
	}
	boundaries := wordBoundaries(value)
	executableEnd := -1
	for _, boundary := range boundaries {
		prefix := strings.TrimSpace(value[:boundary])
		if hasExecutableExtension(prefix) {
			executableEnd = boundary
			break
		}
	}
	if executableEnd < 0 {
		first := strings.IndexAny(value, " \t")
		if first < 0 {
			return value, ""
		}
		return strings.TrimSpace(value[:first]), strings.TrimSpace(value[first:])
	}
	return strings.TrimSpace(value[:executableEnd]), strings.TrimSpace(value[executableEnd:])
}

func candidatePrefixes(value string) []string {
	boundaries := wordBoundaries(value)
	if len(boundaries) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	out := make([]string, 0, len(boundaries)*2)
	for _, boundary := range boundaries {
		prefix := strings.TrimSpace(value[:boundary])
		normalized, ok, _ := normalizeImagePath(prefix)
		if !ok {
			continue
		}
		if !hasExecutableExtension(prefix) {
			normalized += ".exe"
		}
		if !seen[normalized] {
			seen[normalized] = true
			out = append(out, normalized)
		}
	}
	full, ok, _ := normalizeImagePath(value)
	if ok && !seen[full] {
		out = append(out, full)
	}
	return out
}

func hasExecutableExtension(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	for _, extension := range executableExtensions {
		if strings.HasSuffix(lower, extension) {
			return true
		}
	}
	return false
}

func wordBoundaries(value string) []int {
	out := make([]int, 0, 8)
	for index := 0; index < len(value); index++ {
		if value[index] == ' ' || value[index] == '\t' {
			out = append(out, index)
			for index+1 < len(value) && (value[index+1] == ' ' || value[index+1] == '\t') {
				index++
			}
		}
	}
	return out
}

func normalizeImagePath(value string) (string, bool, string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false, "empty executable path"
	}
	for _, r := range value {
		if r == 0 || r < 0x20 {
			return "", false, "control character in image path"
		}
		if r == '<' || r == '>' || r == '|' || r == '*' {
			return "", false, "reserved Windows path character"
		}
		if r == '?' && !strings.HasPrefix(strings.ToLower(value), "\\\\?\\") {
			return "", false, "question mark outside extended path prefix"
		}
	}
	if !isAbsoluteWindowsPath(value) {
		return "", false, "image path is not absolute"
	}

	value = strings.ReplaceAll(value, "/", "\\")
	value = collapseBackslashes(value)
	if len(value) > 3 && strings.HasSuffix(value, "\\") {
		value = strings.TrimRight(value, "\\")
	}
	lower := strings.ToLower(value)
	lastSeparator := strings.LastIndex(lower, "\\")
	if lastSeparator == len(lower)-1 {
		return "", false, "image path does not identify a file"
	}
	base := lower
	if lastSeparator >= 0 {
		base = lower[lastSeparator+1:]
	}
	if base == "" || base == "." || base == ".." {
		return "", false, "invalid executable file name"
	}
	return lower, true, ""
}

func isAbsoluteWindowsPath(value string) bool {
	value = strings.ReplaceAll(value, "/", "\\")
	if len(value) >= 3 && isASCIIAlpha(value[0]) && value[1] == ':' && value[2] == '\\' {
		return true
	}
	return strings.HasPrefix(value, "\\\\")
}

func isASCIIAlpha(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z')
}

func collapseBackslashes(value string) string {
	if value == "" {
		return value
	}
	prefix := ""
	if strings.HasPrefix(value, "\\\\") {
		prefix = "\\\\"
		value = value[2:]
	}
	for strings.Contains(value, "\\\\") {
		value = strings.ReplaceAll(value, "\\\\", "\\")
	}
	return prefix + value
}

func collapseSpaces(value string) string {
	value = strings.TrimSpace(value)
	for strings.Contains(value, "  ") {
		value = strings.ReplaceAll(value, "  ", " ")
	}
	return value
}

func writableDirectory(normalized string, declaredWritable bool) (bool, []string) {
	reasons := make([]string, 0, 3)
	directory := normalized
	if index := strings.LastIndex(directory, "\\"); index >= 0 {
		directory = directory[:index+1]
	}
	check := strings.TrimPrefix(directory, "\\\\?\\")
	prefixes := []struct {
		prefix string
		reason string
	}{
		{"c:\\users\\", "path:user-profile"},
		{"c:\\programdata\\", "path:programdata"},
		{"c:\\windows\\temp\\", "path:windows-temp"},
		{"c:\\temp\\", "path:system-temp"},
		{"c:\\$recycle.bin\\", "path:recycle-bin"},
		{"c:\\public\\", "path:public"},
	}
	for _, candidate := range prefixes {
		if strings.HasPrefix(check, candidate.prefix) {
			reasons = append(reasons, candidate.reason)
		}
	}
	if declaredWritable {
		reasons = append(reasons, "source:declared-writable")
	}
	return len(reasons) > 0, reasons
}
