package normalize

import (
	"fmt"
	"strconv"
	"strings"

	"supplyaudit/internal/model"
)

// ComponentKey lowercases a component name while preserving namespace
// separators such as '/', '@' and ':'.
func ComponentKey(raw string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(raw))
	if key == "" {
		return "", model.NewCodeError(model.CodeRecordInvalid, "component name is empty", 0, 0)
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return "", model.NewCodeError(model.CodeRecordInvalid, "component name contains a control character", 0, 0)
		}
	}
	return key, nil
}

// ParseVersion accepts three- or four-part integer cores, an optional SemVer
// prerelease, and build metadata. Build metadata is retained but excluded from
// Canonical because it does not participate in SemVer precedence.
func ParseVersion(raw string) (model.VersionView, error) {
	view := model.VersionView{Raw: raw, Prerelease: []model.PreReleaseIdentifier{}}
	value := strings.TrimSpace(raw)
	if value == "" {
		return view, model.NewCodeError(model.CodeRecordInvalid, "version is empty", 0, 0)
	}
	if len(value) > 1 && (value[0] == 'v' || value[0] == 'V') && value[1] >= '0' && value[1] <= '9' {
		value = value[1:]
	}

	if strings.Count(value, "+") > 1 {
		return view, model.NewCodeError(model.CodeRecordInvalid, "version has multiple build metadata separators", 0, 0)
	}
	if plus := strings.IndexByte(value, '+'); plus >= 0 {
		view.Build = value[plus+1:]
		value = value[:plus]
		if err := validateIdentifiers(view.Build); err != nil {
			return view, model.NewCodeError(model.CodeRecordInvalid, "invalid build metadata: "+err.Error(), 0, 0)
		}
	}

	coreAndPre := value
	if dash := strings.IndexByte(value, '-'); dash >= 0 {
		coreAndPre = value[:dash]
		pre := value[dash+1:]
		if err := validateIdentifiers(pre); err != nil {
			return view, model.NewCodeError(model.CodeRecordInvalid, "invalid prerelease: "+err.Error(), 0, 0)
		}
		for _, part := range strings.Split(pre, ".") {
			identifier := model.PreReleaseIdentifier{Raw: part}
			if isDigits(part) {
				number, err := strconv.ParseUint(part, 10, 64)
				if err != nil {
					return view, model.NewCodeError(model.CodeRecordInvalid, "prerelease number is too large", 0, 0)
				}
				identifier.Numeric = true
				identifier.Number = number
			}
			view.Prerelease = append(view.Prerelease, identifier)
		}
	}

	parts := strings.Split(coreAndPre, ".")
	if len(parts) != 3 && len(parts) != 4 {
		return view, model.NewCodeError(model.CodeRecordInvalid, "version core must contain three or four integer parts", 0, 0)
	}
	for index, part := range parts {
		if part == "" {
			return view, model.NewCodeError(model.CodeRecordInvalid, "version core contains an empty part", 0, 0)
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return view, model.NewCodeError(model.CodeRecordInvalid, "version core is not an integer", 0, 0)
			}
		}
		number, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return view, model.NewCodeError(model.CodeRecordInvalid, "version core integer is too large", 0, 0)
		}
		view.Core[index] = number
	}

	canonical := fmt.Sprintf("%d.%d.%d", view.Core[0], view.Core[1], view.Core[2])
	if view.Core[3] != 0 {
		canonical += fmt.Sprintf(".%d", view.Core[3])
	}
	if len(view.Prerelease) > 0 {
		parts := make([]string, 0, len(view.Prerelease))
		for _, identifier := range view.Prerelease {
			parts = append(parts, identifier.Raw)
		}
		canonical += "-" + strings.Join(parts, ".")
	}
	view.Canonical = canonical
	return view, nil
}

func validateIdentifiers(value string) error {
	if value == "" {
		return fmt.Errorf("empty identifier set")
	}
	for _, part := range strings.Split(value, ".") {
		if part == "" {
			return fmt.Errorf("empty identifier")
		}
		for _, r := range part {
			if (r < '0' || r > '9') && (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && r != '-' {
				return fmt.Errorf("character %q is not allowed", r)
			}
		}
	}
	return nil
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// CompareVersions implements SemVer precedence over normalized four-part
// cores. Build metadata is intentionally ignored.
func CompareVersions(left, right model.VersionView) int {
	for index := 0; index < 4; index++ {
		if left.Core[index] < right.Core[index] {
			return -1
		}
		if left.Core[index] > right.Core[index] {
			return 1
		}
	}
	if len(left.Prerelease) == 0 && len(right.Prerelease) == 0 {
		return 0
	}
	if len(left.Prerelease) == 0 {
		return 1
	}
	if len(right.Prerelease) == 0 {
		return -1
	}
	limit := len(left.Prerelease)
	if len(right.Prerelease) < limit {
		limit = len(right.Prerelease)
	}
	for index := 0; index < limit; index++ {
		l, r := left.Prerelease[index], right.Prerelease[index]
		switch {
		case l.Numeric && r.Numeric:
			if l.Number < r.Number {
				return -1
			}
			if l.Number > r.Number {
				return 1
			}
		case l.Numeric != r.Numeric:
			if l.Numeric {
				return -1
			}
			return 1
		default:
			if l.Raw < r.Raw {
				return -1
			}
			if l.Raw > r.Raw {
				return 1
			}
		}
	}
	if len(left.Prerelease) < len(right.Prerelease) {
		return -1
	}
	if len(left.Prerelease) > len(right.Prerelease) {
		return 1
	}
	return 0
}
