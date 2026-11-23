package config

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"secverify/internal/model"
)

var allowedKinds = map[string]struct{}{
	model.KindAccount:  {},
	model.KindAudit:    {},
	model.KindService:  {},
	model.KindRegistry: {},
}

// NormalizeItem validates field domains and produces the canonical in-memory
// representation. It does not resolve dependencies or inheritance; those need
// the complete item set.
func NormalizeItem(it model.Item) (model.Item, *model.BizError) {
	out := model.CloneItem(it)
	offset := it.SourceOffset
	if !validID(out.ID) {
		return out, parseErr(out.ID, offset, "item id must be non-empty and contain only identifier characters")
	}
	if _, ok := allowedKinds[out.Kind]; !ok {
		return out, parseErr(out.ID, offset, "unsupported item kind "+out.Kind)
	}
	payloadCount := 0
	if out.Account != nil {
		payloadCount++
	}
	if out.Audit != nil {
		payloadCount++
	}
	if out.Service != nil {
		payloadCount++
	}
	if out.Registry != nil {
		payloadCount++
	}
	if payloadCount != 1 {
		return out, parseErr(out.ID, offset, "exactly one typed payload is required")
	}
	if err := normalizeRefs(&out); err != nil {
		return out, parseErr(out.ID, offset, err.Error())
	}

	switch out.Kind {
	case model.KindAccount:
		if out.Account == nil {
			return out, parseErr(out.ID, offset, "account payload missing")
		}
		if err := normalizeAccount(out.Account); err != nil {
			return out, parseErr(out.ID, offset, err.Error())
		}
	case model.KindAudit:
		if out.Audit == nil {
			return out, parseErr(out.ID, offset, "audit payload missing")
		}
		if err := normalizeAudit(out.Audit); err != nil {
			return out, parseErr(out.ID, offset, err.Error())
		}
	case model.KindService:
		if out.Service == nil {
			return out, parseErr(out.ID, offset, "service payload missing")
		}
		if err := normalizeService(out.Service); err != nil {
			return out, parseErr(out.ID, offset, err.Error())
		}
	case model.KindRegistry:
		if out.Registry == nil {
			return out, parseErr(out.ID, offset, "registry payload missing")
		}
		if err := normalizeRegistry(out.Registry); err != nil {
			return out, parseErr(out.ID, offset, err.Error())
		}
	}
	return out, nil
}

func NormalizeItems(items []model.Item) ([]model.Item, *model.BizError) {
	out := make([]model.Item, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		norm, err := NormalizeItem(item)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[norm.ID]; ok {
			return nil, parseErr(norm.ID, norm.SourceOffset, "duplicate item id")
		}
		seen[norm.ID] = struct{}{}
		out = append(out, norm)
	}
	model.SortItems(out)
	return out, nil
}

func normalizeRefs(it *model.Item) error {
	seen := make(map[string]struct{}, len(it.Refs))
	refs := make([]string, 0, len(it.Refs))
	for _, ref := range it.Refs {
		if !validID(ref) {
			return fmt.Errorf("invalid reference %q", ref)
		}
		if _, ok := seen[ref]; ok {
			return fmt.Errorf("duplicate reference %q", ref)
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	it.Refs = refs
	return nil
}

func normalizeAccount(a *model.AccountPolicy) error {
	if !validName(a.Name) {
		return fmt.Errorf("account policy name is required")
	}
	inputUnit := strings.ToLower(strings.TrimSpace(a.Unit))
	if inputUnit == "" {
		return fmt.Errorf("account policy unit is required")
	}
	canonicalUnit, ok := canonicalUnit(inputUnit)
	if !ok {
		return fmt.Errorf("unsupported account policy unit %q", a.Unit)
	}
	a.Unit = canonicalUnit
	a.Objective = strings.ToLower(strings.TrimSpace(a.Objective))
	switch a.Objective {
	case "at_least", "at_most", "exact":
	default:
		return fmt.Errorf("account objective must be at_least, at_most, or exact")
	}
	if a.InheritFrom != "" && !validID(a.InheritFrom) {
		return fmt.Errorf("invalid inherit_from %q", a.InheritFrom)
	}
	if a.Value == nil && a.InheritFrom == "" {
		return fmt.Errorf("account value or inherit_from is required")
	}
	if a.Value != nil {
		if !finite(*a.Value) {
			return fmt.Errorf("account value must be finite")
		}
		if *a.Value < 0 {
			return fmt.Errorf("account value must be non-negative")
		}
		canonical, err := convertQuantity(*a.Value, inputUnit)
		if err != nil {
			return err
		}
		a.NormalizedValue = canonical
		a.NormalizedUnit = canonicalUnit
	}
	if a.Min != nil {
		if !finite(*a.Min) || *a.Min < 0 {
			return fmt.Errorf("account minimum must be finite and non-negative")
		}
		v, err := convertQuantity(*a.Min, inputUnit)
		if err != nil {
			return err
		}
		a.Min = &v
	}
	if a.Max != nil {
		if !finite(*a.Max) || *a.Max < 0 {
			return fmt.Errorf("account maximum must be finite and non-negative")
		}
		v, err := convertQuantity(*a.Max, inputUnit)
		if err != nil {
			return err
		}
		a.Max = &v
	}
	if a.Min != nil && a.Max != nil && *a.Min > *a.Max {
		return fmt.Errorf("account minimum exceeds maximum")
	}
	if a.Value != nil && !inNormalizedRange(a.NormalizedValue, a.Min, a.Max) {
		return fmt.Errorf("account value outside normalized range")
	}
	return nil
}

func normalizeAudit(a *model.AuditPolicy) error {
	if !validName(a.Name) || !validName(a.Category) {
		return fmt.Errorf("audit name and category are required")
	}
	if strings.TrimSpace(a.Path) == "" || strings.ContainsRune(a.Path, '\x00') {
		return fmt.Errorf("audit path is required")
	}
	mode, err := NormalizeMode(a.Mode)
	if err != nil {
		return fmt.Errorf("audit %w", err)
	}
	a.Mode = mode
	if a.RetentionValue == nil && a.RetentionUnit != "" {
		return fmt.Errorf("audit retention unit requires retention value")
	}
	if a.RetentionValue != nil {
		if !finite(*a.RetentionValue) || *a.RetentionValue <= 0 {
			return fmt.Errorf("audit retention must be positive")
		}
		unit := strings.ToLower(strings.TrimSpace(a.RetentionUnit))
		canonical, ok := canonicalUnit(unit)
		if !ok || (canonical != "seconds" && canonical != "count" && canonical != "bytes") {
			return fmt.Errorf("unsupported audit retention unit %q", a.RetentionUnit)
		}
		converted, err := convertQuantity(*a.RetentionValue, unit)
		if err != nil {
			return err
		}
		a.RetentionValue = &converted
		a.RetentionUnit = canonical
	}
	return nil
}

func normalizeService(s *model.ServicePolicy) error {
	if !validName(s.Name) {
		return fmt.Errorf("service name is required")
	}
	start := strings.ToLower(strings.TrimSpace(s.StartType))
	switch start {
	case "automatic", "auto", "2", "auto_start":
		start = "automatic"
	case "manual", "demand", "3", "demand_start":
		start = "manual"
	case "disabled", "4", "disable":
		start = "disabled"
	default:
		return fmt.Errorf("unsupported service start_type %q", s.StartType)
	}
	s.StartType = start
	if strings.ContainsRune(s.ImagePath, '\x00') {
		return fmt.Errorf("service image_path contains NUL")
	}
	return nil
}

func normalizeRegistry(r *model.RegistryPolicy) error {
	if !validName(r.Key) || strings.TrimSpace(r.Path) == "" {
		return fmt.Errorf("registry key and path are required")
	}
	if strings.ContainsRune(r.Path, '\x00') {
		return fmt.Errorf("registry path contains NUL")
	}
	mode, err := NormalizeMode(r.Mode)
	if err != nil {
		return fmt.Errorf("registry %w", err)
	}
	r.Mode = mode
	if len(r.ACL) == 0 {
		return fmt.Errorf("registry acl must not be empty")
	}
	seen := make(map[string]struct{}, len(r.ACL))
	acl := make([]model.ACLEntry, 0, len(r.ACL))
	for _, entry := range r.ACL {
		if !validName(entry.Principal) {
			return fmt.Errorf("registry acl principal is required")
		}
		access := strings.ToLower(strings.TrimSpace(entry.Access))
		switch access {
		case "read", "write", "full_control", "full":
			if access == "full" {
				access = "full_control"
			}
		default:
			return fmt.Errorf("unsupported registry acl access %q", entry.Access)
		}
		key := entry.Principal + "\x00" + access
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate registry acl entry")
		}
		seen[key] = struct{}{}
		acl = append(acl, model.ACLEntry{Principal: entry.Principal, Access: access})
	}
	sort.Slice(acl, func(i, j int) bool {
		if acl[i].Principal != acl[j].Principal {
			return acl[i].Principal < acl[j].Principal
		}
		return acl[i].Access < acl[j].Access
	})
	r.ACL = acl
	return nil
}

// ResolveAccounts computes effective inherited values after the dependency
// graph has established that all references exist and are acyclic.
func ResolveAccounts(items []model.Item) ([]model.Item, *model.BizError) {
	out := make([]model.Item, len(items))
	for i := range items {
		out[i] = model.CloneItem(items[i])
	}
	model.SortItems(out)
	byID := model.ItemMap(out)
	state := make(map[string]int, len(out))
	var resolve func(string) *model.BizError
	resolve = func(id string) *model.BizError {
		switch state[id] {
		case 1:
			return model.NewError(model.CodeDepCycle, id, byID[id].SourceOffset, "account inheritance cycle")
		case 2:
			return nil
		}
		state[id] = 1
		it := byID[id]
		if it.Kind != model.KindAccount || it.Account == nil {
			state[id] = 2
			return nil
		}
		acc := it.Account
		if acc.InheritFrom != "" {
			parent, ok := byID[acc.InheritFrom]
			if !ok {
				state[id] = 2
				return &model.BizError{
					Code:            model.CodeDepMissing,
					ItemID:          id,
					SourceOffset:    it.SourceOffset,
					Message:         "inherit_from target missing",
					Items:           []string{id, acc.InheritFrom},
					Edges:           []model.Edge{{From: id, To: acc.InheritFrom}},
					FallbackVersion: -1,
				}
			}
			if parent.Kind != model.KindAccount || parent.Account == nil {
				state[id] = 2
				return parseErr(id, it.SourceOffset, "inherit_from target is not an account policy")
			}
			if err := resolve(acc.InheritFrom); err != nil {
				return err
			}
			parent = byID[acc.InheritFrom]
			if acc.Value == nil {
				acc.NormalizedValue = parent.Account.NormalizedValue
				acc.NormalizedUnit = parent.Account.NormalizedUnit
			}
		}
		if acc.NormalizedUnit == "" {
			unit, _ := canonicalUnit(acc.Unit)
			acc.NormalizedUnit = unit
		}
		if !inNormalizedRange(acc.NormalizedValue, acc.Min, acc.Max) {
			return parseErr(id, it.SourceOffset, "effective account value outside normalized range")
		}
		it.Account = acc
		byID[id] = it
		state[id] = 2
		return nil
	}
	for _, it := range out {
		if it.Kind == model.KindAccount {
			if err := resolve(it.ID); err != nil {
				return nil, err
			}
		}
	}
	for i := range out {
		if it, ok := byID[out[i].ID]; ok {
			out[i] = it
		}
	}
	return out, nil
}

func NormalizeMode(raw string) (string, error) {
	s := strings.TrimSpace(strings.ToLower(raw))
	s = strings.TrimPrefix(s, "0o")
	if s == "" {
		return "", fmt.Errorf("mode is required")
	}
	value, err := strconv.ParseUint(s, 8, 16)
	if err != nil || value > 0o777 {
		return "", fmt.Errorf("mode %q must be an octal permission between 0000 and 0777", raw)
	}
	return fmt.Sprintf("%04o", value), nil
}

func ModeBits(mode string) uint32 {
	v, _ := strconv.ParseUint(strings.TrimPrefix(mode, "0o"), 8, 16)
	return uint32(v)
}

func canonicalUnit(unit string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case "seconds", "second", "sec", "secs", "s":
		return "seconds", true
	case "minutes", "minute", "min", "mins", "m":
		return "seconds", true
	case "hours", "hour", "hr", "hrs", "h":
		return "seconds", true
	case "days", "day", "d":
		return "seconds", true
	case "characters", "character", "chars", "count", "counts", "attempts", "entries", "items":
		return "count", true
	case "bytes", "byte", "b", "kb", "kib", "mb", "mib", "gb", "gib":
		return "bytes", true
	default:
		return "", false
	}
}

func convertQuantity(value float64, unit string) (float64, error) {
	u := strings.ToLower(strings.TrimSpace(unit))
	var factor float64
	switch u {
	case "seconds", "second", "sec", "secs", "s", "characters", "character", "chars", "count", "counts", "attempts", "entries", "items", "bytes", "byte", "b":
		factor = 1
	case "minutes", "minute", "min", "mins", "m":
		factor = 60
	case "hours", "hour", "hr", "hrs", "h":
		factor = 3600
	case "days", "day", "d":
		factor = 86400
	case "kb":
		factor = 1000
	case "kib":
		factor = 1024
	case "mb":
		factor = 1000 * 1000
	case "mib":
		factor = 1024 * 1024
	case "gb":
		factor = 1000 * 1000 * 1000
	case "gib":
		factor = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unsupported unit %q", unit)
	}
	return value * factor, nil
}

func inNormalizedRange(value float64, min, max *float64) bool {
	if min != nil && value < *min {
		return false
	}
	if max != nil && value > *max {
		return false
	}
	return true
}

func finite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func validID(id string) bool {
	if id == "" || len(id) > 160 || strings.TrimSpace(id) != id {
		return false
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '.', '_', '-', ':', '@', '/', '\\':
			continue
		default:
			return false
		}
	}
	return true
}

func validName(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || len(s) > 256 || strings.ContainsRune(s, '\x00') {
		return false
	}
	return true
}

func parseErr(id string, offset int, msg string) *model.BizError {
	return model.NewError(model.CodeConfigParse, id, offset, msg)
}

func CanonicalJSON(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}
