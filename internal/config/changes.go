package config

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"secverify/internal/model"
)

const (
	ActionAdd    = "add"
	ActionModify = "modify"
	ActionDelete = "delete"
)

// BuildCandidate applies all operations to a cloned item map. It performs
// operation-level validation but never mutates the base slice.
func BuildCandidate(base []model.Item, ops []Operation) ([]model.Item, *model.BizError) {
	items := make(map[string]model.Item, len(base)+len(ops))
	for _, item := range base {
		items[item.ID] = model.CloneItem(item)
	}
	seenOps := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		if _, ok := seenOps[op.ID]; ok {
			return nil, parseErr(op.ID, op.PayloadOffset, "multiple operations target the same item")
		}
		seenOps[op.ID] = struct{}{}
		_, exists := items[op.ID]
		switch op.Op {
		case ActionAdd:
			if exists {
				return nil, parseErr(op.ID, op.PayloadOffset, "add targets an existing item")
			}
			if op.Item == nil || op.Item.ID != op.ID {
				return nil, parseErr(op.ID, op.PayloadOffset, "add item payload mismatch")
			}
			item := model.CloneItem(*op.Item)
			item.SourceOffset = op.PayloadOffset
			items[op.ID] = item
		case ActionModify:
			if !exists {
				return nil, parseErr(op.ID, op.PayloadOffset, "modify targets a missing item")
			}
			if op.Item == nil || op.Item.ID != op.ID {
				return nil, parseErr(op.ID, op.PayloadOffset, "modify item payload mismatch")
			}
			item := model.CloneItem(*op.Item)
			item.SourceOffset = op.PayloadOffset
			items[op.ID] = item
		case ActionDelete:
			if !exists {
				return nil, parseErr(op.ID, op.PayloadOffset, "delete targets a missing item")
			}
			delete(items, op.ID)
		default:
			return nil, parseErr(op.ID, op.PayloadOffset, "unsupported operation")
		}
	}
	out := make([]model.Item, 0, len(items))
	for _, id := range model.SortedIDs(items) {
		out = append(out, items[id])
	}
	return out, nil
}

// BuildDiffs computes per-operation state differences after both old and new
// states have had inherited account values resolved.
func BuildDiffs(before, after []model.Item, ops []Operation) []model.Diff {
	beforeMap := model.ItemMap(before)
	afterMap := model.ItemMap(after)
	out := make([]model.Diff, 0, len(ops))
	for _, op := range ops {
		oldItem, hadOld := beforeMap[op.ID]
		newItem, hasNew := afterMap[op.ID]
		d := model.Diff{ID: op.ID, Action: op.Op}
		switch op.Op {
		case ActionAdd:
			if hasNew {
				d.AfterHash = model.ItemHash(newItem)
				d.ChangedFields = itemFields(newItem)
			}
		case ActionDelete:
			if hadOld {
				d.BeforeHash = model.ItemHash(oldItem)
				d.ChangedFields = itemFields(oldItem)
			}
		case ActionModify:
			if hadOld && hasNew {
				d.BeforeHash = model.ItemHash(oldItem)
				d.AfterHash = model.ItemHash(newItem)
				d.ChangedFields = changedFields(oldItem, newItem)
				d.Tightened = Tightens(oldItem, newItem)
			}
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Action < out[j].Action
	})
	return out
}

func TriggerIDs(diffs []model.Diff) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, d := range diffs {
		if d.Action == ActionDelete || d.Tightened {
			if _, ok := seen[d.ID]; ok {
				continue
			}
			seen[d.ID] = struct{}{}
			out = append(out, d.ID)
		}
	}
	sort.Strings(out)
	return out
}

func Tightens(before, after model.Item) bool {
	if before.Kind != after.Kind {
		return true
	}
	switch before.Kind {
	case model.KindAccount:
		if before.Account == nil || after.Account == nil {
			return true
		}
		b, a := *before.Account, *after.Account
		if b.Objective != a.Objective {
			return true
		}
		switch b.Objective {
		case "at_least":
			return a.NormalizedValue > b.NormalizedValue
		case "at_most":
			return a.NormalizedValue < b.NormalizedValue
		default:
			return a.NormalizedValue != b.NormalizedValue
		}
	case model.KindAudit:
		if before.Audit == nil || after.Audit == nil {
			return true
		}
		b, a := *before.Audit, *after.Audit
		modeNarrowed := ModeBits(a.Mode)&^ModeBits(b.Mode) == 0 && ModeBits(a.Mode) != ModeBits(b.Mode)
		disabledNow := b.Enabled && !a.Enabled
		retentionIncreased := retention(a) > retention(b)
		return modeNarrowed || disabledNow || retentionIncreased
	case model.KindService:
		if before.Service == nil || after.Service == nil {
			return true
		}
		return serviceRank(after.Service.StartType) > serviceRank(before.Service.StartType)
	case model.KindRegistry:
		if before.Registry == nil || after.Registry == nil {
			return true
		}
		b, a := *before.Registry, *after.Registry
		modeNarrowed := ModeBits(a.Mode)&^ModeBits(b.Mode) == 0 && ModeBits(a.Mode) != ModeBits(b.Mode)
		aclChanged := canonicalACL(b.ACL) != canonicalACL(a.ACL)
		return modeNarrowed || aclChanged
	}
	return false
}

func serviceRank(start string) int {
	switch start {
	case "automatic":
		return 0
	case "manual":
		return 1
	default:
		return 2
	}
}

func retention(a model.AuditPolicy) float64 {
	if a.RetentionValue == nil {
		return 0
	}
	value, _ := convertQuantity(*a.RetentionValue, a.RetentionUnit)
	return value
}

func canonicalACL(acl []model.ACLEntry) string {
	data, _ := json.Marshal(acl)
	return string(data)
}

func changedFields(before, after model.Item) []string {
	b := itemFieldMap(before)
	a := itemFieldMap(after)
	keys := make(map[string]struct{}, len(b)+len(a))
	for k := range b {
		keys[k] = struct{}{}
	}
	for k := range a {
		keys[k] = struct{}{}
	}
	out := make([]string, 0)
	for k := range keys {
		if b[k] != a[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func itemFields(it model.Item) []string {
	m := itemFieldMap(it)
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func itemFieldMap(it model.Item) map[string]string {
	m := map[string]string{
		"kind": it.Kind,
		"refs": joinStrings(it.Refs),
	}
	if it.Account != nil {
		a := it.Account
		m["account.name"] = a.Name
		m["account.value"] = floatText(a.Value)
		m["account.unit"] = a.Unit
		m["account.min"] = floatText(a.Min)
		m["account.max"] = floatText(a.Max)
		m["account.inherit_from"] = a.InheritFrom
		m["account.objective"] = a.Objective
		m["account.effective_value"] = strconv.FormatFloat(a.NormalizedValue, 'f', -1, 64)
		m["account.effective_unit"] = a.NormalizedUnit
	}
	if it.Audit != nil {
		a := it.Audit
		m["audit.name"] = a.Name
		m["audit.category"] = a.Category
		m["audit.path"] = a.Path
		m["audit.mode"] = a.Mode
		m["audit.enabled"] = strconv.FormatBool(a.Enabled)
		m["audit.retention_value"] = floatText(a.RetentionValue)
		m["audit.retention_unit"] = a.RetentionUnit
	}
	if it.Service != nil {
		s := it.Service
		m["service.name"] = s.Name
		m["service.start_type"] = s.StartType
		m["service.image_path"] = s.ImagePath
	}
	if it.Registry != nil {
		r := it.Registry
		m["registry.key"] = r.Key
		m["registry.path"] = r.Path
		m["registry.mode"] = r.Mode
		m["registry.acl"] = canonicalACL(r.ACL)
		m["registry.hive"] = r.Hive
		m["registry.value"] = r.Value
	}
	return m
}

func joinStrings(values []string) string {
	out := ""
	for i, v := range values {
		if i > 0 {
			out += "\x00"
		}
		out += v
	}
	return out
}

func floatText(v *float64) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%g", *v)
}
