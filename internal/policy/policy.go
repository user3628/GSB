package policy

import (
	"regexp"
	"sort"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/fixed"
	"baselineworkbench/internal/units"
)

var idPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._:-]{0,127}$`)

type Item struct {
	ID               string       `json:"id"`
	Unit             string       `json:"unit"`
	RawUnit          string       `json:"raw_unit"`
	RawValue         string       `json:"raw_value"`
	WeightRaw        string       `json:"weight_raw"`
	WeightMicros     int64        `json:"weight_micros"`
	Family           string       `json:"family"`
	Normalized       int64        `json:"normalized"`
	NormalizedScaled int64        `json:"normalized_scaled"`
	ErrorUpperScaled int64        `json:"error_upper_scaled"`
	Conversion       []units.Step `json:"conversion_chain"`
	Source           string       `json:"source"`
	SourceOffset     int          `json:"source_offset"`
}

type Issue struct {
	Code    codes.Code `json:"code"`
	Message string     `json:"message"`
	ItemID  string     `json:"item_id,omitempty"`
	Offset  int        `json:"offset"`
	Source  string     `json:"source"`
}

type RawRecord struct {
	ID     string
	Unit   string
	Value  string
	Weight string
	Offset int
	Source string
}

type ParseResult struct {
	Items  []Item
	Issues []Issue
}

func NormalizeRecord(record RawRecord) (Item, *Issue) {
	if !idPattern.MatchString(record.ID) {
		return Item{}, &Issue{
			Code:    codes.PolicyMalformed,
			Message: "policy id must match ^[A-Za-z][A-Za-z0-9._:-]{0,127}$",
			ItemID:  record.ID,
			Offset:  record.Offset,
			Source:  record.Source,
		}
	}
	weightRaw := record.Weight
	if weightRaw == "" {
		weightRaw = "1.0"
	}
	weight, err := fixed.ParseDecimalToMicros(weightRaw)
	if err != nil {
		business := toBusiness(err, codes.PolicyMalformed)
		return Item{}, &Issue{Code: business.Code, Message: business.Message, ItemID: record.ID, Offset: record.Offset, Source: record.Source}
	}
	if weight.Value < 0 {
		return Item{}, &Issue{Code: codes.PolicyMalformed, Message: "weight must be non-negative", ItemID: record.ID, Offset: record.Offset, Source: record.Source}
	}
	if weight.HasDiscard {
		return Item{}, &Issue{Code: codes.PolicyMalformed, Message: "weight precision exceeds 1e-6", ItemID: record.ID, Offset: record.Offset, Source: record.Source}
	}
	converted, business := units.Convert(record.Unit, record.Value)
	if business != nil {
		return Item{}, &Issue{Code: business.Code, Message: business.Message, ItemID: record.ID, Offset: record.Offset, Source: record.Source}
	}
	return Item{
		ID:               record.ID,
		Unit:             converted.CanonicalUnit,
		RawUnit:          record.Unit,
		RawValue:         record.Value,
		WeightRaw:        weightRaw,
		WeightMicros:     weight.Value,
		Family:           converted.Family,
		Normalized:       converted.Normalized,
		NormalizedScaled: converted.NormalizedScaled,
		ErrorUpperScaled: converted.ErrorUpperScaled,
		Conversion:       converted.Steps,
		Source:           record.Source,
		SourceOffset:     record.Offset,
	}, nil
}

func toBusiness(err error, fallback codes.Code) *codes.BusinessError {
	if business, ok := err.(*codes.BusinessError); ok {
		return business
	}
	return codes.New(fallback, "%v", err)
}

func Effective(baseline, repairs []Item) []Item {
	merged := make(map[string]Item, len(baseline)+len(repairs))
	for _, item := range baseline {
		merged[item.ID] = item
	}
	for _, item := range repairs {
		merged[item.ID] = item
	}
	out := make([]Item, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func SortItems(items []Item) {
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
}
