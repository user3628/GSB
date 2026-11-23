package api

import (
	"encoding/json"
	"strings"

	"localinfer/workbench/internal/domain"
)

type rawFaults struct {
	LateRequestIDs          []string        `json:"late_request_ids"`
	LateRequestID           string          `json:"late_request_id"`
	DuplicateRequestID      string          `json:"duplicate_request_id"`
	VramShortfallBytes      json.RawMessage `json:"vram_shortfall_bytes"`
	PrecisionOffsetUS       *int64          `json:"precision_offset_us"`
	InterruptAfterBatches   *int            `json:"interrupt_after_batches"`
	InterruptAfterQuantiles bool            `json:"interrupt_after_quantiles"`
}

type rawOrganizationOptions struct {
	Mode                    *string         `json:"mode"`
	OrganizationOrder       *string         `json:"organization_order"`
	NodeVRAMBytes           json.RawMessage `json:"node_vram_bytes"`
	NodeVRAM                json.RawMessage `json:"node_vram"`
	OrganizationBudgetMS    *int64          `json:"organization_budget_ms"`
	AllowMixedModels        *bool           `json:"allow_mixed_models"`
	LatencyDecimals         *int            `json:"latency_decimals"`
	MaxAbsDeviationUS       *int64          `json:"max_abs_deviation_us"`
	MaxRelativeDeviationPPM *int64          `json:"max_relative_deviation_ppm"`
	LatePolicy              *string         `json:"late_policy"`
	Faults                  rawFaults       `json:"faults"`
}

func parseOrganizationOptions(data []byte) (domain.OrganizationOptions, error) {
	opts := domain.DefaultOrganizationOptions()
	if len(strings.TrimSpace(string(data))) == 0 {
		return opts, nil
	}
	var raw rawOrganizationOptions
	if err := decodeJSON(data, &raw, domain.CodeBadRequest); err != nil {
		return opts, err
	}
	if raw.Mode != nil {
		opts.Mode = *raw.Mode
	}
	if raw.OrganizationOrder != nil {
		opts.OrganizationOrder = *raw.OrganizationOrder
	}
	nodeRaw := raw.NodeVRAMBytes
	if len(nodeRaw) == 0 || string(nodeRaw) == "null" {
		nodeRaw = raw.NodeVRAM
	}
	if len(nodeRaw) > 0 && string(nodeRaw) != "null" {
		v, err := domain.ParseByteQuantityRaw(nodeRaw)
		if err != nil {
			return opts, domain.NewBizError(domain.CodeBadRequest, "invalid node_vram_bytes", map[string]any{"reason": err.Error()})
		}
		opts.NodeVRAMBytes = v
	}
	if raw.OrganizationBudgetMS != nil {
		opts.OrganizationBudgetMS = *raw.OrganizationBudgetMS
	}
	if raw.AllowMixedModels != nil {
		opts.AllowMixedModels = *raw.AllowMixedModels
	}
	if raw.LatencyDecimals != nil {
		opts.LatencyDecimals = *raw.LatencyDecimals
	}
	if raw.MaxAbsDeviationUS != nil {
		opts.MaxAbsDeviationUS = *raw.MaxAbsDeviationUS
	}
	if raw.MaxRelativeDeviationPPM != nil {
		opts.MaxRelativeDeviationPPM = *raw.MaxRelativeDeviationPPM
	}
	if raw.LatePolicy != nil {
		opts.LatePolicy = *raw.LatePolicy
	}
	opts.Faults.LateRequestIDs = append([]string(nil), raw.Faults.LateRequestIDs...)
	if raw.Faults.LateRequestID != "" {
		opts.Faults.LateRequestIDs = append(opts.Faults.LateRequestIDs, raw.Faults.LateRequestID)
	}
	opts.Faults.DuplicateRequestID = raw.Faults.DuplicateRequestID
	if len(raw.Faults.VramShortfallBytes) > 0 && string(raw.Faults.VramShortfallBytes) != "null" {
		v, err := domain.ParseByteQuantityRaw(raw.Faults.VramShortfallBytes)
		if err != nil {
			return opts, domain.NewBizError(domain.CodeBadRequest, "invalid vram_shortfall_bytes", map[string]any{"reason": err.Error()})
		}
		opts.Faults.VramShortfallBytes = v
	}
	if raw.Faults.PrecisionOffsetUS != nil {
		opts.Faults.PrecisionOffsetUS = *raw.Faults.PrecisionOffsetUS
	}
	opts.Faults.InterruptAfterBatches = raw.Faults.InterruptAfterBatches
	opts.Faults.InterruptAfterQuant = raw.Faults.InterruptAfterQuantiles
	return opts, nil
}
