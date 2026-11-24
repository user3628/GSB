package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	"driftconverge/internal/apperr"
	"driftconverge/internal/batch"
	"driftconverge/internal/detect"
	"driftconverge/internal/fixed"
	"driftconverge/internal/model"
)

const (
	BatchBaseMicros  int64 = 750_000
	ObjectBaseMicros int64 = 20_000
	FieldBaseMicros  int64 = 10_000
	BudgetWeightPPM  int64 = 3_000
	PPMScale         int64 = 1_000_000
	RoundingMode           = "half_away_from_zero"
	CostFormula            = "batch_cost = 750000us + sum(20000us + 10000us*affected_fields + round_half_away(budget*3000/1000000))"
)

type RoundingStep struct {
	Scope            string `json:"scope"`
	Operation        string `json:"operation"`
	Mode             string `json:"mode"`
	Numerator        string `json:"numerator"`
	Denominator      int64  `json:"denominator"`
	Rounded          int64  `json:"rounded"`
	Remainder        string `json:"remainder"`
	ErrorFixedMicros string `json:"errorFixedMicros"`
}

type WriteItem struct {
	ObjectID         string                       `json:"objectId"`
	Kind             string                       `json:"kind"`
	Namespace        string                       `json:"namespace"`
	DriftCategories  []detect.Category            `json:"driftCategories"`
	DriftFields      []string                     `json:"driftFields"`
	DesiredFields    map[string]model.FieldValue  `json:"desiredFields"`
	ExpectedSources  map[string]model.FieldSource `json:"expectedSources,omitempty"`
	RemoveFields     []string                     `json:"removeFields,omitempty"`
	ResourceBudget   int64                        `json:"resourceBudget"`
	AffectedFieldCnt int                          `json:"affectedFieldCount"`
	FieldCostMicros  int64                        `json:"fieldCostMicros"`
	BudgetCostMicros int64                        `json:"budgetCostMicros"`
	ObjectCostMicros int64                        `json:"objectCostMicros"`
}

type BatchPlan struct {
	Index                 int               `json:"index"`
	BatchID               string            `json:"batchId"`
	ObjectIDs             []string          `json:"objectIds"`
	DependsOnBatchIndexes []int             `json:"dependsOnBatchIndexes"`
	MaxSeverityRank       int               `json:"maxSeverityRank"`
	PrimaryCategories     []detect.Category `json:"primaryCategories"`
	ObjectCount           int               `json:"objectCount"`
	DriftFieldCount       int               `json:"driftFieldCount"`
	ResourceBudgetTotal   int64             `json:"resourceBudgetTotal"`
	EstimatedCostMicros   int64             `json:"estimatedCostMicros"`
	EstimatedCost         string            `json:"estimatedCost"`
	CumulativeCostMicros  int64             `json:"cumulativeCostMicros"`
	CumulativeCost        string            `json:"cumulativeCost"`
	BudgetOccupancyFixed6 int64             `json:"budgetOccupancyFixed6"`
	BudgetOccupancy       string            `json:"budgetOccupancy"`
	RoundingRemainder     string            `json:"roundingRemainder"`
	RoundingSteps         []RoundingStep    `json:"roundingSteps"`
	CumulativeRoundingErr string            `json:"cumulativeRoundingErrorFixedMicros"`
	Objects               []WriteItem       `json:"objects"`
}

type Report struct {
	SnapshotVersion          int64       `json:"snapshotVersion"`
	Formula                  string      `json:"formula"`
	FixedPointScale          int64       `json:"fixedPointScale"`
	RoundingMode             string      `json:"roundingMode"`
	Batches                  []BatchPlan `json:"batches"`
	TotalObjectCount         int         `json:"totalObjectCount"`
	TotalDriftFieldCount     int         `json:"totalDriftFieldCount"`
	TotalResourceBudget      int64       `json:"totalResourceBudget"`
	TotalEstimatedCostMicros int64       `json:"totalEstimatedCostMicros"`
	TotalEstimatedCost       string      `json:"totalEstimatedCost"`
	TotalBudgetOccupancy     string      `json:"totalBudgetOccupancy"`
	TotalRoundingError       string      `json:"totalRoundingErrorFixedMicros"`
	RoundTimeBudgetMicros    int64       `json:"roundTimeBudgetMicros"`
	TimeBudgetTolerancePPM   int64       `json:"timeBudgetTolerancePpm"`
	StableSignature          string      `json:"stableSignature"`
}

func Build(cluster *model.ClusterSpec, drift *detect.Report, batches *batch.Report, policy *model.Policy) (*Report, error) {
	driftByID := make(map[string]detect.ObjectDrift, len(drift.Results))
	for _, result := range drift.Results {
		driftByID[result.ObjectID] = result
	}
	objects := cluster.ObjectByID()
	report := &Report{
		SnapshotVersion:        cluster.SnapshotVersion,
		Formula:                CostFormula,
		FixedPointScale:        fixed.Scale,
		RoundingMode:           RoundingMode,
		RoundTimeBudgetMicros:  policy.RoundTimeBudgetMs * 1000,
		TimeBudgetTolerancePPM: policy.TimeBudgetRelativeTolerancePPM,
	}
	var cumulative int64
	var cumulativeRoundingUnits int64
	for _, b := range batches.Batches {
		bp := BatchPlan{
			Index:                 b.Index,
			BatchID:               b.ID,
			ObjectIDs:             append([]string(nil), b.ObjectIDs...),
			DependsOnBatchIndexes: append([]int(nil), b.DependsOnBatchIndexes...),
			MaxSeverityRank:       b.MaxSeverityRank,
			PrimaryCategories:     append([]detect.Category(nil), b.PrimaryCategories...),
			ObjectCount:           len(b.ObjectIDs),
			EstimatedCostMicros:   BatchBaseMicros,
		}
		bp.RoundingSteps = append(bp.RoundingSteps, RoundingStep{
			Scope:            b.ID,
			Operation:        "batch_base",
			Mode:             "exact_integer",
			Numerator:        fmt.Sprintf("%d", BatchBaseMicros),
			Denominator:      1,
			Rounded:          BatchBaseMicros,
			Remainder:        "0",
			ErrorFixedMicros: "0.000000",
		})
		var batchRoundingUnits int64
		for _, id := range b.ObjectIDs {
			obj := objects[id]
			result := driftByID[id]
			if obj == nil || !result.HasDrift {
				continue
			}
			item, roundingUnits, rawRemainder, err := buildWriteItem(obj, result)
			if err != nil {
				return report, err
			}
			bp.Objects = append(bp.Objects, item)
			bp.DriftFieldCount += item.AffectedFieldCnt
			bp.ResourceBudgetTotal, err = addChecked(bp.ResourceBudgetTotal, item.ResourceBudget)
			if err != nil {
				return report, err
			}
			bp.EstimatedCostMicros, err = addChecked(bp.EstimatedCostMicros, item.ObjectCostMicros)
			if err != nil {
				return report, err
			}
			batchRoundingUnits += roundingUnits
			bp.RoundingSteps = append(bp.RoundingSteps, RoundingStep{
				Scope:            id,
				Operation:        "resource_budget_weight",
				Mode:             RoundingMode,
				Numerator:        fmt.Sprintf("%d*%d", item.ResourceBudget, BudgetWeightPPM),
				Denominator:      PPMScale,
				Rounded:          item.BudgetCostMicros,
				Remainder:        rawRemainder,
				ErrorFixedMicros: formatErrorUnits(roundingUnits),
			})
		}
		var err error
		cumulative, err = addChecked(cumulative, bp.EstimatedCostMicros)
		if err != nil {
			return report, err
		}
		bp.CumulativeCostMicros = cumulative
		bp.CumulativeCost = fixed.FormatMicros(cumulative) + "ms"
		bp.EstimatedCost = fixed.FormatMicros(bp.EstimatedCostMicros) + "ms"
		cumulativeRoundingUnits += batchRoundingUnits
		bp.CumulativeRoundingErr = formatErrorUnits(cumulativeRoundingUnits)
		occupancy, remainder, err := occupancyFixed6(bp.EstimatedCostMicros, bp.ResourceBudgetTotal)
		if err != nil {
			return report, err
		}
		bp.BudgetOccupancyFixed6 = occupancy
		bp.BudgetOccupancy = fmt.Sprintf("%d.%06d", occupancy/fixed.Scale, occupancy%fixed.Scale)
		bp.RoundingRemainder = remainder
		report.Batches = append(report.Batches, bp)
		report.TotalObjectCount += bp.ObjectCount
		report.TotalDriftFieldCount += bp.DriftFieldCount
		report.TotalResourceBudget, err = addChecked(report.TotalResourceBudget, bp.ResourceBudgetTotal)
		if err != nil {
			return report, err
		}
	}
	report.TotalEstimatedCostMicros = cumulative
	report.TotalEstimatedCost = fixed.FormatMicros(cumulative) + "ms"
	report.TotalRoundingError = formatErrorUnits(cumulativeRoundingUnits)
	occ, _, err := occupancyFixed6(cumulative, report.TotalResourceBudget)
	if err == nil {
		report.TotalBudgetOccupancy = fmt.Sprintf("%d.%06d", occ/fixed.Scale, occ%fixed.Scale)
	}
	canonical, _ := json.Marshal(report.Batches)
	sum := sha256.Sum256(canonical)
	report.StableSignature = "sha256:" + hex.EncodeToString(sum[:])
	return report, nil
}

func buildWriteItem(obj *model.Object, result detect.ObjectDrift) (WriteItem, int64, string, error) {
	driftFields := make(map[string]struct{})
	for _, fd := range result.FieldDrifts {
		driftFields[fd.Field] = struct{}{}
	}
	fieldNames := make([]string, 0, len(driftFields))
	for field := range driftFields {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)
	desired := make(map[string]model.FieldValue, len(obj.Expected))
	for field, value := range obj.Expected {
		desired[field] = value
	}
	sources := make(map[string]model.FieldSource)
	for field, source := range obj.Sources {
		if source.Expected != "" || source.Actual != "" {
			sources[field] = source
		}
	}
	remove := make([]string, 0)
	for field := range obj.Actual {
		if _, ok := obj.Expected[field]; !ok {
			remove = append(remove, field)
		}
	}
	sort.Strings(remove)
	fieldCost, err := mulChecked(FieldBaseMicros, int64(len(fieldNames)))
	if err != nil {
		return WriteItem{}, 0, "", err
	}
	budgetCost, remainder, err := fixed.MulDivHalfUp(obj.ResourceBudget, BudgetWeightPPM, PPMScale)
	if err != nil {
		return WriteItem{}, 0, "", apperr.New(apperr.SpecInvalid, "resource budget weight overflows for %s: %v", obj.ID, err)
	}
	objectCost, err := addChecked(ObjectBaseMicros, fieldCost)
	if err != nil {
		return WriteItem{}, 0, "", err
	}
	objectCost, err = addChecked(objectCost, budgetCost)
	if err != nil {
		return WriteItem{}, 0, "", err
	}
	// One fixed error unit is 1e-6 microsecond. error = rounded - exact, where
	// exact is budget*BudgetWeightPPM/PPMScale microseconds.
	units := new(big.Int).Mul(big.NewInt(budgetCost), big.NewInt(PPMScale))
	units.Sub(units, new(big.Int).Mul(big.NewInt(obj.ResourceBudget), big.NewInt(BudgetWeightPPM)))
	units.Mul(units, big.NewInt(fixed.Scale))
	units.Quo(units, big.NewInt(PPMScale))
	if !units.IsInt64() {
		return WriteItem{}, 0, "", apperr.New(apperr.SpecInvalid, "rounding remainder overflows for %s", obj.ID)
	}
	return WriteItem{
		ObjectID:         obj.ID,
		Kind:             obj.Kind,
		Namespace:        obj.Namespace,
		DriftCategories:  append([]detect.Category(nil), result.Categories...),
		DriftFields:      fieldNames,
		DesiredFields:    desired,
		ExpectedSources:  sources,
		RemoveFields:     remove,
		ResourceBudget:   obj.ResourceBudget,
		AffectedFieldCnt: len(fieldNames),
		FieldCostMicros:  fieldCost,
		BudgetCostMicros: budgetCost,
		ObjectCostMicros: objectCost,
	}, units.Int64(), remainder.String(), nil
}

func addChecked(a, b int64) (int64, error) {
	if b > 0 && a > (1<<63-1)-b {
		return 0, apperr.New(apperr.SpecInvalid, "fixed-point cost overflows int64")
	}
	if b < 0 && a < (-1<<63)-b {
		return 0, apperr.New(apperr.SpecInvalid, "fixed-point cost underflows int64")
	}
	return a + b, nil
}

func mulChecked(a, b int64) (int64, error) {
	q, _, err := fixed.MulDivHalfUp(a, b, 1)
	return q, err
}

func occupancyFixed6(cost, budget int64) (int64, string, error) {
	if budget == 0 {
		return 0, "budget_zero", nil
	}
	quotient, _, err := fixed.MulDivHalfUp(cost, fixed.Scale, budget)
	if err != nil {
		return 0, "", err
	}
	// Exact error = rounded - cost*Scale/budget, expressed as a ratio whose
	// denominator is budget.
	exactN := new(big.Int).Mul(big.NewInt(cost), big.NewInt(fixed.Scale))
	roundedN := new(big.Int).Mul(big.NewInt(quotient), big.NewInt(budget))
	errorN := new(big.Int).Sub(roundedN, exactN)
	return quotient, fmt.Sprintf("%s/%d", errorN.String(), budget), nil
}

func formatErrorUnits(units int64) string {
	sign := ""
	if units < 0 {
		sign = "-"
		units = -units
	}
	// units are 1e-6 microsecond. Render microseconds with six decimal places.
	return fmt.Sprintf("%s%d.%06d", sign, units/fixed.Scale, units%fixed.Scale)
}
