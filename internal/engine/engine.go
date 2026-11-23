package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/fixed"
	"baselineworkbench/internal/ledger"
	"baselineworkbench/internal/parse"
	"baselineworkbench/internal/policy"
)

type Mode string

const (
	ModeStrict   Mode = "strict"
	ModeTolerant Mode = "tolerant"
)

type Config struct {
	Mode                Mode
	MaxConcurrency      int
	MaxItems            int
	MaxErrorBoundMicros int64
	SampleMarker        string
}

func (c Config) WithDefaults() Config {
	if c.Mode != ModeTolerant {
		c.Mode = ModeStrict
	}
	if c.MaxConcurrency < 1 {
		c.MaxConcurrency = 2
	}
	if c.MaxItems < 1 {
		c.MaxItems = 64
	}
	if c.MaxErrorBoundMicros < 0 {
		c.MaxErrorBoundMicros = 0
	}
	return c
}

type Input struct {
	PackagePath string
	Baseline    []byte
	Repairs     []byte
}

type Prepared struct {
	PackagePath string
	Baseline    []policy.Item
	Repairs     []policy.Item
	Effective   []policy.Item
	Issues      []policy.Issue
}

func Prepare(input Input) Prepared {
	baseline := parse.Snapshot(input.Baseline)
	repairs := parse.Repairs(input.Repairs)
	issues := append([]policy.Issue{}, baseline.Issues...)
	issues = append(issues, repairs.Issues...)
	policy.SortItems(baseline.Items)
	policy.SortItems(repairs.Items)
	return Prepared{
		PackagePath: input.PackagePath,
		Baseline:    baseline.Items,
		Repairs:     repairs.Items,
		Effective:   policy.Effective(baseline.Items, repairs.Items),
		Issues:      issues,
	}
}

type Interval struct {
	Lower int64  `json:"lower"`
	Upper int64  `json:"upper"`
	Unit  string `json:"unit"`
}

type Comparison struct {
	ItemID                    string   `json:"item_id"`
	WeightMicros              int64    `json:"weight_micros"`
	ReferenceUnit             string   `json:"reference_unit"`
	CandidateUnit             string   `json:"candidate_unit"`
	ReferenceNormalizedScaled int64    `json:"reference_normalized_scaled"`
	CandidateNormalizedScaled int64    `json:"candidate_normalized_scaled"`
	ReferenceInterval         Interval `json:"reference_interval"`
	CandidateInterval         Interval `json:"candidate_interval"`
	DeltaInterval             Interval `json:"delta_interval"`
	WeightedDeltaMicros       int64    `json:"weighted_delta_micros"`
	WeightedErrorUpperMicros  int64    `json:"weighted_error_upper_micros"`
	Classification            string   `json:"classification"`
}

const (
	ClassificationDeterministic = "deterministic_deviation"
	ClassificationUncertain     = "uncertain_interval"
)

type Result struct {
	PackagePath          string         `json:"package_path"`
	SessionID            string         `json:"session_id"`
	Code                 codes.Code     `json:"code"`
	Codes                []codes.Code   `json:"codes"`
	Complete             bool           `json:"complete"`
	Issues               []policy.Issue `json:"issues"`
	Items                []policy.Item  `json:"items"`
	BaselineItemCount    int            `json:"baseline_item_count"`
	RepairItemCount      int            `json:"repair_item_count"`
	EffectiveCount       int            `json:"effective_item_count"`
	ProcessedItems       int            `json:"processed_item_count"`
	UnmatchedItems       []string       `json:"unmatched_items"`
	Comparisons          []Comparison   `json:"comparisons"`
	ScoreMicros          int64          `json:"deviation_score_micros"`
	ErrorUpperMicros     int64          `json:"error_upper_micros"`
	ScoreInterval        Interval       `json:"score_interval"`
	Deterministic        int            `json:"deterministic_deviation_count"`
	Uncertain            int            `json:"uncertain_interval_count"`
	Ledger               ledger.Ledger  `json:"ledger"`
	RecoveredFromVersion int64          `json:"recovered_from_version,omitempty"`
	RecoveryCode         codes.Code     `json:"recovery_code,omitempty"`
}

func Run(ctx context.Context, inputs []Input, reference Prepared, cfg Config) []Result {
	cfg = cfg.WithDefaults()
	ordered := append([]Input{}, inputs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].PackagePath < ordered[j].PackagePath })
	results := make([]Result, len(ordered))
	jobs := make(chan int, len(ordered))
	for i := range ordered {
		jobs <- i
	}
	close(jobs)
	workers := cfg.MaxConcurrency
	if workers > len(ordered) {
		workers = len(ordered)
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if ctx.Err() != nil {
					results[index] = Result{
						PackagePath: ordered[index].PackagePath,
						SessionID:   sessionID(index),
						Code:        codes.None,
						Codes:       []codes.Code{},
						Issues:      []policy.Issue{},
						Items:       []policy.Item{},
						Comparisons: []Comparison{},
						Ledger:      ledger.New(),
					}
					continue
				}
				results[index] = runSession(ordered[index], reference, cfg, sessionID(index))
			}
		}()
	}
	wg.Wait()
	return results
}

func sessionID(index int) string { return fmt.Sprintf("session-%04d", index+1) }

func runSession(input Input, reference Prepared, cfg Config, id string) Result {
	prepared := Prepare(input)
	result := Result{
		PackagePath:       input.PackagePath,
		SessionID:         id,
		Code:              codes.None,
		Codes:             []codes.Code{},
		Issues:            append([]policy.Issue{}, prepared.Issues...),
		Items:             prepared.Effective,
		BaselineItemCount: len(prepared.Baseline),
		RepairItemCount:   len(prepared.Repairs),
		EffectiveCount:    len(prepared.Effective),
		Comparisons:       []Comparison{},
		UnmatchedItems:    []string{},
		Ledger:            ledger.New(),
	}
	codeList := make([]codes.Code, 0, len(prepared.Issues)+1)
	for _, issue := range prepared.Issues {
		codeList = append(codeList, issue.Code)
	}
	if cfg.Mode == ModeStrict && len(prepared.Issues) > 0 {
		result.Codes = codes.SortUnique(codeList)
		result.Code = codes.Primary(result.Codes)
		result.Ledger.Terminal = ledger.TerminalRolledBack
		result.ScoreInterval = Interval{Unit: "score_1e-6"}
		return result
	}

	referenceByID := make(map[string]policy.Item, len(reference.Effective))
	for _, item := range reference.Effective {
		referenceByID[item.ID] = item
	}
	itemLimit := cfg.MaxItems
	processCount := len(prepared.Effective)
	if processCount > itemLimit {
		processCount = itemLimit
	}
	budgetExceeded := len(prepared.Effective) > itemLimit
	sessionCheckpoint := result.Ledger.Checkpoint()
	var score int64
	var errorUpper int64
	for _, candidate := range prepared.Effective[:processCount] {
		ref, ok := referenceByID[candidate.ID]
		if !ok {
			result.UnmatchedItems = append(result.UnmatchedItems, candidate.ID)
			result.ProcessedItems++
			continue
		}
		itemCheckpoint := result.Ledger.Checkpoint()
		comparison, itemErr := compareItems(ref, candidate, &result.Ledger)
		if itemErr != nil {
			if itemErr.Code == codes.NumericOverflow {
				result.Ledger.Rollback(sessionCheckpoint)
				codeList = append(codeList, itemErr.Code)
				result.Codes = codes.SortUnique(codeList)
				result.Code = codes.Primary(result.Codes)
				result.Ledger.Terminal = ledger.TerminalRolledBack
				result.ScoreInterval = Interval{Unit: "score_1e-6"}
				return result
			}
			result.Ledger.Rollback(itemCheckpoint)
			result.Issues = append(result.Issues, policy.Issue{
				Code:    itemErr.Code,
				Message: itemErr.Message,
				ItemID:  candidate.ID,
				Offset:  candidate.SourceOffset,
				Source:  candidate.Source,
			})
			codeList = append(codeList, itemErr.Code)
			continue
		}
		newScore, err := fixed.AddInt64(score, comparison.WeightedDeltaMicros)
		if err != nil {
			result.Ledger.Rollback(sessionCheckpoint)
			codeList = append(codeList, codes.NumericOverflow)
			result.Codes = codes.SortUnique(codeList)
			result.Code = codes.Primary(result.Codes)
			result.Ledger.Terminal = ledger.TerminalRolledBack
			result.ScoreInterval = Interval{Unit: "score_1e-6"}
			return result
		}
		newError, err := fixed.AddInt64(errorUpper, comparison.WeightedErrorUpperMicros)
		if err != nil {
			result.Ledger.Rollback(sessionCheckpoint)
			codeList = append(codeList, codes.NumericOverflow)
			result.Codes = codes.SortUnique(codeList)
			result.Code = codes.Primary(result.Codes)
			result.Ledger.Terminal = ledger.TerminalRolledBack
			result.ScoreInterval = Interval{Unit: "score_1e-6"}
			return result
		}
		score, errorUpper = newScore, newError
		result.Comparisons = append(result.Comparisons, comparison)
		if comparison.Classification == ClassificationDeterministic {
			result.Deterministic++
		} else {
			result.Uncertain++
		}
		result.ProcessedItems++
	}
	result.ScoreMicros = score
	result.ErrorUpperMicros = errorUpper
	scoreLower, err := fixed.SubInt64(score, errorUpper)
	if err != nil {
		result.Ledger.Rollback(sessionCheckpoint)
		codeList = append(codeList, codes.NumericOverflow)
		result.Codes = codes.SortUnique(codeList)
		result.Code = codes.Primary(result.Codes)
		result.Ledger.Terminal = ledger.TerminalRolledBack
		result.ScoreInterval = Interval{Unit: "score_1e-6"}
		return result
	}
	scoreUpper, err := fixed.AddInt64(score, errorUpper)
	if err != nil {
		result.Ledger.Rollback(sessionCheckpoint)
		codeList = append(codeList, codes.NumericOverflow)
		result.Codes = codes.SortUnique(codeList)
		result.Code = codes.Primary(result.Codes)
		result.Ledger.Terminal = ledger.TerminalRolledBack
		result.ScoreInterval = Interval{Unit: "score_1e-6"}
		return result
	}
	result.ScoreInterval = Interval{Lower: scoreLower, Upper: scoreUpper, Unit: "score_1e-6"}
	if budgetExceeded {
		codeList = append(codeList, codes.ItemBudget)
		result.Ledger.Terminal = ledger.TerminalPartial
	} else if cfg.MaxErrorBoundMicros > 0 && errorUpper > cfg.MaxErrorBoundMicros {
		if cfg.Mode == ModeStrict {
			result.Ledger.Rollback(sessionCheckpoint)
			codeList = append(codeList, codes.ErrorBound)
			result.Ledger.Terminal = ledger.TerminalRolledBack
			result.Codes = codes.SortUnique(codeList)
			result.Code = codes.Primary(result.Codes)
			return result
		}
		result.Ledger.Terminal = ledger.TerminalCommitted
	} else if len(codeList) > 0 {
		result.Ledger.Terminal = ledger.TerminalPartial
	} else {
		result.Ledger.Terminal = ledger.TerminalCommitted
	}
	result.Codes = codes.SortUnique(codeList)
	result.Code = codes.Primary(result.Codes)
	result.Complete = len(result.Codes) == 0 && result.ProcessedItems == len(prepared.Effective)
	return result
}

func compareItems(reference, candidate policy.Item, target *ledger.Ledger) (Comparison, *codes.BusinessError) {
	if reference.Family != candidate.Family {
		return Comparison{}, codes.New(codes.PolicyMalformed, "reference family %s differs from candidate family %s", reference.Family, candidate.Family)
	}
	if reference.WeightMicros != candidate.WeightMicros {
		return Comparison{}, codes.New(codes.PolicyMalformed, "reference weight differs from candidate weight")
	}
	appendConversionEntries(target, "reference", reference)
	appendConversionEntries(target, "candidate", candidate)

	errScaled, err := fixed.AddInt64(reference.ErrorUpperScaled, candidate.ErrorUpperScaled)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	deltaScaled, err := fixed.SubInt64(candidate.NormalizedScaled, reference.NormalizedScaled)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	weightedDelta, remainder, err := fixed.MulDivInt64(deltaScaled, candidate.WeightMicros, fixed.Scale)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	truncationError := int64(0)
	if remainder != 0 {
		truncationError = 1
		target.Add(ledger.Entry{
			ItemID:           candidate.ID,
			View:             "candidate",
			Stage:            "weighted_delta",
			Operation:        "truncate_divide_1e6",
			Input:            candidate.RawValue,
			Factor:           candidate.WeightMicros,
			Quotient:         weightedDelta,
			Remainder:        remainder,
			ErrorUpperScaled: truncationError,
			Note:             "weighted delta truncated to 1e-6 score unit",
		})
	}
	weightedError, err := fixed.CeilMulDivNonNegative(errScaled, candidate.WeightMicros, fixed.Scale)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	weightedError, err = fixed.AddInt64(weightedError, truncationError)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	target.Add(ledger.Entry{
		ItemID:           candidate.ID,
		View:             "candidate",
		Stage:            "error_upper_propagation",
		Operation:        "conversion_error_times_weight",
		Input:            candidate.RawValue,
		Factor:           candidate.WeightMicros,
		Quotient:         weightedError,
		ErrorUpperScaled: weightedError,
		Note:             "combined reference/candidate truncation upper bound in score_1e-6",
	})
	refLower, err := fixed.SubInt64(reference.NormalizedScaled, reference.ErrorUpperScaled)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	refUpper, err := fixed.AddInt64(reference.NormalizedScaled, reference.ErrorUpperScaled)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	candLower, err := fixed.SubInt64(candidate.NormalizedScaled, candidate.ErrorUpperScaled)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	candUpper, err := fixed.AddInt64(candidate.NormalizedScaled, candidate.ErrorUpperScaled)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	deltaLower, err := fixed.SubInt64(candLower, refUpper)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	deltaUpper, err := fixed.SubInt64(candUpper, refLower)
	if err != nil {
		return Comparison{}, toBusiness(err)
	}
	classification := ClassificationUncertain
	if candLower > refUpper || refLower > candUpper {
		classification = ClassificationDeterministic
	}
	return Comparison{
		ItemID:                    candidate.ID,
		WeightMicros:              candidate.WeightMicros,
		ReferenceUnit:             reference.Unit,
		CandidateUnit:             candidate.Unit,
		ReferenceNormalizedScaled: reference.NormalizedScaled,
		CandidateNormalizedScaled: candidate.NormalizedScaled,
		ReferenceInterval:         Interval{Lower: refLower, Upper: refUpper, Unit: "normalized_scaled"},
		CandidateInterval:         Interval{Lower: candLower, Upper: candUpper, Unit: "normalized_scaled"},
		DeltaInterval:             Interval{Lower: deltaLower, Upper: deltaUpper, Unit: "normalized_scaled"},
		WeightedDeltaMicros:       weightedDelta,
		WeightedErrorUpperMicros:  weightedError,
		Classification:            classification,
	}, nil
}

func appendConversionEntries(target *ledger.Ledger, view string, item policy.Item) {
	for _, step := range item.Conversion {
		target.Add(ledger.Entry{
			ItemID:           item.ID,
			View:             view,
			Stage:            step.Stage,
			Operation:        step.Operation,
			Input:            item.RawValue,
			From:             step.From,
			To:               step.To,
			Factor:           step.Factor,
			Quotient:         step.Quotient,
			Remainder:        step.Remainder,
			ErrorUpperScaled: step.ErrorUpperScaled,
		})
	}
}

func toBusiness(err error) *codes.BusinessError {
	if business, ok := err.(*codes.BusinessError); ok {
		return business
	}
	return codes.New(codes.NumericOverflow, "%v", err)
}
