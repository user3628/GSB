package tm

import (
	"sort"
	"strings"
)

type Occurrence struct {
	SegmentID    string  `json:"segment_id"`
	Paragraph    int     `json:"paragraph"`
	SourceIndex  int     `json:"source_index"`
	TargetIndex  int     `json:"target_index"`
	SourceOffset Offset  `json:"source_offset"`
	TargetOffset Offset  `json:"target_offset"`
	SourceText   string  `json:"source_text"`
	TargetText   string  `json:"target_text"`
	Score        float64 `json:"alignment_score"`
	LogScore     float64 `json:"log_score"`
	UpdateSeq    int64   `json:"update_seq"`
	QualityRank  int     `json:"quality_rank"`
	QualityLabel string  `json:"quality_label"`
	QualityValid bool    `json:"quality_valid"`
}

type Unit struct {
	UnitID               string       `json:"unit_id"`
	SourceLang           string       `json:"source_lang"`
	TargetLang           string       `json:"target_lang"`
	SourceText           string       `json:"source_text"`
	TargetText           string       `json:"target_text"`
	WinningSegmentID     string       `json:"winning_segment_id"`
	WinningUpdateSeq     int64        `json:"winning_update_seq"`
	OccurrenceCount      int          `json:"occurrence_count"`
	DistinctSegmentCount int          `json:"distinct_segment_count"`
	Score                float64      `json:"score"`
	DecisionBasis        string       `json:"decision_basis"`
	Occurrences          []Occurrence `json:"occurrences"`
	Digest               string       `json:"digest,omitempty"`
}

type Suggestion struct {
	SuggestionID    string  `json:"suggestion_id"`
	LogicalID       string  `json:"logical_id"`
	UnitID          string  `json:"unit_id"`
	Version         int64   `json:"version"`
	SourceText      string  `json:"source_text"`
	TargetText      string  `json:"target_text"`
	Score           float64 `json:"score"`
	OccurrenceCount int     `json:"occurrence_count"`
	Digest          string  `json:"digest,omitempty"`
}

type SegmentFact struct {
	Record      SegmentRecord    `json:"record"`
	Source      SegmentedText    `json:"source"`
	Target      SegmentedText    `json:"target"`
	Alignment   SegmentAlignment `json:"alignment"`
	Occurrences []Occurrence     `json:"occurrences"`
	Digest      string           `json:"digest"`
}

type SegmentErrorReport struct {
	SegmentID string    `json:"segment_id"`
	Error     *BizError `json:"error"`
}

type BuildResult struct {
	Version     int64                `json:"version"`
	Config      AlignConfig          `json:"config"`
	Facts       []SegmentFact        `json:"facts"`
	Units       []Unit               `json:"units"`
	Suggestions []Suggestion         `json:"suggestions"`
	Errors      []SegmentErrorReport `json:"errors"`
}

type ExportBundle struct {
	SchemaVersion string       `json:"schema_version"`
	Version       int64        `json:"effective_version"`
	Units         []Unit       `json:"units"`
	Suggestions   []Suggestion `json:"suggestions"`
}

func UnitIDFor(sourceLang, targetLang, sourceText string) string {
	key := strings.Join([]string{
		strings.ToLower(strings.TrimSpace(sourceLang)),
		strings.ToLower(strings.TrimSpace(targetLang)),
		NormalizeText(sourceText),
	}, "|")
	return "u_" + HashString(key)[:32]
}

func SuggestionIDFor(unitID string, targetDigest string) string {
	return "s_" + HashString(unitID, targetDigest)[:32]
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func ProcessSegment(record SegmentRecord, cfg AlignConfig) (*SegmentFact, *BizError) {
	cfg = cfg.withDefaults()
	source := SegmentText(record.SourceText)
	target := SegmentText(record.TargetText)
	if len(ValidSentences(source.Sentences)) == 0 {
		return nil, NewSegmentEmpty(record.SegmentID, StageSegmentation, "source")
	}
	if len(ValidSentences(target.Sentences)) == 0 {
		return nil, NewSegmentEmpty(record.SegmentID, StageSegmentation, "target")
	}
	alignment, err := AlignSegment(record.SegmentID, record.SourceLang, record.TargetLang, source, target, cfg)
	if err != nil {
		if biz, ok := AsBizError(err); ok {
			return nil, biz
		}
		return nil, &BizError{Code: "E_ALIGN_FAILED", Message: err.Error(), SegmentID: record.SegmentID, Stage: StageAlignment, HTTPStatus: 500}
	}

	targetByIndex := map[int]Sentence{}
	for _, s := range target.Sentences {
		targetByIndex[s.Index] = s
	}
	sourceByIndex := map[int]Sentence{}
	for _, s := range source.Sentences {
		sourceByIndex[s.Index] = s
	}

	fact := &SegmentFact{Record: record, Source: source, Target: target, Alignment: alignment}
	for _, p := range alignment.Pairs {
		ts := targetByIndex[p.TargetIndex]
		ss := sourceByIndex[p.SourceIndex]
		fact.Occurrences = append(fact.Occurrences, Occurrence{
			SegmentID:    record.SegmentID,
			Paragraph:    ss.Paragraph,
			SourceIndex:  p.SourceIndex,
			TargetIndex:  p.TargetIndex,
			SourceOffset: p.SourceOffset,
			TargetOffset: p.TargetOffset,
			SourceText:   strings.TrimSpace(p.SourceText),
			TargetText:   strings.TrimSpace(p.TargetText),
			Score:        p.Score.Score,
			LogScore:     p.Score.LogScore,
			UpdateSeq:    record.UpdateSeq,
			QualityRank:  record.Quality.Rank,
			QualityLabel: record.Quality.Label,
			QualityValid: record.Quality.Valid,
		})
		_ = ts
	}
	digest, _, err := DigestCanonical(fact)
	if err != nil {
		return nil, &BizError{Code: "E_DIGEST_FAILED", Message: err.Error(), SegmentID: record.SegmentID, Stage: "merge", HTTPStatus: 500}
	}
	fact.Digest = digest
	return fact, nil
}

func BuildCorpus(records []SegmentRecord, version int64, cfg AlignConfig) *BuildResult {
	cfg = cfg.withDefaults()
	sorted := append([]SegmentRecord(nil), records...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].SegmentID < sorted[j].SegmentID })

	result := &BuildResult{Version: version, Config: cfg}
	unitMap := map[string]*Unit{}
	for _, record := range sorted {
		fact, bizErr := ProcessSegment(record, cfg)
		if bizErr != nil {
			result.Errors = append(result.Errors, SegmentErrorReport{SegmentID: record.SegmentID, Error: bizErr})
			continue
		}
		result.Facts = append(result.Facts, *fact)
		for _, occ := range fact.Occurrences {
			sourceText := occ.SourceText
			id := UnitIDFor(record.SourceLang, record.TargetLang, sourceText)
			u := unitMap[id]
			if u == nil {
				u = &Unit{
					UnitID:     id,
					SourceLang: record.SourceLang,
					TargetLang: record.TargetLang,
					SourceText: NormalizeText(sourceText),
				}
				unitMap[id] = u
			}
			u.Occurrences = append(u.Occurrences, occ)
		}
	}

	ids := make([]string, 0, len(unitMap))
	for id := range unitMap {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		u := unitMap[id]
		FinalizeUnit(u)
		result.Units = append(result.Units, *u)
		targetDigest := SHA256Hex([]byte(u.TargetText))
		sug := Suggestion{
			SuggestionID:    SuggestionIDFor(u.UnitID, targetDigest),
			LogicalID:       "s_" + u.UnitID[2:],
			UnitID:          u.UnitID,
			Version:         u.WinningUpdateSeq,
			SourceText:      u.SourceText,
			TargetText:      u.TargetText,
			Score:           u.Score,
			OccurrenceCount: u.OccurrenceCount,
		}
		sug.Digest, _, _ = DigestCanonical(sug)
		result.Suggestions = append(result.Suggestions, sug)
	}
	return result
}

func FinalizeUnit(u *Unit) {
	sort.SliceStable(u.Occurrences, func(i, j int) bool {
		a, b := u.Occurrences[i], u.Occurrences[j]
		if a.SegmentID != b.SegmentID {
			return a.SegmentID < b.SegmentID
		}
		if a.SourceOffset[0] != b.SourceOffset[0] {
			return a.SourceOffset[0] < b.SourceOffset[0]
		}
		return a.TargetOffset[0] < b.TargetOffset[0]
	})
	u.OccurrenceCount = len(u.Occurrences)
	distinct := map[string]bool{}
	var winner Occurrence
	have := false
	for _, o := range u.Occurrences {
		distinct[o.SegmentID] = true
		if !have || BetterOccurrence(o, winner) {
			winner = o
			have = true
		}
	}
	u.DistinctSegmentCount = len(distinct)
	if have {
		u.TargetText = winner.TargetText
		u.WinningSegmentID = winner.SegmentID
		u.WinningUpdateSeq = winner.UpdateSeq
		u.Score = winner.Score
		u.DecisionBasis = "quality_rank_desc,update_seq_desc,alignment_score_desc,segment_id_asc,source_offset_asc,target_offset_asc"
	}
	u.Digest = ""
	digest, _, _ := DigestCanonical(u)
	u.Digest = digest
}

func BetterOccurrence(a, b Occurrence) bool {
	if a.QualityRank != b.QualityRank {
		return a.QualityRank > b.QualityRank
	}
	if a.UpdateSeq != b.UpdateSeq {
		return a.UpdateSeq > b.UpdateSeq
	}
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	if a.SegmentID != b.SegmentID {
		return a.SegmentID < b.SegmentID
	}
	if a.SourceOffset[0] != b.SourceOffset[0] {
		return a.SourceOffset[0] < b.SourceOffset[0]
	}
	return a.TargetOffset[0] < b.TargetOffset[0]
}

func (r *BuildResult) Export() ExportBundle {
	units := append([]Unit(nil), r.Units...)
	suggestions := append([]Suggestion(nil), r.Suggestions...)
	sort.SliceStable(units, func(i, j int) bool { return units[i].UnitID < units[j].UnitID })
	sort.SliceStable(suggestions, func(i, j int) bool { return suggestions[i].SuggestionID < suggestions[j].SuggestionID })
	return ExportBundle{SchemaVersion: "tm.export.v1", Version: r.Version, Units: units, Suggestions: suggestions}
}

func (r *BuildResult) CanonicalExport() ([]byte, error) {
	return CanonicalJSON(r.Export())
}

func CompareExports(a, b ExportBundle) *BizError {
	ua := map[string]Unit{}
	ub := map[string]Unit{}
	for _, u := range a.Units {
		ua[u.UnitID] = u
	}
	for _, u := range b.Units {
		ub[u.UnitID] = u
	}
	ids := map[string]bool{}
	for id := range ua {
		ids[id] = true
	}
	for id := range ub {
		ids[id] = true
	}
	sortedIDs := make([]string, 0, len(ids))
	for id := range ids {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Strings(sortedIDs)
	for _, id := range sortedIDs {
		x, oka := ua[id]
		y, okb := ub[id]
		if !oka || !okb || x.Digest != y.Digest || x.OccurrenceCount != y.OccurrenceCount {
			xd := "missing"
			yd := "missing"
			if oka {
				xd = x.Digest
			}
			if okb {
				yd = y.Digest
			}
			return NewIncrementalMismatch(id, xd, yd)
		}
	}
	sa, _ := CanonicalJSON(a.Suggestions)
	sb, _ := CanonicalJSON(b.Suggestions)
	if string(sa) != string(sb) {
		ma := map[string]Suggestion{}
		mb := map[string]Suggestion{}
		for _, s := range a.Suggestions {
			ma[s.SuggestionID] = s
		}
		for _, s := range b.Suggestions {
			mb[s.SuggestionID] = s
		}
		sids := map[string]bool{}
		for id := range ma {
			sids[id] = true
		}
		for id := range mb {
			sids[id] = true
		}
		list := make([]string, 0, len(sids))
		for id := range sids {
			list = append(list, id)
		}
		sort.Strings(list)
		first := "suggestions"
		for _, id := range list {
			if ma[id].Digest != mb[id].Digest {
				first = "suggestion:" + id
				break
			}
		}
		return NewIncrementalMismatch(first, SHA256Hex(sa), SHA256Hex(sb))
	}
	return nil
}

const (
	StageSegmentation = "segmentation"
	StageAlignment    = "alignment"
	StageMerge        = "merge"
	StageInvalidation = "invalidation"
	StageUnit         = "unit"
)
