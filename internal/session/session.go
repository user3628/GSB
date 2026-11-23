// Package session implements interactive-side and settlement-side
// adjudication with per-session isolated reasoning versions.
package session

import (
	"fmt"
	"sort"
	"sync"

	"injectctl/internal/codec"
	"injectctl/internal/metrics"
	"injectctl/internal/model"
)

// Correction records an explicit replacement of a prior verdict.
type Correction struct {
	SampleID      string        `json:"sample_id"`
	Seq           int           `json:"seq"`
	FromVerdict   model.Verdict `json:"from_verdict"`
	ToVerdict     model.Verdict `json:"to_verdict"`
	SourceSeq     int           `json:"correction_source_seq"`
	ReasonVersion int           `json:"reason_version"`
}

// Session is an isolated adjudication conversation for one batch.
type Session struct {
	mu sync.Mutex

	BatchID        string
	ContextVersion string
	maxEncoded     int

	interactive map[string]model.Verdict
	order       []string
	final       map[string]model.Verdict
	corrections []Correction
	version     int
	closed      bool

	rec *metrics.Recorder
}

// New creates a session with isolated state and an injected latency recorder.
func New(batchID, contextVersion string, maxEncodedBytes int, rec *metrics.Recorder) *Session {
	return &Session{
		BatchID:        batchID,
		ContextVersion: contextVersion,
		maxEncoded:     maxEncodedBytes,
		interactive:    map[string]model.Verdict{},
		final:          map[string]model.Verdict{},
		version:        1,
		rec:            rec,
	}
}

// AdmitInteractive produces the immediate preliminary verdict on receipt.
func (s *Session) AdmitInteractive(sample model.Sample) (model.Verdict, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return model.Verdict{}, fmt.Errorf("batch %s already closed", s.BatchID)
	}

	var verdict model.Verdict
	var adjCode string
	s.rec.Measure(sample.Seq, sample.Landing, "", func() {
		res, err := codec.Adjudicate(&sample, s.maxEncoded)
		if err != nil {
			adjCode = model.CodeContextInvalid
			return
		}
		verdict = toVerdict(sample, res, s.version)
		adjCode = verdict.Code
	})
	// Re-tag the latency point with the actual adjudication code.
	if pts := s.rec.Points(); len(pts) > 0 {
		s.rec.SetLastCode(adjCode)
	}

	if prior, exists := s.interactive[sample.ID]; exists {
		// Out-of-order / late-arriving duplicate: explicit correction.
		s.version++
		verdict.ReasonVersion = s.version
		s.corrections = append(s.corrections, Correction{
			SampleID: sample.ID, Seq: sample.Seq,
			FromVerdict: prior, ToVerdict: verdict,
			SourceSeq: sample.Seq, ReasonVersion: s.version,
		})
	} else {
		s.order = append(s.order, sample.ID)
	}
	s.interactive[sample.ID] = verdict
	return verdict, nil
}

// Close settles the batch: every sample is re-adjudicated independently and
// matched against the interactive verdict by sample id and rule id.
func (s *Session) Close(samples []model.Sample) (map[string]model.Verdict, []Correction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.final, append([]Correction(nil), s.corrections...), nil
	}

	for _, sample := range samples {
		res, err := codec.Adjudicate(&sample, s.maxEncoded)
		if err != nil {
			return nil, nil, err
		}
		settled := toVerdict(sample, res, s.version)

		prelim, ok := s.interactive[sample.ID]
		if !ok {
			// Sample that never reached the interactive side; still settled.
			s.version++
			settled.ReasonVersion = s.version
			s.corrections = append(s.corrections, Correction{
				SampleID: sample.ID, Seq: sample.Seq,
				FromVerdict: model.Verdict{SampleID: sample.ID, Code: "MISSING"},
				ToVerdict:   settled, SourceSeq: sample.Seq, ReasonVersion: s.version,
			})
			s.final[sample.ID] = settled
			continue
		}

		if !equalAdjudication(prelim, settled) {
			// Contract violation: settlement must match interactive output
			// because both run the same deterministic codec. Record explicitly.
			s.version++
			settled.ReasonVersion = s.version
			s.corrections = append(s.corrections, Correction{
				SampleID: sample.ID, Seq: sample.Seq,
				FromVerdict: prelim, ToVerdict: settled,
				SourceSeq: sample.Seq, ReasonVersion: s.version,
			})
		} else {
			settled.ReasonVersion = prelim.ReasonVersion
		}
		s.final[sample.ID] = settled
	}

	s.closed = true
	return s.final, append([]Correction(nil), s.corrections...), nil
}

// InteractiveVerdicts returns preliminary verdicts in arrival order.
func (s *Session) InteractiveVerdicts() []model.Verdict {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Verdict, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.interactive[id])
	}
	return out
}

// FinalVerdicts returns settlement verdicts sorted by sample sequence.
func (s *Session) FinalVerdicts() []model.Verdict {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Verdict, 0, len(s.final))
	for _, v := range s.final {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Corrections returns all explicit verdict replacements.
func (s *Session) Corrections() []Correction {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Correction(nil), s.corrections...)
}

// Version reports the current reasoning version.
func (s *Session) Version() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version
}

// Closed reports whether the batch has been settled.
func (s *Session) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func toVerdict(sample model.Sample, res codec.Result, version int) model.Verdict {
	return model.Verdict{
		SampleID:      sample.ID,
		Seq:           sample.Seq,
		Surface:       sample.Surface,
		Landing:       sample.Landing,
		Code:          res.Code,
		RuleID:        res.RuleID,
		RawOffset:     res.RawOffset,
		LandedOffset:  res.LandedOffset,
		RawLength:     res.RawLength,
		LandedLength:  res.LandedLength,
		Normalized:    res.Normalized,
		ReasonVersion: version,
		Marker:        res.Marker,
	}
}

func equalAdjudication(a, b model.Verdict) bool {
	return a.SampleID == b.SampleID &&
		a.Code == b.Code &&
		a.RuleID == b.RuleID &&
		a.RawOffset == b.RawOffset &&
		a.LandedOffset == b.LandedOffset &&
		a.Marker == b.Marker &&
		a.Normalized == b.Normalized
}
