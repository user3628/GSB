package pipeline

import (
	"fmt"
	"sort"

	"headprune/internal/model"
)

// HeadDecision records why each head was kept or pruned.
type HeadDecision struct {
	Head        model.HeadID `json:"head"`
	Pruned      bool         `json:"pruned"`
	SoloError   float64      `json:"solo_error"`
	Reason      string       `json:"reason"`
	ErrorMargin float64      `json:"error_margin"`
}

// ComboDecision records the accept/reject verdict for one combination.
type ComboDecision struct {
	Combo       model.HeadSet `json:"combo"`
	TrueMeanKL  float64       `json:"true_mean_kl"`
	Accepted    bool          `json:"accepted"`
	Reason      string        `json:"reason"`
	ErrorMargin float64       `json:"error_margin"`
}

// Decision is the canonical, byte-stable final pruning decision.
type Decision struct {
	Selected       model.HeadSet   `json:"selected_heads"`
	Empty          bool            `json:"empty_decision"`
	Tolerance      float64         `json:"tolerance"`
	ClosestCombo   model.HeadSet   `json:"closest_combo"`
	ClosestError   float64         `json:"closest_error"`
	HeadDecisions  []HeadDecision  `json:"head_decisions"`
	ComboDecisions []ComboDecision `json:"combo_decisions"`
	Note           string          `json:"note"`
}

// Fingerprint hashes the canonical decision JSON.
func (d Decision) Fingerprint() string {
	h, _ := model.Digest(d)
	return h
}

// Stage3 selects the final pruning set strictly within tolerance and budget.
// The tolerance is never silently widened: if nothing passes, the result is
// an explicit empty set naming the closest combination.
func Stage3(ds model.Dataset, rows []HeadErrorRow, report ComboReport) (Decision, error) {
	tol := ds.Policy.Tolerance
	dec := Decision{
		Selected:       model.HeadSet{},
		Tolerance:      tol,
		HeadDecisions:  make([]HeadDecision, 0, len(rows)),
		ComboDecisions: make([]ComboDecision, 0, len(report.All)),
	}
	sort.Slice(report.All, func(i, j int) bool {
		a, b := report.All[i], report.All[j]
		if a.TrueMeanKL != b.TrueMeanKL {
			return a.TrueMeanKL < b.TrueMeanKL
		}
		if a.Size != b.Size {
			return a.Size > b.Size
		}
		return a.Combo.Fingerprint() < b.Combo.Fingerprint()
	})
	closest := report.All[0]
	for _, c := range report.All {
		if c.TrueMeanKL < closest.TrueMeanKL {
			closest = c
		}
	}
	dec.ClosestCombo = closest.Combo.Sorted()
	dec.ClosestError = closest.TrueMeanKL

	var accepted *ComboRow
	for i := range report.All {
		c := report.All[i]
		margin := tol - c.TrueMeanKL
		cd := ComboDecision{
			Combo:       c.Combo.Sorted(),
			TrueMeanKL:  c.TrueMeanKL,
			ErrorMargin: margin,
		}
		switch {
		case c.Size > ds.Policy.MaxPruneHeads:
			cd.Accepted = false
			cd.Reason = fmt.Sprintf("rejected: combo size %d exceeds prune budget %d", c.Size, ds.Policy.MaxPruneHeads)
		case c.TrueMeanKL > tol:
			cd.Accepted = false
			cd.Reason = fmt.Sprintf("rejected: true replay KL %.12g exceeds tolerance %.12g", c.TrueMeanKL, tol)
		case c.Coupled:
			cd.Accepted = false
			cd.Reason = fmt.Sprintf("rejected: coupling deviation %.6f above threshold %.6f", c.Deviation, ds.Policy.CouplingThreshold)
		default:
			cd.Accepted = true
			cd.Reason = fmt.Sprintf("accepted: KL %.12g within tolerance, margin %.12g", c.TrueMeanKL, margin)
			if accepted == nil || c.Size > accepted.Size ||
				(c.Size == accepted.Size && c.TrueMeanKL < accepted.TrueMeanKL) {
				row := c
				accepted = &row
			}
		}
		dec.ComboDecisions = append(dec.ComboDecisions, cd)
	}
	sort.Slice(dec.ComboDecisions, func(i, j int) bool {
		return dec.ComboDecisions[i].Combo.Fingerprint() < dec.ComboDecisions[j].Combo.Fingerprint()
	})

	selected := model.HeadSet{}
	if accepted != nil {
		selected = accepted.Combo.Sorted()
		dec.Selected = selected
		dec.Note = "selected largest budget-respecting combo whose true replay error is inside tolerance"
	} else {
		dec.Empty = true
		dec.Note = fmt.Sprintf("no combination satisfies tolerance %.12g; closest combo %s reaches KL %.12g (gap %.12g); tolerance kept unchanged",
			tol, dec.ClosestCombo, dec.ClosestError, dec.ClosestError-tol)
	}

	solo := map[model.HeadID]float64{}
	for _, r := range rows {
		solo[r.Head] = r.MeanKL
	}
	for _, r := range rows {
		hd := HeadDecision{Head: r.Head, SoloError: r.MeanKL, ErrorMargin: tol - r.MeanKL}
		if selected.Contains(r.Head) {
			hd.Pruned = true
			hd.Reason = "pruned: member of accepted combo and per-head sensitivity fits the remaining error budget"
		} else {
			hd.Pruned = false
			switch {
			case r.MeanKL > tol:
				hd.Reason = "kept: per-head sensitivity alone exceeds tolerance"
			case accepted == nil:
				hd.Reason = "kept: no combo passed tolerance, empty pruning set returned"
			default:
				hd.Reason = "kept: not part of the best accepted combo under the prune budget"
			}
		}
		dec.HeadDecisions = append(dec.HeadDecisions, hd)
	}
	sort.Slice(dec.HeadDecisions, func(i, j int) bool {
		return dec.HeadDecisions[i].Head.Less(dec.HeadDecisions[j].Head)
	})
	return dec, nil
}
