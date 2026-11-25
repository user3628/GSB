package experiment

import (
	"context"

	"regworkbench/internal/checkpoint"
	"regworkbench/internal/cv"
)

func (s *Session) restoreCheckpoints(ctx context.Context) error {
	records, err := s.Store.Records()
	if err != nil {
		return err
	}
	completedStages := []string{}
	completedFolds := []int{}
	var validation *ValidationSummary
	var finalModel *ModelArtifact
	var split *cv.Split
	folds := map[int]*FoldArtifact{}

	for _, rec := range records {
		switch rec.Stage {
		case "preprocess":
			completedStages = append(completedStages, "preprocess")
		case "split":
			completedStages = append(completedStages, "split")
			var payload cv.Split
			if err := s.Store.LoadPayload(rec.Seq, &payload); err != nil {
				return err
			}
			split = &payload
		case "fold":
			var art FoldArtifact
			if err := s.Store.LoadPayload(rec.Seq, &art); err != nil {
				return err
			}
			folds[rec.Fold] = &art
			completedFolds = append(completedFolds, rec.Fold)
			completedStages = append(completedStages, "fold:"+itoa(rec.Fold))
		case "selection":
			completedStages = append(completedStages, "selection")
			var summary ValidationSummary
			if err := s.Store.LoadPayload(rec.Seq, &summary); err != nil {
				return err
			}
			validation = &summary
		case "final":
			completedStages = append(completedStages, "final")
			var model ModelArtifact
			if err := s.Store.LoadPayload(rec.Seq, &model); err != nil {
				return err
			}
			finalModel = &model
		}
	}
	if split != nil {
		s.Split = split
	}
	state := "ready"
	currentFold := -1
	if len(records) > 0 {
		currentFold = records[len(records)-1].Fold
	}
	if finalModel != nil {
		state = "completed"
	} else if len(records) > 0 {
		state = "checkpointed"
	}
	s.update(func(snap *Snapshot) {
		snap.State = state
		snap.CompletedStages = completedStages
		snap.CompletedFolds = completedFolds
		snap.Folds = folds
		snap.Validation = validation
		snap.FinalModel = finalModel
		snap.CurrentFold = currentFold
		snap.MonotonicSequence = int64(len(records))
		if validation != nil {
			snap.CurrentBest = s.recomputeCurrentBestInto(folds)
		} else {
			snap.CurrentBest = s.recomputeCurrentBestInto(folds)
		}
	})
	return nil
}

func itoa(v int) string {
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

func hasStage(records []checkpoint.Record, stage string) bool {
	for _, r := range records {
		if r.Stage == stage {
			return true
		}
	}
	return false
}
