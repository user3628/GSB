package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"profilehotspot/internal/apperr"
	"profilehotspot/internal/engine"
	"profilehotspot/internal/model"
	"profilehotspot/internal/storage"
)

type IngestResponse struct {
	BatchID                  string             `json:"batch_id"`
	PolicyVersion            uint64             `json:"policy_version"`
	CommitSeq                int64              `json:"commit_seq"`
	TotalObservations        int                `json:"total_observations"`
	AcceptedUniqueSamples    int                `json:"accepted_unique_samples"`
	NewSamples               int                `json:"new_samples"`
	DuplicateExistingSamples int                `json:"duplicate_existing_samples"`
	DuplicateObservations    int64              `json:"duplicate_observations"`
	ModeByService            map[string]string  `json:"mode_by_service"`
	Modes                    engine.ModeSummary `json:"modes"`
	ReservoirCount           int                `json:"reservoir_count"`
	ElapsedMS                int64              `json:"elapsed_ms"`
	WithinBudget             bool               `json:"within_budget"`
	IdempotentReplay         bool               `json:"idempotent_replay"`
}

type inputGroup struct {
	Sample      model.SampleInput
	ContentHash string
	Occurrences int64
	ChannelHits map[string]int64
	Order       int
}

type batchItem struct {
	SampleID    string
	Outcome     string
	Occurrences int64
}

func (s *Service) Ingest(ctx context.Context, incoming model.SampleChunk) (*IngestResponse, error) {
	chunk := incoming
	if err := chunk.Normalize(); err != nil {
		return nil, validationAppError(err)
	}
	chunkHash := model.CanonicalChunkHash(chunk)
	var response *IngestResponse
	err := s.store.WithWrite(ctx, storage.IntentSpec{
		RequestKey:  "batch:" + chunk.BatchID,
		Kind:        "sample_batch",
		Target:      chunk.BatchID,
		PayloadHash: chunkHash,
		Context: map[string]any{
			"batch_id": chunk.BatchID,
			"samples":  len(chunk.Samples),
		},
	}, func(tx *sql.Tx) error {
		if existing, found, err := storage.FindBatchTx(ctx, tx, chunk.BatchID); err != nil {
			return err
		} else if found {
			if existing.PayloadHash != chunkHash {
				return apperr.ConflictingRequest("batch id was already committed with a different payload", map[string]any{
					"batch_id":       chunk.BatchID,
					"existing_hash":  existing.PayloadHash,
					"submitted_hash": chunkHash,
				})
			}
			var cached IngestResponse
			if err := json.Unmarshal(existing.Response, &cached); err != nil {
				return err
			}
			cached.IdempotentReplay = true
			response = &cached
			return nil
		}

		state, err := storage.ReadStateTx(ctx, tx)
		if err != nil {
			return err
		}
		if state.CurrentPolicyVersion == 0 {
			return apperr.StaleBase(map[string]any{
				"reason":          "no committed policy snapshot",
				"current_version": 0,
			})
		}
		if chunk.PolicyVersion != nil && *chunk.PolicyVersion != state.CurrentPolicyVersion {
			return apperr.StaleBase(map[string]any{
				"base_version":    *chunk.PolicyVersion,
				"current_version": state.CurrentPolicyVersion,
			})
		}
		policy, err := storage.ReadPolicyTx(ctx, tx, state.CurrentPolicyVersion)
		if err != nil {
			return err
		}
		for index, sample := range chunk.Samples {
			if _, ok := policy.ChannelConfig(sample.Channel); !ok {
				return apperr.SampleInvalid("sample channel is not declared by the current policy", map[string]any{
					"field":          fmt.Sprintf("samples[%d].channel", index),
					"sample_id":      sample.ID,
					"channel":        sample.Channel,
					"policy_version": policy.Version,
				})
			}
		}

		groups, err := groupSamples(chunk.Samples)
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(groups))
		for _, group := range groups {
			ids = append(ids, group.Sample.ID)
		}
		existing, err := storage.LoadSamplesByIDsTx(ctx, tx, ids)
		if err != nil {
			return err
		}
		for _, group := range groups {
			if record, ok := existing[group.Sample.ID]; ok && record.ContentHash != group.ContentHash {
				return apperr.SampleConflict("sample id already exists with different content", map[string]any{
					"sample_id":        group.Sample.ID,
					"existing_hash":    record.ContentHash,
					"submitted_hash":   group.ContentHash,
					"existing_service": record.Service,
				})
			}
		}

		started := time.Now()
		commitSeq := state.CommitSeq + 1
		nextOrder := state.NextSampleOrder
		response = &IngestResponse{
			BatchID:               chunk.BatchID,
			PolicyVersion:         state.CurrentPolicyVersion,
			CommitSeq:             commitSeq,
			TotalObservations:     len(chunk.Samples),
			AcceptedUniqueSamples: len(groups),
			ModeByService:         make(map[string]string),
		}
		batchItems := make([]batchItem, 0, len(groups))

		for _, group := range groups {
			if _, ok := existing[group.Sample.ID]; ok {
				if err := storage.AddSampleObservationsTx(ctx, tx, group.Sample.ID, group.Occurrences, group.ChannelHits); err != nil {
					return err
				}
				batchItems = append(batchItems, batchItem{SampleID: group.Sample.ID, Outcome: "duplicate_existing", Occurrences: group.Occurrences})
				response.DuplicateExistingSamples++
				response.DuplicateObservations += group.Occurrences
				continue
			}
			record := engine.SampleRecord{
				ID:             group.Sample.ID,
				ContentHash:    group.ContentHash,
				TimestampMS:    group.Sample.TimestampMS,
				Service:        group.Sample.Service,
				FirstChannel:   group.Sample.Channel,
				Frames:         append([]model.Frame(nil), group.Sample.Frames...),
				Weight:         group.Sample.Weight,
				SeenOrder:      nextOrder,
				FirstBatchID:   chunk.BatchID,
				FirstCommitSeq: commitSeq,
				DuplicateCount: group.Occurrences - 1,
				ReceivedCount:  group.Occurrences,
			}
			nextOrder++
			if err := storage.InsertSampleTx(ctx, tx, record, group.ChannelHits); err != nil {
				return err
			}
			batchItems = append(batchItems, batchItem{SampleID: group.Sample.ID, Outcome: "accepted_new", Occurrences: group.Occurrences})
			response.NewSamples++
			response.DuplicateObservations += group.Occurrences - 1
		}

		allSamples, err := storage.LoadAllSamplesTx(ctx, tx)
		if err != nil {
			return err
		}
		reservoirs := engine.BuildReservoirs(policy, allSamples)
		hotspots := engine.BuildHotspots(policy, commitSeq, s.now(), allSamples, reservoirs)
		storage.MaybeCrash("during_hotspot_aggregation")
		elapsed := time.Since(started)
		hotspots.ElapsedMS = elapsed.Milliseconds()
		hotspots.WithinBudget = hotspots.ElapsedMS <= policy.RoundTimeBudgetMS
		response.ElapsedMS = hotspots.ElapsedMS
		response.WithinBudget = hotspots.WithinBudget
		response.Modes = hotspots.Modes
		response.ReservoirCount = len(reservoirs)
		for _, service := range hotspots.Services {
			response.ModeByService[service.Service] = service.Path.Mode
		}

		responseJSON, err := json.Marshal(response)
		if err != nil {
			return err
		}
		if err := storage.ReplaceReservoirsTx(ctx, tx, state.CurrentPolicyVersion, commitSeq, reservoirs); err != nil {
			return err
		}
		storage.MaybeCrash("after_reservoir_replace")
		if err := storage.InsertHotspotTx(ctx, tx, hotspots); err != nil {
			return err
		}
		storage.MaybeCrash("after_hotspot_insert")
		if err := storage.InsertBatchTx(ctx, tx, storage.BatchCommit{
			BatchID:       chunk.BatchID,
			PayloadHash:   chunkHash,
			PolicyVersion: state.CurrentPolicyVersion,
			CommitSeq:     commitSeq,
			Response:      responseJSON,
		}); err != nil {
			return err
		}
		for _, item := range batchItems {
			if err := storage.InsertBatchItemTx(ctx, tx, chunk.BatchID, item.SampleID, item.Outcome, item.Occurrences); err != nil {
				return err
			}
		}
		state.CommitSeq = commitSeq
		state.NextSampleOrder = nextOrder
		return storage.SetStateTx(ctx, tx, state)
	})
	if err != nil {
		return nil, apperr.From(err)
	}
	return response, nil
}

func groupSamples(samples []model.SampleInput) ([]*inputGroup, error) {
	order := make([]*inputGroup, 0, len(samples))
	byID := make(map[string]*inputGroup, len(samples))
	for index, sample := range samples {
		hash := sample.ContentHash()
		group := byID[sample.ID]
		if group == nil {
			group = &inputGroup{
				Sample:      sample,
				ContentHash: hash,
				ChannelHits: make(map[string]int64),
				Order:       index,
			}
			byID[sample.ID] = group
			order = append(order, group)
		} else if group.ContentHash != hash {
			return nil, apperr.SampleConflict("sample id occurs more than once in this batch with different content", map[string]any{
				"sample_id":        sample.ID,
				"first_hash":       group.ContentHash,
				"conflicting_hash": hash,
				"first_index":      group.Order,
				"conflict_index":   index,
			})
		}
		group.Occurrences++
		group.ChannelHits[sample.Channel]++
	}
	sort.Slice(order, func(i, j int) bool { return order[i].Order < order[j].Order })
	return order, nil
}
