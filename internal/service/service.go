package service

import (
	"context"
	"fmt"

	"mitmpipeline/internal/codec"
	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/normalize"
	"mitmpipeline/internal/pki"
	"mitmpipeline/internal/replay"
	"mitmpipeline/internal/store"
)

type Service struct {
	Store *store.Store
}

func New(st *store.Store) *Service {
	return &Service{Store: st}
}

type ImportOptions struct {
	BatchID      string
	RoundingRule string
}

type ImportResult struct {
	BatchID       string               `json:"batch_id"`
	CommitVersion int64                `json:"commit_version"`
	Negotiation   domain.Negotiation   `json:"negotiation"`
	Conclusions   []*domain.Conclusion `json:"conclusions"`
}

type batchIdentity struct {
	sni    string
	serial string
}

func (s *Service) ImportData(ctx context.Context, data []byte, opts ImportOptions) (*ImportResult, error) {
	records, negotiation, _, err := codec.DecodeAny(data)
	if err != nil {
		return &ImportResult{Negotiation: negotiation}, err
	}
	format := negotiation.Format
	if format == "" && len(records) > 0 {
		format = records[0].Format
	}
	return s.ImportRecords(ctx, records, opts, negotiation, format)
}

func (s *Service) ImportRecords(ctx context.Context, records []domain.Record, opts ImportOptions, negotiation domain.Negotiation, format string) (*ImportResult, error) {
	if opts.RoundingRule == "" {
		opts.RoundingRule = normalize.RoundTruncate
	}
	if opts.BatchID == "" {
		opts.BatchID = fmt.Sprintf("batch-%d", domain.NowMS())
	}

	conclusions := make([]*domain.Conclusion, 0, len(records))
	batchIDs := map[string]batchIdentity{}

	for index := range records {
		record := &records[index]
		if err := codec.ValidateRecord(record); err != nil {
			if de, ok := domain.AsError(err); ok && de.Context["record_index"] == nil {
				de.WithContext("record_index", index+1)
			}
			return nil, err
		}

		rawID := record.ConnectionID
		canonicalID := normalize.ConnectionID(rawID)
		canonicalSNI := normalize.SNI(record.SNI)
		leafSerial := leafSerial(record.CertChainPEM)

		if prior, ok := batchIDs[canonicalID]; ok {
			if err := normalize.CheckIdentityConflict(canonicalID, prior.sni, prior.serial, canonicalID, canonicalSNI, leafSerial); err != nil {
				return nil, err
			}
		} else {
			existingSNI, existingSerial, err := s.Store.GetIdentity(ctx, canonicalID)
			if err != nil {
				return nil, err
			}
			if existingSNI != "" || existingSerial != "" {
				if err := normalize.CheckIdentityConflict(canonicalID, existingSNI, existingSerial, canonicalID, canonicalSNI, leafSerial); err != nil {
					return nil, err
				}
			}
		}
		batchIDs[canonicalID] = batchIdentity{sni: canonicalSNI, serial: leafSerial}

		record.ConnectionID = canonicalID
		record.SNI = canonicalSNI
		normalizedEvents, _, err := normalize.NormalizeRecordEvents(record, opts.RoundingRule)
		if err != nil {
			return nil, err
		}
		switches, err := s.Store.ConnectionSwitches(ctx, canonicalID)
		if err != nil {
			return nil, err
		}

		conclusion, replayErr := replay.Replay(*record, normalizedEvents, replay.Options{ExtraTimeline: switches})
		if conclusion != nil {
			conclusion.ConnectionID = rawID
			conclusion.CanonicalConnectionID = canonicalID
			conclusion.SNI = canonicalSNI
		}
		if replayErr != nil {
			if replayErr.Context == nil {
				replayErr.Context = map[string]any{}
			}
			replayErr.Context["record_index"] = index + 1
			replayErr.Context["batch_id"] = opts.BatchID
			return nil, replayErr
		}
		conclusions = append(conclusions, conclusion)
	}

	commit, _, err := s.Store.SaveBatch(ctx, store.SaveBatchInput{
		BatchID: opts.BatchID, Format: format, Negotiation: &negotiation,
		Records: records, Conclusions: conclusions,
	})
	if err != nil {
		return nil, err
	}
	return &ImportResult{BatchID: opts.BatchID, CommitVersion: commit, Negotiation: negotiation, Conclusions: conclusions}, nil
}

func (s *Service) ReplayConnection(ctx context.Context, connectionID, roundingRule string) (*domain.Conclusion, error) {
	canonicalID := normalize.ConnectionID(connectionID)
	record, err := s.Store.GetRecord(ctx, canonicalID)
	if err != nil {
		return nil, err
	}
	if roundingRule == "" {
		roundingRule = normalize.RoundTruncate
	}
	events, _, err := normalize.NormalizeRecordEvents(record, roundingRule)
	if err != nil {
		return nil, err
	}
	switches, err := s.Store.ConnectionSwitches(ctx, canonicalID)
	if err != nil {
		return nil, err
	}
	conclusion, perr := replay.Replay(*record, events, replay.Options{ExtraTimeline: switches})
	if perr != nil {
		return conclusion, perr
	}
	_, _, err = s.Store.SaveBatch(ctx, store.SaveBatchInput{
		BatchID: fmt.Sprintf("replay-%s-%d", canonicalID, domain.NowMS()),
		Format:  record.Format, Records: []domain.Record{*record}, Conclusions: []*domain.Conclusion{conclusion},
	})
	if err != nil {
		return nil, err
	}
	if conclusion.ReasonCode == domain.CodeChainInvalid {
		return conclusion, domain.NewError(domain.CodeChainInvalid, "certificate chain validation failed; conclusion is insufficient_evidence, not MITM").
			WithContext("connection_id", canonicalID).
			WithContext("conclusion", conclusion.Conclusion)
	}
	return conclusion, nil
}

type SwitchPolicyInput struct {
	Policy        domain.Policy `json:"policy"`
	ConnectionID  string        `json:"connection_id,omitempty"`
	EffectiveAtMS int64         `json:"effective_at_ms,omitempty"`
	RetireVersion string        `json:"retire_version,omitempty"`
	RoundingRule  string        `json:"rounding_rule,omitempty"`
	Replay        bool          `json:"replay,omitempty"`
}

type SwitchPolicyResult struct {
	Policy        domain.StoredPolicyVersion `json:"policy"`
	CommitVersion int64                      `json:"commit_version"`
	Conclusion    *domain.Conclusion         `json:"conclusion,omitempty"`
}

func (s *Service) SwitchPolicy(ctx context.Context, input SwitchPolicyInput) (*SwitchPolicyResult, error) {
	if input.Policy.Version == "" || input.Policy.Mode == "" {
		return nil, domain.NewError(domain.CodeRecordCorrupt, "policy.version and policy.mode are required")
	}
	if input.EffectiveAtMS == 0 {
		input.EffectiveAtMS = domain.NowMS()
	}
	if input.RetireVersion != "" {
		existing, perr := s.Store.GetPolicyVersion(ctx, input.RetireVersion)
		if perr != nil && perr.Code != domain.CodeNotFound {
			return nil, perr
		}
		if existing != nil && existing.Status == "retired" {
			return nil, perr
		}
	}
	connectionID := ""
	if input.ConnectionID != "" {
		connectionID = normalize.ConnectionID(input.ConnectionID)
	}
	stored, commit, err := store.SavePolicySwitch(ctx, s.Store, input.Policy, connectionID, input.EffectiveAtMS, input.RetireVersion)
	if err != nil {
		return nil, err
	}
	result := &SwitchPolicyResult{Policy: stored, CommitVersion: commit}
	if input.Replay && connectionID != "" {
		conclusion, err := s.ReplayConnection(ctx, connectionID, input.RoundingRule)
		if err != nil {
			return result, err
		}
		result.Conclusion = conclusion
	}
	return result, nil
}

func leafSerial(pemChain string) string {
	certs, err := pki.ParseCertificates(pemChain)
	if err != nil {
		return ""
	}
	for _, cert := range certs {
		if !cert.IsCA {
			return normalize.Serial(cert.SerialNumber.Text(16))
		}
	}
	if len(certs) > 0 {
		return normalize.Serial(certs[0].SerialNumber.Text(16))
	}
	return ""
}
