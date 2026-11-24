package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"keylife/internal/adjudicator"
	"keylife/internal/model"
	"keylife/internal/resource"
	"keylife/internal/store"
)

type Config struct {
	MaxConcurrency      int
	DefaultSubjectLimit int
}

type Hooks struct {
	BeforeCommit func(operation, keyID string)
	AfterCommit  func(operation, keyID string)
}

type Service struct {
	store    *store.Store
	clock    model.Clock
	resource *resource.Controller
	hooks    Hooks

	gateMu sync.Mutex
	gates  map[string]struct{}
}

func New(st *store.Store, clock model.Clock, cfg Config, hooks Hooks) *Service {
	return &Service{
		store:    st,
		clock:    clock,
		resource: resource.New(resource.Config{MaxConcurrency: cfg.MaxConcurrency, DefaultSubjectLimit: cfg.DefaultSubjectLimit}),
		hooks:    hooks,
		gates:    make(map[string]struct{}),
	}
}

type GenerateRequest struct {
	KeyID            string
	Actor            string
	Subject          string
	Algorithm        string
	StrengthBits     int
	Lifetime         model.MonoDuration
	RotationInterval model.MonoDuration
}

type TransitionRequest struct {
	Actor string
	KeyID string
}

type MutationResult struct {
	Operation       string           `json:"operation"`
	Time            model.MonoTime   `json:"time"`
	Key             model.Key        `json:"key"`
	Successor       *model.Key       `json:"successor,omitempty"`
	Metrics         model.KeyMetrics `json:"metrics"`
	SnapshotVersion int64            `json:"snapshot_version"`
	ContentHash     string           `json:"content_hash"`
	ChainHash       string           `json:"chain_hash"`
	AuditEvents     []model.Audit    `json:"audit_events"`
}

type snapshotWrite struct {
	version     int64
	contentHash string
	chainHash   string
}

func (s *Service) Generate(ctx context.Context, req GenerateRequest) (MutationResult, error) {
	now := s.clock.Now()
	policy, err := s.store.GetSubject(ctx, req.Subject)
	if err != nil {
		return MutationResult{}, err
	}
	if req.Actor != req.Subject {
		return MutationResult{}, model.NewError(model.CodeSubjectScope, "actor %q cannot create keys for subject %q", req.Actor, req.Subject)
	}
	release, err := s.resource.Acquire(ctx, req.Subject, policy.MaxPendingRequests)
	if err != nil {
		return MutationResult{}, err
	}
	defer release()

	var resultKey model.Key
	var audit model.Audit
	var snap snapshotWrite
	err = s.store.Write(ctx, func(tx *store.Tx) error {
		policy, err := tx.GetSubject(ctx, req.Subject)
		if err != nil {
			return err
		}
		spec, ok := model.LookupAlgorithm(req.Algorithm)
		if !ok {
			return model.NewError(model.CodeConfig, "unknown algorithm %q", req.Algorithm)
		}
		strength, err := model.ResolveStrength(spec, req.StrengthBits)
		if err != nil {
			return err
		}
		if strength < policy.MinStrengthBits {
			return model.NewError(model.CodeWeakAlgo, "algorithm %s strength %d is below subject policy %d", spec.Name, strength, policy.MinStrengthBits)
		}
		lifetime := req.Lifetime
		if lifetime <= 0 {
			lifetime = policy.DefaultLifetime
		}
		if lifetime <= 0 {
			lifetime = model.D(1000)
		}
		rotation := req.RotationInterval
		if rotation <= 0 {
			rotation = policy.RotationInterval
		}
		keyID := req.KeyID
		if keyID == "" {
			keyID = stableID("key", req.Subject, spec.Name, fmt.Sprint(strength), fmt.Sprint(now.Tick), fmt.Sprint(lifetime))
		}
		if _, err := tx.GetKey(ctx, keyID); err == nil {
			return model.NewError(model.CodeDuplicateKey, "key %q already exists", keyID)
		} else if model.CodeOf(err) != model.CodeKeyNotFound {
			return err
		}
		key := model.Key{
			ID: keyID, Subject: req.Subject, Algorithm: spec.Name, Family: spec.Family,
			StrengthBits: strength, CreatedAt: now, ExpiresAt: now.Add(lifetime),
			RotationInterval: rotation, Phase: model.PhaseGenerated,
		}
		version, err := tx.NextSnapshotVersion(ctx)
		if err != nil {
			return err
		}
		audit = model.Audit{
			EventID: makeEventID("generate", key.Subject, key.ID, version), Subject: key.Subject,
			KeyID: key.ID, Action: "generate", FromPhase: "", ToPhase: model.PhaseGenerated,
			Code: model.CodeOK, At: now, SnapshotVersion: version,
		}
		audit.Seq, err = tx.InsertAudit(ctx, audit)
		if err != nil {
			return err
		}
		if err := tx.PutKey(ctx, key); err != nil {
			return err
		}
		snap, err = commitSnapshot(ctx, tx, version, now)
		if err != nil {
			return err
		}
		resultKey = key
		return nil
	})
	if err != nil {
		return MutationResult{}, err
	}
	s.afterCommit("generate", resultKey.ID)
	return MutationResult{
		Operation: "generate", Time: now, Key: resultKey, Metrics: model.Metrics(resultKey, now),
		SnapshotVersion: snap.version, ContentHash: snap.contentHash, ChainHash: snap.chainHash,
		AuditEvents: []model.Audit{audit},
	}, nil
}

func (s *Service) Activate(ctx context.Context, req TransitionRequest) (MutationResult, error) {
	return s.transition(ctx, "activate", req, false)
}

func (s *Service) Archive(ctx context.Context, req TransitionRequest) (MutationResult, error) {
	return s.transition(ctx, "archive", req, false)
}

func (s *Service) Destroy(ctx context.Context, req TransitionRequest) (MutationResult, error) {
	return s.transition(ctx, "destroy", req, true)
}

func (s *Service) transition(ctx context.Context, action string, req TransitionRequest, crashHook bool) (MutationResult, error) {
	now := s.clock.Now()
	current, err := s.store.GetKey(ctx, req.KeyID)
	if err != nil {
		return MutationResult{}, err
	}
	if req.Actor != current.Subject {
		return MutationResult{}, model.NewError(model.CodeSubjectScope, "actor %q cannot %s key owned by %q", req.Actor, action, current.Subject)
	}
	policy, err := s.store.GetSubject(ctx, current.Subject)
	if err != nil {
		return MutationResult{}, err
	}
	release, err := s.resource.Acquire(ctx, current.Subject, policy.MaxPendingRequests)
	if err != nil {
		return MutationResult{}, err
	}
	defer release()

	var resultKey model.Key
	var audit model.Audit
	var snap snapshotWrite
	err = s.store.Write(ctx, func(tx *store.Tx) error {
		key, err := tx.GetKey(ctx, req.KeyID)
		if err != nil {
			return err
		}
		if req.Actor != key.Subject {
			return model.NewError(model.CodeSubjectScope, "actor %q cannot %s key owned by %q", req.Actor, action, key.Subject)
		}
		policy, err := tx.GetSubject(ctx, key.Subject)
		if err != nil {
			return err
		}
		if action == "activate" {
			if key.StrengthBits < policy.MinStrengthBits {
				return model.NewError(model.CodeWeakAlgo, "key strength %d is below subject policy %d", key.StrengthBits, policy.MinStrengthBits)
			}
			if now.Tick >= key.ExpiresAt.Tick {
				return model.NewError(model.CodeKeyExpired, "key expired at tick %d", key.ExpiresAt.Tick)
			}
		}
		expected, ok := model.ExpectedPhase(action)
		if !ok || key.Phase != expected {
			return model.NewError(model.CodePhaseTransition, "cannot %s key in phase %s", action, key.Phase)
		}
		next, _ := model.NextPhase(key.Phase)
		key.Phase = next
		version, err := tx.NextSnapshotVersion(ctx)
		if err != nil {
			return err
		}
		audit = model.Audit{
			EventID: makeEventID(action, key.Subject, key.ID, version), Subject: key.Subject,
			KeyID: key.ID, Action: action, FromPhase: expected, ToPhase: next,
			Code: model.CodeOK, At: now, SnapshotVersion: version,
		}
		audit.Seq, err = tx.InsertAudit(ctx, audit)
		if err != nil {
			return err
		}
		if err := tx.PutKey(ctx, key); err != nil {
			return err
		}
		if crashHook && s.hooks.BeforeCommit != nil {
			s.hooks.BeforeCommit(action, key.ID)
		}
		snap, err = commitSnapshot(ctx, tx, version, now)
		if err != nil {
			return err
		}
		resultKey = key
		return nil
	})
	if err != nil {
		return MutationResult{}, err
	}
	s.afterCommit(action, resultKey.ID)
	return MutationResult{
		Operation: action, Time: now, Key: resultKey, Metrics: model.Metrics(resultKey, now),
		SnapshotVersion: snap.version, ContentHash: snap.contentHash, ChainHash: snap.chainHash,
		AuditEvents: []model.Audit{audit},
	}, nil
}

func (s *Service) Rotate(ctx context.Context, req TransitionRequest) (MutationResult, error) {
	if !s.claimRotation(req.KeyID) {
		return MutationResult{}, model.NewError(model.CodeRotationContention, "rotation already in progress for key %q", req.KeyID)
	}
	defer s.releaseRotation(req.KeyID)

	now := s.clock.Now()
	current, err := s.store.GetKey(ctx, req.KeyID)
	if err != nil {
		return MutationResult{}, err
	}
	if req.Actor != current.Subject {
		return MutationResult{}, model.NewError(model.CodeSubjectScope, "actor %q cannot rotate key owned by %q", req.Actor, current.Subject)
	}
	policy, err := s.store.GetSubject(ctx, current.Subject)
	if err != nil {
		return MutationResult{}, err
	}
	release, err := s.resource.Acquire(ctx, current.Subject, policy.MaxPendingRequests)
	if err != nil {
		return MutationResult{}, err
	}
	defer release()

	var resultKey, successor model.Key
	var audits []model.Audit
	var snap snapshotWrite
	err = s.store.Write(ctx, func(tx *store.Tx) error {
		key, err := tx.GetKey(ctx, req.KeyID)
		if err != nil {
			return err
		}
		if req.Actor != key.Subject {
			return model.NewError(model.CodeSubjectScope, "actor %q cannot rotate key owned by %q", req.Actor, key.Subject)
		}
		policy, err := tx.GetSubject(ctx, key.Subject)
		if err != nil {
			return err
		}
		if key.StrengthBits < policy.MinStrengthBits {
			return model.NewError(model.CodeWeakAlgo, "key strength %d is below subject policy %d", key.StrengthBits, policy.MinStrengthBits)
		}
		if now.Tick >= key.ExpiresAt.Tick {
			return model.NewError(model.CodeKeyExpired, "key expired at tick %d", key.ExpiresAt.Tick)
		}
		if key.Phase != model.PhaseActive {
			return model.NewError(model.CodePhaseTransition, "cannot rotate key in phase %s", key.Phase)
		}
		successorID := stableID("successor", key.ID, fmt.Sprint(key.CreatedAt.Tick))
		if _, err := tx.GetKey(ctx, successorID); err == nil {
			return model.NewError(model.CodeDuplicateKey, "successor key %q already exists", successorID)
		} else if model.CodeOf(err) != model.CodeKeyNotFound {
			return err
		}
		lifetime := model.MonoDuration(key.ExpiresAt.Tick - key.CreatedAt.Tick)
		successor = model.Key{
			ID: successorID, Subject: key.Subject, Algorithm: key.Algorithm, Family: key.Family,
			StrengthBits: key.StrengthBits, CreatedAt: now, ExpiresAt: now.Add(lifetime),
			RotationInterval: key.RotationInterval, Phase: model.PhaseGenerated, ParentID: key.ID,
		}
		version, err := tx.NextSnapshotVersion(ctx)
		if err != nil {
			return err
		}
		key.Phase = model.PhaseRotated
		key.SuccessorID = successorID
		rotateAudit := model.Audit{
			EventID: makeEventID("rotate", key.Subject, key.ID, version), Subject: key.Subject,
			KeyID: key.ID, Action: "rotate", FromPhase: model.PhaseActive, ToPhase: model.PhaseRotated,
			Code: model.CodeOK, At: now, SnapshotVersion: version, Detail: "successor=" + successorID,
		}
		rotateAudit.Seq, err = tx.InsertAudit(ctx, rotateAudit)
		if err != nil {
			return err
		}
		successorAudit := model.Audit{
			EventID: makeEventID("generate-successor", successor.Subject, successor.ID, version), Subject: successor.Subject,
			KeyID: successor.ID, Action: "generate", FromPhase: "", ToPhase: model.PhaseGenerated,
			Code: model.CodeOK, At: now, SnapshotVersion: version, Detail: "parent=" + key.ID,
		}
		successorAudit.Seq, err = tx.InsertAudit(ctx, successorAudit)
		if err != nil {
			return err
		}
		if err := tx.PutKey(ctx, key); err != nil {
			return err
		}
		if err := tx.PutKey(ctx, successor); err != nil {
			return err
		}
		if s.hooks.BeforeCommit != nil {
			s.hooks.BeforeCommit("rotate", key.ID)
		}
		snap, err = commitSnapshot(ctx, tx, version, now)
		if err != nil {
			return err
		}
		resultKey = key
		audits = []model.Audit{rotateAudit, successorAudit}
		return nil
	})
	if err != nil {
		return MutationResult{}, err
	}
	s.afterCommit("rotate", resultKey.ID)
	return MutationResult{
		Operation: "rotate", Time: now, Key: resultKey, Successor: &successor,
		Metrics: model.Metrics(resultKey, now), SnapshotVersion: snap.version,
		ContentHash: snap.contentHash, ChainHash: snap.chainHash, AuditEvents: audits,
	}, nil
}

func (s *Service) Evaluate(ctx context.Context, req model.DecisionRequest) model.Decision {
	state, err := s.LoadEvaluationState(ctx, s.clock.Now())
	if err != nil {
		return model.Decision{Code: model.CodeConfig, Message: err.Error(), Request: req, Checks: []model.Check{{Name: "state_load", Code: model.CodeConfig, Detail: err.Error()}}}
	}
	return adjudicator.Evaluate(state, req)
}

func (s *Service) LoadEvaluationState(ctx context.Context, now model.MonoTime) (adjudicator.State, error) {
	policies, err := s.store.ListPolicies(ctx)
	if err != nil {
		return adjudicator.State{}, err
	}
	keys, err := s.store.ListKeys(ctx, "")
	if err != nil {
		return adjudicator.State{}, err
	}
	state := adjudicator.State{Now: now, Policies: make(map[string]model.SubjectPolicy), Keys: make(map[string]model.Key)}
	for _, p := range policies {
		state.Policies[p.Subject] = p
	}
	for _, key := range keys {
		state.Keys[key.ID] = key
	}
	return state, nil
}

func (s *Service) Adjudicate(ctx context.Context, requests []model.DecisionRequest, threshold int) (adjudicator.BatchResult, error) {
	state, err := s.LoadEvaluationState(ctx, s.clock.Now())
	if err != nil {
		return adjudicator.BatchResult{}, err
	}
	return adjudicator.Adjudicate(requests, state, threshold)
}

func (s *Service) ResourceStats() resource.Stats   { return s.resource.Stats() }
func (s *Service) ResourceConfig() resource.Config { return s.resource.Config() }
func (s *Service) Store() *store.Store             { return s.store }
func (s *Service) Now() model.MonoTime             { return s.clock.Now() }

func (s *Service) claimRotation(keyID string) bool {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	if _, exists := s.gates[keyID]; exists {
		return false
	}
	s.gates[keyID] = struct{}{}
	return true
}

func (s *Service) releaseRotation(keyID string) {
	s.gateMu.Lock()
	delete(s.gates, keyID)
	s.gateMu.Unlock()
}

func (s *Service) afterCommit(operation, keyID string) {
	if s.hooks.AfterCommit != nil {
		s.hooks.AfterCommit(operation, keyID)
	}
}

func commitSnapshot(ctx context.Context, tx *store.Tx, version int64, now model.MonoTime) (snapshotWrite, error) {
	policies, err := tx.ListPolicies(ctx)
	if err != nil {
		return snapshotWrite{}, err
	}
	keys, err := tx.ListKeys(ctx)
	if err != nil {
		return snapshotWrite{}, err
	}
	audits, err := tx.ListAudits(ctx)
	if err != nil {
		return snapshotWrite{}, err
	}
	state := model.NewStateSnapshot(version, policies, keys, audits)
	payload, err := model.CanonicalJSON(state)
	if err != nil {
		return snapshotWrite{}, err
	}
	contentHash, chainHash, err := tx.InsertSnapshot(ctx, version, payload, now.Tick)
	if err != nil {
		return snapshotWrite{}, err
	}
	return snapshotWrite{version: version, contentHash: contentHash, chainHash: chainHash}, nil
}

func stableID(prefix string, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + "-" + hex.EncodeToString(sum[:])[:20]
}

func makeEventID(action, subject, keyID string, version int64) string {
	return stableID("evt", action, subject, keyID, fmt.Sprint(version))
}
