package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"local/objectstore/internal/model"
	"local/objectstore/internal/store"
)

const (
	defaultTTLMS          = int64(300_000)
	defaultVerifyBudget   = 2 * time.Second
	defaultVerifyParallel = 4
	defaultVerifyQueue    = 32
)

type Options struct {
	VerifyConcurrency      int
	VerifyQueueCapacity    int
	VerifyQueueCapacitySet bool
	VerifyBudget           time.Duration
	VerifyBudgetSet        bool
	Now                    func() time.Time
	NewSessionID           func() (string, error)
}

type Engine struct {
	store              *store.Store
	verifier           *Verifier
	now                func() time.Time
	newID              func() (string, error)
	verificationBudget time.Duration

	operationGate sync.RWMutex
	sessionLocks  keyedMutex
	keyLocks      keyedMutex
}

func New(database *store.Store, options Options) *Engine {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.NewSessionID == nil {
		options.NewSessionID = randomSessionID
	}
	if options.VerifyConcurrency == 0 {
		options.VerifyConcurrency = defaultVerifyParallel
	}
	if !options.VerifyQueueCapacitySet && options.VerifyQueueCapacity == 0 {
		options.VerifyQueueCapacity = defaultVerifyQueue
	}
	if options.VerifyQueueCapacity < 0 {
		options.VerifyQueueCapacity = defaultVerifyQueue
	}
	if options.VerifyBudget == 0 && !options.VerifyBudgetSet {
		options.VerifyBudget = defaultVerifyBudget
	}
	return &Engine{
		store:              database,
		verifier:           NewVerifier(options.VerifyConcurrency, options.VerifyQueueCapacity, options.Now),
		now:                options.Now,
		newID:              options.NewSessionID,
		verificationBudget: options.VerifyBudget,
		sessionLocks:       newKeyedMutex(),
		keyLocks:           newKeyedMutex(),
	}
}

func (e *Engine) Close() { e.verifier.Close() }

type CreateSessionInput struct {
	RequestID  string `json:"request_id,omitempty"`
	ObjectKey  string `json:"object_key"`
	LowerBound int64  `json:"lower_bound"`
	TTLMS      int64  `json:"ttl_ms"`
}

type CreateSessionOutput struct {
	Session                   model.Session `json:"session"`
	Reused                    bool          `json:"reused"`
	ReclaimedExpiredSessionID string        `json:"reclaimed_expired_session_id,omitempty"`
	IntentSeq                 int64         `json:"intent_seq"`
}

func (e *Engine) CreateSession(ctx context.Context, input CreateSessionInput) (CreateSessionOutput, *model.Error) {
	e.operationGate.RLock()
	defer e.operationGate.RUnlock()
	input.ObjectKey = strings.TrimSpace(input.ObjectKey)
	if input.ObjectKey == "" {
		return CreateSessionOutput{}, model.NewError(model.CodeInvalidArgument, "object_key is required")
	}
	if len(input.ObjectKey) > 1024 {
		return CreateSessionOutput{}, model.NewError(model.CodeInvalidArgument, "object_key exceeds 1024 bytes")
	}
	if input.LowerBound < 0 {
		return CreateSessionOutput{}, model.NewError(model.CodeInvalidArgument, "lower_bound must be non-negative")
	}
	if input.TTLMS < 0 {
		return CreateSessionOutput{}, model.NewError(model.CodeInvalidArgument, "ttl_ms must be non-negative")
	}
	if input.TTLMS == 0 {
		input.TTLMS = defaultTTLMS
	}
	nowMS := e.now().UnixMilli()

	unlock := e.keyLocks.Lock(input.ObjectKey)
	defer unlock()
	reclaimed, _, err := e.store.ReclaimExpiredForKey(ctx, input.ObjectKey, nowMS)
	if err != nil {
		return CreateSessionOutput{}, asBusinessError(err)
	}
	sessionID, err := e.newID()
	if err != nil {
		return CreateSessionOutput{}, model.WrapError(model.CodeInternal, "generate session id", err)
	}
	intent, err := e.store.AppendIntent(ctx, model.Intent{
		RequestID: input.RequestID,
		Action:    model.ActionCreateSession,
		ObjectKey: input.ObjectKey,
		PayloadJSON: marshal(map[string]any{
			"session_id": sessionID, "lower_bound": input.LowerBound, "ttl_ms": input.TTLMS,
		}),
	}, nowMS)
	if err != nil {
		return CreateSessionOutput{}, asBusinessError(err)
	}
	result, err := e.store.CreateSession(ctx, store.CreateSessionParams{
		SessionID:         sessionID,
		ObjectKey:         input.ObjectKey,
		LowerBound:        input.LowerBound,
		TTLMS:             input.TTLMS,
		CreationRequestID: input.RequestID,
		IntentSeq:         intent.Seq,
		NowMS:             nowMS,
	})
	if err != nil {
		e.rejectIntent(intent.Seq)
		return CreateSessionOutput{}, asBusinessError(err)
	}
	if result.ReclaimedID != "" {
		reclaimed.SessionID = result.ReclaimedID
	}
	return CreateSessionOutput{
		Session:                   result.Session,
		Reused:                    result.Reused,
		ReclaimedExpiredSessionID: reclaimed.SessionID,
		IntentSeq:                 intent.Seq,
	}, nil
}

type SubmitPartInput struct {
	RequestID   string `json:"request_id,omitempty"`
	SessionID   string `json:"-"`
	PartNumber  int64  `json:"part_number"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	ArrivedAtMS *int64 `json:"arrived_at_ms,omitempty"`
	Content     []byte `json:"-"`
}

type SubmitPartOutput struct {
	Part      model.Part `json:"part"`
	Duplicate bool       `json:"duplicate"`
	IntentSeq int64      `json:"intent_seq"`
}

func (e *Engine) SubmitPart(ctx context.Context, input SubmitPartInput) (SubmitPartOutput, *model.Error) {
	e.operationGate.RLock()
	defer e.operationGate.RUnlock()
	if input.SessionID == "" {
		return SubmitPartOutput{}, model.NewError(model.CodeInvalidArgument, "session_id is required")
	}
	if input.PartNumber < 0 {
		return SubmitPartOutput{}, model.NewError(model.CodeInvalidArgument, "part_number must be non-negative")
	}
	normalizedSHA := model.NormalizeSHA256(input.SHA256)
	if err := validateSHA256(normalizedSHA); err != nil {
		return SubmitPartOutput{}, err
	}
	if input.Size < 0 || int64(len(input.Content)) != input.Size {
		return SubmitPartOutput{}, model.NewError(model.CodeSizeMismatch, "declared part size does not match content length").
			WithContext("declared_size", input.Size).WithContext("actual_size", len(input.Content))
	}
	sum := sha256.Sum256(input.Content)
	actualSHA := hex.EncodeToString(sum[:])
	nowMS := e.now().UnixMilli()
	arrivedAt := nowMS
	if input.ArrivedAtMS != nil {
		if *input.ArrivedAtMS < 0 {
			return SubmitPartOutput{}, model.NewError(model.CodeInvalidArgument, "arrived_at_ms must be non-negative")
		}
		arrivedAt = *input.ArrivedAtMS
	}

	unlock := e.sessionLocks.Lock(input.SessionID)
	defer unlock()
	session, getErr := e.store.GetSession(ctx, input.SessionID)
	if getErr != nil {
		return SubmitPartOutput{}, asBusinessError(getErr)
	}
	if session.Status == model.SessionActive && session.ExpiresAtMS <= nowMS {
		_, _, _ = e.store.ReclaimExpiredForKey(ctx, session.ObjectKey, nowMS)
		return SubmitPartOutput{}, model.NewError(model.CodeSessionState, "upload session has expired").
			WithContext("session_id", input.SessionID).WithContext("expires_at_ms", session.ExpiresAtMS)
	}
	if session.Status != model.SessionActive {
		return SubmitPartOutput{}, model.NewError(model.CodeSessionState, "upload session is not active").
			WithContext("session_id", input.SessionID).WithContext("status", session.Status)
	}
	if input.PartNumber < session.LowerBound {
		return SubmitPartOutput{}, model.NewError(model.CodeInvalidArgument, "part number is below the session lower bound").
			WithContext("part_number", input.PartNumber).WithContext("lower_bound", session.LowerBound)
	}

	intent, appendErr := e.store.AppendIntent(ctx, model.Intent{
		RequestID:  input.RequestID,
		Action:     model.ActionSubmitPart,
		SessionID:  input.SessionID,
		ObjectKey:  session.ObjectKey,
		PartNumber: int64Ptr(input.PartNumber),
		SHA256:     actualSHA,
		Size:       int64Ptr(input.Size),
		PayloadJSON: marshal(map[string]any{
			"part_number": input.PartNumber, "size": input.Size,
			"sha256": actualSHA, "arrived_at_ms": arrivedAt,
		}),
	}, nowMS)
	if appendErr != nil {
		return SubmitPartOutput{}, asBusinessError(appendErr)
	}
	if actualSHA != normalizedSHA {
		e.rejectIntent(intent.Seq)
		return SubmitPartOutput{}, model.NewError(model.CodePartMismatch, "submitted content digest does not match sha256").
			WithContext("session_id", input.SessionID).
			WithContext("part_number", input.PartNumber).
			WithContext("expected_sha256", normalizedSHA).
			WithContext("actual_sha256", actualSHA)
	}
	result, err := e.store.SubmitPart(ctx, store.SubmitPartParams{
		SessionID: input.SessionID, PartNumber: input.PartNumber, Size: input.Size,
		SHA256: actualSHA, ArrivedAtMS: arrivedAt, Content: input.Content,
		IntentSeq: intent.Seq, NowMS: nowMS,
	})
	if err != nil {
		e.rejectIntent(intent.Seq)
		return SubmitPartOutput{}, asBusinessError(err)
	}
	return SubmitPartOutput{Part: result.Part, Duplicate: result.Duplicate, IntentSeq: intent.Seq}, nil
}

type CancelSessionInput struct {
	RequestID string `json:"request_id,omitempty"`
	SessionID string `json:"-"`
	Reason    string `json:"reason,omitempty"`
}

type CancelSessionOutput struct {
	store.CancelSessionResult
	IntentSeq int64 `json:"intent_seq"`
}

func (e *Engine) CancelSession(ctx context.Context, input CancelSessionInput) (CancelSessionOutput, *model.Error) {
	e.operationGate.RLock()
	defer e.operationGate.RUnlock()
	if input.SessionID == "" {
		return CancelSessionOutput{}, model.NewError(model.CodeInvalidArgument, "session_id is required")
	}
	if input.Reason == "" {
		input.Reason = "client_cancelled"
	}
	nowMS := e.now().UnixMilli()
	unlock := e.sessionLocks.Lock(input.SessionID)
	defer unlock()
	session, err := e.store.GetSession(ctx, input.SessionID)
	if err != nil {
		return CancelSessionOutput{}, asBusinessError(err)
	}
	if session.Status == model.SessionActive && session.ExpiresAtMS <= nowMS {
		_, _, _ = e.store.ReclaimExpiredForKey(ctx, session.ObjectKey, nowMS)
	}
	intent, appendErr := e.store.AppendIntent(ctx, model.Intent{
		RequestID: input.RequestID, Action: model.ActionCancelSession,
		SessionID: input.SessionID, ObjectKey: session.ObjectKey,
		PayloadJSON: marshal(map[string]any{"reason": input.Reason}),
	}, nowMS)
	if appendErr != nil {
		return CancelSessionOutput{}, asBusinessError(appendErr)
	}
	result, err := e.store.CancelSession(ctx, input.SessionID, input.Reason, intent.Seq, nowMS)
	if err != nil {
		e.rejectIntent(intent.Seq)
		return CancelSessionOutput{}, asBusinessError(err)
	}
	return CancelSessionOutput{CancelSessionResult: result, IntentSeq: intent.Seq}, nil
}

type MergeSessionInput struct {
	RequestID         string `json:"request_id,omitempty"`
	SessionID         string `json:"-"`
	ExpectedTotalSize *int64 `json:"expected_total_size,omitempty"`
	ExpectedPartCount *int64 `json:"expected_part_count,omitempty"`
}

type MergeSessionOutput struct {
	Object        model.ObjectMetadata     `json:"object"`
	Session       model.Session            `json:"session"`
	Verification  model.VerificationReport `json:"verification"`
	AlreadyMerged bool                     `json:"already_merged"`
	IntentSeq     int64                    `json:"intent_seq"`
}

func (e *Engine) MergeSession(ctx context.Context, input MergeSessionInput) (MergeSessionOutput, *model.Error) {
	e.operationGate.RLock()
	defer e.operationGate.RUnlock()
	if input.SessionID == "" {
		return MergeSessionOutput{}, model.NewError(model.CodeInvalidArgument, "session_id is required")
	}
	if input.ExpectedTotalSize != nil && *input.ExpectedTotalSize < 0 {
		return MergeSessionOutput{}, model.NewError(model.CodeInvalidArgument, "expected_total_size must be non-negative")
	}
	if input.ExpectedPartCount != nil && *input.ExpectedPartCount < 0 {
		return MergeSessionOutput{}, model.NewError(model.CodeInvalidArgument, "expected_part_count must be non-negative")
	}
	nowMS := e.now().UnixMilli()
	unlock := e.sessionLocks.Lock(input.SessionID)
	defer unlock()
	session, err := e.store.GetSession(ctx, input.SessionID)
	if err != nil {
		return MergeSessionOutput{}, asBusinessError(err)
	}
	if session.Status == model.SessionActive && session.ExpiresAtMS <= nowMS {
		_, _, _ = e.store.ReclaimExpiredForKey(ctx, session.ObjectKey, nowMS)
		session.Status = model.SessionExpired
	}
	intent, appendErr := e.store.AppendIntent(ctx, model.Intent{
		RequestID: input.RequestID, Action: model.ActionMerge,
		SessionID: input.SessionID, ObjectKey: session.ObjectKey,
		PayloadJSON: marshal(map[string]any{
			"expected_total_size": input.ExpectedTotalSize, "expected_part_count": input.ExpectedPartCount,
		}),
	}, nowMS)
	if appendErr != nil {
		return MergeSessionOutput{}, asBusinessError(appendErr)
	}
	if session.Status == model.SessionCompleted {
		result, mergeErr := e.store.MergeSession(ctx, store.MergeParams{
			SessionID: input.SessionID, ExpectedTotalSize: input.ExpectedTotalSize,
			ExpectedPartCount: input.ExpectedPartCount, IntentSeq: intent.Seq, NowMS: nowMS,
		})
		if mergeErr != nil {
			e.rejectIntent(intent.Seq)
			return MergeSessionOutput{}, asBusinessError(mergeErr)
		}
		return MergeSessionOutput{
			Object: result.Object, Session: result.Session, AlreadyMerged: true,
			Verification: emptyVerificationReport(e.verifier), IntentSeq: intent.Seq,
		}, nil
	}
	if session.Status != model.SessionActive {
		e.rejectIntent(intent.Seq)
		return MergeSessionOutput{}, model.NewError(model.CodeSessionState, "upload session cannot be merged in its current state").
			WithContext("session_id", input.SessionID).WithContext("status", session.Status)
	}

	parts, partsErr := e.store.ActivePartsWithContent(ctx, input.SessionID)
	if partsErr != nil {
		e.rejectIntent(intent.Seq)
		return MergeSessionOutput{}, asBusinessError(partsErr)
	}
	if validationErr := validateMergeParts(session, parts, input.ExpectedTotalSize, input.ExpectedPartCount); validationErr != nil {
		e.rejectIntent(intent.Seq)
		return MergeSessionOutput{}, validationErr
	}
	verification, verifyErr := e.verifier.Verify(ctx, parts, e.verifyBudget())
	if verifyErr != nil {
		e.rejectIntent(intent.Seq)
		return MergeSessionOutput{}, asBusinessError(verifyErr)
	}
	result, mergeErr := e.store.MergeSession(ctx, store.MergeParams{
		SessionID: input.SessionID, ExpectedTotalSize: input.ExpectedTotalSize,
		ExpectedPartCount: input.ExpectedPartCount, IntentSeq: intent.Seq, NowMS: nowMS,
	})
	if mergeErr != nil {
		e.rejectIntent(intent.Seq)
		return MergeSessionOutput{}, asBusinessError(mergeErr)
	}
	return MergeSessionOutput{
		Object: result.Object, Session: result.Session, Verification: verification,
		AlreadyMerged: result.AlreadyMerged, IntentSeq: intent.Seq,
	}, nil
}

func (e *Engine) verifyBudget() time.Duration {
	return e.verificationBudget
}

func (e *Engine) GetSession(ctx context.Context, sessionID string) (model.Session, *model.Error) {
	unlock := e.sessionLocks.Lock(sessionID)
	defer unlock()
	return e.getCurrentSession(ctx, sessionID)
}

func (e *Engine) getCurrentSession(ctx context.Context, sessionID string) (model.Session, *model.Error) {
	session, err := e.store.GetSession(ctx, sessionID)
	if err != nil {
		return model.Session{}, asBusinessError(err)
	}
	nowMS := e.now().UnixMilli()
	if session.Status == model.SessionActive && session.ExpiresAtMS <= nowMS {
		if _, _, reclaimErr := e.store.ReclaimExpiredForKey(ctx, session.ObjectKey, nowMS); reclaimErr != nil {
			return model.Session{}, asBusinessError(reclaimErr)
		}
		session, err = e.store.GetSession(ctx, sessionID)
		if err != nil {
			return model.Session{}, asBusinessError(err)
		}
	}
	return session, nil
}

func (e *Engine) ListParts(ctx context.Context, sessionID string) ([]model.Part, *model.Error) {
	unlock := e.sessionLocks.Lock(sessionID)
	defer unlock()
	if _, sessionErr := e.getCurrentSession(ctx, sessionID); sessionErr != nil {
		return nil, sessionErr
	}
	parts, err := e.store.ListParts(ctx, sessionID)
	if err != nil {
		return nil, asBusinessError(err)
	}
	return parts, nil
}

func (e *Engine) GetObject(ctx context.Context, objectKey string) (model.ObjectMetadata, *model.Error) {
	object, err := e.store.GetObject(ctx, objectKey)
	if err != nil {
		return model.ObjectMetadata{}, asBusinessError(err)
	}
	return object, nil
}

func (e *Engine) ReclaimExpired(ctx context.Context) ([]store.ReclaimedSession, *model.Error) {
	reclaimed, err := e.store.ReclaimExpired(ctx, e.now().UnixMilli())
	if err != nil {
		return nil, asBusinessError(err)
	}
	return reclaimed, nil
}

func (e *Engine) Recover(ctx context.Context) (store.RecoveryReport, *model.Error) {
	e.operationGate.Lock()
	defer e.operationGate.Unlock()
	report, err := e.store.Recover(ctx, e.now().UnixMilli())
	if err != nil {
		return store.RecoveryReport{}, asBusinessError(err)
	}
	return report, nil
}

func (e *Engine) GetRecoveryReport(ctx context.Context, reportID int64) (store.RecoveryReport, *model.Error) {
	report, err := e.store.GetRecoveryReport(ctx, reportID)
	if err != nil {
		return store.RecoveryReport{}, asBusinessError(err)
	}
	return report, nil
}

func (e *Engine) Store() *store.Store { return e.store }

func (e *Engine) rejectIntent(seq int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = e.store.RejectIntent(ctx, seq, e.now().UnixMilli())
}

func validateMergeParts(session model.Session, parts []model.Part, expectedSize, expectedCount *int64) *model.Error {
	if len(parts) == 0 {
		return model.NewError(model.CodePartMissing, "no parts have been committed").
			WithContext("session_id", session.SessionID).
			WithContext("missing_part_numbers", []int64{session.LowerBound})
	}
	missing := make([]int64, 0)
	next := session.LowerBound
	var total int64
	for _, part := range parts {
		for next < part.PartNumber {
			missing = append(missing, next)
			next++
		}
		if part.PartNumber >= next {
			next = part.PartNumber + 1
		}
		if int64(len(part.Content)) != part.Size {
			return model.NewError(model.CodeSizeMismatch, "stored part size differs from its content length").
				WithContext("part_number", part.PartNumber).
				WithContext("declared_size", part.Size).
				WithContext("actual_size", len(part.Content))
		}
		total += part.Size
	}
	if len(missing) > 0 {
		return model.NewError(model.CodePartMissing, "part sequence contains gaps").
			WithContext("session_id", session.SessionID).
			WithContext("missing_part_numbers", missing)
	}
	if expectedCount != nil && *expectedCount != int64(len(parts)) {
		return model.NewError(model.CodePartMissing, "committed part count does not match the merge request").
			WithContext("expected_part_count", *expectedCount).
			WithContext("actual_part_count", len(parts))
	}
	if expectedSize != nil && *expectedSize != total {
		return model.NewError(model.CodeSizeMismatch, "object size does not match the merge request").
			WithContext("expected_total_size", *expectedSize).
			WithContext("actual_total_size", total)
	}
	return nil
}

func validateSHA256(value string) *model.Error {
	if len(value) != 64 {
		return model.NewError(model.CodeInvalidArgument, "sha256 must be 64 hexadecimal characters")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return model.NewError(model.CodeInvalidArgument, "sha256 must be hexadecimal")
	}
	return nil
}

func asBusinessError(err error) *model.Error {
	if err == nil {
		return nil
	}
	var business *model.Error
	if errors.As(err, &business) {
		return business
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return model.WrapError(model.CodeInternal, "request was cancelled", err)
	}
	return model.WrapError(model.CodeInternal, "storage operation failed", err)
}

func marshal(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func int64Ptr(value int64) *int64 { return &value }

func randomSessionID() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return "upl_" + hex.EncodeToString(buffer), nil
}

func emptyVerificationReport(verifier *Verifier) model.VerificationReport {
	report := model.VerificationReport{
		Concurrency: verifier.concurrency, QueueCapacity: verifier.queueCapacity,
		VerifiedPartNumbers: []int64{}, SkippedPartNumbers: []int64{},
	}
	return report
}

type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*lockRef
}

type lockRef struct {
	mu   sync.Mutex
	refs int
}

func newKeyedMutex() keyedMutex { return keyedMutex{locks: make(map[string]*lockRef)} }

func (k *keyedMutex) Lock(key string) func() {
	k.mu.Lock()
	ref := k.locks[key]
	if ref == nil {
		ref = &lockRef{}
		k.locks[key] = ref
	}
	ref.refs++
	k.mu.Unlock()
	ref.mu.Lock()
	return func() {
		ref.mu.Unlock()
		k.mu.Lock()
		ref.refs--
		if ref.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
