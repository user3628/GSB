package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"netpolcompiler/internal/compiler"
	"netpolcompiler/internal/model"
	"netpolcompiler/internal/store"
)

type Config struct {
	DataDir      string
	CrashPoint   string
	RebuildDelay time.Duration
	Hooks        Hooks
}

type Hooks struct {
	AfterIntent  func(targetVersion int64, namespaces []string)
	BeforeCommit func(targetVersion int64, namespaces []string)
}

type BizError struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Field      string         `json:"field,omitempty"`
	Context    map[string]any `json:"context,omitempty"`
	HTTPStatus int            `json:"-"`
}

func (e *BizError) Error() string { return e.Code + ": " + e.Message }

type Envelope struct {
	OK    bool      `json:"ok"`
	Data  any       `json:"data,omitempty"`
	Error *BizError `json:"error,omitempty"`
}

type RawResult struct {
	Status           int
	Body             []byte
	Biz              *BizError
	RequestPersisted bool
}

type cacheSnapshot struct {
	version int64
	views   map[string]*compiler.CompiledNamespace
}

type PendingRebuild struct {
	TargetVersion int64
	Namespaces    map[string]struct{}
	Reason        string
	Since         time.Time
}

type inflightCall struct {
	done   chan struct{}
	result RawResult
}

type Service struct {
	cfg       Config
	store     *store.Store
	intentLog *os.File
	logMu     sync.Mutex

	writeMu sync.Mutex
	cache   atomic.Pointer[cacheSnapshot]
	pending atomic.Pointer[PendingRebuild]

	inflightMu sync.Mutex
	inflight   map[string]*inflightCall

	recovery atomic.Pointer[RecoveryReport]
}

func New(ctx context.Context, cfg Config) (*Service, *RecoveryReport, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join(os.TempDir(), "netpol-compiler")
	}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(cfg.DataDir, "intent.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		_ = st.Close()
		return nil, nil, err
	}
	svc := &Service{cfg: cfg, store: st, intentLog: logFile, inflight: map[string]*inflightCall{}}
	recoveryReport, err := svc.Recover(ctx)
	if err != nil {
		_ = logFile.Close()
		_ = st.Close()
		return nil, nil, err
	}
	version, err := st.CurrentVersion(ctx)
	if err != nil {
		return nil, nil, err
	}
	views, err := st.LoadCacheViews(ctx, version)
	if err != nil {
		return nil, nil, err
	}
	svc.cache.Store(&cacheSnapshot{version: version, views: views})
	return svc, recoveryReport, nil
}

func (s *Service) Close() error {
	var errs []error
	if s.intentLog != nil {
		if err := s.intentLog.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := s.store.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *Service) Store() *store.Store { return s.store }

func SuccessResult(status int, data any) RawResult {
	body, _ := json.Marshal(Envelope{OK: true, Data: data})
	return RawResult{Status: status, Body: body}
}

func FailureResult(err *BizError) RawResult {
	body, _ := json.Marshal(Envelope{OK: false, Error: err})
	return RawResult{Status: err.HTTPStatus, Body: body, Biz: err}
}

func NewBizError(code string, status int, message string) *BizError {
	return &BizError{Code: code, HTTPStatus: status, Message: message, Context: map[string]any{}}
}

func RuleInvalidError(err error) RawResult {
	var validation *model.ValidationError
	if errors.As(err, &validation) {
		biz := NewBizError("E_RULE_INVALID", 400, "rule validation failed at first illegal field")
		biz.Field = validation.Field
		biz.Context["reason"] = validation.Message
		return FailureResult(biz)
	}
	biz := NewBizError("E_RULE_INVALID", 400, err.Error())
	return FailureResult(biz)
}

func InternalError(message string, cause error) RawResult {
	biz := NewBizError("E_INTERNAL", 500, message)
	if cause != nil {
		biz.Context["cause"] = cause.Error()
	}
	return FailureResult(biz)
}

func HashPayload(kind string, payload any) string {
	data, err := json.Marshal(struct {
		Kind    string `json:"kind"`
		Payload any    `json:"payload"`
	}{Kind: kind, Payload: payload})
	if err != nil {
		data = []byte(kind + ":" + fmt.Sprint(payload))
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (s *Service) Execute(requestID, kind string, payload any, fn func(requestID string) RawResult) RawResult {
	if requestID == "" {
		return fn("")
	}
	ctx := context.Background()

	s.inflightMu.Lock()
	if call, ok := s.inflight[requestID]; ok {
		s.inflightMu.Unlock()
		<-call.done
		return call.result
	}
	hash := HashPayload(kind, payload)

	record, err := s.store.GetRequest(ctx, requestID)
	if err != nil {
		s.inflightMu.Unlock()
		return InternalError("failed to load idempotent request", err)
	}
	if record != nil {
		if record.RequestHash != hash {
			biz := NewBizError("E_CONFLICTING_REQUEST", 409, "request id was reused with different content")
			biz.Context["requestId"] = requestID
			biz.Context["storedHash"] = record.RequestHash
			biz.Context["providedHash"] = hash
			s.inflightMu.Unlock()
			return FailureResult(biz)
		}
		if record.Status == "succeeded" || record.Status == "failed" {
			status := record.HTTPStatus
			if status == 0 {
				status = 200
			}
			s.inflightMu.Unlock()
			return RawResult{Status: status, Body: []byte(record.ResponseJSON)}
		}
		if record.Status == "aborted" {
			claimed, claimErr := s.store.ClaimAbortedRequest(ctx, requestID, hash)
			if claimErr != nil {
				s.inflightMu.Unlock()
				return InternalError("failed to claim aborted request", claimErr)
			}
			if !claimed {
				s.inflightMu.Unlock()
				return InternalError("request id is being recovered by another writer", nil)
			}
		}
	} else {
		now := store.NowRFC3339()
		err := s.store.InsertRequest(ctx, store.RequestRecord{
			RequestID: requestID, RequestHash: hash, Kind: kind, Status: "processing",
			CreatedAt: now,
		})
		if err != nil {
			existing, getErr := s.store.GetRequest(ctx, requestID)
			if getErr != nil || existing == nil {
				s.inflightMu.Unlock()
				return InternalError("failed to persist request identity", err)
			}
			if existing.RequestHash != hash {
				biz := NewBizError("E_CONFLICTING_REQUEST", 409, "request id was reused with different content")
				biz.Context["requestId"] = requestID
				s.inflightMu.Unlock()
				return FailureResult(biz)
			}
			if existing.Status == "succeeded" || existing.Status == "failed" {
				status := existing.HTTPStatus
				if status == 0 {
					status = 200
				}
				s.inflightMu.Unlock()
				return RawResult{Status: status, Body: []byte(existing.ResponseJSON)}
			}
		}
	}

	call := &inflightCall{done: make(chan struct{})}
	s.inflight[requestID] = call
	s.inflightMu.Unlock()

	result := fn(requestID)
	if !result.RequestPersisted {
		status := "succeeded"
		errorCode := ""
		if result.Biz != nil || result.Status >= 400 {
			status = "failed"
			if result.Biz != nil {
				errorCode = result.Biz.Code
			}
		}
		if err := s.store.CompleteRequest(context.Background(), requestID, status, result.Status, string(result.Body), errorCode); err != nil {
			result = InternalError("failed to persist idempotent response", err)
		}
	}

	s.inflightMu.Lock()
	call.result = result
	close(call.done)
	delete(s.inflight, requestID)
	s.inflightMu.Unlock()
	return result
}

func newIntentID(kind string, version int64) string {
	buffer := make([]byte, 6)
	_, _ = rand.Read(buffer)
	return fmt.Sprintf("intent-%s-v%d-%s", kind, version, hex.EncodeToString(buffer))
}

func (s *Service) appendIntentLog(intent store.Intent) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	data, _ := json.Marshal(intent)
	_, _ = s.intentLog.Write(append(data, '\n'))
	_ = s.intentLog.Sync()
}

func (s *Service) setPending(version int64, namespaces []string, reason string) {
	set := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		set[namespace] = struct{}{}
	}
	s.pending.Store(&PendingRebuild{TargetVersion: version, Namespaces: set, Reason: reason, Since: time.Now()})
}

func (s *Service) clearPending() { s.pending.Store(nil) }

func (s *Service) crashIfConfigured(point string) {
	if s.cfg.CrashPoint != point {
		return
	}
	_ = s.intentLog.Sync()
	_ = s.store.DB.Close()
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
}

func (s *Service) waitRebuildDelay() {
	if s.cfg.RebuildDelay > 0 {
		time.Sleep(s.cfg.RebuildDelay)
	}
}

func upsertMetaTx(tx *sql.Tx, version int64) error {
	_, err := tx.Exec(`INSERT INTO meta(key,value) VALUES('current_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprintf("%d", version))
	return err
}

func insertRuleTx(tx *sql.Tx, version int64, rule model.Rule, ruleJSON []byte) error {
	order := 0
	if rule.DeclarationOrder != nil {
		order = *rule.DeclarationOrder
	}
	_, err := tx.Exec(`INSERT INTO rules(snapshot_version,namespace,rule_id,declaration_order,action,protocol,priority,rule_json) VALUES(?,?,?,?,?,?,?,?)`,
		version, rule.Namespace, rule.ID, order, string(rule.Action), string(rule.Protocol), rule.Priority, string(ruleJSON))
	return err
}
