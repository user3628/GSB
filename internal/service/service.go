package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"profilehotspot/internal/apperr"
	"profilehotspot/internal/model"
	"profilehotspot/internal/storage"
)

type Service struct {
	store *storage.Store
	clock func() time.Time
}

func New(store *storage.Store) *Service {
	return &Service{store: store, clock: time.Now}
}

func (s *Service) SetClock(clock func() time.Time) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *Service) Store() *storage.Store { return s.store }

func payloadHash(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validationAppError(err error) *apperr.Error {
	var validation *model.ValidationError
	if errors.As(err, &validation) {
		return apperr.SampleInvalid(validation.Message, map[string]any{
			"field": validation.Field,
			"value": validation.Value,
		})
	}
	return apperr.SampleInvalid(err.Error(), map[string]any{"field": "request"})
}

func notFoundVersion(version uint64) *apperr.Error {
	return apperr.NotFound("policy version", map[string]any{"version": version})
}

func (s *Service) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock().UTC()
}

func (s *Service) Close() error {
	if s.store == nil {
		return nil
	}
	return s.store.Close()
}
