package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type Service struct {
	coordinator   *Coordinator
	completion    *CompletionService
	abort         *AbortService
	defaultLiters float64
}

func NewService(coordinator *Coordinator, completion *CompletionService, abort *AbortService, defaultLiters float64) (*Service, error) {
	if defaultLiters <= 0 {
		return nil, errors.New("default fill volume must be positive")
	}
	return &Service{coordinator: coordinator, completion: completion, abort: abort, defaultLiters: defaultLiters}, nil
}

func (s *Service) OnLowLevel(ctx context.Context, vesselID string, observedAt time.Time) (model.FillSession, error) {
	_ = observedAt
	return s.coordinator.StartAutomatic(ctx, vesselID, s.defaultLiters)
}

func (s *Service) StartManual(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	if liters <= 0 {
		liters = s.defaultLiters
	}
	return s.coordinator.StartManual(ctx, vesselID, liters)
}

func (s *Service) BeginSoak(id uuid.UUID) (model.FillSession, error) {
	return s.coordinator.BeginSoak(id)
}

func (s *Service) Complete(ctx context.Context, id uuid.UUID) (model.FillSession, error) {
	return s.completion.Complete(ctx, id)
}

func (s *Service) Abort(ctx context.Context, id uuid.UUID, reason string) (model.FillSession, error) {
	return s.abort.Abort(ctx, id, reason)
}

func (s *Service) RecoverIsolation(ctx context.Context, id uuid.UUID) error {
	return s.abort.RecoverIsolation(ctx, id)
}

func (s *Service) Get(id uuid.UUID) (model.FillSession, bool) {
	return s.coordinator.Get(id)
}

func (s *Service) List() []model.FillSession {
	return s.coordinator.List()
}
