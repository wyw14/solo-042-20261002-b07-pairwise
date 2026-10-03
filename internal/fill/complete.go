package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/pressure"
)

type CompletionService struct {
	coordinator *Coordinator
	pressure    *pressure.Monitor
	journal     EventAppender
	now         func() time.Time
}

type EventAppender interface {
	Append(model.Event) (model.Event, error)
}

func NewCompletionService(coordinator *Coordinator, pressureMonitor *pressure.Monitor, store EventAppender, now func() time.Time) *CompletionService {
	if now == nil {
		now = time.Now
	}
	return &CompletionService{coordinator: coordinator, pressure: pressureMonitor, journal: store, now: now}
}

func (s *CompletionService) Complete(ctx context.Context, sessionID uuid.UUID) (model.FillSession, error) {
	if err := ctx.Err(); err != nil {
		return model.FillSession{}, err
	}
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	if session.State != model.FillSoaking {
		return model.FillSession{}, errors.New("fill session is not soaking")
	}
	result, err := s.pressure.Evaluate(session.VesselID)
	if err != nil {
		return model.FillSession{}, err
	}
	if !result.Stable {
		return model.FillSession{}, errors.New("pressure soak window is not stable")
	}
	event, err := model.NewEvent("fills", "fill.soak_verified", session.ID.String(), result, s.now())
	if err != nil {
		return model.FillSession{}, err
	}
	if _, err := s.journal.Append(event); err != nil {
		return model.FillSession{}, err
	}
	verified, err := session.Transition(model.FillVerified, s.now())
	if err != nil {
		return model.FillSession{}, err
	}
	completed, err := verified.Transition(model.FillCompleted, s.now())
	if err != nil {
		return model.FillSession{}, err
	}
	if err := s.coordinator.routes.Close(ctx, session.ID); err != nil {
		// The valve may still be open. Retain the circuit as faulted and
		// persist the session as failed with the vessel kept filling: only
		// explicit isolation recovery, after a confirmed close, may release
		// the circuit and return the vessel to ready.
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		failed := session.Failed("isolation close failed: "+err.Error(), s.now())
		s.coordinator.sessions[session.ID] = failed
		return model.FillSession{}, err
	}
	if err := s.coordinator.circuits.Release(session.CircuitID, session.ID); err != nil {
		return model.FillSession{}, err
	}
	if _, err := s.coordinator.vessels.FinishFill(session.VesselID); err != nil {
		return model.FillSession{}, err
	}
	s.coordinator.finishState(completed)
	return completed, nil
}
