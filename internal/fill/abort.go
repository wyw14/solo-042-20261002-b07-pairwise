package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
)

type AbortService struct {
	coordinator *Coordinator
	now         func() time.Time
}

func NewAbortService(coordinator *Coordinator, now func() time.Time) *AbortService {
	if now == nil {
		now = time.Now
	}
	return &AbortService{coordinator: coordinator, now: now}
}

func (s *AbortService) Abort(ctx context.Context, sessionID uuid.UUID, reason string) (model.FillSession, error) {
	if reason == "" {
		return model.FillSession{}, errors.New("abort reason is required")
	}
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	if !session.Active() {
		return session, nil
	}
	if err := s.coordinator.routes.Close(ctx, session.ID); err != nil {
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		failed := session.Failed(reason+": "+err.Error(), s.now())
		s.coordinator.sessions[session.ID] = failed
		return failed, err
	}
	if err := s.coordinator.circuits.Release(session.CircuitID, session.ID); err != nil {
		return model.FillSession{}, err
	}
	failed := session.Failed(reason, s.now())
	if _, err := s.coordinator.vessels.FinishFill(session.VesselID); err != nil {
		return model.FillSession{}, err
	}
	s.coordinator.finishState(failed)
	return failed, nil
}

func (s *AbortService) RecoverIsolation(ctx context.Context, sessionID uuid.UUID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return errors.New("fill session not found")
	}
	// Recovery is reserved for sessions that failed specifically because the
	// circuit could not be isolated. A live session (including one still
	// opening its valve) owns a healthy reservation and must not be touched.
	if session.State != model.FillFailed {
		return errors.New("fill session has not been faulted")
	}
	if session.CircuitID == "" {
		return errors.New("faulted fill session never reserved a circuit")
	}
	reservation, occupied := s.coordinator.circuits.Get(session.CircuitID)
	if !occupied {
		return errors.New("faulted circuit is no longer retained")
	}
	if reservation.SessionID != session.ID || reservation.VesselID != session.VesselID {
		return circuit.ErrReservationMismatch
	}
	if !reservation.Faulted {
		return circuit.ErrCircuitNotFaulted
	}
	vessel, ok := s.coordinator.vessels.Registry().Get(session.VesselID)
	if !ok {
		return errors.New("vessel not found")
	}
	if vessel.Status != model.VesselFilling {
		return errors.New("vessel is no longer blocked by the faulted fill")
	}
	// Confirm physical isolation first: the valve must be driven closed
	// before the circuit is released. If this fails the fault stays
	// retained and the vessel remains filling, so a later retry is safe.
	if err := s.coordinator.routes.EnsureClosed(ctx, session.ID); err != nil {
		return err
	}
	if err := s.coordinator.circuits.Recover(session.CircuitID, session.ID); err != nil {
		return err
	}
	delete(s.coordinator.active, session.VesselID)
	if _, err := s.coordinator.vessels.FinishFill(session.VesselID); err != nil {
		return err
	}
	return nil
}
