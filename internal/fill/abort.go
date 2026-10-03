package fill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

var (
	// ErrRecoveryRequiresFault is returned when isolation recovery is
	// requested for a session that is not a failed fill whose circuit was
	// retained after an isolation failure.
	ErrRecoveryRequiresFault = errors.New("isolation recovery requires a failed session with a fault-retained circuit")
	// ErrPhysicalIsolation is returned when the close valve command cannot
	// be confirmed; the circuit must stay retained until it can.
	ErrPhysicalIsolation = errors.New("physical isolation could not be confirmed")
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
	// Recovery exists solely for fills that failed because the isolation
	// valve could not be closed. A live (filling/soaking/...) session still
	// owns its route; a session that failed after a clean close has no
	// retained circuit. Neither may be recovered here.
	if session.State != model.FillFailed {
		return ErrRecoveryRequiresFault
	}
	reservation, occupied := s.coordinator.circuits.Get(session.CircuitID)
	if !occupied || reservation.SessionID != session.ID || !reservation.Faulted {
		return ErrRecoveryRequiresFault
	}
	// Physical isolation must be confirmed before the reservation is
	// released: re-issue the close command and only release the circuit
	// once the valve is actually shut. A failed close leaves everything
	// retained so the operator can retry without ever exposing the circuit
	// to a second fill.
	if !s.coordinator.routes.IsOpen(session.ID) {
		return ErrPhysicalIsolation
	}
	if err := s.coordinator.routes.Close(ctx, session.ID); err != nil {
		return fmt.Errorf("%w: %s", ErrPhysicalIsolation, err.Error())
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
