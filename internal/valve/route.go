package valve

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

type Route struct {
	SessionID         uuid.UUID
	CircuitID         string
	CircuitGeneration uint64
	ValveID           string
	OpenCommandID     uuid.UUID
	CloseCommandID    uuid.UUID
}

type RouteService struct {
	commands *CommandService
	mu       sync.Mutex
	open     map[uuid.UUID]Route
}

func NewRouteService(commands *CommandService) *RouteService {
	return &RouteService{commands: commands, open: make(map[uuid.UUID]Route)}
}

func (s *RouteService) Open(ctx context.Context, route Route) error {
	if route.SessionID == uuid.Nil || route.OpenCommandID == uuid.Nil {
		return errors.New("route command identity is incomplete")
	}
	if _, err := s.commands.Open(
		ctx,
		route.OpenCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
	); err != nil {
		return err
	}
	return s.TrackOpen(route)
}

func (s *RouteService) TrackOpen(route Route) error {
	if route.SessionID == uuid.Nil || route.CloseCommandID == uuid.Nil {
		return errors.New("route tracking identity is incomplete")
	}
	s.mu.Lock()
	s.open[route.SessionID] = route
	s.mu.Unlock()
	return nil
}

func (s *RouteService) Close(ctx context.Context, sessionID uuid.UUID) error {
	s.mu.Lock()
	route, ok := s.open[sessionID]
	s.mu.Unlock()
	if !ok {
		return errors.New("route is not open")
	}
	if _, err := s.commands.Close(
		ctx,
		route.CloseCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
	); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.open, sessionID)
	s.mu.Unlock()
	return nil
}

func (s *RouteService) IsOpen(sessionID uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.open[sessionID]
	return ok
}

// EnsureClosed confirms physical isolation before a fault-retained circuit is
// released: it drives an open route's valve closed and only clears the route
// once the close command has been accepted. A route that is already closed is
// a no-op, so recovery can safely be retried. If the valve cannot be closed,
// the route stays tracked and the fault must remain retained.
func (s *RouteService) EnsureClosed(ctx context.Context, sessionID uuid.UUID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	route, ok := s.open[sessionID]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	if _, err := s.commands.Close(
		ctx,
		route.CloseCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
	); err != nil {
		return err
	}
	s.mu.Lock()
	// Drop the tracking only for the route we actually closed; a route that
	// was re-opened in the meantime must not be cleared by a stale receipt.
	if current, ok := s.open[sessionID]; ok && current.CloseCommandID == route.CloseCommandID {
		delete(s.open, sessionID)
	}
	s.mu.Unlock()
	return nil
}
