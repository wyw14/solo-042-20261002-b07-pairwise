package circuit

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrCircuitBusy         = errors.New("circuit is already reserved")
	ErrReservationMismatch = errors.New("circuit reservation does not match")
	ErrCircuitNotFaulted   = errors.New("circuit reservation is not retained after a fault")
)

type Reservation struct {
	CircuitID  string    `json:"circuit_id"`
	SessionID  uuid.UUID `json:"session_id"`
	VesselID   string    `json:"vessel_id"`
	Generation uint64    `json:"generation"`
	ReservedAt time.Time `json:"reserved_at"`
	Faulted    bool      `json:"faulted"`
}

type Manager struct {
	mu           sync.Mutex
	reservations map[string]Reservation
	generations  map[string]uint64
}

func NewManager(circuitIDs ...string) *Manager {
	m := &Manager{
		reservations: make(map[string]Reservation),
		generations:  make(map[string]uint64),
	}
	for _, id := range circuitIDs {
		if id != "" {
			m.generations[id] = 0
		}
	}
	return m
}

func (m *Manager) Reserve(circuitID, vesselID string, sessionID uuid.UUID, now time.Time) (Reservation, error) {
	if circuitID == "" || vesselID == "" || sessionID == uuid.Nil {
		return Reservation{}, errors.New("invalid circuit reservation")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, occupied := m.reservations[circuitID]; occupied {
		return Reservation{}, ErrCircuitBusy
	}
	m.generations[circuitID]++
	reservation := Reservation{
		CircuitID:  circuitID,
		SessionID:  sessionID,
		VesselID:   vesselID,
		Generation: m.generations[circuitID],
		ReservedAt: now.UTC(),
	}
	m.reservations[circuitID] = reservation
	return reservation, nil
}

func (m *Manager) Release(circuitID string, sessionID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.reservations[circuitID]
	if !ok {
		return nil
	}
	if current.SessionID != sessionID {
		return ErrReservationMismatch
	}
	if current.Faulted {
		return errors.New("faulted circuit requires explicit recovery")
	}
	delete(m.reservations, circuitID)
	return nil
}

func (m *Manager) RetainFault(circuitID string, sessionID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.reservations[circuitID]
	if !ok || current.SessionID != sessionID {
		return ErrReservationMismatch
	}
	// Idempotent: a re-delivered fault notification for the same retained
	// reservation must not flip the circuit back into a releasable state.
	current.Faulted = true
	m.reservations[circuitID] = current
	return nil
}

func (m *Manager) Recover(circuitID string, sessionID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.reservations[circuitID]
	if !ok || current.SessionID != sessionID {
		return ErrReservationMismatch
	}
	// Only a circuit retained after an isolation failure may be recovered.
	// A healthy reservation belongs to a live fill session and must stay
	// reserved until that session completes or aborts normally.
	if !current.Faulted {
		return ErrCircuitNotFaulted
	}
	delete(m.reservations, circuitID)
	return nil
}

func (m *Manager) Get(circuitID string) (Reservation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reservation, ok := m.reservations[circuitID]
	return reservation, ok
}
