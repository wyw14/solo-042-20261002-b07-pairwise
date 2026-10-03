package circuit

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFaultedReservationRequiresRecovery(t *testing.T) {
	manager := NewManager("C-A")
	sessionID := uuid.New()
	if _, err := manager.Reserve("C-A", "V-01", sessionID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.RetainFault("C-A", sessionID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Release("C-A", sessionID); err == nil {
		t.Fatal("faulted reservation was released normally")
	}
	if err := manager.Recover("C-A", sessionID); err != nil {
		t.Fatal(err)
	}
	if _, occupied := manager.Get("C-A"); occupied {
		t.Fatal("recovered circuit remained occupied")
	}
}

func TestHealthyReservationCannotBeRecovered(t *testing.T) {
	manager := NewManager("C-A")
	sessionID := uuid.New()
	if _, err := manager.Reserve("C-A", "V-01", sessionID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover("C-A", sessionID); !errors.Is(err, ErrCircuitNotFaulted) {
		t.Fatalf("expected ErrCircuitNotFaulted, got %v", err)
	}
	reservation, occupied := manager.Get("C-A")
	if !occupied {
		t.Fatal("healthy reservation was released by recovery")
	}
	if reservation.Faulted {
		t.Fatal("healthy reservation was marked faulted")
	}
}

func TestRecoverIsIdempotentAfterRelease(t *testing.T) {
	manager := NewManager("C-A")
	sessionID := uuid.New()
	if _, err := manager.Reserve("C-A", "V-01", sessionID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.RetainFault("C-A", sessionID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover("C-A", sessionID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover("C-A", sessionID); !errors.Is(err, ErrReservationMismatch) {
		t.Fatalf("duplicate recovery should fail with ErrReservationMismatch, got %v", err)
	}
}
