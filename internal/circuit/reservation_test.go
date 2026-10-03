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

func TestRecoverRejectsHealthyReservation(t *testing.T) {
	manager := NewManager("C-A")
	sessionID := uuid.New()
	if _, err := manager.Reserve("C-A", "V-01", sessionID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover("C-A", sessionID); !errors.Is(err, ErrCircuitNotFaulted) {
		t.Fatalf("healthy reservation recover error = %v, want ErrCircuitNotFaulted", err)
	}
	reservation, occupied := manager.Get("C-A")
	if !occupied {
		t.Fatal("healthy reservation was released by recovery")
	}
	if reservation.Faulted {
		t.Fatal("healthy reservation was marked faulted by recovery")
	}
	if reservation.SessionID != sessionID {
		t.Fatal("healthy reservation changed owner during recovery")
	}
}

func TestRecoverUnknownReservation(t *testing.T) {
	manager := NewManager("C-A")
	if err := manager.Recover("C-A", uuid.New()); !errors.Is(err, ErrReservationMismatch) {
		t.Fatalf("missing reservation recover error = %v, want ErrReservationMismatch", err)
	}
}
