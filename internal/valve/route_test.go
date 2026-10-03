package valve

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type closeScriptTransport struct {
	failClose atomic.Bool
	mu        sync.Mutex
	closeIDs  []uuid.UUID
}

func (t *closeScriptTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ValveReceipt{}, err
	}
	if command.Action == model.ValveClose && t.failClose.Load() {
		return model.ValveReceipt{}, errors.New("field bus offline")
	}
	if command.Action == model.ValveClose {
		t.mu.Lock()
		t.closeIDs = append(t.closeIDs, command.ID)
		t.mu.Unlock()
	}
	return model.ValveReceipt{CommandID: command.ID, Executed: true, ReceivedAt: time.Now().UTC()}, nil
}

func TestEnsureClosedFailsSafeAndIsIdempotent(t *testing.T) {
	transport := &closeScriptTransport{}
	controller, err := NewController(transport, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	routes := NewRouteService(NewCommandService(controller, time.Now))
	sessionID := uuid.New()
	route := Route{
		SessionID:         sessionID,
		CircuitID:         "C-1",
		CircuitGeneration: 1,
		ValveID:           "VLV-1",
		OpenCommandID:     uuid.New(),
		CloseCommandID:    uuid.New(),
	}
	if err := routes.Open(context.Background(), route); err != nil {
		t.Fatal(err)
	}

	// While the valve cannot be closed, isolation is not confirmed and the
	// route must remain tracked so the fault stays retained.
	transport.failClose.Store(true)
	if err := routes.EnsureClosed(context.Background(), sessionID); err == nil {
		t.Fatal("EnsureClosed succeeded while close command failed")
	}
	if !routes.IsOpen(sessionID) {
		t.Fatal("route dropped despite failed physical isolation")
	}

	// Once the field device accepts, isolation completes and a repeat is a
	// safe no-op (no second close command).
	transport.failClose.Store(false)
	if err := routes.EnsureClosed(context.Background(), sessionID); err != nil {
		t.Fatalf("EnsureClosed after recovery: %v", err)
	}
	if routes.IsOpen(sessionID) {
		t.Fatal("route remained open after confirmed isolation")
	}
	if err := routes.EnsureClosed(context.Background(), sessionID); err != nil {
		t.Fatalf("idempotent EnsureClosed: %v", err)
	}
	if got := len(transport.closeIDs); got != 1 {
		t.Fatalf("close commands issued = %d, want 1", got)
	}
}
