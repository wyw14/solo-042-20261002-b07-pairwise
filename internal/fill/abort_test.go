package fill

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type scriptedTransport struct {
	mu        sync.Mutex
	failClose atomic.Bool
	executed  map[uuid.UUID]model.ValveReceipt
	now       func() time.Time
}

func newScriptedTransport(now func() time.Time) *scriptedTransport {
	return &scriptedTransport{executed: make(map[uuid.UUID]model.ValveReceipt), now: now}
}

func (t *scriptedTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ValveReceipt{}, err
	}
	if command.Action == model.ValveClose && t.failClose.Load() {
		return model.ValveReceipt{}, errors.New("valve close rejected by field device")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if receipt, ok := t.executed[command.ID]; ok {
		receipt.Duplicate = true
		return receipt, nil
	}
	receipt := model.ValveReceipt{
		CommandID:  command.ID,
		Executed:   true,
		Message:    "accepted",
		ReceivedAt: t.now().UTC(),
	}
	t.executed[command.ID] = receipt
	return receipt, nil
}

type recoveryHarness struct {
	circuits  *circuit.Manager
	routes    *valve.RouteService
	vessels   *vessel.StateService
	fills     *Service
	abort     *AbortService
	transport *scriptedTransport
}

func newRecoveryHarness(t *testing.T) recoveryHarness {
	t.Helper()
	now := func() time.Time { return time.Unix(1700000000, 0).UTC() }
	registry := vessel.NewRegistry()
	if err := registry.Register(mustVessel(now)); err != nil {
		t.Fatal(err)
	}
	circuits := circuit.NewManager("C-1")
	circuitRegistry := circuit.NewRegistry(circuit.Circuit{
		ID: "C-1", Zone: "Z-1", ValveID: "VLV-1", Enabled: true,
	})
	transport := newScriptedTransport(now)
	controller, err := valve.NewController(transport, now)
	if err != nil {
		t.Fatal(err)
	}
	commands := valve.NewCommandService(controller, now)
	routes := valve.NewRouteService(commands)
	vessels := vessel.NewStateService(registry, now)
	coordinator := NewCoordinator(circuits, circuitRegistry, routes, vessels, now)
	abortService := NewAbortService(coordinator, now)
	completion := NewCompletionService(coordinator, nil, nil, now)
	fills, err := NewService(coordinator, completion, abortService, 10)
	if err != nil {
		t.Fatal(err)
	}
	return recoveryHarness{
		circuits:  circuits,
		routes:    routes,
		vessels:   vessels,
		fills:     fills,
		abort:     abortService,
		transport: transport,
	}
}

func mustVessel(now func() time.Time) model.Vessel {
	v, err := model.NewVessel("V-1", "tank", "Z-1", now())
	if err != nil {
		panic(err)
	}
	return v
}

// A normal fill that is still in progress owns a healthy, reserved circuit.
// Calling the isolation recovery endpoint must not release anything.
func TestRecoverRejectedWhileFilling(t *testing.T) {
	h := newRecoveryHarness(t)
	ctx := context.Background()

	session, err := h.fills.StartManual(ctx, "V-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.abort.RecoverIsolation(ctx, session.ID); err == nil {
		t.Fatal("recovery succeeded against an in-progress fill session")
	}

	reservation, occupied := h.circuits.Get("C-1")
	if !occupied {
		t.Fatal("circuit was released by recovery while fill was active")
	}
	if reservation.Faulted {
		t.Fatal("healthy reservation was marked faulted by recovery")
	}
	if reservation.SessionID != session.ID {
		t.Fatal("reservation owner changed during recovery")
	}
	if !h.routes.IsOpen(session.ID) {
		t.Fatal("open route was torn down by recovery")
	}
	if v, _ := h.vessels.Registry().Get("V-1"); v.Status != model.VesselFilling {
		t.Fatalf("vessel status = %s, want filling", v.Status)
	}
	if _, err := h.fills.StartManual(ctx, "V-1", 10); !errors.Is(err, ErrActiveSession) {
		t.Fatalf("second fill error = %v, want ErrActiveSession", err)
	}
}

// Only a session whose abort failed at the valve (fault-retained circuit) can
// be recovered; recovery drives the valve closed first and frees the circuit
// and vessel exactly once.
func TestRecoverOnlyFaultRetainedSession(t *testing.T) {
	h := newRecoveryHarness(t)
	ctx := context.Background()

	session, err := h.fills.StartManual(ctx, "V-1", 10)
	if err != nil {
		t.Fatal(err)
	}

	h.transport.failClose.Store(true)
	failed, err := h.abort.Abort(ctx, session.ID, "operator abort")
	if err == nil {
		t.Fatal("expected abort to fail while valve cannot close")
	}
	if failed.State != model.FillFailed {
		t.Fatalf("session state = %s, want failed", failed.State)
	}
	reservation, occupied := h.circuits.Get("C-1")
	if !occupied || !reservation.Faulted {
		t.Fatal("circuit was not retained as faulted after failed isolation")
	}

	// Retrying abort/recovery while the valve still cannot close must keep
	// everything retained.
	if err := h.abort.RecoverIsolation(ctx, session.ID); err == nil {
		t.Fatal("recovery succeeded without physical isolation")
	}
	if _, occupied := h.circuits.Get("C-1"); !occupied {
		t.Fatal("faulted circuit released even though valve close failed")
	}
	if v, _ := h.vessels.Registry().Get("V-1"); v.Status != model.VesselFilling {
		t.Fatalf("vessel status = %s, want still filling", v.Status)
	}

	h.transport.failClose.Store(false)
	if err := h.abort.RecoverIsolation(ctx, session.ID); err != nil {
		t.Fatalf("recovery after valve restored: %v", err)
	}
	if _, occupied := h.circuits.Get("C-1"); occupied {
		t.Fatal("circuit remained occupied after recovery")
	}
	if h.routes.IsOpen(session.ID) {
		t.Fatal("route remained open after recovery")
	}
	if v, _ := h.vessels.Registry().Get("V-1"); v.Status != model.VesselReady {
		t.Fatalf("vessel status = %s, want ready", v.Status)
	}

	// Duplicate recovery must not release resources a second time.
	if err := h.abort.RecoverIsolation(ctx, session.ID); err == nil {
		t.Fatal("duplicate recovery succeeded")
	}

	// The same vessel/circuit can now open a new fill session.
	next, err := h.fills.StartManual(ctx, "V-1", 10)
	if err != nil {
		t.Fatalf("new fill after recovery: %v", err)
	}
	reservation, occupied = h.circuits.Get("C-1")
	if !occupied || reservation.SessionID != next.ID {
		t.Fatal("new session did not exclusively reserve the recovered circuit")
	}
}

// Concurrent recovery calls must be serialized: exactly one frees the
// circuit, the rest are rejected, and resources are released once only.
func TestConcurrentRecoverReleasesOnce(t *testing.T) {
	h := newRecoveryHarness(t)
	ctx := context.Background()

	session, err := h.fills.StartManual(ctx, "V-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	h.transport.failClose.Store(true)
	if _, err := h.abort.Abort(ctx, session.ID, "operator abort"); err == nil {
		t.Fatal("expected abort to fail while valve cannot close")
	}
	h.transport.failClose.Store(false)

	const callers = 16
	var successes atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := h.abort.RecoverIsolation(ctx, session.ID); err == nil {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := successes.Load(); got != 1 {
		t.Fatalf("successful recoveries = %d, want exactly 1", got)
	}
	if _, occupied := h.circuits.Get("C-1"); occupied {
		t.Fatal("circuit remained occupied after concurrent recovery")
	}
	if v, _ := h.vessels.Registry().Get("V-1"); v.Status != model.VesselReady {
		t.Fatalf("vessel status = %s, want ready", v.Status)
	}
}
