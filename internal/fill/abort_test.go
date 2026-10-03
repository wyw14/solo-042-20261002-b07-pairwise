package fill

import (
	"context"
	"errors"
	"sync"
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
	failClose bool
	executed  map[uuid.UUID]model.ValveReceipt
	now       time.Time
}

func newScriptedTransport(now time.Time) *scriptedTransport {
	return &scriptedTransport{executed: make(map[uuid.UUID]model.ValveReceipt), now: now}
}

func (t *scriptedTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ValveReceipt{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if receipt, ok := t.executed[command.ID]; ok {
		receipt.Duplicate = true
		return receipt, nil
	}
	if command.Action == model.ValveClose && t.failClose {
		return model.ValveReceipt{}, errors.New("valve unreachable")
	}
	receipt := model.ValveReceipt{
		CommandID:  command.ID,
		Executed:   true,
		Message:    "accepted",
		ReceivedAt: t.now.UTC(),
	}
	t.executed[command.ID] = receipt
	return receipt, nil
}

func (t *scriptedTransport) setFailClose(fail bool) {
	t.mu.Lock()
	t.failClose = fail
	t.mu.Unlock()
}

type recoveryFixture struct {
	now         time.Time
	transport   *scriptedTransport
	manager     *circuit.Manager
	circuits    *circuit.Registry
	vessels     *vessel.StateService
	routes      *valve.RouteService
	coordinator *Coordinator
	aborts      *AbortService
}

func newRecoveryFixture(t *testing.T) recoveryFixture {
	t.Helper()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	transport := newScriptedTransport(now)
	controller, err := valve.NewController(transport, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	commands := valve.NewCommandService(controller, func() time.Time { return now })
	routes := valve.NewRouteService(commands)

	registry := vessel.NewRegistry()
	v, err := model.NewVessel("V-01", "Test vessel", "north", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(v); err != nil {
		t.Fatal(err)
	}
	vessels := vessel.NewStateService(registry, func() time.Time { return now })

	circuits := circuit.NewRegistry(circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true})
	manager := circuit.NewManager("C-A")
	coordinator := NewCoordinator(manager, circuits, routes, vessels, func() time.Time { return now })
	aborts := NewAbortService(coordinator, func() time.Time { return now })
	return recoveryFixture{
		now:         now,
		transport:   transport,
		manager:     manager,
		circuits:    circuits,
		vessels:     vessels,
		routes:      routes,
		coordinator: coordinator,
		aborts:      aborts,
	}
}

func startFilling(t *testing.T, f recoveryFixture) model.FillSession {
	t.Helper()
	session, err := f.coordinator.StartManual(context.Background(), "V-01", 10)
	if err != nil {
		t.Fatalf("start fill: %v", err)
	}
	return session
}

func assertCircuitOccupied(t *testing.T, f recoveryFixture, sessionID uuid.UUID, faulted bool) {
	t.Helper()
	reservation, ok := f.manager.Get("C-A")
	if !ok {
		t.Fatal("circuit reservation was released")
	}
	if reservation.SessionID != sessionID {
		t.Fatalf("circuit reservation belongs to %s, want %s", reservation.SessionID, sessionID)
	}
	if reservation.Faulted != faulted {
		t.Fatalf("circuit faulted = %v, want %v", reservation.Faulted, faulted)
	}
}

func assertVesselStatus(t *testing.T, f recoveryFixture, want model.VesselStatus) {
	t.Helper()
	got, ok := f.vessels.Registry().Get("V-01")
	if !ok {
		t.Fatal("vessel missing")
	}
	if got.Status != want {
		t.Fatalf("vessel status = %s, want %s", got.Status, want)
	}
}

// Recovery must not be callable against a healthy, in-progress fill: the
// circuit stays reserved and the vessel stays filling, so no second session
// can share the route.
func TestRecoverIsolationRejectedWhileFilling(t *testing.T) {
	f := newRecoveryFixture(t)
	session := startFilling(t, f)

	err := f.aborts.RecoverIsolation(context.Background(), session.ID)
	if !errors.Is(err, ErrRecoveryRequiresFault) {
		t.Fatalf("want ErrRecoveryRequiresFault, got %v", err)
	}
	assertCircuitOccupied(t, f, session.ID, false)
	assertVesselStatus(t, f, model.VesselFilling)

	if _, err := f.coordinator.StartManual(context.Background(), "V-01", 10); !errors.Is(err, ErrActiveSession) {
		t.Fatalf("second fill on filling vessel should fail, got %v", err)
	}
	if f.routes.IsOpen(session.ID) != true {
		t.Fatal("open route was dropped by recovery attempt")
	}
	assertCircuitOccupied(t, f, session.ID, false)
}

// Concurrent/duplicate recovery calls against a live fill must not release
// the reservation ahead of time; every call is rejected.
func TestRecoverIsolationConcurrentWhileFilling(t *testing.T) {
	f := newRecoveryFixture(t)
	session := startFilling(t, f)

	const callers = 16
	var wg sync.WaitGroup
	errs := make([]error, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = f.aborts.RecoverIsolation(context.Background(), session.ID)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, ErrRecoveryRequiresFault) {
			t.Fatalf("caller %d: want ErrRecoveryRequiresFault, got %v", i, err)
		}
	}
	assertCircuitOccupied(t, f, session.ID, false)
	assertVesselStatus(t, f, model.VesselFilling)
}

// A clean abort releases the circuit normally; recovery must still be
// rejected because there is no fault-retained circuit.
func TestRecoverIsolationRejectedAfterCleanAbort(t *testing.T) {
	f := newRecoveryFixture(t)
	session := startFilling(t, f)

	if _, err := f.aborts.Abort(context.Background(), session.ID, "operator abort"); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, occupied := f.manager.Get("C-A"); occupied {
		t.Fatal("circuit should have been released after clean abort")
	}
	if err := f.aborts.RecoverIsolation(context.Background(), session.ID); !errors.Is(err, ErrRecoveryRequiresFault) {
		t.Fatalf("want ErrRecoveryRequiresFault, got %v", err)
	}
	assertVesselStatus(t, f, model.VesselReady)
}

// After a close failure the circuit is retained as faulted. Recovery that
// still cannot close the valve must leave everything in place; once the
// close succeeds, exactly one recovery releases the circuit.
func TestRecoverIsolationRequiresConfirmedClose(t *testing.T) {
	f := newRecoveryFixture(t)
	session := startFilling(t, f)

	f.transport.setFailClose(true)
	if _, err := f.aborts.Abort(context.Background(), session.ID, "leak alarm"); err == nil {
		t.Fatal("abort with unreachable valve should fail")
	}
	failed, ok := f.coordinator.Get(session.ID)
	if !ok || failed.State != model.FillFailed {
		t.Fatalf("session should be persisted as failed, got %+v", failed)
	}
	assertCircuitOccupied(t, f, session.ID, true)
	assertVesselStatus(t, f, model.VesselFilling)

	// Close still failing: recovery must not release anything.
	if err := f.aborts.RecoverIsolation(context.Background(), session.ID); !errors.Is(err, ErrPhysicalIsolation) {
		t.Fatalf("want ErrPhysicalIsolation, got %v", err)
	}
	assertCircuitOccupied(t, f, session.ID, true)
	assertVesselStatus(t, f, model.VesselFilling)
	if f.routes.IsOpen(session.ID) != true {
		t.Fatal("route should remain tracked while the valve is still open")
	}

	// Valve reachable again: close confirmed, then release.
	f.transport.setFailClose(false)
	if err := f.aborts.RecoverIsolation(context.Background(), session.ID); err != nil {
		t.Fatalf("recovery after confirmed close: %v", err)
	}
	if _, occupied := f.manager.Get("C-A"); occupied {
		t.Fatal("faulted circuit was not released by recovery")
	}
	if f.routes.IsOpen(session.ID) {
		t.Fatal("route remained tracked after recovery")
	}
	assertVesselStatus(t, f, model.VesselReady)

	// Duplicate recovery must be a no-op error: circuit stays free and the
	// vessel must not be touched again.
	if err := f.aborts.RecoverIsolation(context.Background(), session.ID); !errors.Is(err, ErrRecoveryRequiresFault) {
		t.Fatalf("duplicate recovery want ErrRecoveryRequiresFault, got %v", err)
	}
	if _, occupied := f.manager.Get("C-A"); occupied {
		t.Fatal("duplicate recovery re-created a reservation")
	}
	assertVesselStatus(t, f, model.VesselReady)
}

// When several recovery calls race after the valve becomes reachable,
// exactly one performs the close+release; the rest are rejected and the
// circuit ends up released exactly once.
func TestRecoverIsolationConcurrentFaulted(t *testing.T) {
	f := newRecoveryFixture(t)
	session := startFilling(t, f)

	f.transport.setFailClose(true)
	if _, err := f.aborts.Abort(context.Background(), session.ID, "leak alarm"); err == nil {
		t.Fatal("abort with unreachable valve should fail")
	}
	f.transport.setFailClose(false)

	const callers = 16
	var wg sync.WaitGroup
	successes, rejections := 0, 0
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := f.aborts.RecoverIsolation(context.Background(), session.ID)
			mu.Lock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrRecoveryRequiresFault):
				rejections++
			default:
				t.Errorf("unexpected recovery error: %v", err)
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if successes != 1 {
		t.Fatalf("want exactly 1 successful recovery, got %d (%d rejected)", successes, rejections)
	}
	if rejections != callers-1 {
		t.Fatalf("want %d rejections, got %d", callers-1, rejections)
	}
	if _, occupied := f.manager.Get("C-A"); occupied {
		t.Fatal("circuit remained occupied after concurrent recovery")
	}
	assertVesselStatus(t, f, model.VesselReady)
}
