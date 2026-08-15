package harness

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type recoveryComponent struct {
	name     string
	reqs     []string
	mu       sync.Mutex
	state    []byte
	wake     int32
	sleep    int32
	restores int32
}

func newRecoveryComponent(name string, reqs ...string) *recoveryComponent {
	return &recoveryComponent{name: name, reqs: append([]string(nil), reqs...)}
}

func (c *recoveryComponent) Name() string { return c.name }

func (c *recoveryComponent) Inject() []string {
	return append([]string(nil), c.reqs...)
}

func (c *recoveryComponent) OnWakeUp(*SpatioTemporalContext) {
	atomic.AddInt32(&c.wake, 1)
}

func (c *recoveryComponent) OnSleep() {
	atomic.AddInt32(&c.sleep, 1)
}

func (c *recoveryComponent) RestoreState(p StatePayload) error {
	c.mu.Lock()
	c.state = append([]byte(nil), p.Data...)
	c.mu.Unlock()
	atomic.AddInt32(&c.restores, 1)
	return nil
}

func (c *recoveryComponent) State() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.state...)
}

func activateChain(t *testing.T, dt *DependencyTracker, cs ...*recoveryComponent) {
	t.Helper()
	for _, c := range cs {
		dt.RegisterComponent(context.Background(), c)
	}
	if err := dt.ActivateServiceErr(context.Background(), cs[0].Name()); err != nil {
		t.Fatal(err)
	}
}

func failAndReplace(t *testing.T, dt *DependencyTracker, name, state string) *recoveryComponent {
	t.Helper()
	failed := dt.GetActiveContext(name)
	if failed == nil {
		t.Fatalf("missing failed context %q", name)
	}
	failed.UpdateStateSnapshot([]byte(state))
	failed.Rollback()

	replacement := newRecoveryComponent(name, "A")
	if name == "A" {
		replacement.reqs = nil
	}
	if err := dt.RecoverWithReplacement(context.Background(), name, replacement); err != nil {
		t.Fatal(err)
	}
	return replacement
}

func TestRecoverWithReplacementSimple(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	activateChain(t, dt, a, b)

	replacement := failAndReplace(t, dt, "B", "checkpoint-7")
	info, ok := dt.Graph().Node("B")
	if !ok || !info.Active || info.Component != replacement {
		t.Fatalf("invalid replacement node: %+v", info)
	}
	if len(dt.Graph().Nodes()) != 2 || dt.Graph().EdgeCount() != 1 {
		t.Fatal("logical identity or edge count changed")
	}
	if string(replacement.State()) != "checkpoint-7" || replacement.wake != 1 || replacement.restores != 1 {
		t.Fatal("replacement state/wake/restore incorrect")
	}
	if err := dt.Graph().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverWithReplacementDeepChain(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	c := newRecoveryComponent("C", "B")
	d := newRecoveryComponent("D", "C")
	activateChain(t, dt, a, b, c, d)

	failAndReplace(t, dt, "B", "B-state")
	for _, name := range []string{"A", "B", "C", "D"} {
		if info, _ := dt.Graph().Node(name); !info.Active {
			t.Fatalf("%s inactive after recovery", name)
		}
	}
	if c.sleep != 1 || c.wake != 2 || d.sleep != 1 || d.wake != 2 {
		t.Fatalf("downstream transition counts C=%d/%d D=%d/%d", c.sleep, c.wake, d.sleep, d.wake)
	}
	if err := dt.Graph().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverWithReplacementMultiParentPreservesEligibleDependent(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B")
	c := newRecoveryComponent("C", "A", "B")
	dt.RegisterComponent(context.Background(), c)
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	dt.ActivateService(context.Background(), "A")
	dt.ActivateService(context.Background(), "B")

	failAndReplace(t, dt, "B", "B-state")
	if c.sleep != 0 || c.wake != 1 || !dt.Graph().Node("C").Active {
		t.Fatal("C was incorrectly reconciled")
	}
}

func TestRecoverWithReplacementDiamondNoDuplicateWake(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	c := newRecoveryComponent("C", "A")
	d := newRecoveryComponent("D", "B", "C")
	activateChain(t, dt, a, b, c, d)

	failAndReplace(t, dt, "B", "B-state")
	// Current DAG semantics treat multiple dependencies as eligibility when
	// any provider remains active, so C and D remain active through B's fault.
	if c.sleep != 0 || d.sleep != 0 || c.wake != 1 || d.wake != 1 {
		t.Fatal("diamond produced duplicate/unnecessary downstream execution")
	}
	if !dt.Graph().Node("D").Active {
		t.Fatal("D became inactive")
	}
}

func TestRecoverWithReplacementRepeated(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	activateChain(t, dt, a, b)
	for i := 0; i < 10; i++ {
		replacement := failAndReplace(t, dt, "B", fmt.Sprintf("checkpoint-%d", i))
		if string(replacement.State()) != fmt.Sprintf("checkpoint-%d", i) {
			t.Fatalf("round %d state mismatch", i)
		}
		if len(dt.Graph().Nodes()) != 2 || dt.Graph().EdgeCount() != 1 {
			t.Fatalf("round %d graph leaked nodes/edges", i)
		}
		if err := dt.Graph().Validate(); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
	}
}

func TestRollbackIdempotentConcurrently(t *testing.T) {
	ctx := NewSTContext(context.Background(), "rollback", nil)
	var cleanup int32
	ctx.RegisterEffect("cleanup", func() { atomic.AddInt32(&cleanup, 1) })
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx.Rollback()
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&cleanup); got != 1 {
		t.Fatalf("cleanup count=%d", got)
	}
	ctx.Rollback()
}

func TestSnapshotCheckpointSurvivesFailureMutation(t *testing.T) {
	ctx := NewSTContext(context.Background(), "snapshot", nil)
	checkpoint := []byte("known-good")
	ctx.UpdateStateSnapshot(checkpoint)
	checkpoint[0] = 'X'
	ctx.RaiseError(fmt.Errorf("fault"))
	// No provider is registered, so failure recovery must preserve the
	// explicit last-known-good checkpoint.
	payload, err := ctx.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload.Data) != "known-good" {
		t.Fatalf("snapshot changed: %q", payload.Data)
	}
}

func TestRecoveryConcurrentTransitions(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	activateChain(t, dt, a, b)
	for i := 0; i < 20; i++ {
		failed := dt.GetActiveContext("B")
		failed.UpdateStateSnapshot([]byte("state"))
		failed.Rollback()
		done := make(chan error, 2)
		go func() {
			done <- dt.RecoverWithReplacement(context.Background(), "B", newRecoveryComponent("B", "A"))
		}()
		go func() {
			done <- dt.ActivateServiceErr(context.Background(), "A")
		}()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err := dt.Graph().Validate(); err != nil {
		t.Fatal(err)
	}
}

var _ StateRestorer = (*recoveryComponent)(nil)
