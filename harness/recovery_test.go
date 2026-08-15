package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recoveryComponent struct {
	name string
	reqs []string

	mu       sync.Mutex
	state    []byte
	wake     int32
	sleep    int32
	restores int32
	log      *[]string
}

func newRecoveryComponent(name string, reqs ...string) *recoveryComponent {
	return &recoveryComponent{name: name, reqs: append([]string(nil), reqs...)}
}

func (c *recoveryComponent) Name() string { return c.name }
func (c *recoveryComponent) Inject() []string { return append([]string(nil), c.reqs...) }

func (c *recoveryComponent) OnWakeUp(ctx *SpatioTemporalContext) {
	atomic.AddInt32(&c.wake, 1)
	c.mu.Lock()
	if c.log != nil {
		*c.log = append(*c.log, "wake "+c.name)
	}
	c.mu.Unlock()
	_ = ctx
}

func (c *recoveryComponent) OnSleep() {
	atomic.AddInt32(&c.sleep, 1)
	c.mu.Lock()
	if c.log != nil {
		*c.log = append(*c.log, "sleep "+c.name)
	}
	c.mu.Unlock()
}

func (c *recoveryComponent) RestoreState(payload StatePayload) error {
	c.mu.Lock()
	c.state = append([]byte(nil), payload.Data...)
	c.mu.Unlock()
	atomic.AddInt32(&c.restores, 1)
	return nil
}

func (c *recoveryComponent) State() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.state...)
}

func TestRecoverWithReplacementSimple(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	dt.ActivateService(context.Background(), "A")

	failed := dt.GetActiveContext("B")
	if failed == nil {
		t.Fatal("missing B context")
	}
	failed.UpdateStateSnapshot([]byte("checkpoint-7"))
	failed.Rollback()

	replacement := newRecoveryComponent("B", "A")
	if err := dt.RecoverWithReplacement(context.Background(), "B", replacement); err != nil {
		t.Fatal(err)
	}

	info, ok := dt.Graph().Node("B")
	if !ok || !info.Active {
		t.Fatalf("replacement not active: %+v", info)
	}
	if info.Component != replacement {
		t.Fatal("logical B node did not take replacement component")
	}
	if len(dt.Graph().Nodes()) != 2 {
		t.Fatalf("duplicate logical node created: %d nodes", len(dt.Graph().Nodes()))
	}
	if got := string(replacement.State()); got != "checkpoint-7" {
		t.Fatalf("replacement state=%q", got)
	}
	if atomic.LoadInt32(&replacement.wake) != 1 {
		t.Fatalf("replacement wake count=%d", replacement.wake)
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
	for _, component := range []*recoveryComponent{a, b, c, d} {
		dt.RegisterComponent(context.Background(), component)
	}
	dt.ActivateService(context.Background(), "A")

	failed := dt.GetActiveContext("B")
	failed.UpdateStateSnapshot([]byte("B-state"))
	failed.Rollback()

	replacement := newRecoveryComponent("B", "A")
	if err := dt.RecoverWithReplacement(context.Background(), "B", replacement); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"A", "B", "C", "D"} {
		if info, _ := dt.Graph().Node(name); !info.Active {
			t.Fatalf("%s is not active after recovery", name)
		}
	}
	if c.sleep != 1 || d.sleep != 1 {
		t.Fatalf("downstream sleep counts C=%d D=%d", c.sleep, d.sleep)
	}
	if c.wake != 2 || d.wake != 2 {
		t.Fatalf("downstream wake counts C=%d D=%d", c.wake, d.wake)
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

	failed := dt.GetActiveContext("B")
	failed.UpdateStateSnapshot([]byte("B-state"))
	failed.Rollback()

	replacement := newRecoveryComponent("B")
	if err := dt.RecoverWithReplacement(context.Background(), "B", replacement); err != nil {
		t.Fatal(err)
	}

	if c.sleep != 0 || c.wake != 1 {
		t.Fatalf("C changed despite alternate active dependency: sleep=%d wake=%d", c.sleep, c.wake)
	}
	if !dt.Graph().Node("C").Active {
		t.Fatal("C became inactive")
	}
}

func TestRecoverWithReplacementDiamondNoDuplicateWake(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	c := newRecoveryComponent("C", "A")
	d := newRecoveryComponent("D", "B", "C")
	for _, component := range []*recoveryComponent{a, b, c, d} {
		dt.RegisterComponent(context.Background(), component)
	}
	dt.ActivateService(context.Background(), "A")

	failed := dt.GetActiveContext("B")
	failed.UpdateStateSnapshot([]byte("B-state"))
	failed.Rollback()

	replacement := newRecoveryComponent("B", "A")
	if err := dt.RecoverWithReplacement(context.Background(), "B", replacement); err != nil {
		t.Fatal(err)
	}

	if d.sleep != 1 || d.wake != 2 {
		t.Fatalf("D transition counts sleep=%d wake=%d", d.sleep, d.wake)
	}
	if c.sleep != 0 || c.wake != 1 {
		t.Fatalf("C should remain active: sleep=%d wake=%d", c.sleep, c.wake)
	}
	if !dt.Graph().Node("D").Active {
		t.Fatal("D remained inactive")
	}
}

func TestRecoverWithReplacementRepeated(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	dt.ActivateService(context.Background(), "A")

	for i := 0; i < 10; i++ {
		failed := dt.GetActiveContext("B")
		if failed == nil {
			t.Fatalf("round %d: missing B context", i)
		}
		failed.UpdateStateSnapshot([]byte(fmt.Sprintf("checkpoint-%d", i)))
		failed.Rollback()

		replacement := newRecoveryComponent("B", "A")
		if err := dt.RecoverWithReplacement(context.Background(), "B", replacement); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if got := string(replacement.State()); got != fmt.Sprintf("checkpoint-%d", i) {
			t.Fatalf("round %d: state=%q", i, got)
		}
		if len(dt.Graph().Nodes()) != 2 || dt.Graph().EdgeCount() != 1 {
			t.Fatalf("round %d: nodes=%d edges=%d", i, len(dt.Graph().Nodes()), dt.Graph().EdgeCount())
		}
		if err := dt.Graph().Validate(); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
	}
}

func TestRecoverWithReplacementLockBoundary(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	dt.ActivateService(context.Background(), "A")

	failed := dt.GetActiveContext("B")
	failed.UpdateStateSnapshot([]byte("safe"))
	failed.Rollback()

	replacement := newRecoveryComponent("B", "A")
	replacement.log = &[]string{}
	if err := dt.RecoverWithReplacement(context.Background(), "B", replacement); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&replacement.restores) != 1 {
		t.Fatalf("restore count=%d", replacement.restores)
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

	live := []byte("partially-mutated")
	_ = live
	ctx.RaiseError(errors.New("fault"))
	time.Sleep(10 * time.Millisecond)

	payload, err := ctx.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload.Data) != "known-good" {
		t.Fatalf("snapshot changed after failure: %q", payload.Data)
	}
}

func TestRecoveryConcurrentTransitions(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newRecoveryComponent("A")
	b := newRecoveryComponent("B", "A")
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	dt.ActivateService(context.Background(), "A")

	const rounds = 20
	for i := 0; i < rounds; i++ {
		failed := dt.GetActiveContext("B")
		failed.UpdateStateSnapshot([]byte("state"))
		failed.Rollback()

		done := make(chan error, 2)
		go func() { done <- dt.RecoverWithReplacement(context.Background(), "B", newRecoveryComponent("B", "A")) }()
		go func() { dt.ActivateService(context.Background(), "A"); done <- nil }()
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
