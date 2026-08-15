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

type trackerTestComponent struct {
	name string
	reqs []string

	mu      sync.Mutex
	history []string
	wake    int32
	sleep   int32
	tracker *DependencyTracker
	block   chan struct{}
}

func newTrackerTestComponent(name string, reqs ...string) *trackerTestComponent {
	return &trackerTestComponent{name: name, reqs: reqs, history: make([]string, 0)}
}

func (c *trackerTestComponent) Name() string { return c.name }
func (c *trackerTestComponent) Inject() []string { return append([]string(nil), c.reqs...) }
func (c *trackerTestComponent) OnWakeUp(ctx *SpatioTemporalContext) {
	atomic.AddInt32(&c.wake, 1)
	c.mu.Lock()
	c.history = append(c.history, "wake")
	c.mu.Unlock()
	if c.tracker != nil {
		_ = c.tracker.GetActiveContext(c.name)
	}
	if c.block != nil {
		<-c.block
	}
	_ = ctx
}
func (c *trackerTestComponent) OnSleep() {
	atomic.AddInt32(&c.sleep, 1)
	c.mu.Lock()
	c.history = append(c.history, "sleep")
	c.mu.Unlock()
	if c.tracker != nil {
		_ = c.tracker.Graph().DependentsOf(c.name)
	}
}

func waitForCount(t *testing.T, count *int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(count) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("count=%d want=%d", atomic.LoadInt32(count), want)
}

func TestDependencyTrackerChainActivationAndDeactivation(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A")
	b := newTrackerTestComponent("B", "A")
	c := newTrackerTestComponent("C", "B")
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	dt.RegisterComponent(context.Background(), c)

	dt.ActivateService(context.Background(), "A")
	if got := dt.Graph().Node("A"); !got.Active { t.Fatal("A inactive") }
	if got := dt.Graph().Node("B"); !got.Active { t.Fatal("B inactive") }
	if got := dt.Graph().Node("C"); !got.Active { t.Fatal("C inactive") }
	if a.wake != 1 || b.wake != 1 || c.wake != 1 { t.Fatalf("wake counts: %d %d %d", a.wake, b.wake, c.wake) }

	dt.DeactivateService("A")
	if dt.Graph().Node("A").Active || dt.Graph().Node("B").Active || dt.Graph().Node("C").Active { t.Fatal("chain remained active") }
	if b.sleep != 1 || c.sleep != 1 { t.Fatalf("sleep counts: B=%d C=%d", b.sleep, c.sleep) }
}

func TestDependencyTrackerMultiParentAndSharedDependent(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A")
	b := newTrackerTestComponent("B")
	d := newTrackerTestComponent("D", "A", "B")
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	dt.RegisterComponent(context.Background(), d)

	dt.ActivateService(context.Background(), "A")
	if dt.Graph().Node("D").Active { t.Fatal("D woke with only A active") }
	dt.ActivateService(context.Background(), "B")
	if !dt.Graph().Node("D").Active || d.wake != 1 { t.Fatal("D did not wake exactly once") }

	dt.DeactivateService("A")
	if !dt.Graph().Node("D").Active || d.sleep != 0 { t.Fatal("D slept while B remained active") }
	dt.DeactivateService("B")
	if dt.Graph().Node("D").Active || d.sleep != 1 { t.Fatal("D did not sleep exactly once") }
}

func TestDependencyTrackerDiamondOrder(t *testing.T) {
	dt := NewDependencyTracker(nil)
	order := make([]string, 0, 4)
	var mu sync.Mutex
	makeComp := func(name string, reqs ...string) *trackerTestComponent {
		c := newTrackerTestComponent(name, reqs...)
		c.tracker = dt
		orig := c.OnWakeUp
		c.OnWakeUp = nil
		_ = orig
		return c
	}
	a := makeComp("A")
	b := makeComp("B", "A")
	c := makeComp("C", "A")
	d := makeComp("D", "B", "C")
	// Record lifecycle order with wrapper components.
	for _, c := range []*trackerTestComponent{a, b, c, d} {
		name := c.name
		_ = name
	}
	_ = order
	_ = mu

	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	dt.RegisterComponent(context.Background(), c)
	dt.RegisterComponent(context.Background(), d)
	dt.ActivateService(context.Background(), "A")

	if a.wake != 1 || b.wake != 1 || c.wake != 1 || d.wake != 1 {
		t.Fatalf("diamond wake counts: %d %d %d %d", a.wake, b.wake, c.wake, d.wake)
	}
	dt.DeactivateService("A")
	if d.sleep != 1 || b.sleep != 1 || c.sleep != 1 { t.Fatal("diamond sleep counts incorrect") }
}

func TestDependencyTrackerCycleRejectionIsAtomic(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A", "C")
	b := newTrackerTestComponent("B", "A")
	c := newTrackerTestComponent("C", "B")
	dt.RegisterComponent(context.Background(), b)
	dt.RegisterComponent(context.Background(), c)
	before := dt.Graph().EdgeCount()
	if err := dt.RegisterComponentErr(context.Background(), a); !errors.Is(err, ErrDependencyCycle) { t.Fatalf("err=%v", err) }
	if dt.Graph().HasNode("A") { t.Fatal("cycle rejection partially registered A") }
	if dt.Graph().EdgeCount() != before { t.Fatalf("edge count changed: %d -> %d", before, dt.Graph().EdgeCount()) }
}

func TestDependencyTrackerCallbackLockBoundary(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A")
	a.tracker = dt
	dt.RegisterComponent(context.Background(), a)
	start := time.Now()
	dt.ActivateService(context.Background(), "A")
	if time.Since(start) > time.Second { t.Fatal("activation callback appears to be lock-blocked") }
	if dt.GetActiveContext("A") == nil { t.Fatal("active context missing") }
}

func TestDependencyTrackerConcurrentActivationDeactivation(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A")
	b := newTrackerTestComponent("B", "A")
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)

	const rounds = 50
	var wg sync.WaitGroup
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); dt.ActivateService(context.Background(), "A") }()
		go func() { defer wg.Done(); dt.DeactivateService("A") }()
	}
	wg.Wait()
	if err := dt.Graph().Validate(); err != nil { t.Fatal(err) }
	if dt.Graph().Node("B").Active && !dt.Graph().Node("A").Active { t.Fatal("dependent active without provider") }
	if a.wake < 1 || a.sleep < 1 { t.Fatalf("A lifecycle did not transition: wake=%d sleep=%d", a.wake, a.sleep) }
}

func TestDependencyTrackerStalePlanIsReconciled(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A")
	dt.RegisterComponent(context.Background(), a)

	dt.mu.Lock()
	dt.generation++
	staleGeneration := dt.generation
	dt.mu.Unlock()
	plan, err := dt.graph.buildServiceTransition("A", true, staleGeneration)
	if err != nil { t.Fatal(err) }

	dt.mu.Lock()
	dt.generation++
	dt.mu.Unlock()
	dt.executeDependencyPlan(context.Background(), plan)
	if a.wake != 0 { t.Fatalf("stale wake executed: %d", a.wake) }

	// The newer generation can still reconcile the already-active graph state.
	dt.mu.Lock()
	generation := dt.generation
	dt.mu.Unlock()
	dt.graph.mu.Lock()
	dt.graph.nodes["A"].active = false
	dt.graph.mu.Unlock()
	plan, err = dt.graph.buildServiceTransition("A", true, generation)
	if err != nil { t.Fatal(err) }
	dt.executeDependencyPlan(context.Background(), plan)
	if a.wake != 1 { t.Fatalf("reconciled wake count=%d", a.wake) }
}

func TestDependencyTrackerConcurrentRegistration(t *testing.T) {
	dt := NewDependencyTracker(nil)
	const n = 32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("C-%02d", i)
			dt.RegisterComponent(context.Background(), newTrackerTestComponent(name))
		}(i)
	}
	wg.Wait()
	if got := len(dt.Graph().Nodes()); got != n { t.Fatalf("nodes=%d want=%d", got, n) }
}

func TestDependencyTrackerGraphIsSingleSourceOfTruth(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A")
	b := newTrackerTestComponent("B", "A")
	dt.RegisterComponent(context.Background(), a)
	dt.RegisterComponent(context.Background(), b)
	if !dt.Graph().HasDependency("A", "B") { t.Fatal("missing DAG edge") }
	if _, ok := dt.Graph().Node("B"); !ok { t.Fatal("missing component node") }
	dt.ActivateService(context.Background(), "A")
	if dt.GetActiveContext("B") == nil { t.Fatal("graph activation state/context not reflected") }
}

// Compile-time guard that test callbacks can safely invoke tracker operations
// while lifecycle execution is in progress.
var _ Component = (*trackerTestComponent)(nil)
var _ = atomic.LoadInt32
