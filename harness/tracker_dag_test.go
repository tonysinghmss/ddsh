package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type trackerTestComponent struct {
	name string
	reqs []string
	mu sync.Mutex
	history []string
	wakeOrder *[]string
	sleepOrder *[]string
	wake int32
	sleep int32
	tracker *DependencyTracker
}

func newTrackerTestComponent(name string, reqs ...string) *trackerTestComponent { return &trackerTestComponent{name: name, reqs: reqs, history: make([]string, 0)} }
func (c *trackerTestComponent) Name() string { return c.name }
func (c *trackerTestComponent) Inject() []string { return append([]string(nil), c.reqs...) }
func (c *trackerTestComponent) OnWakeUp(ctx *SpatioTemporalContext) {
	atomic.AddInt32(&c.wake, 1)
	c.mu.Lock()
	c.history = append(c.history, "wake")
	if c.wakeOrder != nil { *c.wakeOrder = append(*c.wakeOrder, c.name) }
	c.mu.Unlock()
	if c.tracker != nil { _ = c.tracker.GetActiveContext(c.name) }
	_ = ctx
}
func (c *trackerTestComponent) OnSleep() {
	atomic.AddInt32(&c.sleep, 1)
	c.mu.Lock()
	c.history = append(c.history, "sleep")
	if c.sleepOrder != nil { *c.sleepOrder = append(*c.sleepOrder, c.name) }
	c.mu.Unlock()
	if c.tracker != nil { _ = c.tracker.Graph().DependentsOf(c.name) }
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
	if !dt.Graph().Node("A").Active || !dt.Graph().Node("B").Active || !dt.Graph().Node("C").Active { t.Fatal("chain did not fully activate") }
	if a.wake != 1 || b.wake != 1 || c.wake != 1 { t.Fatalf("wake counts: %d %d %d", a.wake, b.wake, c.wake) }
	sleepOrder := make([]string, 0, 3)
	a.sleepOrder, b.sleepOrder, c.sleepOrder = &sleepOrder, &sleepOrder, &sleepOrder
	dt.DeactivateService("A")
	if dt.Graph().Node("A").Active || dt.Graph().Node("B").Active || dt.Graph().Node("C").Active { t.Fatal("chain remained active") }
	if a.sleep != 1 || b.sleep != 1 || c.sleep != 1 { t.Fatalf("sleep counts: %d %d %d", a.sleep, b.sleep, c.sleep) }
	if got := fmt.Sprint(sleepOrder); got != "[C B A]" { t.Fatalf("sleep order=%s", got) }
}

func TestDependencyTrackerDiamondTopologicalOrder(t *testing.T) {
	dt := NewDependencyTracker(nil)
	order := make([]string, 0, 4)
	a := newTrackerTestComponent("A")
	b := newTrackerTestComponent("B", "A")
	c := newTrackerTestComponent("C", "A")
	d := newTrackerTestComponent("D", "B", "C")
	for _, component := range []*trackerTestComponent{a, b, c, d} { component.wakeOrder = &order; dt.RegisterComponent(context.Background(), component) }
	if fmt.Sprint(order) != "[A B C D]" { t.Fatalf("registration wake order=%v", order) }
	dt.ActivateService(context.Background(), "A")
	sleepOrder := make([]string, 0, 4)
	a.sleepOrder, b.sleepOrder, c.sleepOrder, d.sleepOrder = &sleepOrder, &sleepOrder, &sleepOrder, &sleepOrder
	dt.DeactivateService("A")
	if d.sleep != 1 || b.sleep != 1 || c.sleep != 1 || a.sleep != 1 { t.Fatal("diamond sleep counts incorrect") }
	if got := fmt.Sprint(sleepOrder); got != "[D C B A]" { t.Fatalf("diamond sleep order=%s", got) }
}

func TestDependencyTrackerMultiParentAndSharedDependent(t *testing.T) {
	dt := NewDependencyTracker(nil)
	d := newTrackerTestComponent("D", "A", "B")
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

func TestDependencyTrackerCycleRejectionIsAtomic(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A", "C")
	b := newTrackerTestComponent("B", "A")
	c := newTrackerTestComponent("C", "B")
	dt.RegisterComponent(context.Background(), b)
	dt.RegisterComponent(context.Background(), c)
	before := dt.Graph().EdgeCount()
	if err := dt.RegisterComponentErr(context.Background(), a); !errors.Is(err, ErrDependencyCycle) { t.Fatalf("err=%v", err) }
	if _, ok := dt.Graph().Node("A"); ok { t.Fatal("failed cycle registration partially installed provider A") }
	if dt.Graph().EdgeCount() != before { t.Fatalf("edge count changed: %d -> %d", before, dt.Graph().EdgeCount()) }
}

func TestDependencyTrackerCallbackLockBoundary(t *testing.T) {
	dt := NewDependencyTracker(nil)
	a := newTrackerTestComponent("A")
	a.tracker = dt
	dt.RegisterComponent(context.Background(), a)
	if dt.GetActiveContext("A") == nil { t.Fatal("active context missing") }
}

func TestDependencyTrackerConcurrentActivationDeactivation(t *testing.T) {
	dt := NewDependencyTracker(nil)
	b := newTrackerTestComponent("B", "A")
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
}

func TestDependencyTrackerStalePlanIsRejectedByPublicExecutor(t *testing.T) {
	dt := NewDependencyTracker(nil)
	d := newTrackerTestComponent("D", "A")
	dt.RegisterComponent(context.Background(), d)
	wakePlan, err := dt.Graph().PlanWake("A")
	if err != nil { t.Fatal(err) }
	if len(wakePlan.Actions) != 2 { t.Fatalf("wake actions=%d", len(wakePlan.Actions)) }
	sleepPlan, err := dt.Graph().PlanSleep("A")
	if err != nil { t.Fatal(err) }
	if sleepPlan.Generation <= wakePlan.Generation { t.Fatalf("generation did not advance: wake=%d sleep=%d", wakePlan.Generation, sleepPlan.Generation) }
	beforeWake := d.wake
	if err := dt.Graph().ExecuteWithOptions(wakePlan, DependencyExecutionOptions{}); !errors.Is(err, ErrDependencyPlanStale) { t.Fatalf("err=%v", err) }
	if d.wake != beforeWake { t.Fatalf("stale wake executed: before=%d after=%d", beforeWake, d.wake) }
}

func TestDependencyTrackerConcurrentRegistration(t *testing.T) {
	dt := NewDependencyTracker(nil)
	const n = 32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ { wg.Add(1); go func(i int) { defer wg.Done(); dt.RegisterComponent(context.Background(), newTrackerTestComponent(fmt.Sprintf("C-%02d", i))) }(i) }
	wg.Wait()
	if got := len(dt.Graph().Nodes()); got != n { t.Fatalf("nodes=%d want=%d", got, n) }
}

func TestDependencyTrackerGraphIsSingleSourceOfTruth(t *testing.T) {
	dt := NewDependencyTracker(nil)
	b := newTrackerTestComponent("B", "A")
	dt.RegisterComponent(context.Background(), b)
	if !dt.Graph().HasDependency("A", "B") { t.Fatal("missing DAG edge") }
	if _, ok := dt.Graph().Node("B"); !ok { t.Fatal("missing component node") }
	dt.ActivateService(context.Background(), "A")
	if dt.GetActiveContext("B") == nil { t.Fatal("graph activation state/context not reflected") }
}

var _ Component = (*trackerTestComponent)(nil)
