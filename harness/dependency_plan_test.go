package harness

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

type planTestComponent struct {
	name string
	mu   sync.Mutex
	log  *[]string
	g    *DependencyGraph
}

func (c *planTestComponent) Name() string     { return c.name }
func (c *planTestComponent) Inject() []string { return nil }
func (c *planTestComponent) OnWakeUp(*SpatioTemporalContext) {
	if c.g != nil {
		_ = c.g.Generation()
	}
	c.mu.Lock()
	if c.log != nil {
		*c.log = append(*c.log, "wake "+c.name)
	}
	c.mu.Unlock()
}
func (c *planTestComponent) OnSleep() {
	if c.g != nil {
		_ = c.g.Generation()
	}
	c.mu.Lock()
	if c.log != nil {
		*c.log = append(*c.log, "sleep "+c.name)
	}
	c.mu.Unlock()
}

func newPlanGraph(t *testing.T, names ...string) (*DependencyGraph, map[string]*planTestComponent) {
	t.Helper()
	g := NewDependencyGraph()
	components := make(map[string]*planTestComponent, len(names))
	for _, name := range names {
		component := &planTestComponent{name: name}
		components[name] = component
		if err := g.RegisterNode(name, component, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, component := range components {
		component.g = g
	}
	return g, components
}

func addPlanEdge(t *testing.T, g *DependencyGraph, provider, dependent string) {
	t.Helper()
	if err := g.RegisterDependency(provider, dependent); err != nil {
		t.Fatal(err)
	}
}

func planNames(plan DependencyPlan) []string {
	out := make([]string, 0, len(plan.Actions))
	for _, action := range plan.Actions {
		out = append(out, fmt.Sprintf("%s:%d", action.NodeName, action.Action))
	}
	return out
}

func TestDependencyPlanWakeChain(t *testing.T) {
	g, _ := newPlanGraph(t, "A", "B", "C")
	addPlanEdge(t, g, "A", "B")
	addPlanEdge(t, g, "B", "C")
	plan, err := g.PlanWake("A")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(planNames(plan)); got != "[A:1 B:1 C:1]" {
		t.Fatalf("actions=%s", got)
	}
	if err := g.Execute(plan); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"A", "B", "C"} {
		if info, _ := g.Node(name); !info.Active {
			t.Fatalf("%s inactive", name)
		}
	}
}

func TestDependencyPlanSleepChain(t *testing.T) {
	g, _ := newPlanGraph(t, "A", "B", "C")
	addPlanEdge(t, g, "A", "B")
	addPlanEdge(t, g, "B", "C")
	if _, err := g.PlanWake("A"); err != nil {
		t.Fatal(err)
	}
	plan, err := g.PlanSleep("A")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(planNames(plan)); got != "[C:2 B:2]" {
		t.Fatalf("actions=%s", got)
	}
	if info, _ := g.Node("A"); info.Active {
		t.Fatal("A remained active")
	}
}

func TestDependencyPlanMultiParentEligibility(t *testing.T) {
	g, _ := newPlanGraph(t, "A", "B", "D")
	addPlanEdge(t, g, "A", "D")
	addPlanEdge(t, g, "B", "D")
	first, err := g.PlanWake("A")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(planNames(first)); got != "[A:1]" {
		t.Fatalf("actions=%s", got)
	}
	second, err := g.PlanWake("B")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(planNames(second)); got != "[B:1 D:1]" {
		t.Fatalf("actions=%s", got)
	}
}

func TestDependencyPlanDiamondSleepsSharedDependentOnce(t *testing.T) {
	g, _ := newPlanGraph(t, "A", "B", "C", "D")
	addPlanEdge(t, g, "A", "B")
	addPlanEdge(t, g, "A", "C")
	addPlanEdge(t, g, "B", "D")
	addPlanEdge(t, g, "C", "D")
	if _, err := g.PlanWake("A"); err != nil {
		t.Fatal(err)
	}
	plan, err := g.PlanSleep("A")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(planNames(plan)); got != "[D:2 C:2 B:2]" {
		t.Fatalf("actions=%s", got)
	}
	count := 0
	for _, action := range plan.Actions {
		if action.NodeName == "D" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("D action count=%d", count)
	}
}

func TestDependencyPlanDiamondWakeDeterministic(t *testing.T) {
	g, _ := newPlanGraph(t, "A", "B", "C", "D")
	addPlanEdge(t, g, "A", "B")
	addPlanEdge(t, g, "A", "C")
	addPlanEdge(t, g, "B", "D")
	addPlanEdge(t, g, "C", "D")
	first, err := g.PlanWake("A")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(planNames(first)); got != "[A:1 B:1 C:1 D:1]" {
		t.Fatalf("actions=%s", got)
	}
	for i := 0; i < 20; i++ {
		g2, _ := newPlanGraph(t, "A", "B", "C", "D")
		addPlanEdge(t, g2, "A", "C")
		addPlanEdge(t, g2, "C", "D")
		addPlanEdge(t, g2, "A", "B")
		addPlanEdge(t, g2, "B", "D")
		plan, err := g2.PlanWake("A")
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprint(planNames(plan)); got != "[A:1 B:1 C:1 D:1]" {
			t.Fatalf("iteration %d actions=%s", i, got)
		}
	}
}

func TestDependencyPlanGenerationAndStalePlan(t *testing.T) {
	g, _ := newPlanGraph(t, "A")
	before := g.Generation()
	plan, err := g.PlanWake("A")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Generation <= before {
		t.Fatalf("generation did not advance: before=%d plan=%d", before, plan.Generation)
	}
	if err := g.SetNodeActive("A", false); err != nil {
		t.Fatal(err)
	}
	if err := g.Execute(plan); !errors.Is(err, ErrDependencyPlanStale) {
		t.Fatalf("err=%v", err)
	}
}

func TestDependencyPlanActionSliceOwned(t *testing.T) {
	g, _ := newPlanGraph(t, "A")
	plan, err := g.PlanWake("A")
	if err != nil {
		t.Fatal(err)
	}
	plan.Actions[0].NodeName = "mutated"
	info, _ := g.Node("A")
	if !info.Active {
		t.Fatal("graph state changed through plan slice mutation")
	}
	if plan.Actions[0].NodeName != "mutated" {
		t.Fatal("expected caller-local mutation")
	}
}

func TestDependencyPlanCallbacksRunWithoutGraphMutex(t *testing.T) {
	g, components := newPlanGraph(t, "A", "B")
	addPlanEdge(t, g, "A", "B")
	plan, err := g.PlanWake("A")
	if err != nil {
		t.Fatal(err)
	}
	components["A"].log = new([]string)
	components["B"].log = components["A"].log
	if err := g.Execute(plan); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(*components["A"].log); got != "[wake A wake B]" {
		t.Fatalf("log=%s", got)
	}
}

func TestDependencyPlanConcurrentPlanning(t *testing.T) {
	g, _ := newPlanGraph(t, "A", "B", "C")
	addPlanEdge(t, g, "A", "B")
	addPlanEdge(t, g, "B", "C")
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, err := g.PlanWake("A"); err != nil {
					t.Errorf("wake: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	plan, err := g.PlanSleep("A")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 2 {
		t.Fatalf("sleep actions=%v", planNames(plan))
	}
}

func TestDependencyPlanConcurrentActivationDeactivation(t *testing.T) {
	g, _ := newPlanGraph(t, "A", "B")
	addPlanEdge(t, g, "A", "B")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = g.PlanWake("A")
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = g.PlanSleep("A")
			}
		}()
	}
	wg.Wait()
	if err := g.Validate(); err != nil {
		t.Fatal(err)
	}
}
