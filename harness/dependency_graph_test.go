package harness

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

type graphTestComponent struct{ name string }

func (c graphTestComponent) Name() string                         { return c.name }
func (c graphTestComponent) Inject() []string                     { return nil }
func (c graphTestComponent) OnWakeUp(*SpatioTemporalContext)      {}
func (c graphTestComponent) OnSleep()                             {}

func newTestGraph(t *testing.T, names ...string) *DependencyGraph {
	t.Helper()
	g := NewDependencyGraph()
	for _, name := range names {
		if err := g.RegisterNode(name, graphTestComponent{name: name}, nil); err != nil {
			t.Fatal(err)
		}
	}
	return g
}

func addEdge(t *testing.T, g *DependencyGraph, provider, dependent string) {
	t.Helper()
	if err := g.RegisterDependency(provider, dependent); err != nil {
		t.Fatal(err)
	}
}

func TestDependencyGraphChain(t *testing.T) {
	g := newTestGraph(t, "A", "B", "C")
	addEdge(t, g, "A", "B")
	addEdge(t, g, "B", "C")
	if err := g.Validate(); err != nil { t.Fatal(err) }
	deps, _ := g.DependenciesOf("C")
	if fmt.Sprint(deps) != "[B]" { t.Fatalf("deps=%v", deps) }
	dependents, _ := g.DependentsOf("A")
	if fmt.Sprint(dependents) != "[B]" { t.Fatalf("dependents=%v", dependents) }
	order, err := g.TopologicalOrder()
	if err != nil { t.Fatal(err) }
	if fmt.Sprint(order) != "[A B C]" { t.Fatalf("order=%v", order) }
}

func TestDependencyGraphDiamond(t *testing.T) {
	g := newTestGraph(t, "A", "B", "C", "D")
	addEdge(t, g, "A", "B")
	addEdge(t, g, "A", "C")
	addEdge(t, g, "B", "D")
	addEdge(t, g, "C", "D")
	if err := g.Validate(); err != nil { t.Fatal(err) }
	deps, _ := g.DependenciesOf("D")
	if fmt.Sprint(deps) != "[B C]" { t.Fatalf("deps=%v", deps) }
}

func TestDependencyGraphMultipleDependencies(t *testing.T) {
	g := newTestGraph(t, "A", "B", "D")
	addEdge(t, g, "A", "D")
	addEdge(t, g, "B", "D")
	deps, _ := g.DependenciesOf("D")
	if fmt.Sprint(deps) != "[A B]" { t.Fatalf("deps=%v", deps) }
}

func TestDependencyGraphDuplicateEdge(t *testing.T) {
	g := newTestGraph(t, "A", "B")
	addEdge(t, g, "A", "B")
	if err := g.RegisterDependency("A", "B"); err != nil { t.Fatal(err) }
	if got := g.EdgeCount(); got != 1 { t.Fatalf("edge count=%d", got) }
	deps, _ := g.DependenciesOf("B")
	if len(deps) != 1 { t.Fatalf("deps=%v", deps) }
}

func TestDependencyGraphDirectCycle(t *testing.T) {
	g := newTestGraph(t, "A", "B")
	addEdge(t, g, "A", "B")
	err := g.RegisterDependency("B", "A")
	if !errors.Is(err, ErrDependencyCycle) { t.Fatalf("err=%v", err) }
	if !strings.Contains(err.Error(), "B -> A -> B") { t.Fatalf("cycle path=%q", err) }
	if g.EdgeCount() != 1 { t.Fatal("graph mutated") }
}

func TestDependencyGraphIndirectCycle(t *testing.T) {
	g := newTestGraph(t, "A", "B", "C")
	addEdge(t, g, "A", "B")
	addEdge(t, g, "B", "C")
	err := g.RegisterDependency("C", "A")
	if !errors.Is(err, ErrDependencyCycle) { t.Fatalf("err=%v", err) }
	if !strings.Contains(err.Error(), "C -> A -> B -> C") { t.Fatalf("cycle path=%q", err) }
	if g.EdgeCount() != 2 { t.Fatal("graph mutated") }
}

func TestDependencyGraphFailedCycleLeavesGraphUnchanged(t *testing.T) {
	g := newTestGraph(t, "A", "B", "C", "D")
	addEdge(t, g, "A", "B")
	addEdge(t, g, "B", "C")
	addEdge(t, g, "A", "D")
	before, err := g.TopologicalOrder()
	if err != nil { t.Fatal(err) }
	if err := g.RegisterDependency("C", "A"); err == nil { t.Fatal("expected cycle") }
	after, err := g.TopologicalOrder()
	if err != nil { t.Fatal(err) }
	if fmt.Sprint(before) != fmt.Sprint(after) { t.Fatalf("before=%v after=%v", before, after) }
	if g.EdgeCount() != 3 { t.Fatalf("edge count=%d", g.EdgeCount()) }
}

func TestDependencyGraphValidationMissingNodes(t *testing.T) {
	g := NewDependencyGraph()
	if err := g.RegisterDependency("A", "B"); !errors.Is(err, ErrDependencyNodeNotFound) { t.Fatalf("err=%v", err) }
	if err := g.RegisterNode(" ", nil, nil); !errors.Is(err, ErrInvalidDependencyNode) { t.Fatalf("err=%v", err) }
}

func TestDependencyGraphConcurrentReads(t *testing.T) {
	g := newTestGraph(t, "A", "B", "C", "D")
	addEdge(t, g, "A", "B")
	addEdge(t, g, "A", "C")
	addEdge(t, g, "B", "D")
	addEdge(t, g, "C", "D")
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = g.Nodes()
				_ = g.HasNode("A")
				_, _ = g.DependenciesOf("D")
				_, _ = g.DependentsOf("A")
				_, _ = g.TopologicalOrder()
				_ = g.Validate()
			}
		}()
	}
	wg.Wait()
}

func TestDependencyGraphConcurrentRegistration(t *testing.T) {
	g := NewDependencyGraph()
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("N-%03d", i)
			if err := g.RegisterNode(name, nil, nil); err != nil { t.Errorf("register %s: %v", name, err) }
		}(i)
	}
	wg.Wait()
	if got := len(g.Nodes()); got != n { t.Fatalf("nodes=%d", got) }
	for i := 0; i < n-1; i++ {
		if err := g.RegisterDependency(fmt.Sprintf("N-%03d", i), fmt.Sprintf("N-%03d", i+1)); err != nil { t.Fatal(err) }
	}
	if err := g.Validate(); err != nil { t.Fatal(err) }
}

func TestDependencyGraphNodeMetadataAndActivity(t *testing.T) {
	g := newTestGraph(t, "A")
	if err := g.SetNodeActive("A", true); err != nil { t.Fatal(err) }
	info, ok := g.Node("A")
	if !ok || !info.Active || info.Name != "A" || info.Component == nil { t.Fatalf("info=%+v ok=%v", info, ok) }
}
