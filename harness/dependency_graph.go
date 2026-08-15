package harness

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrDependencyCycle        = errors.New("dependency cycle detected")
	ErrDependencyNodeExists   = errors.New("dependency node already exists")
	ErrDependencyNodeNotFound = errors.New("dependency node not found")
	ErrInvalidDependencyNode  = errors.New("invalid dependency node")
	ErrDependencyGraphInvalid = errors.New("invalid dependency graph")
)

type dependencyNode struct {
	name       string
	component  Component
	context    *SpatioTemporalContext
	providers  map[string]*dependencyEdge
	dependents map[string]*dependencyEdge
	active     bool
}

type dependencyEdge struct {
	provider  string
	dependent string
}

type dependencyEdgeKey struct {
	provider  string
	dependent string
}

// DependencyGraph is the structural dependency DAG. Its lock protects only
// graph metadata/state and is independent from SpatioTemporalContext.mu.
type DependencyGraph struct {
	mu         sync.RWMutex
	nodes      map[string]*dependencyNode
	edges      map[dependencyEdgeKey]*dependencyEdge
	generation uint64
}

type DependencyNodeInfo struct {
	Name      string
	Component Component
	Context   *SpatioTemporalContext
	Active    bool
}

func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		nodes: make(map[string]*dependencyNode),
		edges: make(map[dependencyEdgeKey]*dependencyEdge),
	}
}

func (g *DependencyGraph) RegisterNode(name string, component Component, ctx *SpatioTemporalContext) error {
	if err := validateDependencyNodeName(name); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.nodes[name]; ok {
		return fmt.Errorf("%w: %q", ErrDependencyNodeExists, name)
	}
	g.nodes[name] = &dependencyNode{
		name: name, component: component, context: ctx,
		providers: make(map[string]*dependencyEdge),
		dependents: make(map[string]*dependencyEdge),
	}
	return nil
}

func (g *DependencyGraph) RegisterDependency(provider, dependent string) error {
	if err := validateDependencyNodeName(provider); err != nil {
		return fmt.Errorf("provider: %w", err)
	}
	if err := validateDependencyNodeName(dependent); err != nil {
		return fmt.Errorf("dependent: %w", err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	p, ok := g.nodes[provider]
	if !ok {
		return fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, provider)
	}
	d, ok := g.nodes[dependent]
	if !ok {
		return fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, dependent)
	}
	key := dependencyEdgeKey{provider: provider, dependent: dependent}
	if _, ok := g.edges[key]; ok {
		return nil
	}
	if path := g.findPathLocked(dependent, provider); len(path) != 0 {
		cycle := append([]string{provider}, path...)
		return fmt.Errorf("%w: %s", ErrDependencyCycle, strings.Join(cycle, " -> "))
	}
	edge := &dependencyEdge{provider: provider, dependent: dependent}
	g.edges[key] = edge
	p.dependents[dependent] = edge
	d.providers[provider] = edge
	return nil
}

// SetNodeActive changes graph activity metadata only; it does not execute the
// component or context. A real state change advances the graph generation.
func (g *DependencyGraph) SetNodeActive(name string, active bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	node, ok := g.nodes[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, name)
	}
	if node.active != active {
		node.active = active
		g.generation++
	}
	return nil
}

func (g *DependencyGraph) HasNode(name string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.nodes[name]
	return ok
}

func (g *DependencyGraph) HasDependency(provider, dependent string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.edges[dependencyEdgeKey{provider: provider, dependent: dependent}]
	return ok
}

func (g *DependencyGraph) Nodes() []DependencyNodeInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]DependencyNodeInfo, 0, len(g.nodes))
	for _, node := range g.nodes {
		out = append(out, DependencyNodeInfo{Name: node.name, Component: node.component, Context: node.context, Active: node.active})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (g *DependencyGraph) Node(name string) (DependencyNodeInfo, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	node, ok := g.nodes[name]
	if !ok {
		return DependencyNodeInfo{}, false
	}
	return DependencyNodeInfo{Name: node.name, Component: node.component, Context: node.context, Active: node.active}, true
}

func (g *DependencyGraph) DependenciesOf(name string) ([]string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	node, ok := g.nodes[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, name)
	}
	out := make([]string, 0, len(node.providers))
	for provider := range node.providers {
		out = append(out, provider)
	}
	sort.Strings(out)
	return out, nil
}

func (g *DependencyGraph) DependentsOf(name string) ([]string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	node, ok := g.nodes[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, name)
	}
	out := make([]string, 0, len(node.dependents))
	for dependent := range node.dependents {
		out = append(out, dependent)
	}
	sort.Strings(out)
	return out, nil
}

func (g *DependencyGraph) EdgeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.edges)
}

func (g *DependencyGraph) Validate() error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.validateLocked()
}

func (g *DependencyGraph) TopologicalOrder() ([]string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if err := g.validateLocked(); err != nil {
		return nil, err
	}
	indegree := make(map[string]int, len(g.nodes))
	for name := range g.nodes { indegree[name] = 0 }
	for _, edge := range g.edges { indegree[edge.dependent]++ }
	ready := make([]string, 0)
	for name, degree := range indegree {
		if degree == 0 { ready = append(ready, name) }
	}
	sort.Strings(ready)
	order := make([]string, 0, len(g.nodes))
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		order = append(order, name)
		next := make([]string, 0, len(g.nodes[name].dependents))
		for dependent := range g.nodes[name].dependents { next = append(next, dependent) }
		sort.Strings(next)
		for _, dependent := range next {
			indegree[dependent]--
			if indegree[dependent] == 0 { ready = append(ready, dependent); sort.Strings(ready) }
		}
	}
	if len(order) != len(g.nodes) { return nil, ErrDependencyCycle }
	return order, nil
}

func (g *DependencyGraph) Generation() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.generation
}

func validateDependencyNodeName(name string) error {
	if strings.TrimSpace(name) == "" { return fmt.Errorf("%w: node name must not be empty", ErrInvalidDependencyNode) }
	return nil
}

func (g *DependencyGraph) validateLocked() error {
	for name, node := range g.nodes {
		if node == nil { return fmt.Errorf("%w: nil node %q", ErrDependencyGraphInvalid, name) }
		if err := validateDependencyNodeName(name); err != nil { return err }
		if node.name != name { return fmt.Errorf("%w: node key/name mismatch for %q", ErrDependencyGraphInvalid, name) }
		for dependent, edge := range node.dependents {
			if edge == nil || edge.provider != name || edge.dependent != dependent { return fmt.Errorf("%w: invalid dependent edge from %q to %q", ErrDependencyGraphInvalid, name, dependent) }
			if _, ok := g.nodes[dependent]; !ok { return fmt.Errorf("%w: edge %q -> %q references missing node", ErrDependencyGraphInvalid, name, dependent) }
			indexed, ok := g.edges[dependencyEdgeKey{provider: name, dependent: dependent}]
			if !ok || indexed != edge { return fmt.Errorf("%w: edge index mismatch for %q -> %q", ErrDependencyGraphInvalid, name, dependent) }
		}
		for provider, edge := range node.providers {
			if edge == nil || edge.provider != provider || edge.dependent != name { return fmt.Errorf("%w: invalid provider edge from %q to %q", ErrDependencyGraphInvalid, provider, name) }
			if _, ok := g.nodes[provider]; !ok { return fmt.Errorf("%w: edge %q -> %q references missing node", ErrDependencyGraphInvalid, provider, name) }
			indexed, ok := g.edges[dependencyEdgeKey{provider: provider, dependent: name}]
			if !ok || indexed != edge { return fmt.Errorf("%w: edge index mismatch for %q -> %q", ErrDependencyGraphInvalid, provider, name) }
		}
	}
	for key, edge := range g.edges {
		if edge == nil || edge.provider != key.provider || edge.dependent != key.dependent { return fmt.Errorf("%w: malformed edge", ErrDependencyGraphInvalid) }
		provider, providerOK := g.nodes[key.provider]
		dependent, dependentOK := g.nodes[key.dependent]
		if !providerOK || !dependentOK { return fmt.Errorf("%w: edge %q -> %q references missing node", ErrDependencyGraphInvalid, key.provider, key.dependent) }
		if provider.dependents[key.dependent] != edge || dependent.providers[key.provider] != edge { return fmt.Errorf("%w: asymmetric edge %q -> %q", ErrDependencyGraphInvalid, key.provider, key.dependent) }
	}
	return g.validateAcyclicLocked()
}

func (g *DependencyGraph) validateAcyclicLocked() error {
	indegree := make(map[string]int, len(g.nodes))
	for name := range g.nodes { indegree[name] = 0 }
	for _, edge := range g.edges { indegree[edge.dependent]++ }
	queue := make([]string, 0)
	for name, degree := range indegree { if degree == 0 { queue = append(queue, name) } }
	count := 0
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		count++
		for dependent := range g.nodes[name].dependents {
			indegree[dependent]--
			if indegree[dependent] == 0 { queue = append(queue, dependent) }
		}
	}
	if count != len(g.nodes) { return ErrDependencyCycle }
	return nil
}

func (g *DependencyGraph) findPathLocked(from, to string) []string {
	if from == to { return []string{from} }
	visited := map[string]bool{from: true}
	parent := make(map[string]string)
	queue := []string{from}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		node := g.nodes[current]
		if node == nil { continue }
		neighbors := make([]string, 0, len(node.dependents))
		for next := range node.dependents { neighbors = append(neighbors, next) }
		sort.Strings(neighbors)
		for _, next := range neighbors {
			if visited[next] { continue }
			visited[next] = true
			parent[next] = current
			if next == to { return reconstructDependencyPath(parent, from, to) }
			queue = append(queue, next)
		}
	}
	return nil
}

func reconstructDependencyPath(parent map[string]string, from, to string) []string {
	path := []string{to}
	for path[len(path)-1] != from {
		previous, ok := parent[path[len(path)-1]]
		if !ok { return nil }
		path = append(path, previous)
	}
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 { path[i], path[j] = path[j], path[i] }
	return path
}
