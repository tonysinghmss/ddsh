package harness

import (
	"fmt"
	"sort"
)

func (g *DependencyGraph) registerComponentAtomic(name string, component Component, requirements []string) error {
	if err := validateDependencyNodeName(name); err != nil {
		return err
	}
	for _, provider := range requirements {
		if err := validateDependencyNodeName(provider); err != nil {
			return fmt.Errorf("provider: %w", err)
		}
	}
	g.planMu.Lock()
	defer g.planMu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()

	existing, exists := g.nodes[name]
	if exists && existing.component != nil {
		return fmt.Errorf("%w: %q", ErrDependencyNodeExists, name)
	}
	adj := make(map[string]map[string]struct{}, len(g.nodes)+len(requirements))
	for nodeName, node := range g.nodes {
		adj[nodeName] = make(map[string]struct{}, len(node.dependents))
		for dependent := range node.dependents {
			adj[nodeName][dependent] = struct{}{}
		}
	}
	if _, ok := adj[name]; !ok {
		adj[name] = make(map[string]struct{})
	}
	for _, provider := range requirements {
		if _, ok := adj[provider]; !ok {
			adj[provider] = make(map[string]struct{})
		}
		adj[provider][name] = struct{}{}
	}
	if cycle := findCycleInAdjacency(adj); len(cycle) != 0 {
		return fmt.Errorf("%w: %s", ErrDependencyCycle, joinPath(cycle))
	}
	if !exists {
		g.nodes[name] = &dependencyNode{name: name, component: component, providers: make(map[string]*dependencyEdge), dependents: make(map[string]*dependencyEdge)}
	} else {
		existing.component = component
	}
	for provider := range adj {
		if _, exists := g.nodes[provider]; exists {
			continue
		}
		g.nodes[provider] = &dependencyNode{name: provider, providers: make(map[string]*dependencyEdge), dependents: make(map[string]*dependencyEdge)}
	}
	for _, provider := range requirements {
		key := dependencyEdgeKey{provider: provider, dependent: name}
		if _, exists := g.edges[key]; exists {
			continue
		}
		edge := &dependencyEdge{provider: provider, dependent: name}
		g.edges[key] = edge
		g.nodes[provider].dependents[name] = edge
		g.nodes[name].providers[provider] = edge
	}
	g.generation++
	return nil
}

func findCycleInAdjacency(adj map[string]map[string]struct{}) []string {
	const (
		white uint8 = iota
		gray
		black
	)
	state := make(map[string]uint8, len(adj))
	stack := make([]string, 0, len(adj))
	var visit func(string) []string
	visit = func(node string) []string {
		state[node] = gray
		stack = append(stack, node)
		neighbors := make([]string, 0, len(adj[node]))
		for next := range adj[node] {
			neighbors = append(neighbors, next)
		}
		sort.Strings(neighbors)
		for _, next := range neighbors {
			switch state[next] {
			case gray:
				for i := range stack {
					if stack[i] == next {
						return append(append([]string(nil), stack[i:]...), next)
					}
				}
			case white:
				if cycle := visit(next); len(cycle) != 0 {
					return cycle
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[node] = black
		return nil
	}
	for node := range adj {
		if state[node] == white {
			if cycle := visit(node); len(cycle) != 0 {
				return cycle
			}
		}
	}
	return nil
}

func joinPath(path []string) string {
	result := ""
	for i, name := range path {
		if i > 0 {
			result += " -> "
		}
		result += name
	}
	return result
}
