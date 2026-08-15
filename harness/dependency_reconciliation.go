package harness

import (
	"context"
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

func (g *DependencyGraph) ensureProviderLocked(name string) (bool, error) {
	if err := validateDependencyNodeName(name); err != nil {
		return false, err
	}
	if _, ok := g.nodes[name]; ok {
		return false, nil
	}
	g.nodes[name] = &dependencyNode{name: name, providers: make(map[string]*dependencyEdge), dependents: make(map[string]*dependencyEdge)}
	return true, nil
}

func (g *DependencyGraph) buildServiceTransition(serviceName string, active bool) (DependencyPlan, error) {
	if err := validateDependencyNodeName(serviceName); err != nil {
		return DependencyPlan{}, err
	}
	g.planMu.Lock()
	defer g.planMu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()

	created, err := g.ensureProviderLocked(serviceName)
	if err != nil {
		return DependencyPlan{}, err
	}
	if created {
		g.generation++
	}
	trigger := g.nodes[serviceName]
	if trigger.active == active {
		return DependencyPlan{kind: executionKind(active), generation: g.generation}, nil
	}

	order, err := g.topologicalOrderLocked()
	if err != nil {
		return DependencyPlan{}, err
	}
	steps := make([]ExecutionStep, 0)
	if active {
		trigger.active = true
		for _, name := range order {
			node := g.nodes[name]
			if node.active || node.component == nil || !g.allProvidersActiveLocked(node) || !g.reachableFromLocked(serviceName, name) {
				continue
			}
			node.active = true
			steps = append(steps, ExecutionStep{Name: name, Component: node.component, Context: node.context})
		}
	} else {
		triggerWasActive := trigger.active
		trigger.active = false
		candidates := make(map[string]struct{})
		if triggerWasActive && trigger.component != nil {
			candidates[serviceName] = struct{}{}
		}
		queue := []string{serviceName}
		seen := map[string]bool{serviceName: true}
		for len(queue) > 0 {
			name := queue[0]
			queue = queue[1:]
			for dependent := range g.nodes[name].dependents {
				if !seen[dependent] {
					seen[dependent] = true
					queue = append(queue, dependent)
				}
			}
		}
		for name := range seen {
			node := g.nodes[name]
			if node != nil && node.active && node.component != nil && !g.allProvidersActiveLocked(node) {
				candidates[name] = struct{}{}
			}
		}
		for i := len(order) - 1; i >= 0; i-- {
			name := order[i]
			if _, ok := candidates[name]; !ok {
				continue
			}
			node := g.nodes[name]
			node.active = false
			steps = append(steps, ExecutionStep{Name: name, Component: node.component, Context: node.context})
		}
	}
	g.generation++
	return DependencyPlan{kind: executionKind(active), generation: g.generation, steps: steps}, nil
}

func executionKind(active bool) ExecutionKind {
	if active {
		return ExecutionWake
	}
	return ExecutionSleep
}

func (g *DependencyGraph) allProvidersActiveLocked(node *dependencyNode) bool {
	for provider := range node.providers {
		if !g.nodes[provider].active {
			return false
		}
	}
	return true
}

func (g *DependencyGraph) reachableFromLocked(from, target string) bool {
	if from == target {
		return true
	}
	queue := []string{from}
	seen := map[string]bool{from: true}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		neighbors := make([]string, 0, len(g.nodes[name].dependents))
		for dependent := range g.nodes[name].dependents {
			neighbors = append(neighbors, dependent)
		}
		sort.Strings(neighbors)
		for _, dependent := range neighbors {
			if dependent == target {
				return true
			}
			if !seen[dependent] {
				seen[dependent] = true
				queue = append(queue, dependent)
			}
		}
	}
	return false
}

func (g *DependencyGraph) topologicalOrderLocked() ([]string, error) {
	indegree := make(map[string]int, len(g.nodes))
	for name := range g.nodes {
		indegree[name] = 0
	}
	for _, edge := range g.edges {
		indegree[edge.dependent]++
	}
	ready := make([]string, 0)
	for name, degree := range indegree {
		if degree == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(g.nodes))
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		order = append(order, name)
		neighbors := make([]string, 0, len(g.nodes[name].dependents))
		for dependent := range g.nodes[name].dependents {
			neighbors = append(neighbors, dependent)
		}
		sort.Strings(neighbors)
		for _, dependent := range neighbors {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	if len(order) != len(g.nodes) {
		return nil, ErrDependencyCycle
	}
	return order, nil
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

func (dt *DependencyTracker) executeDependencyPlan(parentCtx context.Context, plan DependencyPlan) {
	if err := plan.validate(); err != nil {
		dt.recordError(err)
		return
	}
	dt.graph.planMu.Lock()
	defer dt.graph.planMu.Unlock()
	for _, step := range plan.steps {
		if !dt.planCurrent(plan.generation) {
			return
		}
		if plan.kind == ExecutionWake {
			dt.executeWakeStep(parentCtx, step)
		} else {
			dt.executeSleepStep(step)
		}
	}
}

func (dt *DependencyTracker) executeWakeStep(parentCtx context.Context, step ExecutionStep) {
	ctx := step.Context
	if ctx == nil {
		ctx = NewSTContext(parentCtx, step.Name, dt.onRecovery)
		dt.graph.mu.Lock()
		if node := dt.graph.nodes[step.Name]; node != nil {
			node.context = ctx
		}
		dt.graph.mu.Unlock()
	}
	step.Component.OnWakeUp(ctx)
}

func (dt *DependencyTracker) executeSleepStep(step ExecutionStep) {
	step.Component.OnSleep()
	if step.Context != nil {
		step.Context.Rollback()
	}
	dt.graph.mu.Lock()
	if node := dt.graph.nodes[step.Name]; node != nil {
		node.context = nil
	}
	dt.graph.mu.Unlock()
}

func (dt *DependencyTracker) registerAndPlan(parentCtx context.Context, comp Component) (DependencyPlan, error) {
	if err := dt.graph.registerComponentAtomic(comp.Name(), comp, append([]string(nil), comp.Inject()...)); err != nil {
		return DependencyPlan{}, err
	}
	return dt.reconcileNewRegistration(parentCtx, comp.Name())
}

func (dt *DependencyTracker) reconcileNewRegistration(_ context.Context, name string) (DependencyPlan, error) {
	dt.graph.planMu.Lock()
	defer dt.graph.planMu.Unlock()
	dt.graph.mu.Lock()
	defer dt.graph.mu.Unlock()
	node := dt.graph.nodes[name]
	if node == nil || node.active || node.component == nil || !dt.graph.allProvidersActiveLocked(node) {
		return DependencyPlan{kind: ExecutionWake, generation: dt.graph.generation}, nil
	}
	node.active = true
	dt.graph.generation++
	return DependencyPlan{kind: ExecutionWake, generation: dt.graph.generation, steps: []ExecutionStep{{Name: name, Component: node.component, Context: node.context}}}, nil
}

func (dt *DependencyTracker) planCurrent(generation uint64) bool {
	return dt.graph.Generation() == generation
}

func (dt *DependencyTracker) recordError(err error) {
	dt.mu.Lock()
	dt.lastErr = err
	dt.mu.Unlock()
}

func (dt *DependencyTracker) LastError() error {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	return dt.lastErr
}
