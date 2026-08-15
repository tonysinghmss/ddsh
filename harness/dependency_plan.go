package harness

import (
	"errors"
	"fmt"
	"sort"
)

type DependencyActionType uint8

const (
	DependencyActionWake DependencyActionType = iota + 1
	DependencyActionSleep
)

var (
	ErrDependencyPlanStale   = errors.New("dependency plan is stale")
	ErrDependencyPlanInvalid = errors.New("invalid dependency plan")
)

// DependencyAction is a detached lifecycle instruction. The graph never
// invokes Component or Context methods while its graph mutex is held.
type DependencyAction struct {
	NodeName  string
	Action    DependencyActionType
	Component Component
	Context   *SpatioTemporalContext
}

// DependencyPlan owns its Actions backing slice and records the graph
// generation whose state it represents.
type DependencyPlan struct {
	Generation uint64
	Actions    []DependencyAction
}

// PlanWake atomically calculates a wake transition and commits the resulting
// activation metadata before returning the immutable plan. No callbacks run
// during planning.
func (g *DependencyGraph) PlanWake(name string) (DependencyPlan, error) {
	g.planMu.Lock()
	defer g.planMu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.nodes[name]; !ok {
		return DependencyPlan{}, fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, name)
	}
	actions, changed, err := g.planWakeLocked(name)
	if err != nil {
		return DependencyPlan{}, err
	}
	if changed {
		g.generation++
	}
	return DependencyPlan{Generation: g.generation, Actions: actions}, nil
}

// PlanSleep atomically calculates downstream invalidation. The named root is
// transitioned inactive in graph state, but its own sleep callback is not part
// of the cascade plan: callers requested a downstream invalidation plan.
func (g *DependencyGraph) PlanSleep(name string) (DependencyPlan, error) {
	g.planMu.Lock()
	defer g.planMu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.nodes[name]; !ok {
		return DependencyPlan{}, fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, name)
	}
	actions, changed, err := g.planSleepLocked(name)
	if err != nil {
		return DependencyPlan{}, err
	}
	if changed {
		g.generation++
	}
	return DependencyPlan{Generation: g.generation, Actions: actions}, nil
}

// Execute rejects a plan if its generation is no longer current. planMu
// serializes execution against every graph state/topology transition, while
// DependencyGraph.mu remains released for all application callbacks.
func (g *DependencyGraph) Execute(plan DependencyPlan) error {
	g.planMu.Lock()
	defer g.planMu.Unlock()
	g.mu.RLock()
	generation := g.generation
	g.mu.RUnlock()
	if plan.Generation != generation {
		return fmt.Errorf("%w: plan generation=%d graph generation=%d", ErrDependencyPlanStale, plan.Generation, generation)
	}
	if err := g.validatePlanNodes(plan); err != nil {
		return err
	}

	for _, action := range plan.Actions {
		switch action.Action {
		case DependencyActionWake:
			if action.Component != nil {
				action.Component.OnWakeUp(action.Context)
			}
		case DependencyActionSleep:
			if action.Component != nil {
				action.Component.OnSleep()
			}
			if action.Context != nil {
				action.Context.Rollback()
			}
		default:
			return fmt.Errorf("%w: unknown action type %d", ErrDependencyPlanInvalid, action.Action)
		}
	}
	return nil
}

func (g *DependencyGraph) Generation() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.generation
}

func (g *DependencyGraph) validatePlanNodes(plan DependencyPlan) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for i, action := range plan.Actions {
		if action.NodeName == "" {
			return fmt.Errorf("%w: action %d has empty node name", ErrDependencyPlanInvalid, i)
		}
		if action.Action != DependencyActionWake && action.Action != DependencyActionSleep {
			return fmt.Errorf("%w: action %d has unknown type %d", ErrDependencyPlanInvalid, i, action.Action)
		}
		node, ok := g.nodes[action.NodeName]
		if !ok {
			return fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, action.NodeName)
		}
		if node.component != action.Component || node.context != action.Context {
			return fmt.Errorf("%w: action %d does not match current node metadata", ErrDependencyPlanInvalid, i)
		}
	}
	return nil
}

func (g *DependencyGraph) planWakeLocked(name string) ([]DependencyAction, bool, error) {
	if g.nodes[name].active {
		return nil, false, nil
	}

	candidate := make(map[string]struct{})
	queue := []string{name}
	candidate[name] = struct{}{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependent := range sortedDependencyNames(g.nodes[current].dependents) {
			if _, seen := candidate[dependent]; seen {
				continue
			}
			candidate[dependent] = struct{}{}
			queue = append(queue, dependent)
		}
	}

	// Kahn indegree counts every currently-inactive provider, including
	// providers outside candidate. Active providers contribute zero. Thus a
	// blocked parent prevents all descendants from being woken.
	indegree := make(map[string]int, len(candidate))
	for nodeName := range candidate {
		for provider := range g.nodes[nodeName].providers {
			if !g.nodes[provider].active {
				indegree[nodeName]++
			}
		}
	}
	ready := make([]string, 0, len(candidate))
	for nodeName := range candidate {
		if indegree[nodeName] == 0 {
			ready = append(ready, nodeName)
		}
	}
	sort.Strings(ready)

	actions := make([]DependencyAction, 0, len(candidate))
	changed := false
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		node := g.nodes[current]
		if !node.active {
			node.active = true
			changed = true
			actions = append(actions, DependencyAction{
				NodeName: current, Action: DependencyActionWake,
				Component: node.component, Context: node.context,
			})
		}
		for _, dependent := range sortedDependencyNames(node.dependents) {
			if _, included := candidate[dependent]; !included {
				continue
			}
			if indegree[dependent] > 0 {
				indegree[dependent]--
			}
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	return actions, changed, nil
}

func (g *DependencyGraph) planSleepLocked(name string) ([]DependencyAction, bool, error) {
	if !g.nodes[name].active {
		return nil, false, nil
	}

	candidate := make(map[string]struct{})
	queue := []string{name}
	candidate[name] = struct{}{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependent := range sortedDependencyNames(g.nodes[current].dependents) {
			if _, seen := candidate[dependent]; seen {
				continue
			}
			candidate[dependent] = struct{}{}
			queue = append(queue, dependent)
		}
	}

	// Compute the complete invalidation closure first. A dependent becomes
	// invalid exactly when every provider is either inactive or also invalid.
	invalid := map[string]struct{}{name: {}}
	ordered, err := g.topologicalSubsetLocked(candidate)
	if err != nil {
		return nil, false, err
	}
	for _, nodeName := range ordered {
		if nodeName == name {
			continue
		}
		node := g.nodes[nodeName]
		if !node.active {
			continue
		}
		valid := false
		for provider := range node.providers {
			if _, bad := invalid[provider]; bad {
				continue
			}
			if g.nodes[provider].active {
				valid = true
				break
			}
		}
		if len(node.providers) == 0 {
			valid = true
		}
		if !valid {
			invalid[nodeName] = struct{}{}
		}
	}

	// Reverse topological order guarantees dependents sleep before providers.
	actions := make([]DependencyAction, 0, len(invalid))
	for i := len(ordered) - 1; i >= 0; i-- {
		nodeName := ordered[i]
		if _, bad := invalid[nodeName]; !bad {
			continue
		}
		node := g.nodes[nodeName]
		if !node.active {
			continue
		}
		node.active = false
		if nodeName == name {
			continue
		}
		actions = append(actions, DependencyAction{
			NodeName: node.name, Action: DependencyActionSleep,
			Component: node.component, Context: node.context,
		})
	}
	return actions, true, nil
}

func (g *DependencyGraph) topologicalSubsetLocked(subset map[string]struct{}) ([]string, error) {
	indegree := make(map[string]int, len(subset))
	for nodeName := range subset {
		for provider := range g.nodes[nodeName].providers {
			if _, included := subset[provider]; included {
				indegree[nodeName]++
			}
		}
	}
	ready := make([]string, 0, len(subset))
	for nodeName := range subset {
		if indegree[nodeName] == 0 {
			ready = append(ready, nodeName)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(subset))
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		order = append(order, current)
		for _, dependent := range sortedDependencyNames(g.nodes[current].dependents) {
			if _, included := subset[dependent]; !included {
				continue
			}
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	if len(order) != len(subset) {
		return nil, ErrDependencyCycle
	}
	return order, nil
}

func sortedDependencyNames(edges map[string]*dependencyEdge) []string {
	out := make([]string, 0, len(edges))
	for name := range edges {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
