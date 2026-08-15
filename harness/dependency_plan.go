package harness

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// DependencyActionType identifies a lifecycle transition represented by a
// dependency execution plan.
type DependencyActionType uint8

const (
	DependencyActionWake DependencyActionType = iota + 1
	DependencyActionSleep
)

var (
	ErrDependencyPlanStale      = errors.New("dependency plan is stale")
	ErrDependencyPlanInvalid    = errors.New("invalid dependency plan")
	ErrDependencyPlanNodeAbsent = errors.New("dependency plan references missing node")
)

// DependencyAction is an immutable description of one lifecycle transition.
// The graph owns neither the action nor its backing storage after plan
// construction. Component and Context are references to application-owned
// objects and are never invoked while DependencyGraph.mu is held.
type DependencyAction struct {
	NodeName  string
	Action    DependencyActionType
	Component Component
	Context   *SpatioTemporalContext
}

// DependencyPlan is an immutable execution plan produced from one graph
// generation. Actions owns its backing slice; callers may not observe or
// mutate graph-internal action storage.
type DependencyPlan struct {
	Generation uint64
	Actions    []DependencyAction
}

// PlanWake creates and commits a deterministic wake plan rooted at name.
//
// Eligibility is evaluated against the graph state while the graph lock is
// held. Nodes that cannot yet satisfy all providers remain inactive and are
// not included in the plan. The resulting plan is immutable and must be
// executed separately after this method returns.
func (g *DependencyGraph) PlanWake(name string) (DependencyPlan, error) {
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

// PlanSleep creates and commits a deterministic sleep plan for the specified
// node. The root itself is not emitted as a sleep action unless it is active;
// active downstream dependents are invalidated in reverse dependency order.
func (g *DependencyGraph) PlanSleep(name string) (DependencyPlan, error) {
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

// Execute executes an immutable plan without taking DependencyGraph.mu.
//
// A plan is accepted only if its generation still matches the graph. This
// prevents a caller from executing actions calculated against a state that has
// since changed. A rejected plan has no side effects.
func (g *DependencyGraph) Execute(plan DependencyPlan) error {
	g.mu.RLock()
	generation := g.generation
	g.mu.RUnlock()

	if plan.Generation != generation {
		return fmt.Errorf("%w: plan generation=%d graph generation=%d", ErrDependencyPlanStale, plan.Generation, generation)
	}

	if err := validateDependencyPlan(plan); err != nil {
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

// ExecuteLatest executes only when the plan still describes the current graph
// generation. It is intentionally an alias-like helper kept small so Phase 3
// can choose between explicit rejection and another reconciliation strategy.
func (g *DependencyGraph) ExecuteLatest(plan DependencyPlan) error {
	return g.Execute(plan)
}

// Generation returns the current graph state generation.
func (g *DependencyGraph) Generation() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.generation
}

func validateDependencyPlan(plan DependencyPlan) error {
	for i, action := range plan.Actions {
		if action.NodeName == "" {
			return fmt.Errorf("%w: action %d has empty node name", ErrDependencyPlanInvalid, i)
		}
		switch action.Action {
		case DependencyActionWake, DependencyActionSleep:
		default:
			return fmt.Errorf("%w: action %d has unknown type %d", ErrDependencyPlanInvalid, i, action.Action)
		}
	}
	return nil
}

func (g *DependencyGraph) planWakeLocked(name string) ([]DependencyAction, bool, error) {
	root := g.nodes[name]
	if root.active {
		return nil, false, nil
	}

	candidate := make(map[string]struct{})
	queue := []string{name}
	candidate[name] = struct{}{}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		dependents := sortedDependencyNames(g.nodes[current].dependents)
		for _, dependent := range dependents {
			if _, seen := candidate[dependent]; seen {
				continue
			}
			candidate[dependent] = struct{}{}
			queue = append(queue, dependent)
		}
	}

	indegree := make(map[string]int, len(candidate))
	for nodeName := range candidate {
		indegree[nodeName] = 0
	}
	for nodeName := range candidate {
		for provider := range g.nodes[nodeName].providers {
			if _, included := candidate[provider]; included {
				indegree[nodeName]++
			}
		}
	}

	ready := make([]string, 0, len(candidate))
	for nodeName, degree := range indegree {
		if degree == 0 {
			ready = append(ready, nodeName)
		}
	}
	sort.Strings(ready)

	ordered := make([]string, 0, len(candidate))
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		ordered = append(ordered, current)

		for _, dependent := range sortedDependencyNames(g.nodes[current].dependents) {
			if _, included := candidate[dependent]; !included {
				continue
			}
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}

	if len(ordered) != len(candidate) {
		return nil, false, ErrDependencyCycle
	}

	actions := make([]DependencyAction, 0, len(ordered))
	changed := false
	for _, nodeName := range ordered {
		node := g.nodes[nodeName]
		if node.active {
			continue
		}

		eligible := true
		for provider := range node.providers {
			if _, included := candidate[provider]; included {
				continue
			}
			if !g.nodes[provider].active {
				eligible = false
				break
			}
		}
		if !eligible {
			continue
		}

		node.active = true
		changed = true
		actions = append(actions, DependencyAction{
			NodeName:  node.name,
			Action:    DependencyActionWake,
			Component: node.component,
			Context:   node.context,
		})
	}

	return actions, changed, nil
}

func (g *DependencyGraph) planSleepLocked(name string) ([]DependencyAction, bool, error) {
	root := g.nodes[name]
	if !root.active {
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

	// A dependent only needs to be invalidated if, after the root deactivation,
	// no remaining active provider satisfies it. Evaluate from the leaves back
	// toward the root so a multi-parent diamond invalidates a shared node once.
	ordered, err := g.reverseTopologicalSubsetLocked(candidate)
	if err != nil {
		return nil, false, err
	}

	actions := make([]DependencyAction, 0, len(candidate))
	changed := false
	for _, nodeName := range ordered {
		node := g.nodes[nodeName]
		if !node.active {
			continue
		}

		valid := true
		for provider := range node.providers {
			if _, included := candidate[provider]; included {
				if !g.nodes[provider].active {
					valid = false
					break
				}
				continue
			}
			if !g.nodes[provider].active {
				valid = false
				break
			}
		}

		if nodeName == name || !valid {
			node.active = false
			changed = true
			actions = append(actions, DependencyAction{
				NodeName:  node.name,
				Action:    DependencyActionSleep,
				Component: node.component,
				Context:   node.context,
			})
		}
	}

	return actions, changed, nil
}

func (g *DependencyGraph) reverseTopologicalSubsetLocked(subset map[string]struct{}) ([]string, error) {
	indegree := make(map[string]int, len(subset))
	for nodeName := range subset {
		indegree[nodeName] = 0
	}
	for nodeName := range subset {
		for provider := range g.nodes[nodeName].providers {
			if _, included := subset[provider]; included {
				indegree[nodeName]++
			}
		}
	}

	ready := make([]string, 0, len(subset))
	for nodeName, degree := range indegree {
		if degree == 0 {
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

	for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
		order[i], order[j] = order[j], order[i]
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

// Keep sync imported by this file's tests and future plan executors without
// forcing those tests to use a separate helper package.
var _ sync.Locker
