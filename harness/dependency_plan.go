package harness

import (
	"context"
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

type DependencyAction struct {
	NodeName  string
	Action    DependencyActionType
	Component Component
	Context   *SpatioTemporalContext
}

type DependencyPlan struct {
	Generation uint64
	Actions    []DependencyAction
}

type DependencyExecutionOptions struct {
	ParentContext context.Context
	Recovery      RecoveryStrategy
}

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

// PlanSleep includes the requested root's own sleep action followed by every
// downstream node invalidated by the transition, in reverse dependency order.
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

func (g *DependencyGraph) Execute(plan DependencyPlan) error {
	return g.ExecuteWithOptions(plan, DependencyExecutionOptions{})
}

// ExecuteWithOptions is the one lifecycle execution path for both wake and
// sleep plans. The graph mutex is never held while application callbacks,
// context creation, or rollback execute.
func (g *DependencyGraph) ExecuteWithOptions(plan DependencyPlan, options DependencyExecutionOptions) error {
	if options.ParentContext == nil {
		options.ParentContext = context.Background()
	}
	if err := validateDependencyPlan(plan); err != nil {
		return err
	}

	// planMu serializes graph transitions with plan execution. g.mu remains
	// available while callbacks execute, so callbacks can safely inspect graph
	// state without lock inversion.
	g.planMu.Lock()
	defer g.planMu.Unlock()

	for _, action := range plan.Actions {
		g.mu.RLock()
		generation := g.generation
		valid := generation == plan.Generation && g.actionMatchesNodeLocked(action)
		g.mu.RUnlock()
		if !valid {
			return fmt.Errorf("%w: plan generation=%d graph generation=%d", ErrDependencyPlanStale, plan.Generation, generation)
		}

		switch action.Action {
		case DependencyActionWake:
			if err := g.executeWakeAction(options, action); err != nil {
				return err
			}
		case DependencyActionSleep:
			if action.Component != nil {
				action.Component.OnSleep()
			}
			if action.Context != nil {
				action.Context.Rollback()
			}
			g.clearExecutedContext(action)
		}
	}
	return nil
}

func validateDependencyPlan(plan DependencyPlan) error {
	seen := make(map[string]struct{}, len(plan.Actions))
	for _, action := range plan.Actions {
		if action.NodeName == "" || action.Component == nil {
			return fmt.Errorf("%w: action for %q has missing node/component", ErrDependencyPlanInvalid, action.NodeName)
		}
		if action.Action != DependencyActionWake && action.Action != DependencyActionSleep {
			return fmt.Errorf("%w: unknown action type %d", ErrDependencyPlanInvalid, action.Action)
		}
		if _, exists := seen[action.NodeName]; exists {
			return fmt.Errorf("%w: duplicate action for %q", ErrDependencyPlanInvalid, action.NodeName)
		}
		seen[action.NodeName] = struct{}{}
	}
	return nil
}

func (g *DependencyGraph) executeWakeAction(options DependencyExecutionOptions, action DependencyAction) error {
	ctx := action.Context
	if ctx == nil {
		ctx = NewSTContext(options.ParentContext, action.NodeName, options.Recovery)
		g.mu.Lock()
		node, ok := g.nodes[action.NodeName]
		if !ok || !node.active || node.component != action.Component || node.context != nil {
			g.mu.Unlock()
			return fmt.Errorf("%w: wake context materialization changed node %q", ErrDependencyPlanStale, action.NodeName)
		}
		node.context = ctx
		g.mu.Unlock()
	}
	action.Component.OnWakeUp(ctx)
	return nil
}

func (g *DependencyGraph) clearExecutedContext(action DependencyAction) {
	g.mu.Lock()
	defer g.mu.Unlock()
	node, ok := g.nodes[action.NodeName]
	if !ok {
		return
	}
	if node.context == action.Context {
		node.context = nil
	}
}

func (g *DependencyGraph) Generation() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.generation
}

func (g *DependencyGraph) actionMatchesNodeLocked(action DependencyAction) bool {
	if action.NodeName == "" || (action.Action != DependencyActionWake && action.Action != DependencyActionSleep) {
		return false
	}
	node, ok := g.nodes[action.NodeName]
	if !ok || node.component != action.Component {
		return false
	}
	if action.Action == DependencyActionWake {
		return node.active && (action.Context == nil || node.context == action.Context)
	}
	return !node.active && node.context == action.Context
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
			actions = append(actions, DependencyAction{NodeName: current, Action: DependencyActionWake, Component: node.component, Context: node.context})
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
		actions = append(actions, DependencyAction{NodeName: node.name, Action: DependencyActionSleep, Component: node.component, Context: node.context})
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
