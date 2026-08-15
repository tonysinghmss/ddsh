package harness

import "context"

// TransitionWake plans and executes a wake transition under the graph
// transition serializer. Application callbacks execute without graph or
// tracker locks held.
func (g *DependencyGraph) TransitionWake(name string, o DependencyExecutionOptions) error {
	if o.ParentContext == nil {
		o.ParentContext = context.Background()
	}
	g.planMu.Lock()
	defer g.planMu.Unlock()

	g.mu.Lock()
	if _, ok := g.nodes[name]; !ok {
		g.mu.Unlock()
		return wrapNodeNotFound(name)
	}
	actions, changed, err := g.planWakeLocked(name)
	if err != nil {
		g.mu.Unlock()
		return err
	}
	if changed {
		g.generation++
	}
	plan := DependencyPlan{Generation: g.generation, Actions: actions}
	g.mu.Unlock()

	return g.executePlanLocked(plan, o)
}

// TransitionSleep plans and executes a sleep transition under the graph
// transition serializer. Application callbacks execute without graph or
// tracker locks held.
func (g *DependencyGraph) TransitionSleep(name string, o DependencyExecutionOptions) error {
	if o.ParentContext == nil {
		o.ParentContext = context.Background()
	}
	g.planMu.Lock()
	defer g.planMu.Unlock()

	g.mu.Lock()
	if _, ok := g.nodes[name]; !ok {
		g.mu.Unlock()
		return wrapNodeNotFound(name)
	}
	actions, changed, err := g.planSleepLocked(name)
	if err != nil {
		g.mu.Unlock()
		return err
	}
	if changed {
		g.generation++
	}
	plan := DependencyPlan{Generation: g.generation, Actions: actions}
	g.mu.Unlock()

	return g.executePlanLocked(plan, o)
}

func wrapNodeNotFound(name string) error {
	return &dependencyNodeNotFoundError{name: name}
}

type dependencyNodeNotFoundError struct{ name string }

func (e *dependencyNodeNotFoundError) Error() string { return "dependency node not found: " + e.name }
func (e *dependencyNodeNotFoundError) Unwrap() error { return ErrDependencyNodeNotFound }
