package harness

import "fmt"

// replaceNodeRuntimeUnlocked performs the runtime swap while planMu is owned
// by the caller. It only holds graph metadata locks while publishing state.
func (g *DependencyGraph) replaceNodeRuntimeUnlocked(name string, component Component, ctx *SpatioTemporalContext) (DependencyPlan, error) {
	if component == nil || ctx == nil {
		return DependencyPlan{}, fmt.Errorf("%w: nil replacement runtime", ErrReplacementUnavailable)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	node, ok := g.nodes[name]
	if !ok {
		return DependencyPlan{}, fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, name)
	}
	oldComponent, oldContext, wasActive := node.component, node.context, node.active
	node.active = false
	node.component = component
	node.context = ctx

	actions := make([]DependencyAction, 0)
	if wasActive {
		invalid := g.invalidDescendantsLocked(name)
		ordered, err := g.topologicalSubsetLocked(invalid)
		if err != nil {
			node.component, node.context, node.active = oldComponent, oldContext, wasActive
			return DependencyPlan{}, err
		}
		for i := len(ordered) - 1; i >= 0; i-- {
			current := ordered[i]
			if current == name || !g.nodes[current].active {
				continue
			}
			d := g.nodes[current]
			d.active = false
			actions = append(actions, DependencyAction{
			NodeName: current,
			Action: DependencyActionSleep,
			Component: d.component,
			Context: d.context,
		})
		}
	}

	node.active = true
	actions = append(actions, DependencyAction{
		NodeName: name,
		Action: DependencyActionWake,
		Component: component,
		Context: ctx,
	})
	actions = append(actions, g.planEligibleDependentsLocked(name)...)
	g.generation++
	return DependencyPlan{Generation: g.generation, Actions: actions}, nil
}
