package harness

import "fmt"

// executePlanLocked executes an immutable plan while planMu is held by the
// caller. graph.mu is acquired only for short validation/state publication
// windows; component and context callbacks always run after it is released.
func (g *DependencyGraph) executePlanLocked(plan DependencyPlan, o DependencyExecutionOptions) error {
	if err := validateDependencyPlan(plan); err != nil {
		return err
	}

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
			if err := g.executeWakeAction(o, action); err != nil {
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
