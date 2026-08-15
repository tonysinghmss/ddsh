package harness

import (
	"fmt"
	"sort"
)

// replaceNodeRuntime atomically changes the runtime instance behind a logical
// node. The logical node name and all dependency edges are preserved.
//
// The failed node is made unavailable only inside the graph transition. Any
// active descendants that become invalid are marked asleep and returned as
// sleep actions. The replacement is then made active and newly eligible
// descendants are returned as wake actions. Callbacks execute later through
// DependencyGraph.ExecuteWithOptions, never while graph locks are held.
func (g *DependencyGraph) replaceNodeRuntime(name string, component Component, ctx *SpatioTemporalContext) (DependencyPlan, error) {
	if component == nil || ctx == nil {
		return DependencyPlan{}, fmt.Errorf("%w: nil replacement runtime", ErrReplacementUnavailable)
	}

	g.planMu.Lock()
	defer g.planMu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()

	node, ok := g.nodes[name]
	if !ok {
		return DependencyPlan{}, fmt.Errorf("%w: %q", ErrDependencyNodeNotFound, name)
	}

	oldComponent := node.component
	oldContext := node.context
	wasActive := node.active
	if oldComponent == component && oldContext == ctx {
		return DependencyPlan{Generation: g.generation}, nil
	}

	node.active = false
	node.component = component
	node.context = ctx

	actions := make([]DependencyAction, 0)
	if wasActive {
		invalid := g.invalidDescendantsLocked(name)
		ordered, err := g.topologicalSubsetLocked(invalid)
		if err != nil {
			node.component = oldComponent
			node.context = oldContext
			node.active = wasActive
			return DependencyPlan{}, err
		}

		for i := len(ordered) - 1; i >= 0; i-- {
			descendant := ordered[i]
			if descendant == name {
				continue
			}
			d := g.nodes[descendant]
			if !d.active {
				continue
			}
			d.active = false
			actions = append(actions, DependencyAction{
				NodeName:  descendant,
				Action:    DependencyActionSleep,
				Component: d.component,
				Context:   d.context,
			})
		}
	}

	node.active = true
	actions = append(actions, DependencyAction{
		NodeName:  name,
		Action:    DependencyActionWake,
		Component: component,
		Context:   ctx,
	})

	wakeActions := g.planEligibleDependentsLocked(name)
	actions = append(actions, wakeActions...)

	g.generation++
	return DependencyPlan{Generation: g.generation, Actions: actions}, nil
}

func (g *DependencyGraph) invalidDescendantsLocked(name string) map[string]struct{} {
	candidate := map[string]struct{}{name: {}}
	queue := []string{name}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependent := range sortedDependencyNames(g.nodes[current].dependents) {
			if _, exists := candidate[dependent]; exists {
				continue
			}
			candidate[dependent] = struct{}{}
			queue = append(queue, dependent)
		}
	}

	invalid := map[string]struct{}{name: {}}
	ordered, _ := g.topologicalSubsetLocked(candidate)
	for _, current := range ordered {
		if current == name {
			continue
		}
		node := g.nodes[current]
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
			invalid[current] = struct{}{}
		}
	}
	return invalid
}

func (g *DependencyGraph) planEligibleDependentsLocked(name string) []DependencyAction {
	candidate := make(map[string]struct{})
	queue := []string{name}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependent := range sortedDependencyNames(g.nodes[current].dependents) {
			if _, exists := candidate[dependent]; exists {
				continue
			}
			candidate[dependent] = struct{}{}
			queue = append(queue, dependent)
		}
	}

	indegree := make(map[string]int, len(candidate))
	for current := range candidate {
		for provider := range g.nodes[current].providers {
			if _, inside := candidate[provider]; inside && !g.nodes[provider].active {
				indegree[current]++
			}
		}
	}

	ready := make([]string, 0, len(candidate))
	for current := range candidate {
		if g.nodes[current].active {
			continue
		}
		if indegree[current] == 0 {
			ready = append(ready, current)
		}
	}
	sort.Strings(ready)

	actions := make([]DependencyAction, 0, len(candidate))
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		node := g.nodes[current]
		if !node.active {
			node.active = true
			actions = append(actions, DependencyAction{
				NodeName:  current,
				Action:    DependencyActionWake,
				Component: node.component,
				Context:   node.context,
			})
		}
		for _, dependent := range sortedDependencyNames(node.dependents) {
			if _, inside := candidate[dependent]; !inside || g.nodes[dependent].active {
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
	return actions
}
