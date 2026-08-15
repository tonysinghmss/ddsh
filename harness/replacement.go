package harness

import "sort"

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
				NodeName: current,
				Action: DependencyActionWake,
				Component: node.component,
				// A node that was slept has its old context cleared by the
				// sleep action. nil asks the executor to materialize a new one.
				Context: nil,
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
