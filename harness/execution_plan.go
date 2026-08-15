package harness

import "fmt"

type ExecutionKind uint8

const (
	ExecutionWake ExecutionKind = iota + 1
	ExecutionSleep
)

// ExecutionStep is immutable after plan construction.
type ExecutionStep struct {
	Name      string
	Component Component
	Context   *SpatioTemporalContext
}

// DependencyPlan is an immutable lifecycle transition calculated from a graph
// snapshot. It contains only the nodes that are expected to transition.
type DependencyPlan struct {
	kind       ExecutionKind
	generation uint64
	steps      []ExecutionStep
}

func (p DependencyPlan) Kind() ExecutionKind { return p.kind }
func (p DependencyPlan) Generation() uint64   { return p.generation }
func (p DependencyPlan) Steps() []ExecutionStep {
	return append([]ExecutionStep(nil), p.steps...)
}

func (p DependencyPlan) validate() error {
	if p.kind != ExecutionWake && p.kind != ExecutionSleep {
		return fmt.Errorf("invalid execution kind %d", p.kind)
	}
	return nil
}
