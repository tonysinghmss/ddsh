package harness

import (
	"context"
	"fmt"
	"sync"
)

type Component interface {
	Name() string
	Inject() []string
	OnWakeUp(ctx *SpatioTemporalContext)
	OnSleep()
}

type DependencyTracker struct {
	mu           sync.Mutex
	transitionMu sync.Mutex
	graph        *DependencyGraph
	onRecovery   RecoveryStrategy
	lastErr      error
}

func NewDependencyTracker(recovery RecoveryStrategy) *DependencyTracker {
	return &DependencyTracker{graph: NewDependencyGraph(), onRecovery: recovery}
}

func (dt *DependencyTracker) Graph() *DependencyGraph {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	return dt.graph
}

func (dt *DependencyTracker) GetActiveContext(name string) *SpatioTemporalContext {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	if node, ok := dt.graph.Node(name); ok && node.Active {
		return node.Context
	}
	return nil
}

func (dt *DependencyTracker) RegisterComponent(parentCtx context.Context, comp Component) {
	if err := dt.RegisterComponentErr(parentCtx, comp); err != nil {
		dt.recordError(err)
		if comp != nil {
			fmt.Printf("[Tracker]: failed to register component '%s': %v\n", comp.Name(), err)
		}
	}
}

func (dt *DependencyTracker) RegisterComponentErr(parentCtx context.Context, comp Component) error {
	if comp == nil {
		return fmt.Errorf("component is nil")
	}
	dt.transitionMu.Lock()
	defer dt.transitionMu.Unlock()
	dt.clearError()
	plan, err := dt.registerAndPlan(parentCtx, comp)
	if err != nil {
		dt.recordError(err)
		return err
	}
	dt.executeDependencyPlan(parentCtx, plan)
	return dt.LastErrorIfCurrent(plan.generation)
}

func (dt *DependencyTracker) ActivateService(parentCtx context.Context, serviceName string) {
	if err := dt.ActivateServiceErr(parentCtx, serviceName); err != nil {
		dt.recordError(err)
		fmt.Printf("[Tracker]: failed to activate service '%s': %v\n", serviceName, err)
	}
}

func (dt *DependencyTracker) ActivateServiceErr(parentCtx context.Context, serviceName string) error {
	dt.transitionMu.Lock()
	defer dt.transitionMu.Unlock()
	dt.clearError()
	fmt.Printf("\n📡 [Dependency Engine]: Service Registry altered -> '%s' is now ONLINE\n", serviceName)
	plan, err := dt.graph.buildServiceTransition(serviceName, true)
	if err != nil {
		dt.recordError(err)
		return err
	}
	dt.executeDependencyPlan(parentCtx, plan)
	return dt.LastErrorIfCurrent(plan.generation)
}

func (dt *DependencyTracker) DeactivateService(serviceName string) {
	if err := dt.DeactivateServiceErr(serviceName); err != nil {
		dt.recordError(err)
		fmt.Printf("[Tracker]: failed to deactivate service '%s': %v\n", serviceName, err)
	}
}

func (dt *DependencyTracker) DeactivateServiceErr(serviceName string) error {
	dt.transitionMu.Lock()
	defer dt.transitionMu.Unlock()
	dt.clearError()
	fmt.Printf("\n🛑 [Dependency Engine]: Service Registry altered -> '%s' went OFFLINE\n", serviceName)
	plan, err := dt.graph.buildServiceTransition(serviceName, false)
	if err != nil {
		dt.recordError(err)
		return err
	}
	dt.executeDependencyPlan(context.Background(), plan)
	return dt.LastErrorIfCurrent(plan.generation)
}

func (dt *DependencyTracker) clearError() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.lastErr = nil
}

func (dt *DependencyTracker) LastErrorIfCurrent(generation uint64) error {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	if dt.graph.Generation() != generation {
		return nil
	}
	return dt.lastErr
}
