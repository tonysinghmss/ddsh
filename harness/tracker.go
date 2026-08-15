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

func (dt *DependencyTracker) Graph() *DependencyGraph { return dt.graph }

func (dt *DependencyTracker) GetActiveContext(name string) *SpatioTemporalContext {
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

	if err := dt.graph.registerComponentAtomic(comp.Name(), comp, append([]string(nil), comp.Inject()...)); err != nil {
		dt.recordError(err)
		return err
	}
	plan, err := dt.graph.PlanWake(comp.Name())
	if err != nil {
		dt.recordError(err)
		return err
	}
	if err := dt.graph.ExecuteWithOptions(plan, DependencyExecutionOptions{ParentContext: parentCtx, Recovery: dt.onRecovery}); err != nil {
		dt.recordError(err)
		return err
	}
	return nil
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
	plan, err := dt.graph.PlanWake(serviceName)
	if err != nil {
		dt.recordError(err)
		return err
	}
	if err := dt.graph.ExecuteWithOptions(plan, DependencyExecutionOptions{ParentContext: parentCtx, Recovery: dt.onRecovery}); err != nil {
		dt.recordError(err)
		return err
	}
	return nil
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
	plan, err := dt.graph.PlanSleep(serviceName)
	if err != nil {
		dt.recordError(err)
		return err
	}
	if err := dt.graph.ExecuteWithOptions(plan, DependencyExecutionOptions{ParentContext: context.Background(), Recovery: dt.onRecovery}); err != nil {
		dt.recordError(err)
		return err
	}
	return nil
}

func (dt *DependencyTracker) clearError() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.lastErr = nil
}

func (dt *DependencyTracker) LastError() error {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	return dt.lastErr
}

// LastErrorIfCurrent remains as a compatibility helper. Plan execution now
// performs generation validation itself.
func (dt *DependencyTracker) LastErrorIfCurrent(generation uint64) error {
	if dt.graph.Generation() != generation {
		return nil
	}
	return dt.LastError()
}

func (dt *DependencyTracker) recordError(err error) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.lastErr = err
}
