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
	mu          sync.Mutex
	services    map[string]bool
	components  map[string]Component
	activeCtxs  map[string]*SpatioTemporalContext
	onRecovery  RecoveryStrategy
}

func NewDependencyTracker(recovery RecoveryStrategy) *DependencyTracker {
	return &DependencyTracker{
		services:   make(map[string]bool),
		components: make(map[string]Component),
		activeCtxs: make(map[string]*SpatioTemporalContext),
		onRecovery: recovery,
	}
}

func (dt *DependencyTracker) GetActiveContext(name string) *SpatioTemporalContext {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	return dt.activeCtxs[name]
}

func (dt *DependencyTracker) RegisterComponent(parentCtx context.Context, comp Component) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.components[comp.Name()] = comp
	dt.evaluateComponentState(parentCtx, comp)
}

func (dt *DependencyTracker) ActivateService(parentCtx context.Context, serviceName string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	fmt.Printf("\n📡 [Dependency Engine]: Service Registry altered -> '%s' is now ONLINE\n", serviceName)
	dt.services[serviceName] = true

	for _, comp := range dt.components {
		dt.evaluateComponentState(parentCtx, comp)
	}
}

func (dt *DependencyTracker) DeactivateService(serviceName string) {
	dt.mu.Lock()

	var contextsToRollback []*SpatioTemporalContext
	var componentsToSleep []Component

	fmt.Printf("\n🛑 [Dependency Engine]: Service Registry altered -> '%s' went OFFLINE\n", serviceName)
	dt.services[serviceName] = false

	for _, comp := range dt.components {
		for _, req := range comp.Inject() {
			if req == serviceName {
				if stCtx, active := dt.activeCtxs[comp.Name()]; active {
					componentsToSleep = append(componentsToSleep, comp)
					contextsToRollback = append(contextsToRollback, stCtx)
					delete(dt.activeCtxs, comp.Name())
				}
			}
		}
	}
	dt.mu.Unlock() 

	for i, comp := range componentsToSleep {
		fmt.Printf("[Tracker]: Dependency severed for '%s'. Transitioning to SLEEP...\n", comp.Name())
		comp.OnSleep()
		contextsToRollback[i].Rollback()
	}
}

func (dt *DependencyTracker) evaluateComponentState(parentCtx context.Context, comp Component) {
	if _, active := dt.activeCtxs[comp.Name()]; active {
		return
	}

	for _, req := range comp.Inject() {
		if !dt.services[req] {
			fmt.Printf("[Tracker]: Component '%s' remains ASLEEP. Missing required coeffect: '%s'\n", comp.Name(), req)
			return
		}
	}

	fmt.Printf("[Tracker]: Spatial coeffects aligned for '%s'. Transitioning to WAKE UP...\n", comp.Name())
	stCtx := NewSTContext(parentCtx, comp.Name(), dt.onRecovery)
	dt.activeCtxs[comp.Name()] = stCtx
	
	go comp.OnWakeUp(stCtx)
}
