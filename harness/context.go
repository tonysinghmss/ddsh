package harness

import (
	"context"
	"fmt"
	"sync"
)

type Effect func()
type RecoveryStrategy func(ctx *SpatioTemporalContext, failedName string)

type SpatioTemporalContext struct {
	context.Context
	mu        sync.Mutex
	name      string
	effects   []Effect
	children  []*SpatioTemporalContext
	parent    *SpatioTemporalContext
	errChan   chan error
	doneChan  chan struct{}
	onFailure RecoveryStrategy
	isRolling bool
}

func NewSTContext(parent context.Context, name string, fallback RecoveryStrategy) *SpatioTemporalContext {
	ctx := &SpatioTemporalContext{
		Context:   parent,
		name:      name,
		effects:   make([]Effect, 0),
		children:  make([]*SpatioTemporalContext, 0),
		errChan:   make(chan error, 1),
		doneChan:  make(chan struct{}),
		onFailure: fallback,
	}

	if pCtx, ok := parent.(*SpatioTemporalContext); ok {
		ctx.parent = pCtx
		pCtx.addChild(ctx)
	}

	go ctx.supervise()
	return ctx
}

func (c *SpatioTemporalContext) ParentST() *SpatioTemporalContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.parent
}

func (c *SpatioTemporalContext) addChild(child *SpatioTemporalContext) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.children = append(c.children, child)
}

func (c *SpatioTemporalContext) RegisterEffect(effectName string, cleanup Effect) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.effects = append([]Effect{cleanup}, c.effects...)
	fmt.Printf("[Effect Registered] %s -> Tracked teardown for '%s'\n", c.name, effectName)
}

func (c *SpatioTemporalContext) RaiseError(err error) {
	select {
	case c.errChan <- err:
	default:
	}
}

func (c *SpatioTemporalContext) supervise() {
	select {
	case err := <-c.errChan:
		fmt.Printf("\n⚠️  [Fault Intercepted] Module '%s' threw an async error: %v\n", c.name, err)
		c.Rollback()
		
		if c.onFailure != nil {
			fmt.Printf("🔄 [Self-Healing] Activating substitution loop for '%s'...\n", c.name)
			c.onFailure(c, c.name)
		}
	case <-c.doneChan:
		return
	}
}

func (c *SpatioTemporalContext) Rollback() {
	c.mu.Lock()
	if c.isRolling {
		c.mu.Unlock()
		return
	}
	c.isRolling = true
	
	select {
	case <-c.doneChan:
	default:
		close(c.doneChan)
	}

	childrenToRollback := c.children
	c.children = nil

	effectsToUnwind := c.effects
	c.effects = nil
	
	parentToClean := c.parent
	c.mu.Unlock() 

	if len(childrenToRollback) > 0 {
		fmt.Printf("[Cascade] '%s' propagating rollback downward to %d active children...\n", c.name, len(childrenToRollback))
		for _, child := range childrenToRollback {
			child.Rollback()
		}
	}

	if len(effectsToUnwind) > 0 {
		fmt.Printf("--- Unwinding %d Effects for [%s] concurrently ---\n", len(effectsToUnwind), c.name)
		
		const maxWorkers = 2 
		effectChan := make(chan Effect, len(effectsToUnwind))
		var wg sync.WaitGroup

		for _, effect := range effectsToUnwind {
			effectChan <- effect
		}
		close(effectChan)

		for i := 0; i < maxWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for work := range effectChan {
					work()
				}
			}()
		}
		wg.Wait()
	}

	if parentToClean != nil {
		parentToClean.removeChild(c)
	}
}

func (c *SpatioTemporalContext) removeChild(target *SpatioTemporalContext) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, child := range c.children {
		if child == target {
			c.children = append(c.children[:i], c.children[i+1:]...)
			break
		}
	}
}
