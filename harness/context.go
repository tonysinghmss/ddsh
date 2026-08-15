package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Effect func()

type RecoveryStrategy func(
	ctx *SpatioTemporalContext,
	failedName string,
)

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

	// State snapshot subsystem.
	//
	// snapshotProvider is only a function reference. The provider itself is
	// never called while mu is held.
	snapshotProvider SnapshotProvider

	// lastGoodState is always owned by this context.
	//
	// All reads and writes are protected by mu and values crossing the
	// context boundary are deep-copied.
	lastGoodState StatePayload
	hasSnapshot  bool

	// snapshotVersion monotonically increases whenever a new checkpoint is
	// committed through UpdateStateSnapshot.
	snapshotVersion uint64
}

func NewSTContext(
	parent context.Context,
	name string,
	fallback RecoveryStrategy,
) *SpatioTemporalContext {
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

// ParentST returns the structural parent of this context.
//
// The returned pointer is safe to use as a structural reference. It does not
// imply ownership of the parent's mutex.
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

// RegisterStateSnapshot installs a provider capable of materializing the
// component's current operational state. The provider is not executed here.
func (c *SpatioTemporalContext) RegisterStateSnapshot(provider SnapshotProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshotProvider = provider
}

// UpdateStateSnapshot commits a known-good state checkpoint and copies the
// supplied bytes before returning.
func (c *SpatioTemporalContext) UpdateStateSnapshot(data []byte) {
	copied := append([]byte(nil), data...)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshotVersion++
	c.lastGoodState = StatePayload{Version: c.snapshotVersion, Captured: nowUTC(), Data: copied}
	c.hasSnapshot = true
}

// CaptureStateSnapshot asks the registered provider to materialize current
// state. The provider executes without c.mu held.
func (c *SpatioTemporalContext) CaptureStateSnapshot() error {
	c.mu.Lock()
	provider := c.snapshotProvider
	c.mu.Unlock()
	if provider == nil {
		return ErrNoStateSnapshot
	}
	data, err := provider()
	if err != nil {
		return fmt.Errorf("capture state for %q: %w", c.name, err)
	}
	c.UpdateStateSnapshot(data)
	return nil
}

// StateSnapshot returns an independent copy of the latest known-good state.
func (c *SpatioTemporalContext) StateSnapshot() (StatePayload, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasSnapshot {
		return StatePayload{}, ErrNoStateSnapshot
	}
	return c.lastGoodState.clone(), nil
}

// RestoreStateSnapshot installs a previously captured state payload. Older
// payloads never replace a newer local checkpoint.
func (c *SpatioTemporalContext) RestoreStateSnapshot(payload StatePayload) {
	copied := payload.clone()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hasSnapshot && copied.Version < c.snapshotVersion {
		return
	}
	c.lastGoodState = copied
	c.hasSnapshot = true
	if copied.Version > c.snapshotVersion {
		c.snapshotVersion = copied.Version
	}
}

// TransferStateTo copies the latest known-good state into target. Source and
// destination retain independent byte-array ownership.
func (c *SpatioTemporalContext) TransferStateTo(target *SpatioTemporalContext) error {
	if target == nil {
		return errors.New("target context is nil")
	}
	payload, err := c.StateSnapshot()
	if err != nil {
		return err
	}
	target.RestoreStateSnapshot(payload)
	return nil
}

func (c *SpatioTemporalContext) RaiseError(err error) {
	if err == nil {
		return
	}
	select {
	case c.errChan <- err:
	default:
	}
}

func (c *SpatioTemporalContext) supervise() {
	select {
	case err := <-c.errChan:
		fmt.Printf("\n⚠️  [Fault Intercepted] Module '%s' threw an async error: %v\n", c.name, err)

		// An explicit checkpoint is already the framework's last-known-good
		// boundary. Do not replace it with a provider result that may observe
		// partially mutated live state during asynchronous failure.
		c.mu.Lock()
		hasCheckpoint := c.hasSnapshot
		c.mu.Unlock()
		if !hasCheckpoint {
			if snapshotErr := c.CaptureStateSnapshot(); snapshotErr != nil && !errors.Is(snapshotErr, ErrNoStateSnapshot) {
				fmt.Printf("[Snapshot] Failed to capture state for '%s': %v\n", c.name, snapshotErr)
			}
		}

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

func nowUTC() time.Time { return time.Now().UTC() }
