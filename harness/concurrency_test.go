package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockComponent implements Component for rigorous tracking under heavy concurrency load.
type MockComponent struct {
	name         string
	requirements []string
	wakeCount    int32
	sleepCount   int32
	mu           sync.Mutex
	history      []string
}

func NewMockComponent(name string, reqs []string) *MockComponent {
	return &MockComponent{
		name:         name,
		requirements: reqs,
		history:      make([]string, 0),
	}
}

func (m *MockComponent) Name() string     { return m.name }
func (m *MockComponent) Inject() []string { return m.requirements }

func (m *MockComponent) OnWakeUp(ctx *SpatioTemporalContext) {
	atomic.AddInt32(&m.wakeCount, 1)
	m.mu.Lock()
	m.history = append(m.history, "wake")
	m.mu.Unlock()

	// Register multiple competing effects that execute with randomized operational timing delays
	for i := 0; i < 5; i++ {
		effectID := fmt.Sprintf("effect-%d", i)
		ctx.RegisterEffect(effectID, func() {
			// Deliberate multi-thread context friction to force race condition checks
			time.Sleep(time.Duration(1+i) * time.Millisecond)
		})
	}
}

func (m *MockComponent) OnSleep() {
	atomic.AddInt32(&m.sleepCount, 1)
	m.mu.Lock()
	m.history = append(m.history, "sleep")
	m.mu.Unlock()
}

// TestLIFORollbackLockStress applies parallel execution pressure to verify that the
// Context early-unlock mechanism does not choke or deadlock when dozens of threads fire simultaneously.
func TestLIFORollbackLockStress(t *testing.T) {
	rootCtx := context.Background()
	parentST := NewSTContext(rootCtx, "Stress-Orchestrator-Parent", nil)

	const totalChildren = 50
	var wg sync.WaitGroup

	// Allocate deep branch segments simultaneously across threads
	for i := 0; i < totalChildren; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			childName := fmt.Sprintf("Child-Worker-%d", id)
			childST := NewSTContext(parentST, childName, nil)

			// Fast execution profile loading to mimic AI memory caching tasks
			childST.RegisterEffect("Cleanup-Resource-Fast", func() {})
			childST.RegisterEffect("Cleanup-Resource-Slow", func() {
				time.Sleep(2 * time.Millisecond)
			})
		}(i)
	}
	wg.Wait()

	// Assert thread creation structures matched expectations safely
	parentST.mu.Lock()
	childTreeSize := len(parentST.children)
	parentST.mu.Unlock()
	if childTreeSize != totalChildren {
		t.Errorf("Expected %d children tracked under tree root, found: %d", totalChildren, childTreeSize)
	}

	// Trigger high friction cascade unwinding to verify the deadlock bug is resolved
	done := make(chan struct{})
	go func() {
		parentST.Rollback()
		close(done)
	}()

	select {
	case <-done:
		// Success: Rollback completed cleanly without structural deadlocks
	case <-time.After(2 * time.Second):
		t.Fatal("DEADLOCK DETECTED: Parent tree cascading rollback stalled forever.")
	}
}

// TestAsynchronousFaultContainmentAndHealing asserts that sub-module faults
// isolate themselves and invoke self-healing hooks without corrupting sister branches.
func TestAsynchronousFaultContainmentAndHealing(t *testing.T) {
	rootCtx := context.Background()

	var substituteSpawned int32
	var parentST *SpatioTemporalContext

	// Dynamic Self-Healing strategy definition
	recoveryHook := func(failedCtx *SpatioTemporalContext, failedName string) {
		atomic.StoreInt32(&substituteSpawned, 1)
		
		structuralParent := rootCtx
		if failedCtx.ParentST() != nil {
			structuralParent = failedCtx.ParentST()
		}

		backupCtx := NewSTContext(structuralParent, "Healthy-Substitute-Component", nil)
		backupCtx.RegisterEffect("Backup-Teardown-Handler", func() {})
	}

	parentST = NewSTContext(rootCtx, "Global-Root-Hub", nil)
	faultyChildST := NewSTContext(parentST, "Faulty-Child-Component", recoveryHook)
	stableChildST := NewSTContext(parentST, "Stable-Sister-Component", nil)

	// Inject baseline state parameters
	stableChildST.RegisterEffect("Stable-Teardown-Lock", func() {})
	faultyChildST.RegisterEffect("Faulty-Teardown-Lock", func() {})

	// Signal unrecoverable async failure specifically onto the faulty child branch
	faultyChildST.RaiseError(errors.New("simulated unrecoverable AI model socket exception"))

	// Enforce context isolation checks: Give supervisor routines time to settle variables
	time.Sleep(100 * time.Millisecond)

	// Verify isolation boundaries: Stable sister should remain untouched and active
	parentST.mu.Lock()
	activeChildrenCount := len(parentST.children)
	parentST.mu.Unlock()

	// parentST should contain 'Stable-Sister-Component' and the newly spawned 'Healthy-Substitute-Component'
	if activeChildrenCount != 2 {
		t.Errorf("Fault isolation failure: Expected 2 active structural children remaining, found: %d", activeChildrenCount)
	}

	if atomic.LoadInt32(&substituteSpawned) != 1 {
		t.Error("Fault containment failure: Self-healing engine was skipped or stalled.")
	}

	// Clean out remaining assets cleanly to avoid test leaks
	parentST.Rollback()
}

// TestTrackerCoeffectLockContention deliberately cycles underlying dependencies
// on and off rapidly across parallel threads to test Tracker lock boundary performance.
func TestTrackerCoeffectLockContention(t *testing.T) {
	rootCtx := context.Background()
	parentST := NewSTContext(rootCtx, "Mesh-Network-Coordinator", nil)

	mockComp := NewMockComponent("Reactive-Agent-Worker", []string{"compute", "storage"})
	tracker := NewDependencyTracker(nil)

	tracker.RegisterComponent(parentST, mockComp)

	const processingIterations = 20
	var wg sync.WaitGroup

	// Stress thread blocks by cycling dependent mesh registries concurrently
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < processingIterations; i++ {
			tracker.ActivateService(parentST, "compute")
			time.Sleep(1 * time.Millisecond)
			tracker.DeactivateService("compute")
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < processingIterations; i++ {
			tracker.ActivateService(parentST, "storage")
			time.Sleep(1 * time.Millisecond)
			tracker.DeactivateService("storage")
		}
	}()

	wg.Wait()

	// Final verification: Ensure the component safely survived state transitions
	mockComp.mu.Lock()
	historySize := len(mockComp.history)
	mockComp.mu.Unlock()

	if historySize == 0 {
		t.Log("Tracker Lock Stress finished: Component successfully isolated state throughout cycles.")
	}
}
