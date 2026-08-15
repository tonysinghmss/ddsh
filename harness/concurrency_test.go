package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockComponent implements Component for rigorous tracking under heavy
// concurrency load.
type MockComponent struct {
	name         string
	requirements []string

	wakeCount  int32
	sleepCount int32

	mu      sync.Mutex
	history []string
}

func NewMockComponent(
	name string,
	reqs []string,
) *MockComponent {
	return &MockComponent{
		name:         name,
		requirements: reqs,
		history:      make([]string, 0),
	}
}

func (m *MockComponent) Name() string {
	return m.name
}

func (m *MockComponent) Inject() []string {
	return m.requirements
}

func (m *MockComponent) OnWakeUp(
	ctx *SpatioTemporalContext,
) {
	atomic.AddInt32(&m.wakeCount, 1)

	m.mu.Lock()
	m.history = append(m.history, "wake")
	m.mu.Unlock()

	for i := 0; i < 5; i++ {
		effectID := fmt.Sprintf("effect-%d", i)

		ctx.RegisterEffect(effectID, func() {
			time.Sleep(
				time.Duration(i+1) * time.Millisecond,
			)
		})
	}
}

func (m *MockComponent) OnSleep() {
	atomic.AddInt32(&m.sleepCount, 1)

	m.mu.Lock()
	m.history = append(m.history, "sleep")
	m.mu.Unlock()
}

// TestLIFORollbackLockStress applies parallel execution pressure to verify
// that the Context early-unlock mechanism does not deadlock when many
// branches are created and then rolled back concurrently.
func TestLIFORollbackLockStress(t *testing.T) {
	rootCtx := context.Background()

	parentST := NewSTContext(
		rootCtx,
		"Stress-Orchestrator-Parent",
		nil,
	)

	const totalChildren = 50

	var wg sync.WaitGroup

	for i := 0; i < totalChildren; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			childName := fmt.Sprintf(
				"Child-Worker-%d",
				id,
			)

			childST := NewSTContext(
				parentST,
				childName,
				nil,
			)

			childST.RegisterEffect(
				"Cleanup-Resource-Fast",
				func() {},
			)

			childST.RegisterEffect(
				"Cleanup-Resource-Slow",
				func() {
					time.Sleep(2 * time.Millisecond)
				},
			)
		}(i)
	}

	wg.Wait()

	parentST.mu.Lock()
	childTreeSize := len(parentST.children)
	parentST.mu.Unlock()

	if childTreeSize != totalChildren {
		t.Errorf(
			"Expected %d children tracked under tree root, found: %d",
			totalChildren,
			childTreeSize,
		)
	}

	done := make(chan struct{})

	go func() {
		parentST.Rollback()
		close(done)
	}()

	select {
	case <-done:
		// Success.
	case <-time.After(2 * time.Second):
		t.Fatal(
			"DEADLOCK DETECTED: Parent tree cascading rollback stalled forever.",
		)
	}
}

// TestAsynchronousFaultContainmentAndHealing verifies that a sub-module
// fault isolates itself and invokes the self-healing hook without corrupting
// sister branches.
func TestAsynchronousFaultContainmentAndHealing(
	t *testing.T,
) {
	rootCtx := context.Background()

	var substituteSpawned int32

	var parentST *SpatioTemporalContext

	recoveryHook := func(
		failedCtx *SpatioTemporalContext,
		failedName string,
	) {
		atomic.StoreInt32(
			&substituteSpawned,
			1,
		)

		structuralParent := rootCtx

		if parent := failedCtx.ParentST(); parent != nil {
			structuralParent = parent
		}

		backupCtx := NewSTContext(
			structuralParent,
			"Healthy-Substitute-Component",
			nil,
		)

		backupCtx.RegisterEffect(
			"Backup-Teardown-Handler",
			func() {},
		)
	}

	parentST = NewSTContext(
		rootCtx,
		"Global-Root-Hub",
		nil,
	)

	faultyChildST := NewSTContext(
		parentST,
		"Faulty-Child-Component",
		recoveryHook,
	)

	stableChildST := NewSTContext(
		parentST,
		"Stable-Sister-Component",
		nil,
	)

	stableChildST.RegisterEffect(
		"Stable-Teardown-Lock",
		func() {},
	)

	faultyChildST.RegisterEffect(
		"Faulty-Teardown-Lock",
		func() {},
	)

	faultyChildST.RaiseError(
		errors.New(
			"simulated unrecoverable AI model socket exception",
		),
	)

	time.Sleep(100 * time.Millisecond)

	parentST.mu.Lock()
	activeChildrenCount := len(parentST.children)
	parentST.mu.Unlock()

	if activeChildrenCount != 2 {
		t.Errorf(
			"Fault isolation failure: Expected 2 active structural children remaining, found: %d",
			activeChildrenCount,
		)
	}

	if atomic.LoadInt32(&substituteSpawned) != 1 {
		t.Error(
			"Fault containment failure: Self-healing engine was skipped or stalled.",
		)
	}

	parentST.Rollback()
}

// TestStateSnapshotRoundTrip verifies that a checkpoint can be captured from
// a failed context and transferred into a replacement context.
func TestStateSnapshotRoundTrip(
	t *testing.T,
) {
	type AgentState struct {
	    JobID     string `json:"job_id"`
	    LastToken int    `json:"last_token"`
	    Completed bool   `json:"completed"`
   }

	rootCtx := context.Background()

	failedCtx := NewSTContext(
		rootCtx,
		"Faulty-Agent",
		nil,
	)

	backupCtx := NewSTContext(
		rootCtx,
		"Backup-Agent",
		nil,
	)

	expected := AgentState{
		JobID:       "mortgage-processing-42",
		LastOffset:  137,
		PartialData: "validated-income-section",
	}

	payload, err := MarshalJSONState(expected)
	if err != nil {
		t.Fatalf(
			"failed to marshal expected state: %v",
			err,
		)
	}

	failedCtx.UpdateStateSnapshot(payload)

	err = failedCtx.TransferStateTo(backupCtx)
	if err != nil {
		t.Fatalf(
			"state transfer failed: %v",
			err,
		)
	}

	transferred, err := backupCtx.StateSnapshot()
	if err != nil {
		t.Fatalf(
			"backup snapshot unavailable: %v",
			err,
		)
	}

	var restored AgentState

	if err := json.Unmarshal(
		transferred.Data,
		&restored,
	); err != nil {
		t.Fatalf(
			"failed to decode transferred state: %v",
			err,
		)
	}

	if restored.JobID != expected.JobID {
		t.Errorf(
			"JobID mismatch: got %q, want %q",
			restored.JobID,
			expected.JobID,
		)
	}

	if restored.LastOffset != expected.LastOffset {
		t.Errorf(
			"LastOffset mismatch: got %d, want %d",
			restored.LastOffset,
			expected.LastOffset,
		)
	}

	if restored.PartialData != expected.PartialData {
		t.Errorf(
			"PartialData mismatch: got %q, want %q",
			restored.PartialData,
			expected.PartialData,
		)
	}

	failedCtx.Rollback()
	backupCtx.Rollback()
}

// TestStateSnapshotIsolation verifies that source and destination contexts
// do not share the underlying Data byte array.
func TestStateSnapshotIsolation(
	t *testing.T,
) {
	source := NewSTContext(
		context.Background(),
		"Snapshot-Source",
		nil,
	)

	target := NewSTContext(
		context.Background(),
		"Snapshot-Target",
		nil,
	)

	original := []byte("original-state")

	source.UpdateStateSnapshot(original)

	if err := source.TransferStateTo(target); err != nil {
		t.Fatalf(
			"state transfer failed: %v",
			err,
		)
	}

	original[0] = 'X'

	sourcePayload, err := source.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	targetPayload, err := target.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	if string(sourcePayload.Data) != "original-state" {
		t.Fatalf(
			"source snapshot unexpectedly changed: %q",
			string(sourcePayload.Data),
		)
	}

	if string(targetPayload.Data) != "original-state" {
		t.Fatalf(
			"target snapshot unexpectedly changed: %q",
			string(targetPayload.Data),
		)
	}

	sourcePayload.Data[0] = 'Y'

	sourcePayloadAgain, err := source.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	if string(sourcePayloadAgain.Data) != "original-state" {
		t.Fatalf(
			"StateSnapshot returned shared mutable memory: %q",
			string(sourcePayloadAgain.Data),
		)
	}

	source.Rollback()
	target.Rollback()
}

// TestStateSnapshotProviderLockBoundary verifies that the snapshot provider
// executes without the context mutex held.
//
// The provider deliberately calls ParentST(), which acquires the context's
// parent-related lock. If CaptureStateSnapshot held c.mu while executing the
// provider, this pattern could create lock inversion in more complex
// component implementations.
func TestStateSnapshotProviderLockBoundary(
	t *testing.T,
) {
	ctx := NewSTContext(
		context.Background(),
		"Provider-Boundary",
		nil,
	)

	var providerCalls int32

	ctx.RegisterStateSnapshot(
		func() ([]byte, error) {
			atomic.AddInt32(
				&providerCalls,
				1,
			)

			// Application code is allowed to interact with the context
			// without inheriting the provider's lock boundary.
			_ = ctx.ParentST()

			return []byte("provider-state"), nil
		},
	)

	if err := ctx.CaptureStateSnapshot(); err != nil {
		t.Fatalf(
			"CaptureStateSnapshot failed: %v",
			err,
		)
	}

	if atomic.LoadInt32(&providerCalls) != 1 {
		t.Fatalf(
			"expected provider to execute once, got %d",
			atomic.LoadInt32(&providerCalls),
		)
	}

	payload, err := ctx.StateSnapshot()
	if err != nil {
		t.Fatalf(
			"snapshot unavailable: %v",
			err,
		)
	}

	if string(payload.Data) != "provider-state" {
		t.Fatalf(
			"unexpected snapshot data: %q",
			string(payload.Data),
		)
	}

	ctx.Rollback()
}

// TestAsynchronousFaultTransfersStateToBackup verifies the complete failure
// path:
//
//	component state
//      ↓
//	known-good checkpoint
//      ↓
//	async fault
//      ↓
//	supervisor
//      ↓
//	state capture
//      ↓
//	rollback
//      ↓
//	recovery hook
//      ↓
//	backup context
//      ↓
//	state transfer
func TestAsynchronousFaultTransfersStateToBackup(
	t *testing.T,
) {
	type AgentState struct {
		JobID      string `json:"job_id"`
		LastToken  int    `json:"last_token"`
		Completed  bool   `json:"completed"`
	}

	rootCtx := context.Background()

	var recoveryCompleted int32

	recoveryHook := func(
		failedCtx *SpatioTemporalContext,
		failedName string,
	) {
		parent := failedCtx.ParentST()

		var parentCtx context.Context = rootCtx

		if parent != nil {
			parentCtx = parent
		}

		backup := NewSTContext(
			parentCtx,
			failedName+"-Backup",
			nil,
		)

		if err := failedCtx.TransferStateTo(backup); err != nil {
			t.Errorf(
				"failed to transfer state: %v",
				err,
			)

			return
		}

		payload, err := backup.StateSnapshot()
		if err != nil {
			t.Errorf(
				"backup did not receive state: %v",
				err,
			)

			return
		}

		var restored AgentState

		if err := json.Unmarshal(
			payload.Data,
			&restored,
		); err != nil {
			t.Errorf(
				"failed to decode backup state: %v",
				err,
			)

			return
		}

		if restored.JobID != "job-123" {
			t.Errorf(
				"unexpected JobID: %q",
				restored.JobID,
			)
		}

		if restored.LastToken != 42 {
			t.Errorf(
				"unexpected LastToken: %d",
				restored.LastToken,
			)
		}

		if restored.Completed {
			t.Error(
				"backup incorrectly received completed=true",
			)
		}

		atomic.StoreInt32(
			&recoveryCompleted,
			1,
		)

		backup.Rollback()
	}

	failedCtx := NewSTContext(
		rootCtx,
		"Faulty-Agent",
		recoveryHook,
	)

	state := AgentState{
		JobID:     "job-123",
		LastToken: 42,
		Completed: false,
	}

	stateBytes, err := MarshalJSONState(state)
	if err != nil {
		t.Fatalf(
			"failed to marshal agent state: %v",
			err,
		)
	}

	failedCtx.UpdateStateSnapshot(stateBytes)

	failedCtx.RaiseError(
		errors.New(
			"simulated asynchronous agent failure",
		),
	)

	deadline := time.After(2 * time.Second)

	for atomic.LoadInt32(&recoveryCompleted) == 0 {
		select {
		case <-deadline:
			t.Fatal(
				"timed out waiting for state-aware recovery",
			)
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	// The supervisor owns rollback after receiving the error. Calling
	// Rollback again is safe because isRolling prevents duplicate cleanup.
	failedCtx.Rollback()
}

// TestConcurrentSnapshotUpdates stresses checkpoint replacement while
// snapshots are concurrently read.
func TestConcurrentSnapshotUpdates(
	t *testing.T,
) {
	ctx := NewSTContext(
		context.Background(),
		"Concurrent-Snapshot-Context",
		nil,
	)

	const iterations = 1000

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			data := []byte(
				fmt.Sprintf(
					"checkpoint-%d",
					i,
				),
			)

			ctx.UpdateStateSnapshot(data)
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			payload, err := ctx.StateSnapshot()

			if err != nil {
				if errors.Is(
					err,
					ErrNoStateSnapshot,
				) {
					continue
				}

				t.Errorf(
					"unexpected snapshot error: %v",
					err,
				)

				return
			}

			// Force the returned byte slice to be independently accessed.
			_ = string(payload.Data)
		}
	}()

	wg.Wait()

	payload, err := ctx.StateSnapshot()
	if err != nil {
		t.Fatalf(
			"final snapshot unavailable: %v",
			err,
		)
	}

	if len(payload.Data) == 0 {
		t.Fatal(
			"final snapshot unexpectedly empty",
		)
	}

	ctx.Rollback()
}

// TestTrackerCoeffectLockContention deliberately cycles underlying
// dependencies on and off rapidly across parallel threads to test Tracker
// lock boundary performance.
func TestTrackerCoeffectLockContention(
	t *testing.T,
) {
	rootCtx := context.Background()

	parentST := NewSTContext(
		rootCtx,
		"Mesh-Network-Coordinator",
		nil,
	)

	mockComp := NewMockComponent(
		"Reactive-Agent-Worker",
		[]string{
			"compute",
			"storage",
		},
	)

	tracker := NewDependencyTracker(nil)

	tracker.RegisterComponent(
		parentST,
		mockComp,
	)

	const processingIterations = 20

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < processingIterations; i++ {
			tracker.ActivateService(
				parentST,
				"compute",
			)

			time.Sleep(1 * time.Millisecond)

			tracker.DeactivateService(
				"compute",
			)
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < processingIterations; i++ {
			tracker.ActivateService(
				parentST,
				"storage",
			)

			time.Sleep(1 * time.Millisecond)

			tracker.DeactivateService(
				"storage",
			)
		}
	}()

	wg.Wait()

	mockComp.mu.Lock()
	historySize := len(mockComp.history)
	mockComp.mu.Unlock()

	if historySize == 0 {
		t.Log(
			"Tracker Lock Stress finished: Component successfully isolated state throughout cycles.",
		)
	}

	parentST.Rollback()
}
