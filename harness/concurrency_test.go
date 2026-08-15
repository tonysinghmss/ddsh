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

// MockComponent exercises component callbacks while allowing the tests to
// inspect callback execution without sharing mutable state unsafely.
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
		requirements: append([]string(nil), reqs...),
		history:      make([]string, 0),
	}
}

func (m *MockComponent) Name() string { return m.name }

func (m *MockComponent) Inject() []string {
	return append([]string(nil), m.requirements...)
}

func (m *MockComponent) OnWakeUp(ctx *SpatioTemporalContext) {
	atomic.AddInt32(&m.wakeCount, 1)
	m.mu.Lock()
	m.history = append(m.history, "wake")
	m.mu.Unlock()

	for i := 0; i < 5; i++ {
		effectID := fmt.Sprintf("effect-%d", i)
		delay := time.Duration(i+1) * time.Millisecond
		ctx.RegisterEffect(effectID, func() { time.Sleep(delay) })
	}
}

func (m *MockComponent) OnSleep() {
	atomic.AddInt32(&m.sleepCount, 1)
	m.mu.Lock()
	m.history = append(m.history, "sleep")
	m.mu.Unlock()
}

func TestLIFORollbackLockStress(t *testing.T) {
	parent := NewSTContext(context.Background(), "Stress-Orchestrator-Parent", nil)
	const totalChildren = 50

	var wg sync.WaitGroup
	for i := 0; i < totalChildren; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			child := NewSTContext(parent, fmt.Sprintf("Child-Worker-%d", id), nil)
			child.RegisterEffect("fast-cleanup", func() {})
			child.RegisterEffect("slow-cleanup", func() { time.Sleep(2 * time.Millisecond) })
		}(i)
	}
	wg.Wait()

	parent.mu.Lock()
	childCount := len(parent.children)
	parent.mu.Unlock()
	if childCount != totalChildren {
		t.Fatalf("tracked children=%d want=%d", childCount, totalChildren)
	}

	done := make(chan struct{})
	go func() {
		parent.Rollback()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("parent rollback deadlocked")
	}
}

func TestAsynchronousFaultContainmentAndHealing(t *testing.T) {
	root := context.Background()
	parent := NewSTContext(root, "Global-Root-Hub", nil)

	var substituteSpawned int32
	recoveryHook := func(failedCtx *SpatioTemporalContext, failedName string) {
		atomic.StoreInt32(&substituteSpawned, 1)
		structuralParent := root
		if p := failedCtx.ParentST(); p != nil {
			structuralParent = p
		}
		backup := NewSTContext(structuralParent, "Healthy-Substitute-Component", nil)
		backup.RegisterEffect("backup-cleanup", func() {})
	}

	faulty := NewSTContext(parent, "Faulty-Child-Component", recoveryHook)
	stable := NewSTContext(parent, "Stable-Sister-Component", nil)
	stable.RegisterEffect("stable-cleanup", func() {})
	faulty.RegisterEffect("faulty-cleanup", func() {})

	faulty.RaiseError(errors.New("simulated asynchronous fault"))

	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&substituteSpawned) == 0 {
		select {
		case <-deadline:
			t.Fatal("self-healing hook did not execute")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	parent.mu.Lock()
	children := len(parent.children)
	parent.mu.Unlock()
	if children != 2 {
		t.Fatalf("structural children after recovery=%d want=2", children)
	}
	parent.Rollback()
}

func TestStateSnapshotRoundTrip(t *testing.T) {
	type AgentState struct {
		JobID     string `json:"job_id"`
		LastToken int    `json:"last_token"`
		Completed bool   `json:"completed"`
	}

	failed := NewSTContext(context.Background(), "Faulty-Agent", nil)
	backup := NewSTContext(context.Background(), "Backup-Agent", nil)
	expected := AgentState{JobID: "mortgage-processing-42", LastToken: 137, Completed: false}

	payload, err := MarshalJSONState(expected)
	if err != nil {
		t.Fatal(err)
	}
	failed.UpdateStateSnapshot(payload)
	if err := failed.TransferStateTo(backup); err != nil {
		t.Fatal(err)
	}

	transferred, err := backup.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var restored AgentState
	if err := json.Unmarshal(transferred.Data, &restored); err != nil {
		t.Fatal(err)
	}
	if restored != expected {
		t.Fatalf("restored state=%+v want=%+v", restored, expected)
	}

	failed.Rollback()
	backup.Rollback()
}

func TestStateSnapshotIsolation(t *testing.T) {
	source := NewSTContext(context.Background(), "Snapshot-Source", nil)
	target := NewSTContext(context.Background(), "Snapshot-Target", nil)
	original := []byte("original-state")

	source.UpdateStateSnapshot(original)
	if err := source.TransferStateTo(target); err != nil {
		t.Fatal(err)
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
	if string(sourcePayload.Data) != "original-state" || string(targetPayload.Data) != "original-state" {
		t.Fatalf("snapshot ownership corrupted: source=%q target=%q", sourcePayload.Data, targetPayload.Data)
	}

	sourcePayload.Data[0] = 'Y'
	again, err := source.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if string(again.Data) != "original-state" {
		t.Fatalf("StateSnapshot returned shared mutable memory: %q", again.Data)
	}

	source.Rollback()
	target.Rollback()
}

func TestStateSnapshotProviderLockBoundary(t *testing.T) {
	ctx := NewSTContext(context.Background(), "Provider-Boundary", nil)
	var providerCalls int32

	ctx.RegisterStateSnapshot(func() ([]byte, error) {
		atomic.AddInt32(&providerCalls, 1)
		_ = ctx.ParentST()
		return []byte("provider-state"), nil
	})

	if err := ctx.CaptureStateSnapshot(); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&providerCalls) != 1 {
		t.Fatalf("provider calls=%d want=1", providerCalls)
	}
	payload, err := ctx.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload.Data) != "provider-state" {
		t.Fatalf("snapshot=%q", payload.Data)
	}
	ctx.Rollback()
}

func TestAsynchronousFaultTransfersStateToBackup(t *testing.T) {
	type AgentState struct {
		JobID     string `json:"job_id"`
		LastToken int    `json:"last_token"`
		Completed bool   `json:"completed"`
	}

	root := context.Background()
	var recoveryCompleted int32

	recoveryHook := func(failedCtx *SpatioTemporalContext, failedName string) {
		parent := failedCtx.ParentST()
		var parentCtx context.Context = root
		if parent != nil {
			parentCtx = parent
		}
		backup := NewSTContext(parentCtx, failedName+"-Backup", nil)
		if err := failedCtx.TransferStateTo(backup); err != nil {
			t.Errorf("state transfer failed: %v", err)
			return
		}

		payload, err := backup.StateSnapshot()
		if err != nil {
			t.Errorf("backup snapshot unavailable: %v", err)
			return
		}
		var restored AgentState
		if err := json.Unmarshal(payload.Data, &restored); err != nil {
			t.Errorf("restore decode failed: %v", err)
			return
		}
		if restored != (AgentState{JobID: "job-123", LastToken: 42, Completed: false}) {
			t.Errorf("restored state=%+v", restored)
			return
		}
		atomic.StoreInt32(&recoveryCompleted, 1)
		backup.Rollback()
	}

	failed := NewSTContext(root, "Faulty-Agent", recoveryHook)
	state := AgentState{JobID: "job-123", LastToken: 42, Completed: false}
	stateBytes, err := MarshalJSONState(state)
	if err != nil {
		t.Fatal(err)
	}
	failed.UpdateStateSnapshot(stateBytes)
	failed.RaiseError(errors.New("simulated asynchronous agent failure"))

	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&recoveryCompleted) == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for state-aware recovery")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	failed.Rollback()
}

func TestConcurrentSnapshotUpdates(t *testing.T) {
	ctx := NewSTContext(context.Background(), "Concurrent-Snapshot-Context", nil)
	const iterations = 1000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			ctx.UpdateStateSnapshot([]byte(fmt.Sprintf("checkpoint-%d", i)))
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			payload, err := ctx.StateSnapshot()
			if err != nil && !errors.Is(err, ErrNoStateSnapshot) {
				t.Errorf("unexpected snapshot error: %v", err)
				return
			}
			if err == nil {
				_ = string(payload.Data)
			}
		}
	}()
	wg.Wait()

	payload, err := ctx.StateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(payload.Data) == 0 {
		t.Fatal("final snapshot is empty")
	}
	ctx.Rollback()
}

func TestTrackerCoeffectLockContention(t *testing.T) {
	parent := NewSTContext(context.Background(), "Mesh-Network-Coordinator", nil)
	mockComp := NewMockComponent("Reactive-Agent-Worker", []string{"compute", "storage"})
	tracker := NewDependencyTracker(nil)
	tracker.RegisterComponent(parent, mockComp)

	const iterations = 20
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			tracker.ActivateService(parent, "compute")
			time.Sleep(time.Millisecond)
			tracker.DeactivateService("compute")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			tracker.ActivateService(parent, "storage")
			time.Sleep(time.Millisecond)
			tracker.DeactivateService("storage")
		}
	}()
	wg.Wait()

	mockComp.mu.Lock()
	historySize := len(mockComp.history)
	mockComp.mu.Unlock()
	if historySize == 0 {
		t.Fatal("component never observed a dependency transition")
	}
	parent.Rollback()
}
