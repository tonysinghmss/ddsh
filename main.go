package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ddsh/harness"
)

type AgentWorker struct {
	ModuleName   string
	Requirements []string
	State        []byte
}

func (aw *AgentWorker) Name() string    { return aw.ModuleName }
func (aw *AgentWorker) Inject() []string { return aw.Requirements }

func (aw *AgentWorker) OnWakeUp(ctx *harness.SpatioTemporalContext) {
	fmt.Printf("   -> [%s]: Waking up and initializing execution scopes...\n", aw.ModuleName)

	ctx.RegisterEffect("Unbind Network Sockets", func() {
		time.Sleep(10 * time.Millisecond)
		fmt.Printf("   <- [Teardown Worker (%s)] Detached socket listeners.\n", aw.ModuleName)
	})
	ctx.RegisterEffect("Flush Working Buffer", func() {
		fmt.Printf("   <- [Teardown Worker (%s)] Invalidated scratchpad arrays.\n", aw.ModuleName)
	})

	fmt.Printf("   -> Status: [%s] is now actively running.\n", aw.ModuleName)
}

func (aw *AgentWorker) OnSleep() {
	fmt.Printf("   [Lifecycle Notification]: Component '%s' forced into sleep configuration.\n", aw.ModuleName)
}

func (aw *AgentWorker) RestoreState(payload harness.StatePayload) error {
	aw.State = append([]byte(nil), payload.Data...)
	fmt.Printf("   -> [%s]: Restored %d bytes of last-known-good state.\n", aw.ModuleName, len(aw.State))
	return nil
}

func main() {
	rootCtx := context.Background()

	// Keep the recovery callback wired to the same tracker that owns the DAG.
	// This exercises the current state-aware replacement API instead of
	// constructing an unrelated fallback context outside the graph.
	var tracker *harness.DependencyTracker
	globalRecovery := func(failedCtx *harness.SpatioTemporalContext, failedName string) {
		fmt.Printf("🚀 [Self-Healing Engine]: Initiating hot-swap substitution for crashed '%s'\n", failedName)

		replacement := &AgentWorker{
			ModuleName:   failedName,
			Requirements: []string{"database"},
		}
		if err := tracker.RecoverWithReplacement(rootCtx, failedName, replacement); err != nil {
			fmt.Printf("   [Recovery Error]: %v\n", err)
			return
		}
		fmt.Println("Base Setup Status: Fallback component hot-swapped successfully.")
	}

	tracker = harness.NewDependencyTracker(globalRecovery)
	parentST := harness.NewSTContext(rootCtx, "Core-Orchestrator-Agent", nil)
	parentST.RegisterEffect("Clear Global Hash Tree", func() {
		fmt.Println("   <- [Teardown] Purged top-level coordination structures.")
	})

	fmt.Println("=== Scenario 1: Registering Component and Verifying Coeffect Restraints ===")
	primaryWorker := &AgentWorker{
		ModuleName:   "Primary-Data-Subagent",
		Requirements: []string{"database"},
	}
	if err := tracker.RegisterComponentErr(parentST, primaryWorker); err != nil {
		fmt.Printf("[Registration Error]: %v\n", err)
		return
	}
	time.Sleep(50 * time.Millisecond)

	fmt.Println("\n=== Scenario 2: Satisfying Dependencies and Observing Reactive Wake Up ===")
	if err := tracker.ActivateServiceErr(parentST, "database"); err != nil {
		fmt.Printf("[Activation Error]: %v\n", err)
		return
	}
	time.Sleep(50 * time.Millisecond)

	fmt.Println("\n=== Scenario 3: Simulating Localized Asynchronous Failure and State-Aware Self-Healing ===")
	targetWorkerCtx := tracker.GetActiveContext("Primary-Data-Subagent")
	if targetWorkerCtx != nil {
		state, err := harness.MarshalJSONState(map[string]any{
			"job":     "demo-42",
			"step":    7,
			"healthy": true,
		})
		if err == nil {
			targetWorkerCtx.UpdateStateSnapshot(state)
		}
		targetWorkerCtx.RaiseError(errors.New("unrecoverable socket timeout inside processing threads"))
	}
	time.Sleep(150 * time.Millisecond)

	fmt.Println("\n=== Scenario 4: Verifying Global Downward Cascading Rollback ===")
	parentST.Rollback()
	time.Sleep(50 * time.Millisecond)
}
