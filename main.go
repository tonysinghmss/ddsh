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
}

func (aw *AgentWorker) Name() string   { return aw.ModuleName }
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

func main() {
	rootCtx := context.Background()

	globalRecovery := func(failedCtx *harness.SpatioTemporalContext, failedName string) {
		fmt.Printf("🚀 [Self-Healing Engine]: Initiating hot-swap substitution for crashed '%s'\n", failedName)
		
		backupWorker := &AgentWorker{
			ModuleName:   "Backup-Substitute-Subagent",
			Requirements: []string{"database"},
		}
		
		var structuralParent context.Context = rootCtx
		if failedCtx.ParentST() != nil {
			structuralParent = failedCtx.ParentST()
		}
		
		backupCtx := harness.NewSTContext(structuralParent, backupWorker.Name(), nil)
		backupCtx.RegisterEffect("Close Substitute DB Handles", func() {
			fmt.Println("   <- [Teardown] Safely severed substitute database channels.")
		})
		
		go backupWorker.OnWakeUp(backupCtx)
		fmt.Println("Base Setup Status: Fallback component hot-swapped successfully.")
	}

	tracker := harness.NewDependencyTracker(globalRecovery)
	parentST := harness.NewSTContext(rootCtx, "Core-Orchestrator-Agent", nil)
	parentST.RegisterEffect("Clear Global Hash Tree", func() {
		fmt.Println("   <- [Teardown] Purged top-level coordination structures.")
	})

	fmt.Println("=== Scenario 1: Registering Component and Verifying Coeffect Restraints ===")
	primaryWorker := &AgentWorker{
		ModuleName:   "Primary-Data-Subagent",
		Requirements: []string{"database"},
	}
	tracker.RegisterComponent(parentST, primaryWorker)
	time.Sleep(50 * time.Millisecond)

	fmt.Println("\n=== Scenario 2: Satisfying Dependencies and Observing Reactive Wake Up ===")
	tracker.ActivateService(parentST, "database")
	time.Sleep(50 * time.Millisecond)

	fmt.Println("\n=== Scenario 3: Simulating Localized Asynchronous Failure and Self-Healing ===")
	targetWorkerCtx := tracker.GetActiveContext("Primary-Data-Subagent")
	if targetWorkerCtx != nil {
		targetWorkerCtx.RaiseError(errors.New("unrecoverable socket timeout inside processing threads"))
	}
	time.Sleep(150 * time.Millisecond)

	fmt.Println("\n=== Scenario 4: Verifying Global Downward Cascading Rollback ===")
	parentST.Rollback()
	time.Sleep(50 * time.Millisecond)
}
