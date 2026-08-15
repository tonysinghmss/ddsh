package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"ddsh/harness"
)

// plugin is a deliberately small application component. It demonstrates the
// Component interface and also implements StateRestorer so the same logical
// plugin can be replaced without losing its last-known-good state.
type plugin struct {
	name         string
	requirements []string

	mu           sync.Mutex
	state        []byte
	wakeCount    int32
	sleepCount   int32
	restoreCount int32
}

func newPlugin(name string, requirements ...string) *plugin {
	return &plugin{
		name:         name,
		requirements: append([]string(nil), requirements...),
	}
}

func (p *plugin) Name() string { return p.name }

func (p *plugin) Inject() []string {
	return append([]string(nil), p.requirements...)
}

func (p *plugin) OnWakeUp(ctx *harness.SpatioTemporalContext) {
	atomic.AddInt32(&p.wakeCount, 1)
	fmt.Printf("[PLUGIN] %s -> wake\n", p.name)

	// Every resource acquired by the plugin is represented by a reversible
	// effect. Rollback later invokes these effects in LIFO order.
	ctx.RegisterEffect("release-"+p.name+"-worker", func() {
		fmt.Printf("[EFFECT] %s -> release worker resources\n", p.name)
	})
	ctx.RegisterEffect("release-"+p.name+"-connection", func() {
		fmt.Printf("[EFFECT] %s -> release connection\n", p.name)
	})
}

func (p *plugin) OnSleep() {
	atomic.AddInt32(&p.sleepCount, 1)
	fmt.Printf("[PLUGIN] %s -> sleep\n", p.name)
}

// RestoreState is the optional capability consumed by RecoverWithReplacement.
func (p *plugin) RestoreState(payload harness.StatePayload) error {
	if len(payload.Data) == 0 {
		return errors.New("replacement received an empty checkpoint")
	}
	p.mu.Lock()
	p.state = append([]byte(nil), payload.Data...)
	p.mu.Unlock()
	atomic.AddInt32(&p.restoreCount, 1)
	fmt.Printf("[STATE] %s -> restored checkpoint v%d: %q\n", p.name, payload.Version, payload.Data)
	return nil
}

func (p *plugin) setState(value string) {
	p.mu.Lock()
	p.state = []byte(value)
	p.mu.Unlock()
}

func (p *plugin) stateValue() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return string(p.state)
}

func main() {
	root := context.Background()
	tracker := harness.NewDependencyTracker(nil)

	fmt.Println("=== ddsh plugin example ===")
	fmt.Println("This example demonstrates registration, coeffects, DAG planning,\n" +
		"spatiotemporal effects, snapshots, replacement and reconciliation.")

	// -------------------------------------------------------------------------
	// 1. Create plugins and declare their coeffects.
	// -------------------------------------------------------------------------
	// storage has no requirements. processor requires storage. reporter has two
	// providers; with the current ddsh semantics it remains eligible while at
	// least one provider is active.
	storage := newPlugin("storage")
	processor := newPlugin("processor", "storage")
	reporter := newPlugin("reporter", "storage", "metrics")

	fmt.Println("\n--- 1. Register plugins and declare coeffects ---")
	tracker.RegisterComponent(root, storage)
	tracker.RegisterComponent(root, processor)
	tracker.RegisterComponent(root, reporter)
	printGraph(tracker.Graph())

	// storage is active immediately because it has no requirements. The other
	// plugins are represented in the graph but wait for their coeffects.

	// -------------------------------------------------------------------------
	// 2. Reactively satisfy dependencies.
	// -------------------------------------------------------------------------
	fmt.Println("\n--- 2. Activate dependency services ---")
	tracker.ActivateService(root, "metrics")
	tracker.ActivateService(root, "storage")
	printGraph(tracker.Graph())

	// -------------------------------------------------------------------------
	// 3. Show the explicit public planning API.
	// -------------------------------------------------------------------------
	fmt.Println("\n--- 3. Build and execute an explicit dependency plan ---")
	plan, err := tracker.Graph().PlanSleep("processor")
	must(err)
	fmt.Printf("planned generation=%d actions=%d\n", plan.Generation, len(plan.Actions))
	for _, action := range plan.Actions {
		fmt.Printf("  %s: %v\n", action.NodeName, action.Action)
	}
	must(tracker.Graph().ExecuteWithOptions(plan, harness.DependencyExecutionOptions{
		ParentContext: root,
	}))
	printGraph(tracker.Graph())

	// Wake it again through the high-level transition API.
	fmt.Println("\n--- 4. Wake the plugin through the transition API ---")
	must(tracker.Graph().TransitionWake("processor", harness.DependencyExecutionOptions{
		ParentContext: root,
	}))

	// -------------------------------------------------------------------------
	// 5. Capture a known-good state.
	// -------------------------------------------------------------------------
	fmt.Println("\n--- 5. Create a last-known-good state checkpoint ---")
	processorCtx := tracker.GetActiveContext("processor")
	if processorCtx == nil {
		panic("processor should be active before checkpointing")
	}

	processor.setState("job=42;offset=137;status=running")
	checkpoint, err := harness.MarshalJSONState(struct {
		JobID  string `json:"job_id"`
		Offset int    `json:"offset"`
		Status string `json:"status"`
	}{
		JobID:  "job-42",
		Offset: 137,
		Status: "running",
	})
	must(err)
	processorCtx.UpdateStateSnapshot(checkpoint)

	payload, err := processorCtx.StateSnapshot()
	must(err)
	fmt.Printf("checkpoint version=%d captured=%s bytes=%d\n",
		payload.Version, payload.Captured.Format(time.RFC3339), len(payload.Data))

	// -------------------------------------------------------------------------
	// 6. Simulate failure and perform state-aware replacement.
	// -------------------------------------------------------------------------
	fmt.Println("\n--- 6. Fail the plugin and replace it without changing DAG identity ---")
	// Rollback tears down the old runtime. RecoverWithReplacement then uses the
	// saved logical node plus its checkpoint to construct the replacement.
	processorCtx.RaiseError(errors.New("processor connection failed"))
	waitForRollback(processorCtx)

	replacement := newPlugin("processor", "storage")
	must(tracker.RecoverWithReplacement(root, "processor", replacement))

	if replacement.stateValue() == "" {
		panic("replacement did not receive the checkpoint")
	}
	fmt.Printf("replacement state=%q wakes=%d restores=%d\n",
		replacement.stateValue(),
		atomic.LoadInt32(&replacement.wakeCount),
		atomic.LoadInt32(&replacement.restoreCount))
	printGraph(tracker.Graph())

	// -------------------------------------------------------------------------
	// 7. Demonstrate hierarchical rollback.
	// -------------------------------------------------------------------------
	fmt.Println("\n--- 7. Roll back the application context ---")
	rootScope := harness.NewSTContext(root, "application", nil)
	rootScope.RegisterEffect("application-cleanup", func() {
		fmt.Println("[EFFECT] application -> final cleanup")
	})
	childScope := harness.NewSTContext(rootScope, "plugin-scope", nil)
	childScope.RegisterEffect("plugin-cleanup", func() {
		fmt.Println("[EFFECT] plugin-scope -> final cleanup")
	})
	rootScope.Rollback()

	fmt.Println("\n=== Example completed successfully ===")
	fmt.Printf("processor replacement wake=%d restore=%d\n",
		atomic.LoadInt32(&replacement.wakeCount),
		atomic.LoadInt32(&replacement.restoreCount))
}

func printGraph(graph *harness.DependencyGraph) {
	fmt.Printf("graph generation=%d nodes=%d edges=%d\n",
		graph.Generation(), len(graph.Nodes()), graph.EdgeCount())
	for _, node := range graph.Nodes() {
		fmt.Printf("  - %-10s active=%-5t\n", node.Name, node.Active)
	}
}

func waitForRollback(ctx *harness.SpatioTemporalContext) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := ctx.StateSnapshot(); err == nil {
			// The explicit checkpoint is retained after rollback, so the snapshot
			// itself cannot tell us whether supervision has completed. The short
			// sleep here gives the supervisor goroutine a deterministic scheduling
			// opportunity before replacement is requested.
			time.Sleep(10 * time.Millisecond)
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("timed out waiting for failure supervision")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
