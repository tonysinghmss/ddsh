package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"ddsh/harness"
	"github.com/jackc/pgx/v5/pgxpool"
)

// plugin is a realistic application component. The processor plugin can own
// a PostgreSQL connection pool while still using the same Component contract
// as an in-memory plugin.
type plugin struct {
	name         string
	requirements []string
	databaseURL  string

	mu           sync.Mutex
	state        []byte
	pool         *pgxpool.Pool
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

func newPostgresPlugin(name, databaseURL string, requirements ...string) *plugin {
	p := newPlugin(name, requirements...)
	p.databaseURL = databaseURL
	return p
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

	if p.databaseURL == "" {
		fmt.Printf("[POSTGRES] %s -> disabled (DDSH_POSTGRES_URL is not set)\n", p.name)
		return
	}

	if err := p.openPostgres(); err != nil {
		panic(fmt.Sprintf("plugin %q PostgreSQL startup failed: %v", p.name, err))
	}

	ctx.RegisterEffect("close-"+p.name+"-postgres", func() {
		p.closePostgres()
		fmt.Printf("[POSTGRES] %s -> connection pool closed by rollback\n", p.name)
	})
}

func (p *plugin) OnSleep() {
	atomic.AddInt32(&p.sleepCount, 1)
	fmt.Printf("[PLUGIN] %s -> sleep\n", p.name)
}

func (p *plugin) openPostgres() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pool != nil {
		return nil
	}

	connectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(connectCtx, p.databaseURL)
	if err != nil {
		return err
	}
	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return err
	}

	_, err = pool.Exec(connectCtx, `
		CREATE TABLE IF NOT EXISTS ddsh_plugin_events (
			id BIGSERIAL PRIMARY KEY,
			plugin_name TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`) 
	if err != nil {
		pool.Close()
		return err
	}

	_, err = pool.Exec(connectCtx,
		`INSERT INTO ddsh_plugin_events (plugin_name, status) VALUES ($1, $2)`,
		p.name, "woke")
	if err != nil {
		pool.Close()
		return err
	}

	p.pool = pool
	fmt.Printf("[POSTGRES] %s -> connected and recorded wake event\n", p.name)
	return nil
}

func (p *plugin) closePostgres() {
	p.mu.Lock()
	pool := p.pool
	p.pool = nil
	p.mu.Unlock()
	if pool != nil {
		pool.Close()
	}
}

// RestoreState is the optional capability consumed by RecoverWithReplacement.
// The checkpoint is application state, not harness internals: the replacement
// receives a copy and decides how to apply it.
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

func (p *plugin) snapshotState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := struct {
		Plugin string `json:"plugin"`
		Status string `json:"status"`
		DB     bool   `json:"postgres_connected"`
	}{
		Plugin: p.name,
		Status: string(p.state),
		DB:     p.pool != nil,
	}
	return json.Marshal(state)
}

func (p *plugin) databaseEventCount() (int64, error) {
	p.mu.Lock()
	pool := p.pool
	p.mu.Unlock()
	if pool == nil {
		return 0, nil
	}

	queryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var count int64
	err := pool.QueryRow(queryCtx,
		`SELECT COUNT(*) FROM ddsh_plugin_events WHERE plugin_name = $1`,
		p.name).Scan(&count)
	return count, err
}

func main() {
	root := context.Background()
	tracker := harness.NewDependencyTracker(nil)

	fmt.Println("=== ddsh plugin example ===")
	fmt.Println("This example demonstrates registration, coeffects, DAG planning,\n" +
		"spatiotemporal effects, PostgreSQL resource ownership, snapshots,\n" +
		"replacement and reconciliation.")

	// Set DDSH_POSTGRES_URL to enable the real PostgreSQL integration. Without
	// it, the example remains fully runnable as an offline harness demo.
	databaseURL := os.Getenv("DDSH_POSTGRES_URL")

	// 1. Create plugins and declare their coeffects.
	storage := newPlugin("storage")
	processor := newPostgresPlugin("processor", databaseURL, "storage")
	reporter := newPlugin("reporter", "storage", "metrics")

	fmt.Println("\n--- 1. Register plugins and declare coeffects ---")
	tracker.RegisterComponent(root, storage)
	tracker.RegisterComponent(root, processor)
	tracker.RegisterComponent(root, reporter)
	printGraph(tracker.Graph())

	// storage is active immediately because it has no requirements. processor
	// is active because storage is already active. reporter waits for metrics.

	// 2. Reactively satisfy the remaining dependency.
	fmt.Println("\n--- 2. Activate dependency services ---")
	tracker.ActivateService(root, "metrics")
	printGraph(tracker.Graph())

	// 3. Use the explicit planning API. Planning and execution are separate
	// public operations, which is useful when an application wants to inspect
	// or persist a transition before executing it.
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

	// Wake it again through the high-level transition API. This is where a
	// real plugin would establish resources such as a database pool.
	fmt.Println("\n--- 4. Wake the PostgreSQL-backed plugin ---")
	must(tracker.Graph().TransitionWake("processor", harness.DependencyExecutionOptions{
		ParentContext: root,
	}))

	if databaseURL != "" {
		count, err := processor.databaseEventCount()
		must(err)
		fmt.Printf("[POSTGRES] processor -> persisted event count=%d\n", count)
	}

	// 5. Capture a known-good state.
	fmt.Println("\n--- 5. Create a last-known-good state checkpoint ---")
	processorCtx := tracker.GetActiveContext("processor")
	if processorCtx == nil {
		panic("processor should be active before checkpointing")
	}

	processor.setState("job=42;offset=137;status=running")
	checkpoint, err := processor.snapshotState()
	must(err)
	processorCtx.UpdateStateSnapshot(checkpoint)

	payload, err := processorCtx.StateSnapshot()
	must(err)
	fmt.Printf("checkpoint version=%d captured=%s bytes=%d\n",
		payload.Version, payload.Captured.Format(time.RFC3339), len(payload.Data))

	// 6. Simulate a failure boundary. We explicitly roll back the failed
	// runtime so the example is deterministic. The PostgreSQL pool is one of
	// the reversible resources owned by the context and is closed here.
	fmt.Println("\n--- 6. Fail the plugin and replace it without changing DAG identity ---")
	processorCtx.Rollback()

	replacement := newPostgresPlugin("processor", databaseURL, "storage")
	must(tracker.RecoverWithReplacement(root, "processor", replacement))

	if replacement.stateValue() == "" {
		panic("replacement did not receive the checkpoint")
	}
	fmt.Printf("replacement state=%q wakes=%d restores=%d\n",
		replacement.stateValue(),
		atomic.LoadInt32(&replacement.wakeCount),
		atomic.LoadInt32(&replacement.restoreCount))
	if databaseURL != "" {
		count, err := replacement.databaseEventCount()
		must(err)
		fmt.Printf("[POSTGRES] replacement -> persisted event count=%d\n", count)
	}
	printGraph(tracker.Graph())

	// 7. Demonstrate hierarchical rollback independently of the DAG tracker.
	fmt.Println("\n--- 7. Roll back a hierarchical application context ---")
	rootScope := harness.NewSTContext(root, "application", nil)
	rootScope.RegisterEffect("application-cleanup", func() {
		fmt.Println("[EFFECT] application -> final cleanup")
	})
	childScope := harness.NewSTContext(rootScope, "plugin-scope", nil)
	childScope.RegisterEffect("plugin-cleanup", func() {
		fmt.Println("[EFFECT] plugin-scope -> final cleanup")
	})
	if childScope.ParentST() != rootScope {
		panic("plugin scope lost its structural parent")
	}
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

func must(err error) {
	if err != nil {
		panic(err)
	}
}
