# Comprehensive Plugin Example

This example shows how a new application plugin can use the current `ddsh` harness APIs without modifying the harness itself.

Run it from the repository root:

```sh
go run ./examples/plugin_demo
```

The example is intentionally self-contained. The only dependency is the local `ddsh/harness` package.

## What the example demonstrates

| Step | Feature | API demonstrated |
|---|---|---|
| 1 | Plugin contract | `Component`, `Name`, `Inject`, `OnWakeUp`, `OnSleep` |
| 2 | Reactive coeffects | `Inject()` + `DependencyGraph` |
| 3 | Service activation | `DependencyTracker.ActivateService` |
| 4 | Explicit DAG planning | `PlanSleep`, `DependencyPlan`, `ExecuteWithOptions` |
| 5 | Transition execution | `TransitionWake` |
| 6 | Reversible effects | `SpatioTemporalContext.RegisterEffect`, `Rollback` |
| 7 | State checkpoints | `StatePayload`, `UpdateStateSnapshot`, `StateSnapshot` |
| 8 | State serialization | `MarshalJSONState` |
| 9 | Runtime replacement | `RecoverWithReplacement` |
| 10 | State restoration | `StateRestorer.RestoreState` |
| 11 | Hierarchical rollback | parent/child `SpatioTemporalContext` |

## Plugin model

A plugin only needs to satisfy the application-facing `Component` interface:

```go
type Component interface {
    Name() string
    Inject() []string
    OnWakeUp(ctx *SpatioTemporalContext)
    OnSleep()
}
```

`Name` supplies logical identity. `Inject` declares the plugin's coeffects—runtime capabilities/providers that must be available for the plugin to become eligible. `OnWakeUp` acquires resources and installs reversible effects. `OnSleep` receives the lifecycle notification when the dependency planner removes the plugin from the active configuration.

## Example topology

```text
storage
   │
   ├──────────────► processor
   │
   └──────────────► reporter ◄──────── metrics
```

`storage` has no requirements and can wake immediately. `processor` requires `storage`. `reporter` declares `storage` and `metrics`; under the current ddsh OR-style multi-provider eligibility rule, it becomes eligible when at least one required provider remains active. The example activates `metrics` after registration so the reactive transition is visible.

## Why effects belong to the context

The plugin does not implement a separate `Close`/`Cleanup` loop. Instead, resources acquired during `OnWakeUp` are represented by effects:

```go
ctx.RegisterEffect("release-worker", func() {
    // undo resource acquisition
})
```

`Rollback` owns the teardown boundary and unwinds the registered effects. This demonstrates the paper's temporal-composability idea: runtime transformations carry reversible cleanup.

## Why the snapshot is explicit

Before the processor is failed, the example writes a known-good checkpoint into its context:

```go
processorCtx.UpdateStateSnapshot(checkpoint)
```

The snapshot is copied into context ownership. `StateSnapshot` also returns a copy, so application code does not accidentally mutate harness-owned state.

## Replacement flow

The example then rolls back the failed processor runtime and creates a replacement with the same logical name:

```go
replacement := newPlugin("processor", "storage")
err := tracker.RecoverWithReplacement(root, "processor", replacement)
```

The harness:

1. locates the failed logical node;
2. obtains its last-known-good snapshot;
3. creates the replacement runtime context;
4. calls `RestoreState` when the replacement implements `StateRestorer`;
5. atomically publishes the new runtime behind the same DAG node;
6. reconciles downstream dependents through the DAG planner.

This is why the replacement keeps the logical identity `processor` instead of adding a second unrelated node.

## Failure boundary in the example

The example uses an explicit `Rollback()` call rather than an asynchronous timer to make `go run` deterministic. The important recovery contract is the same: the runtime is torn down while the explicit last-known-good checkpoint remains available, and `RecoverWithReplacement` consumes that checkpoint.

The asynchronous failure supervisor is separately covered by the harness test suite.

## Learning path

After running this example, read the repository documentation in this order:

1. `docs/context.md` — runtime context and effects
2. `docs/snapshot.md` — state ownership and transfer
3. `docs/dependency-graph.md` — DAG model
4. `docs/dependency-registration.md` — how plugins enter the graph
5. `docs/dependency-plan.md` — planning and generation checks
6. `docs/transition-execution.md` — transition serialization and callbacks
7. `docs/recovery.md` — replacement recovery
8. `docs/replacement.md` — downstream reconciliation
9. `docs/overall-flow.md` — complete end-to-end lifecycle

## Paper mapping

The example demonstrates the repository's practical interpretation of three core Cordis ideas:

- **Temporal composability:** reversible effects and rollback.
- **Spatial composability:** dependency/coeffect declarations and reactive DAG transitions.
- **Unified context:** component runtime state, effects and checkpoints live within `SpatioTemporalContext`.

It also demonstrates repository-specific engineering extensions: explicit plans, generation-based stale-plan rejection, state snapshots and state-aware runtime replacement.
