# Comprehensive Plugin Example

This example shows how a new application plugin can use the current `ddsh` harness APIs without modifying the harness itself. The `processor` plugin is intentionally realistic: it owns a PostgreSQL connection pool as a runtime resource, records lifecycle events, checkpoints application state, and is replaced with state transfer after rollback.

Run it from the repository root:

```sh
go run ./examples/plugin_demo
```

The example is runnable without infrastructure. If `DDSH_POSTGRES_URL` is not set, the PostgreSQL portion is skipped and the rest of the harness demo still runs. To exercise the real database path, start the bundled PostgreSQL container and export the connection string described below.

## What the example demonstrates

| Step | Feature | API demonstrated |
|---|---|---|
| 1 | Plugin contract | `Component`, `Name`, `Inject`, `OnWakeUp`, `OnSleep` |
| 2 | Reactive coeffects | `Inject()` + `DependencyGraph` |
| 3 | Service activation | `DependencyTracker.ActivateService` |
| 4 | Explicit DAG planning | `PlanSleep`, `DependencyPlan`, `ExecuteWithOptions` |
| 5 | Transition execution | `TransitionWake` |
| 6 | External resource ownership | PostgreSQL `pgxpool.Pool` inside the plugin |
| 7 | Reversible effects | `SpatioTemporalContext.RegisterEffect`, `Rollback` |
| 8 | State checkpoints | `StatePayload`, `UpdateStateSnapshot`, `StateSnapshot` |
| 9 | State serialization | JSON checkpoint created by the plugin |
| 10 | Runtime replacement | `RecoverWithReplacement` |
| 11 | State restoration | `StateRestorer.RestoreState` |
| 12 | Hierarchical rollback | parent/child `SpatioTemporalContext` |

## PostgreSQL setup

The example uses [`github.com/jackc/pgx/v5`](https://github.com/jackc/pgx) and `pgxpool` rather than hiding the database behind another abstraction. That makes the lifecycle boundary visible: the plugin opens the pool during `OnWakeUp`, registers a rollback effect that closes it, and the replacement opens a fresh pool when it is reconciled.

### Option A — bundled Docker Compose

From the repository root:

```sh
docker compose -f examples/plugin_demo/docker-compose.yml up -d
```

Then run:

```sh
export DDSH_POSTGRES_URL='postgres://ddsh:ddsh@localhost:54329/ddsh?sslmode=disable'
go run ./examples/plugin_demo
```

On Windows PowerShell:

```powershell
$env:DDSH_POSTGRES_URL = 'postgres://ddsh:ddsh@localhost:54329/ddsh?sslmode=disable'
go run ./examples/plugin_demo
```

Stop PostgreSQL when finished:

```sh
docker compose -f examples/plugin_demo/docker-compose.yml down -v
```

### What the database plugin actually does

When `DDSH_POSTGRES_URL` is configured, `processor.OnWakeUp()`:

1. creates a `pgxpool.Pool`;
2. pings PostgreSQL with a timeout;
3. creates `ddsh_plugin_events` if necessary;
4. records a `woke` event;
5. registers a reversible effect that closes the pool during rollback.

The example then queries the event count, so the database connection is not merely decorative.

During replacement:

```text
processor runtime
      │
      ├── pgxpool.Pool
      │
      ├── lifecycle event in PostgreSQL
      │
      └── StateSnapshot
             │
             ▼
          Rollback
             │
             └── close pool
             │
             ▼
      RecoverWithReplacement
             │
             ├── restore StateSnapshot
             ├── create replacement runtime
             ├── open new pgxpool.Pool
             └── reconcile DAG
```

This is the useful part of the example: the database connection is treated as a **runtime resource owned by the plugin context**, rather than as global application state.

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

The PostgreSQL plugin additionally implements the optional `StateRestorer` capability:

```go
type StateRestorer interface {
    RestoreState(StatePayload) error
}
```

This is deliberately optional. Plugins that do not need state transfer do not have to implement it.

## Example topology

```text
storage
   │
   ├──────────────► processor ───► PostgreSQL
   │
   └──────────────► reporter ◄──────── metrics
```

`storage` has no requirements and can wake immediately. `processor` requires `storage`. `reporter` declares `storage` and `metrics`, so it initially waits until both providers are active. Once a multi-parent dependent is already active, the current sleep/reconciliation logic keeps it eligible as long as at least one provider remains active. This distinction is important: the current implementation uses all-provider readiness for initial activation and remaining-provider eligibility during downstream reconciliation.

## Why effects belong to the context

The plugin does not implement a separate global `Close`/`Cleanup` loop. Instead, resources acquired during `OnWakeUp` are represented by effects:

```go
ctx.RegisterEffect("close-processor-postgres", func() {
    pool.Close()
})
```

`Rollback` owns the teardown boundary and unwinds the registered effects. This demonstrates the paper's temporal-composability idea: runtime transformations carry reversible cleanup.

For the PostgreSQL path, this means a failed plugin cannot leave its connection pool owned by an abandoned runtime context.

## Why the snapshot is explicit

Before the processor is failed, the example creates a JSON checkpoint containing application state and whether the PostgreSQL resource was connected:

```go
checkpoint, err := processor.snapshotState()
processorCtx.UpdateStateSnapshot(checkpoint)
```

The harness copies the snapshot into context ownership. `StateSnapshot` also returns a copy, so application code does not accidentally mutate harness-owned state.

## Replacement flow

The example then rolls back the failed processor runtime and creates a replacement with the same logical name:

```go
replacement := newPostgresPlugin("processor", databaseURL, "storage")
err := tracker.RecoverWithReplacement(root, "processor", replacement)
```

The harness:

1. locates the failed logical node;
2. obtains its last-known-good snapshot;
3. creates the replacement runtime context;
4. restores the snapshot through `StateRestorer`;
5. atomically publishes the new runtime behind the same DAG node;
6. runs the planned lifecycle callbacks, causing the replacement to acquire its own PostgreSQL pool;
7. reconciles downstream dependents through the DAG planner.

The replacement therefore keeps the logical identity `processor` while owning a **new** physical database connection pool.

## Failure boundary in the example

The example uses an explicit `Rollback()` call rather than an asynchronous timer to make `go run` deterministic. The important recovery contract is the same: the runtime is torn down, reversible resources are released, the explicit last-known-good checkpoint remains available, and `RecoverWithReplacement` consumes that checkpoint.

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

- **Temporal composability:** reversible effects and rollback, including cleanup of an external PostgreSQL resource.
- **Spatial composability:** dependency/coeffect declarations and reactive DAG transitions.
- **Unified context:** component runtime state, effects, external-resource ownership and checkpoints live within `SpatioTemporalContext`.

It also demonstrates repository-specific engineering extensions: explicit plans, generation-based stale-plan rejection, state snapshots and state-aware runtime replacement.
