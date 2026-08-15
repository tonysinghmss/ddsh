# ddsh: Spatiotemporal Composability Framework

`ddsh` is a lightweight Go implementation of the core runtime ideas behind the **Cordis spatiotemporal composability paradigm** and the DeepSeek Harness “everything is a plugin” architecture.

> This repository is an educational/experimental implementation, not a full port of DeepSeek Harness. The upstream paper describes two orthogonal dimensions—**temporal composability** through revertible effects and **spatial composability** through reactive coeffects—and unifies them in a runtime context. `ddsh` implements those core mechanisms with Go interfaces, a dependency DAG, hierarchical contexts, deterministic plans and state-aware replacement.

## What is implemented

### 1. Temporal composability

- `SpatioTemporalContext` provides hierarchical runtime scope.
- `RegisterEffect` records reversible cleanup operations.
- `Rollback` performs idempotent cascading teardown.
- Effects are retained in LIFO order and executed through a bounded worker pool.
- Asynchronous failures are supervised locally and can trigger recovery.

### 2. Spatial composability

- `Component` exposes `Name`, `Inject`, `OnWakeUp` and `OnSleep`.
- `Inject()` declares runtime coeffect/dependency requirements.
- `DependencyGraph` stores provider → dependent relationships.
- Cycle creation is rejected atomically.
- Wake/sleep transitions propagate through the DAG.
- Multi-parent dependencies are reconciled according to the current implementation’s OR-style eligibility rule: one remaining active provider is sufficient.

### 3. Deterministic dependency planning

- `PlanWake` and `PlanSleep` produce explicit `DependencyPlan` values.
- Kahn-style traversal gives deterministic topological ordering.
- Sleep invalidation executes downstream components before their providers.
- Graph generations prevent stale plans from executing after a competing transition.
- Application callbacks execute without the graph metadata lock held.

### 4. State snapshot transfer

- `StatePayload` stores versioned last-known-good state.
- `UpdateStateSnapshot` copies state into context ownership.
- `StateSnapshot` returns an independent copy.
- `CaptureStateSnapshot` supports application-provided state providers.
- `TransferStateTo` and `RestoreStateSnapshot` provide explicit state handoff.
- Snapshot tests verify isolation and concurrent updates.

### 5. State-aware runtime replacement

- `StateRestorer` is an optional replacement capability.
- `RecoverWithReplacement` preserves the failed component’s logical DAG identity.
- The replacement receives the last-known-good snapshot before activation.
- Replacement and dependency reconciliation are serialized through the same DAG planner.
- Invalid downstream nodes are temporarily slept and eligible nodes are reactivated without duplicate execution.
- Repeated replacement and concurrent-transition tests protect the graph from leaks and stale recovery attempts.

## Architecture

```text
                         +----------------------+
                         |  Application         |
                         |  Component           |
                         +----------+-----------+
                                    |
                             Name() / Inject()
                                    |
                                    v
                         +----------------------+
                         | DependencyTracker    |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | DependencyGraph      |
                         | nodes / edges / DAG  |
                         +----------+-----------+
                                    |
                       PlanWake / PlanSleep
                                    |
                                    v
                         +----------------------+
                         | DependencyPlan       |
                         | generation + actions |
                         +----------+-----------+
                                    |
                             executePlanLocked
                                    |
                                    v
                    +---------------+---------------+
                    |                               |
                    v                               v
          +-------------------+          +-------------------+
          | Component         |          | ST Context        |
          | OnWakeUp/Sleep    |          | effects/snapshot  |
          +-------------------+          +-------------------+
                                                    |
                                              failure / rollback
                                                    |
                                                    v
                                         +----------------------+
                                         | RecoverWithReplacement|
                                         +----------+-----------+
                                                    |
                                             state restore
                                                    |
                                                    v
                                         Replacement runtime
```

## Repository structure

```text
ddsh/
├── main.go
├── go.mod
├── harness/
│   ├── context.go
│   ├── snapshot.go
│   ├── dependency_graph.go
│   ├── dependency_registration.go
│   ├── dependency_plan.go
│   ├── transition.go
│   ├── execution_internal.go
│   ├── tracker.go
│   ├── recovery.go
│   ├── replacement.go
│   ├── replacement_atomic.go
│   └── *_test.go
├── .github/workflows/go.yml
└── docs/
    ├── README.md
    ├── context.md
    ├── snapshot.md
    ├── dependency-graph.md
    ├── dependency-registration.md
    ├── dependency-plan.md
    ├── transition-execution.md
    ├── tracker.md
    ├── recovery.md
    ├── replacement.md
    ├── tests.md
    └── overall-flow.md
```

## Running the demo

```sh
go run .
```

The demo now exercises the current public architecture: component registration, dependency activation, a last-known-good snapshot, asynchronous failure, `RecoverWithReplacement`, state restoration and final hierarchical rollback.

## Testing

```sh
go test ./...
go test -race ./...
go vet ./...
```

CI tests Go 1.21 and 1.22 and runs formatting validation, `go vet`, and the race-enabled test suite.

## Documentation

Start with [`docs/README.md`](docs/README.md), then read [`docs/overall-flow.md`](docs/overall-flow.md) for the complete runtime sequence.

Each module document includes:

- source file path;
- public/internal type and function names;
- implementation responsibilities;
- paper/Cordis feature mapping;
- explicit notes where `ddsh` is an approximation or extension rather than a literal upstream implementation.

## Relationship to the paper

The current paper describes **revertible effects**, **reactive coeffects**, a unified **context** abstraction, and dynamic composition implemented by Cordis. `ddsh` directly demonstrates the first three runtime mechanisms and adds deterministic DAG planning, generation-based stale-plan protection, state snapshots and runtime replacement as engineering mechanisms for a robust Go implementation.

It does **not** reproduce the complete DeepSeek Harness product, including its web UI, profiles/bundles, session log, LLM adapters, tool pipeline, sandboxing, persistence, telemetry or declarative patch system.

## License

MIT
