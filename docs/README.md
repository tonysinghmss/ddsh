# ddsh Architecture Documentation

This directory documents the **current implementation** of `ddsh`, a Go demonstration of the spatiotemporal composability ideas behind Cordis and the DeepSeek Harness "everything is a plugin" paradigm.

> **Scope note:** this project is an educational Go implementation, not a line-for-line port of DeepSeek Harness or Cordis. The mapping below explicitly identifies where the implementation is direct, where it is an approximation, and where the upstream system has capabilities that this repository does not implement.

## Module documentation

| Module | Source | Primary responsibility |
|---|---|---|
| Spatiotemporal Context | `harness/context.go` | Hierarchical contexts, reversible effects, rollback, fault supervision and snapshot boundaries |
| Snapshot Model | `harness/snapshot.go` | Immutable last-known-good state payloads and JSON snapshot serialization |
| Dependency Graph | `harness/dependency_graph.go` | DAG nodes/edges, validation, cycle prevention, topology and generation tracking |
| Dependency Registration | `harness/dependency_registration.go` | Atomic component/coeffect registration and cycle-safe graph construction |
| Dependency Planning | `harness/dependency_plan.go` | Deterministic wake/sleep plans and stale-plan protection |
| Transition Execution | `harness/transition.go` + `harness/execution_internal.go` | Serialized plan transitions and lock-free application callbacks |
| Dependency Tracker | `harness/tracker.go` | Public component/service API over the DAG |
| Recovery | `harness/recovery.go` | State-aware runtime replacement and recovery reconciliation |
| Replacement Planning | `harness/replacement.go` + `harness/replacement_atomic.go` | Downstream invalidation and atomic runtime substitution |
| Tests | `harness/*_test.go` | DAG, lifecycle, state-transfer, recovery and concurrency verification |
| Demo | `main.go` | End-to-end lifecycle simulation |
| CI | `.github/workflows/go.yml` | Formatting, vet, race detector and tests |

## Paper feature mapping

The August 13, 2026 paper describes two orthogonal dimensions: **temporal composability** through revertible effects and **spatial composability** through reactive coeffects. It then unifies them in a context type and component model. `ddsh` implements those core ideas using Go interfaces, hierarchical contexts and a dependency DAG.

### Implemented strongly

- Revertible effects with LIFO registration and rollback.
- Hierarchical context structure with parent/child relationships.
- Reactive dependency resolution through a DAG.
- Component coeffect declaration through `Inject()`.
- Wake/sleep transitions driven by dependency state.
- Deterministic topological planning.
- Stale-plan rejection using graph generations.
- State snapshot transfer across runtime replacement.
- Runtime component replacement while preserving logical DAG identity.
- Downstream dependency reconciliation after replacement.
- Concurrency/race-oriented tests.

### Implemented as a focused approximation

- The upstream Cordis ecosystem has a richer plugin/service/event model. `ddsh` represents the central composability mechanism with a smaller Go `Component` interface.
- Upstream configuration reconciliation and declarative component loading are represented here by programmatic registration and graph transitions.
- Hot module replacement is represented by `RecoverWithReplacement`, not by a complete module loader.
- The demo's fallback callback is intentionally simpler than a full replacement-aware recovery workflow; the production-oriented replacement API lives in `recovery.go`.

### Not implemented by this repository

The project does not attempt to reproduce the complete DeepSeek Harness product: web UI, profiles/bundles, LLM adapters, session/event log, tool execution pipeline, sandboxing, persistence, telemetry, or declarative patch configuration.

## Overall execution flow

See [`overall-flow.md`](overall-flow.md) for the end-to-end lifecycle from component registration through dependency activation, plan execution, failure, state-aware replacement, and rollback.

## Reading order

1. `context.md` — understand the temporal context and rollback boundary.
2. `snapshot.md` — understand last-known-good state ownership.
3. `dependency-graph.md` — understand spatial dependency structure.
4. `dependency-registration.md` — see how `Inject()` becomes DAG edges.
5. `dependency-plan.md` — understand deterministic wake/sleep planning.
6. `transition-execution.md` — understand the callback/lock boundary.
7. `tracker.md` — understand the public orchestration API.
8. `recovery.md` and `replacement.md` — understand failure replacement and reconciliation.
9. `overall-flow.md` — connect all modules.
