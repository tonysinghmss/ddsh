# Dependency Plan Module

**Source:** `harness/dependency_plan.go`

## Purpose

The planner separates **deciding what should happen** from **executing application callbacks**. Plans carry the graph generation at which they were produced, making an old plan invalid after a competing graph transition.

## Types and functions

| Symbol | Kind | Role |
|---|---|---|
| `DependencyActionType` | enum-like type | Wake or sleep action |
| `DependencyActionWake` / `DependencyActionSleep` | constants | Action values |
| `DependencyAction` | struct | Node, action, component and optional context |
| `DependencyPlan` | struct | Immutable action list plus graph generation |
| `DependencyExecutionOptions` | struct | Parent context and recovery strategy for execution |
| `PlanWake` | method | Produces deterministic dependency-aware wake plan |
| `PlanSleep` | method | Produces reverse invalidation/sleep plan |
| `Execute` | method | Executes a plan with default options |
| `ExecuteWithOptions` | method | Public plan execution boundary |
| `validateDependencyPlan` | function | Rejects malformed/duplicate actions |
| `executeWakeAction` | method | Materializes a context when needed and invokes `OnWakeUp` |
| `clearExecutedContext` | method | Clears a completed sleep context |
| `actionMatchesNodeLocked` | method | Ensures an action still describes current graph state |
| `planWakeLocked` | method | Kahn-style wake planning |
| `planSleepLocked` | method | Reverse topological invalidation |
| `topologicalSubsetLocked` | method | Topological order over a candidate subgraph |
| `sortedDependencyNames` | function | Deterministic adjacency ordering |

## Paper mapping

| Paper concept | Implementation | Status |
|---|---|---|
| Reactive coeffects | Dependency state drives wake/sleep planning | **Implemented** |
| Dynamic composition | Plans derive new runtime state from current DAG | **Implemented** |
| Deterministic composition | Sorted Kahn traversal | **Implementation strengthening** |
| Temporal/spatial unification | Plan actions bind graph nodes to ST contexts | **Implemented** |
| Safe interleaving | Generation-based stale-plan rejection | **Implementation strengthening** |

## Critical invariant

`graph.mu` is never held while component callbacks run. The plan is validated under short graph read-lock windows and callbacks execute outside the graph metadata lock. `planMu` serializes transitions so two incompatible plans cannot execute concurrently.
