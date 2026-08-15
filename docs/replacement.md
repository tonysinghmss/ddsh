# Replacement Planning Module

**Sources:** `harness/replacement.go`, `harness/replacement_atomic.go`

## Purpose

Replacement planning determines which downstream components must temporarily sleep when a provider/runtime is replaced and which can remain active. The atomic runtime swap then publishes the new component/context and creates a plan that wakes eligible dependents.

## Symbols

| Symbol | Kind | Role |
|---|---|---|
| `invalidDescendantsLocked` | method | Finds active descendants that lose all valid providers |
| `planEligibleDependentsLocked` | method | Finds downstream nodes that can wake after replacement |
| `replaceNodeRuntimeUnlocked` | method | Atomically swaps component/context while preserving node identity and edges |

## Paper mapping

| Paper concept | Implementation | Status |
|---|---|---|
| Dynamic composition | Runtime node can change implementation | **Implemented approximation** |
| Spatial reconciliation | Invalid/eligible descendants computed from DAG | **Implemented** |
| Temporal lifecycle preservation | Old runtime is represented by its context and rolled back as needed | **Implemented** |
| Hot module replacement | `replaceNodeRuntimeUnlocked` + generated plan | **Focused implementation** |

## Replacement behavior

For a deep chain `A → B → C → D`, replacing active `B` temporarily invalidates downstream nodes whose provider set no longer satisfies the implementation's eligibility rule. The replacement then becomes active and the planner reactivates eligible downstream nodes in deterministic order.

For multi-parent nodes, a remaining active provider keeps the dependent eligible under the current OR-style implementation. Tests cover this explicitly, including a diamond graph and repeated replacement cycles.

The graph node's logical identity and edges are preserved; only the runtime `Component` and `SpatioTemporalContext` are replaced.
