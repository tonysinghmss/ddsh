# Recovery Module

**Source:** `harness/recovery.go`

## Purpose

`RecoverWithReplacement` is the production-oriented recovery path added after the initial context-level recovery callback. It replaces the runtime component behind an existing logical node, transfers the failed context's checkpoint, optionally invokes application-defined state restoration, and then reconciles the dependency graph through the same planner used for ordinary transitions.

## Types and functions

| Symbol | Kind | Role |
|---|---|---|
| `StateRestorer` | interface | Optional replacement capability: `RestoreState(StatePayload) error` |
| `ErrReplacementNameMismatch` | error | Prevents replacement of one logical node with another name |
| `ErrReplacementUnavailable` | error | Indicates missing target/runtime/snapshot prerequisites |
| `RecoverWithReplacement` | method | Performs state-aware logical runtime replacement |

## Paper mapping

| Paper/Cordis idea | Implementation | Status |
|---|---|---|
| Component replacement | `RecoverWithReplacement` | **Focused implementation** |
| Hot replacement | Runtime swap while preserving DAG node name and edges | **Implemented approximation** |
| Temporal rollback | Failed context checkpoint + `Rollback` | **Implemented** |
| Spatial reconciliation | Replacement executes a `DependencyPlan` | **Implemented** |
| State continuity | `StateSnapshot` → `RestoreStateSnapshot` / `StateRestorer` | **Extension beyond core paper mechanism** |

## Recovery sequence

1. Validate replacement component and logical name.
2. Read the failed logical node and context.
3. Obtain its latest checkpoint.
4. Determine structural parent.
5. Create replacement context.
6. Restore checkpoint into the replacement context.
7. If implemented, invoke `StateRestorer.RestoreState` outside harness locks.
8. Serialize the graph transition with `planMu`.
9. Re-check that the failed runtime has not already been replaced.
10. Atomically replace runtime state while retaining logical DAG identity.
11. Execute the generated reconciliation plan.

This prevents recovery from becoming an out-of-band callback chain that can diverge from graph state.
