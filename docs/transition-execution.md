# Transition and Execution Module

**Sources:** `harness/transition.go`, `harness/execution_internal.go`

## Purpose

These files provide the runtime boundary between graph planning and application callbacks. `TransitionWake` and `TransitionSleep` serialize a transition, construct a generation-stamped plan, release graph metadata locks, and then execute the plan.

## Symbols

| Symbol | Kind | Role |
|---|---|---|
| `TransitionWake` | method | Plan and execute a wake transition |
| `TransitionSleep` | method | Plan and execute a sleep transition |
| `wrapNodeNotFound` | function | Creates a typed missing-node error |
| `dependencyNodeNotFoundError` | type | Missing-node error preserving `ErrDependencyNodeNotFound` through `Unwrap` |
| `executePlanLocked` | method | Internal serialized executor for validated plans |

## Paper mapping

| Paper concept | Implementation | Status |
|---|---|---|
| Reactive response to context changes | Transition methods re-plan on service state changes | **Implemented** |
| Reversible component lifecycle | Sleep invokes `OnSleep` then context rollback | **Implemented** |
| Unified runtime context | Wake materializes `SpatioTemporalContext` per component | **Implemented** |
| Interleaved dynamic composition safety | `planMu` + generation validation | **Implementation strengthening** |

## Execution sequence

`TransitionWake(name)`:

1. Normalize parent context.
2. Acquire `planMu`.
3. Lock graph metadata.
4. Verify node exists.
5. Compute wake actions.
6. Advance generation if state changed.
7. Release graph lock.
8. Execute each action without graph lock.
9. Reject execution if generation/action state no longer matches.

`TransitionSleep(name)` follows the same boundary but computes invalidated dependents and executes them in reverse topological order.

The design is deliberately stricter than a direct callback chain: the graph is the source of truth, and callbacks cannot mutate graph metadata under a graph lock.
