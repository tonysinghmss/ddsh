# Spatiotemporal Context Module

**Source:** `harness/context.go`

## Purpose

`SpatioTemporalContext` is the runtime context that combines the two dimensions of the paradigm: temporal state through reversible effects and spatial structure through parent/child context relationships. It also owns the asynchronous fault boundary and last-known-good snapshot.

## Types and functions

| Symbol | Kind | Role |
|---|---|---|
| `Effect` | type | Cleanup function recorded by a context |
| `RecoveryStrategy` | type | Recovery callback receiving failed context and logical component name |
| `SpatioTemporalContext` | struct | Hierarchical runtime context and lifecycle boundary |
| `NewSTContext` | function | Creates a context, attaches it to an ST parent, and starts supervision |
| `ParentST` | method | Returns the structural parent |
| `RegisterEffect` | method | Registers a cleanup effect in LIFO order |
| `RegisterStateSnapshot` | method | Installs a snapshot provider without invoking it |
| `UpdateStateSnapshot` | method | Commits a copied last-known-good checkpoint |
| `CaptureStateSnapshot` | method | Calls the provider outside the context mutex and commits its result |
| `StateSnapshot` | method | Returns an independent snapshot copy |
| `RestoreStateSnapshot` | method | Installs a snapshot while rejecting older versions |
| `TransferStateTo` | method | Copies a checkpoint into another context |
| `RaiseError` | method | Signals an asynchronous fault |
| `supervise` | method | Internal fault supervisor that captures/preserves state, rolls back and invokes recovery |
| `Rollback` | method | Idempotently closes the context, cascades to children and executes cleanup |
| `addChild` / `removeChild` | methods | Maintain structural parent/child membership |

## Temporal composability mapping

| Paper concept | `ddsh` implementation | Status |
|---|---|---|
| Revertible effects | `RegisterEffect` + `Rollback` | **Implemented** |
| Effect context | `SpatioTemporalContext` | **Implemented** |
| Complete removal of component side effects | Child cascade + effect cleanup | **Implemented for tracked effects/context state** |
| Reverse/LIFO teardown | Effects are prepended and later consumed in stored order | **Implemented** |
| Runtime context lifetime | `NewSTContext` / `Rollback` | **Implemented** |

## Spatial composability mapping

| Paper concept | `ddsh` implementation | Status |
|---|---|---|
| Unified context | `SpatioTemporalContext` | **Implemented** |
| Component-local context | One context per active component | **Implemented** |
| Hierarchical composition | `parent`, `children`, `ParentST` | **Implemented** |
| Fault isolation boundary | `supervise` + context rollback | **Implemented** |

## Important implementation boundary

The paper's formal effect system is broader than a Go callback slice. `ddsh` tracks only effects explicitly registered with `RegisterEffect`; arbitrary external side effects remain the application's responsibility.
