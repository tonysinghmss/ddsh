# Snapshot Module

**Source:** `harness/snapshot.go`

## Purpose

The snapshot subsystem provides a small, explicit state-transfer contract for failure recovery. It treats the latest known-good state as a versioned payload and copies byte slices at context boundaries so a failed runtime and its replacement do not share mutable backing storage.

## Types and functions

| Symbol | Kind | Role |
|---|---|---|
| `StatePayload` | struct | Version, capture time and state bytes |
| `ErrNoStateSnapshot` | error | Indicates that no checkpoint exists |
| `SnapshotProvider` | function type | Application callback that materializes state |
| `RestoreHandler` | function type | Application-side restore contract; the context does not invoke it directly |
| `newStatePayload` | function | Internal payload constructor with byte copying |
| `StatePayload.clone` | method | Deep-copy payload helper |
| `MarshalJSONState` | function | Serializes JSON-compatible application state |

## Paper mapping

| Paper/Cordis idea | Implementation | Status |
|---|---|---|
| Context carries runtime state | `StatePayload` owned by `SpatioTemporalContext` | **Focused implementation** |
| Replacement after failure | Snapshot can be transferred/restored before replacement wake | **Implemented** |
| State continuity across composition changes | `TransferStateTo` / `RestoreStateSnapshot` | **Implemented** |
| Immutable/context-isolated state boundary | `clone` and byte copying | **Implemented** |

The paper itself is about spatiotemporal composability rather than prescribing this exact snapshot API. Snapshot transfer is therefore documented as an extension that makes the replacement/recovery story practical rather than as a claim of literal paper API parity.

## Safety properties

- Providers execute without the context mutex held.
- State entering a context is copied.
- State leaving a context is copied.
- Older snapshot versions cannot overwrite newer local checkpoints.
- Explicit checkpoints are preserved during asynchronous failure rather than replaced by potentially inconsistent live state.
