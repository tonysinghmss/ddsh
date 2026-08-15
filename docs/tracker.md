# Dependency Tracker Module

**Source:** `harness/tracker.go`

## Purpose

`DependencyTracker` is the public orchestration facade. It exposes component registration and service activation/deactivation without requiring callers to manipulate graph internals directly.

## Types and functions

| Symbol | Kind | Role |
|---|---|---|
| `Component` | interface | Minimal plugin-like contract: `Name`, `Inject`, `OnWakeUp`, `OnSleep` |
| `DependencyTracker` | struct | Owns graph reference, recovery callback and last error |
| `NewDependencyTracker` | function | Creates tracker and graph |
| `Graph` | method | Exposes graph for advanced callers |
| `GetActiveContext` | method | Finds a currently active component context |
| `RegisterComponent` | method | Non-error-returning convenience registration API |
| `RegisterComponentErr` | method | Atomic registration plus wake transition |
| `ActivateService` | method | Convenience service activation |
| `ActivateServiceErr` | method | Error-returning service activation |
| `DeactivateService` | method | Convenience service deactivation |
| `DeactivateServiceErr` | method | Error-returning service deactivation |
| `LastError` | method | Returns tracker's most recent operation error |
| `LastErrorIfCurrent` | method | Compatibility helper keyed by graph generation |
| `clearError` / `recordError` | methods | Internal error-state management |

## Paper mapping

| Paper concept | Implementation | Status |
|---|---|---|
| Component abstraction | `Component` interface | **Strong approximation** |
| Coeffect declaration | `Inject()` | **Implemented** |
| Reactive component activation | `ActivateServiceErr` → `TransitionWake` | **Implemented** |
| Reactive component deactivation | `DeactivateServiceErr` → `TransitionSleep` | **Implemented** |
| Everything-is-a-plugin philosophy | Components are registered dynamically behind one contract | **Focused Go approximation** |

## API guidance

Use the `Err` variants when building production-style callers because they expose graph, plan and recovery errors. The non-`Err` variants are intended for simple demos and preserve failures through `LastError()`.

For advanced recovery, use `RecoverWithReplacement` rather than constructing an unrelated context in a raw recovery callback; the replacement API preserves logical DAG identity and reconciles dependents.
