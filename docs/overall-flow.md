# Overall ddsh Execution Flow

## 1. Component registration

```text
Application Component
        |
        | Name() + Inject()
        v
DependencyTracker.RegisterComponentErr
        |
        v
DependencyGraph.registerComponentAtomic
        |
        +--> validate names
        +--> build proposed adjacency
        +--> reject cycle atomically
        +--> create provider/component nodes
        +--> create provider -> dependent edges
        v
TransitionWake
```

## 2. Dependency-driven wake

```text
TransitionWake(name)
        |
        v
planWakeLocked
        |
        +--> collect reachable dependents
        +--> calculate missing-provider counts
        +--> deterministic topological traversal
        +--> mark eligible nodes active
        v
DependencyPlan{Generation, Actions}
        |
        v
executePlanLocked
        |
        +--> validate generation/action against graph
        +--> materialize SpatioTemporalContext if needed
        +--> Component.OnWakeUp(ctx)
        +--> component registers Effects
```

The key separation is **planning vs execution**. Graph locks are not held while application callbacks execute.

## 3. Temporal context lifecycle

```text
NewSTContext
   |
   +--> attach to structural parent
   +--> start supervisor goroutine
   |
   v
OnWakeUp
   |
   +--> RegisterEffect(...)
   +--> optional UpdateStateSnapshot(...)
   |
   v
Running Context
```

Effects represent reversible runtime changes. They are retained by the context until rollback.

## 4. Service activation/deactivation

```text
External service state change
        |
        +--------------------+
        |                    |
      online               offline
        |                    |
        v                    v
 TransitionWake       TransitionSleep
        |                    |
        v                    v
 wake eligible         invalidate dependents
 components            in reverse topology
```

For multiple providers, the current implementation keeps a dependent eligible when at least one provider remains active.

## 5. Failure and state-aware recovery

```text
Active Component
      |
      | RaiseError
      v
SpatioTemporalContext.supervise
      |
      +--> preserve explicit last-known-good checkpoint
      +--> otherwise attempt provider capture
      |
      v
Rollback failed context
      |
      +--> rollback children
      +--> unwind registered effects
      +--> detach from parent
      |
      v
Recovery strategy / RecoverWithReplacement
      |
      v
StateSnapshot
      |
      v
Replacement Context
      |
      +--> RestoreStateSnapshot
      +--> optional StateRestorer.RestoreState
      |
      v
replaceNodeRuntimeUnlocked
      |
      +--> preserve logical node name
      +--> preserve DAG edges
      +--> publish replacement component/context
      +--> identify invalid descendants
      +--> plan eligible dependents
      v
executePlanLocked
      |
      +--> wake replacement
      +--> reconcile downstream runtime
      v
Recovered DAG
```

## 6. Top-level demo flow (`main.go`)

The current demo performs four scenarios:

1. Create the root ST context and register `Primary-Data-Subagent` with a `database` coeffect.
2. Activate `database`, causing the dependency tracker to wake the worker.
3. Raise an asynchronous worker error and invoke the configured fallback recovery callback.
4. Roll back the root context, demonstrating downward lifecycle cleanup.

The demo is intentionally simpler than the library's newest recovery API. `main.go` currently demonstrates the original context-level fallback path; `RecoverWithReplacement` is the more complete state-aware DAG replacement API covered by `recovery_test.go`.

## 7. Paper-to-runtime correspondence

```text
Paper: Temporal composability
        |
        +--> Revertible effects
        |       |
        |       +--> RegisterEffect / Rollback
        |
        +--> Runtime context
                |
                +--> SpatioTemporalContext

Paper: Spatial composability
        |
        +--> Reactive coeffects
                |
                +--> Component.Inject()
                +--> DependencyGraph
                +--> PlanWake / PlanSleep
                +--> TransitionWake / TransitionSleep

Unified runtime model
        |
        +--> DependencyPlan binds DAG state to ST contexts
        +--> Recovery replaces runtime while retaining logical DAG identity
```

## 8. CI flow

```text
Push / Pull Request
        |
        v
.github/workflows/go.yml
        |
        +--> checkout
        +--> Go 1.21 / 1.22 matrix
        +--> gofmt validation
        +--> go vet ./...
        +--> go test -race ./...
```

This verifies both functional correctness and the concurrency assumptions that are central to the implementation.
