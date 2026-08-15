# Test and Verification Module

**Sources:** `harness/dependency_graph_test.go`, `harness/dependency_plan_test.go`, `harness/tracker_dag_test.go`, `harness/concurrency_test.go`, `harness/recovery_test.go`

## Coverage

| Test area | Evidence |
|---|---|
| DAG chain/diamond/multiple dependencies | `dependency_graph_test.go` |
| Cycle rejection and atomicity | `dependency_graph_test.go`, `tracker_dag_test.go` |
| Concurrent graph reads/registration | `dependency_graph_test.go` |
| Deterministic wake/sleep plans | `dependency_plan_test.go` |
| Generation and stale-plan rejection | `dependency_plan_test.go`, `tracker_dag_test.go` |
| Callback lock boundaries | `dependency_plan_test.go`, `tracker_dag_test.go` |
| Service activation/deactivation | `tracker_dag_test.go` |
| Multi-parent eligibility | `tracker_dag_test.go`, `recovery_test.go` |
| Snapshot round trip and isolation | `concurrency_test.go` |
| Snapshot provider lock boundary | `concurrency_test.go` |
| Asynchronous fault containment | `concurrency_test.go` |
| Rollback idempotency | `recovery_test.go` |
| Runtime replacement and state restoration | `recovery_test.go` |
| Deep-chain and diamond replacement | `recovery_test.go` |
| Repeated replacement without graph leaks | `recovery_test.go` |
| Recovery concurrent with transitions | `recovery_test.go` |

## Paper mapping

The tests are not formal proofs of the paper's calculus. They are executable invariants for the Go implementation of its central runtime ideas: effects must unwind safely, coeffect changes must propagate consistently, and dynamic composition must remain structurally valid under concurrency.

## CI verification

The canonical workflow is `.github/workflows/go.yml` and runs:

- `gofmt -l .` validation
- `go vet ./...`
- `go test -v -race -timeout 3m ./...`
- Go 1.21 and Go 1.22 matrix builds

The repository previously stored this workflow under a malformed directory name containing a leading space. The documentation branch moves it to the conventional `.github/workflows/go.yml` path so GitHub Actions can discover it.
