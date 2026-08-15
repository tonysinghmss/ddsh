# Dependency Graph Module

**Source:** `harness/dependency_graph.go`

## Purpose

`DependencyGraph` is the spatial-composability core. It stores component nodes and provider→dependent edges, rejects cycles, exposes topology, tracks activity and increments a generation whenever graph state changes.

## Types and functions

| Symbol | Kind | Role |
|---|---|---|
| `dependencyNode` | internal struct | Component, context, provider/dependent edges and active state |
| `dependencyEdge` | internal struct | Directed provider→dependent relationship |
| `dependencyEdgeKey` | internal struct | Stable edge-map key |
| `DependencyGraph` | struct | Thread-safe DAG plus transition serialization mutex |
| `DependencyNodeInfo` | struct | Safe public node view |
| `NewDependencyGraph` | function | Initializes node/edge maps |
| `RegisterNode` | method | Adds a node atomically under graph serialization |
| `RegisterDependency` | method | Adds an edge after cycle detection |
| `SetNodeActive` | method | Changes activity metadata and generation |
| `HasNode` / `HasDependency` | methods | Membership queries |
| `Nodes` / `Node` | methods | Deterministic node inspection |
| `DependenciesOf` / `DependentsOf` | methods | Relationship queries |
| `EdgeCount` | method | Number of edges |
| `Validate` | method | Checks node/edge consistency and acyclicity |
| `TopologicalOrder` | method | Kahn-style deterministic topological ordering |
| `Generation` | method | Current graph generation |
| `validateDependencyNodeName` | function | Node-name validation |
| `validateLocked` | method | Internal structural consistency validation |
| `validateAcyclicLocked` | method | Internal DAG validation |
| `findPathLocked` / `reconstructDependencyPath` | methods | Cycle-prevention path search |

## Paper mapping

| Paper concept | Implementation | Status |
|---|---|---|
| Spatial composability | `DependencyGraph` | **Implemented** |
| Coeffect dependency declaration | Graph edges created from component requirements | **Implemented** |
| Reactive dependency management | Wake/sleep transitions consume graph state | **Implemented** |
| Component dependency structure | Provider/dependent edges | **Implemented** |
| Dynamic composition | Registration + activation/deactivation | **Implemented** |
| Cycle-safe composition | Atomic cycle detection | **Implemented** |

## Key semantic detail

A component with multiple providers is considered eligible when at least one provider remains active in the current transition model. The tests explicitly establish this OR-style eligibility. This is an implementation decision and should not be described as a universal statement about every Cordis coeffect semantics.
