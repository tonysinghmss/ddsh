# Dependency Registration Module

**Source:** `harness/dependency_registration.go`

## Purpose

This module turns a `Component`'s declared requirements into DAG structure. It performs registration as one atomic graph mutation: if the resulting graph would contain a cycle, no component node or dependency edge is partially installed.

## Types and functions

| Symbol | Kind | Role |
|---|---|---|
| `registerComponentAtomic` | method | Installs component identity, creates missing provider placeholder nodes and edges, and advances generation |
| `findCycleInAdjacency` | function | DFS cycle detector over a proposed adjacency map |
| `joinPath` | function | Formats a cycle path for diagnostics |

## Paper mapping

| Paper concept | Implementation | Status |
|---|---|---|
| Component coeffect specification | `Component.Inject()` requirements | **Implemented** |
| Reactive spatial constraints | Requirements become provider→dependent DAG edges | **Implemented** |
| Dynamic component composition | Registration can create nodes/providers incrementally | **Implemented** |
| Atomic composition consistency | Proposed adjacency is checked before mutation | **Strong implementation safeguard** |

## Registration flow

1. Validate the component name and each declared provider.
2. Copy the requirement list before graph processing.
3. Build a proposed adjacency view.
4. Run cycle detection before mutating the live graph.
5. Create the component node and any missing provider placeholder nodes.
6. Create provider→dependent edges.
7. Increment the graph generation.
8. Let `TransitionWake` decide whether the new component can immediately run.

This separates **structural registration** from **runtime eligibility**, which is important for the planner's deterministic semantics.
