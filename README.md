# ddsh: Spatiotemporal Composability Framework

A lightweight, deadlock-free, high-performance execution framework in Go implementing the foundational principles of Spatiotemporal Composability as introduced by the DeepSeek ecosystem (Cordis paradigm).This framework provides an autonomous runtime environment optimized for AI agents and distributed systems. It guarantees structural flexibility alongside strict, predictable temporal boundary isolations during hot-swaps, component upgrades, or runtime systemic failures.

## Architectural Core Principles
Traditional plugin systems rely on rigid interfaces and hardcoded static hooks. This repository shifts the paradigm by breaking composition down into two orthogonal runtime dimensions:
1. Spatial Composability (The Structural Dimension)Implicit Capability Mapping: Components do not explicitly declare or register interfaces via explicit keywords. Instead, they implicitly satisfy runtime shapes purely by exposing matching method footprints.Spatio-Reactive Dependencies: Components describe their structural constraints as coeffects (declared via the Inject() block). Modules remain completely Asleep in memory until all nested environmental dependencies are satisfied, at which point the registry reactively wakes them up.

2. Temporal Composability (The Clock & Timeline Dimension)Dynamic Revertible Effects: Rather than using hardcoded teardown loops, actions register atomic inverse cleanup logic (Effect) on the fly. If an operation breaks or unloads, the framework automatically unwinds the runtime state in an append-only, Last-In, First-Out (LIFO) order.Asynchronous Fault Containment: Every subagent or component executes inside a self-similar, isolated tree context. An internal failure or panic fires localized teardown processes and self-healing hooks without threatening or leaking resource space into parent orchestrator timelines.

## System Topology & Flow
The tree topology enforces strict hierarchical propagation: parent contexts govern children, and children can fail and reconstruct without corrupting global execution roots.

       [rootCtx (Standard Go Context)]
                     │
         [Core-Orchestrator-Agent]      <── Parent Node (Tracks Root Effects)
                     │
       ┌─────────────┴─────────────┐
       │                           │
 [Primary-Data-Subagent]      [Backup-Substitute-Subagent] <── Children Nodes
 (Fails & self-destructs)  (Spawned via Recovery Hook)


## Key Framework Features

### Deadlock-Free Rollbacks: 
Refactored lock release pattern copies context metadata and frees internal mutex structures before propagating cascades to child nodes or evaluating block metrics.

### Concurrent Teardown Pipeline:
When winding down expansive software state trees, a bounded worker pool concurrently consumes the LIFO effect channel buffer, accelerating resource release times under dense system load.

### Non-Orphaned Autonomous Healing:
Built-in traversal hooks allow failed contexts to securely consult parent trees before destruction. This ensures newly generated hot-swap substitutes are re-nested into the active living hierarchy seamlessly.

## Directory & File Structure


ddsh/
├── go.mod             # Go Module Declaration
├── harness/
│   ├── context.go     # Spatiotemporal Context & Concurrent Rollback Engine
│   └── tracker.go     # Coeffect Evaluator & Service Dependency Tracker
└── main.go            # Operational Simulation Driver
