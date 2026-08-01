# Documentation

Design phase. No user-facing documentation exists yet, because nothing runs yet.

## Design documents

Read in order — each depends on the previous one.

| document | contents |
| --- | --- |
| [design/01-thesis.md](design/01-thesis.md) | The problem, and why four complaints about Dagster are two root causes |
| [design/02-identity.md](design/02-identity.md) | Content-addressed materialization identity and the freshness rule. The load-bearing document |
| [design/03-state-and-log.md](design/03-state-and-log.md) | Bitmap partition state, ordinal invariants, paging, the separated audit log |
| [design/04-compute.md](design/04-compute.md) | Two-tier compute, the planner's edge decision, the tier-independence invariant |
| [design/05-non-goals.md](design/05-non-goals.md) | Deliberate refusals with reasons, and the open questions |
| [design/interfaces.py](design/interfaces.py) | Every protocol in one importable, dependency-free file |

## The three commitments

Stated here so they are easy to find and easy to attack.

**Materialization identity is content-addressed, not temporal.** A partition is identified by a hash of its code version and its resolved input versions. Staleness is a hash comparison rather than a walk over recorded history, which makes freshness evaluation independent of how many times the pipeline has ever run.

**Current state and history are stored separately, because they are different questions.** State is small, transactional, constantly mutated, and lives in a deliberately non-pluggable store. History is append-only, unbounded, and lives wherever the adopter wants. Losing history costs observability; it never costs correctness.

**Engines that can share memory are allowed to, without forking the programming model.** A narrow `Launcher` that every engine implements, plus an optional `ColocatedRuntime` for engines that can host a live multi-step session. Asset code is identical either way.

## What would falsify this

The design is worth exactly as much as these tests coming back clean, and none of them have been run.

1. One unmodified asset module runs under both the subprocess `Launcher` and the Ray `ColocatedRuntime`, with no tier-aware branching anywhere in the asset body. ([04-compute.md](design/04-compute.md))
2. `Colocation` can be implemented for Spark without weakening `Ref` into something meaningless. ([open question 1](design/05-non-goals.md))
3. Postgres `BYTEA` with compare-and-swap sustains a million partitions per asset at scheduling frequency. ([open question 4](design/05-non-goals.md))
4. Non-deterministic assets — anything calling an LLM — do not cascade unconditional staleness through the graph. ([02-identity.md](design/02-identity.md))

## Intended next step

A throwaway prototype of roughly two thousand lines: the provenance/freshness engine, the bitmap partition store on SQLite, one subprocess `Launcher`, and the Ray `ColocatedRuntime`. No scheduler, no UI, no Postgres, no packaging.

Its only job is to answer test 1 above. If the same asset definition cannot run under both tiers untouched, the rest of the design does not matter yet.
