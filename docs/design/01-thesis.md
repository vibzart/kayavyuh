# 01 — Thesis

## Four complaints

The project began as four separate frustrations with Dagster.

1. Postgres is used as the partition materialization tracker, with a row written per event per partition, so a large backfill produces tens of thousands of inserts and then reads them back to answer "is this asset up to date?"
2. There is no first-class Ray integration; the sanctioned boundary is Pipes, a subprocess protocol.
3. The UI becomes unusable past a few hundred assets, and the runs-versus-assets split makes the mental model harder than the problem.
4. There is no lineage granularity below a partition.

## One root cause

Complaints 1 and 4 are the same bug: **the orchestrator maintains its own copy of state that the storage layer already holds, in a database shaped wrong for the access pattern.**

Dagster's `event_logs` table conflates two different things.

The **audit trail** is append-only, high-volume, never queried transactionally, and read for analytics.
The **current state** is small, mutated constantly, read on every scheduling tick, and needs transactional guarantees.

Putting both in one row-per-event OLTP table means the write path is sized for the audit trail and the read path is sized for the state, and neither is good. The `AssetStatusCacheValue` layer is a patch over the design rather than a fix.

Complaint 4 follows from the same instinct. Dagster tracks lineage at whatever granularity it records events at, so lineage stops where its own bookkeeping stops. But the warehouse already knows more than that. Iceberg v3 tracks row lineage natively via `_row_id` and `_last_updated_sequence_number`. An orchestrator that treats the table format as the authority on data identity gets finer lineage for free and stops maintaining a shadow copy.

Complaint 2 is a different root cause: Dagster's execution model assumes a step is a process. That assumption runs from the old `StepLauncher` API through to Pipes. It means two assets in the same run cannot hand an Arrow table to each other through shared memory — they serialize it to object storage and read it back, having already had it in memory. Pipes cannot fix this, because Pipes *is* a subprocess protocol.

Complaint 3 is real but it is a grind, not a thesis. It is not a moat and it is not what this design is about.

## Why the gap persists

Worth stating plainly, because it determines whether the gap is likely to close on its own.

Dagster+ has a different state backend from Dagster OSS. The scaling limits of the open-source event log are the commercial funnel. There is no strong incentive inside Dagster Labs to make the OSS partition tracker scale, and the recurring answer in their support channels to high-partition-count performance complaints is to consider the hosted product.

That is a legitimate business model, but it means complaint 1 is structural rather than a backlog item.

## What follows

Three design commitments come out of this.

**Separate the two storage concerns along the seam that already exists.** Current state gets a small, opinionated, transactional store. The audit trail gets a wide-open pluggable sink, where "point it at the warehouse your analysts already use" is a real feature rather than a compromise. See [03-state-and-log.md](03-state-and-log.md).

**Make materialization identity content-addressed rather than temporal.** "Partition P was materialized at time T by run R" is a fact about history. "Partition P currently holds the output of code version C applied to inputs with versions V" is a fact about identity, and it is the one scheduling actually needs. Staleness becomes a hash comparison. See [02-identity.md](02-identity.md).

**Let engines that can share memory share memory, without forking the programming model.** A narrow universal interface for every engine, plus an optional richer interface for engines that can host a live multi-step runtime. See [04-compute.md](04-compute.md).

## What this thesis does not claim

It does not claim the resulting system will be adopted.
Orchestrators have the highest switching cost in a data platform, and a correct design is necessary but nowhere near sufficient.

It does not claim feature parity is close.
Partition mapping correctness, backfill semantics under partial failure, freshness policies, sensors, secrets, and a dbt integration good enough for a 500-model project are all prerequisites for anyone to evaluate this seriously, and most of them are semantic problems that are discovered from real usage rather than designed up front.

The claim is narrower: the three commitments above are correct, they are load-bearing, and they are hard to retrofit. If they are right, they are worth getting right before anything is built on top of them.
