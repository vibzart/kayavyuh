# 03 — State and log

Two stores, split along the seam described in [01-thesis.md](01-thesis.md).

| | state store | audit log |
| --- | --- | --- |
| answers | what is currently true | what happened |
| size | small, bounded by partition count | unbounded, grows forever |
| access | read on every tick, mutated constantly | append-only, scanned for analytics |
| needs | transactions, CAS, low latency | throughput, compression, cheap retention |
| pluggable | **no** — Postgres or SQLite | **yes** — anything |
| losing it | correctness failure | observability failure |

## Why the state store is not pluggable

This is the most likely place for the project to make Dagster's mistake, so the reasoning is recorded explicitly.

Dagster already has a pluggable state layer: `DagsterInstance` with swappable `EventLogStorage`, `RunStorage`, and `ScheduleStorage`, implemented for Postgres, MySQL, and SQLite. That pluggability is *why* the event log is a row-per-event table. A row-per-event table is the intersection of what those three backends can all do well. **An abstraction that admits every backend is forced to the semantics of the weakest one.**

The design here depends on capabilities that are not universal: atomic compare-and-swap on a binary blob at high frequency, real transactions across several blob columns, and sub-millisecond reads. Postgres has all of these. SQLite has all of these. Snowflake has none of them — it has no row-level locking, DML latency measured in hundreds of milliseconds to seconds, and micro-partition rewrites on update. Using an OLAP warehouse as an orchestrator state store is a category error, not a slow configuration.

So the state store ships with exactly two implementations, and they are chosen because they support the same semantics rather than because they are popular.

**Postgres** for anything real. Adopters get "use the database we already run" for free through wire compatibility — Aurora, RDS, Neon, CockroachDB, Yugabyte all work with no abstraction layer and no second SQL dialect to maintain.

**SQLite** for local development and single-node deployments. This is also the "easy to get started" story: `pip install kayavyuh`, no container, no compose file.

The audit log is where "point it at your existing stack" is genuinely delivered, and Snowflake works fine there.

## Partition state layout

Per asset, four things are stored.

```
asset_state(asset_key) = {
    logical_time      : int                        # monotonic, for CAS
    status            : {PartitionStatus -> Bitmap}  # roaring, keyed by ordinal
    data_version      : Vec<DataVersion>           # indexed by ordinal
    provenance_hash   : Vec<ProvenanceHash>        # indexed by ordinal
}
```

Bitmaps answer set questions cheaply: how many partitions are materialized, which are failed, which are missing, set difference between requested and completed. A roaring bitmap over 100,000 partitions with dense runs is a few kilobytes.

**Bitmaps do not answer version questions.** Freshness needs the actual hash at an ordinal, so the two version vectors are separate and are the larger objects — roughly 32 bytes per partition per vector, so about 6 MB total for 100,000 partitions.

That size is the reason for paging.

## Paging

Rewriting a 6 MB blob because one partition materialized is unacceptable. The vectors are therefore stored in fixed-size pages by ordinal range — 65,536 ordinals per page as a starting point.

A backfill over one contiguous time range touches one or two pages. A transaction commits only the pages it dirtied. The interface exposes whole-asset reads but accepts an optional ordinal range so callers can scope them, and implementations are free to fetch lazily.

Page size is not yet validated. See open questions.

## Writes

All mutation goes through one atomic call:

```python
def apply(self, txn: StateTransaction) -> LogicalTime: ...
```

`StateTransaction` carries an `expected` logical time for optimistic concurrency, a batch of `Materialization` records, and a batch of bulk `StatusDelta` records. Applying it either succeeds and returns a new logical time, or raises `ConcurrentModification` and changes nothing.

A `Materialization` sets status, `data_version`, and `provenance_hash` at one ordinal. A `StatusDelta` carries an *ordinal set* and a target status, so bulk transitions are one bitmap operation regardless of cardinality.

This is where the central claim about writes lands. Requesting a 20,000-partition backfill is one `apply()` containing one `StatusDelta` with 20,000 ordinals — one bitmap union and one page write, not 20,000 inserts.

## Ordinals

Bitmaps require every partition key to map to a dense, stable integer. That constraint propagates into the partition definition, and it forces decisions that Dagster left soft.

```python
class PartitionDef(Protocol):
    def ordinal(self, key: PartitionKey) -> int: ...
    def key(self, ordinal: int) -> PartitionKey: ...
    def cardinality(self) -> int: ...
```

The invariants are strict and permanent.

**Ordinals are stable for the lifetime of the asset.** Nothing may ever be renumbered. A renumber silently corrupts every bitmap and every version vector at once, with no way to detect it after the fact.

**Ordinals are dense.** Roaring tolerates sparsity but degrades, and sparse allocation wastes vector slots. Time partitions are naturally dense — ordinal is periods elapsed since the definition's start. Dynamic partitions get ordinals from an append-only registry that only ever assigns the next unused integer.

**Deletion is a tombstone.** A removed partition keeps its ordinal, permanently allocated, marked in a `tombstoned` bitmap. Reclaiming ordinals would require renumbering.

**Changing granularity creates a new asset.** Converting a daily partition definition to hourly is not a mutation. There is no correct mapping from old ordinals to new ones, and pretending otherwise is where Dagster's partition semantics get sharp. Granularity change means a new asset identity plus an explicit migration that the user writes.

**Multi-dimensional partitions use a row-major index over the dimension cross-product.** Cardinality is the product of the dimensions, so a sparsely populated two-dimensional space allocates ordinals it never uses. Roaring's run-length encoding absorbs this for the status bitmaps, but the version vectors pay for it. Sparse multi-dimensional spaces are a known weak spot.

## The audit log

Deliberately tiny, because everything about it should be swappable:

```python
class AuditLog(Protocol):
    def append(self, events: Sequence[Event]) -> None: ...
    def scan(self, filter: EventFilter) -> Iterator[Event]: ...
```

Two methods is the whole contract. Parquet on object storage, ClickHouse, Postgres, BigQuery, and Snowflake are all reasonable implementations.

The invariants matter more than the interface.

**State commits first, the log appends second.** The log is derived and never authoritative.

**`append` must be idempotent on `event_id`.** Delivery is at-least-once, and a retry after a partial failure must not duplicate history.

**A failed log write is a monitoring incident, not a correctness incident.** The system still knows exactly what is materialized and whether it is fresh, because that lives in the state store.

One thing not to overclaim: the log cannot be rebuilt from state. State holds only current values, so losing the log permanently loses history. The claim is that losing it costs observability and never correctness — which is precisely the inversion of a system where the event log *is* the state.

## Leases

The scheduler needs single-writer semantics for its own bookkeeping, so the state store also provides leases with holder identity and TTL. This is another capability an OLAP backend cannot supply, and another reason the state store is not pluggable.

Whether a single leased scheduler is sufficient, or whether scheduling should shard by asset across several holders, is unresolved.
