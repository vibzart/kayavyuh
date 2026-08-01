# 02 — Materialization identity

This is the load-bearing document. Everything else is downstream of it.

## The primitives

```
AssetKey       = tuple[str, ...]     # ("warehouse", "orders")
PartitionKey   = str                 # "2026-08-01" or "us-west|2026-08-01"
CodeVersion    = str                 # hash over asset function + declared deps
DataVersion    = str                 # hash over output bytes, OR a foreign snapshot id
ProvenanceHash = str                 # hash over (CodeVersion, sorted input DataVersions)
```

`DataVersion` is deliberately opaque. It may be a content hash the framework computed, or it may be an identifier borrowed from the storage layer — an Iceberg snapshot id, a Delta table version. The framework never interprets it, only compares it.

## The freshness rule

A partition `P` of asset `A` is **fresh** if and only if:

```
stored_provenance(A, P) == H( code_version(A),
                              { (U, data_version(U, map(A, U, P))) for U in upstream(A) } )
```

where `map(A, U, P)` is the partition mapping that resolves which upstream partitions feed `P`.

Three properties follow, and they are the reason for the whole design.

**No history is consulted.** The check reads the current code version and the current upstream data versions. It never scans an event log. Dagster answers the same question by reconstructing state from recorded events; this answers it by comparing two hashes.

**Cost is proportional to fan-in, not to history or to run count.** A pipeline that has run ten million times costs exactly as much to evaluate as one that has run once.

**Reuse becomes possible.** If a provenance hash has been seen before and its output still exists, the computation can be skipped entirely — even across runs, even across branches, even if the last materialization was months ago. This is Nix and Bazel semantics applied to data assets, and it falls out of the identity scheme rather than being bolted on.

## Where data versions come from

The rule above requires `data_version(U, P)` to be knowable without recomputing `U`. Two cases.

**Managed assets.** The framework materialized it, so it recorded the version at commit time. Trivial.

**External and source assets.** Nothing in the system produced it, so the version has to be *observed*. This requires a `VersionOracle` per storage system:

```python
class VersionOracle(Protocol):
    def observe(self, asset: AssetKey, partition: PartitionKey | None,
                location: Location) -> DataVersion | None: ...
```

| storage | observed version |
| --- | --- |
| Iceberg | snapshot id, or sequence number for the partition |
| Delta Lake | table version |
| object storage | digest of (size, etag, last-modified), or content hash if cheap |
| relational table | user-supplied watermark query |
| unobservable | `None` — see below |

Returning `None` means "cannot observe." The asset is then treated as *always potentially stale*, and freshness must come from an explicit policy (a cron, a sensor, a user-declared version) rather than from content. This is an honest degradation rather than a silent lie.

Dagster has this concept as observable source assets, but as an optional add-on beside the event log. Here it is load-bearing: without observation, content-addressing has no ground truth to stand on.

## Non-determinism

**This is the sharpest unsolved problem in the design, and it is getting worse over time.**

Content-addressing assumes that recomputing an asset from the same inputs yields the same output. Increasingly it does not. An asset that calls an LLM, samples a random seed, reads the wall clock, or hits a third-party API produces a different `DataVersion` on every run. Every downstream provenance hash then changes, cascading unconditional staleness through the rest of the graph forever.

The mitigation is an explicit declaration:

```python
@asset(nondeterministic=True)
def enriched_text(ctx, raw: Table) -> Table: ...
```

For such an asset, the recorded `DataVersion` is set to its own `ProvenanceHash` rather than to a digest of its output bytes. In effect the system asserts "this output is *defined* by the inputs and code that produced it, whatever bytes came out." Downstream staleness then propagates on input change, which is the desired behaviour, at the cost of no longer detecting that the output actually changed.

This is a real tradeoff and not obviously the right default. Recorded as an open question in [05-non-goals.md](05-non-goals.md).

## Wide fan-in

Some partition mappings are narrow: a monthly aggregate over daily partitions resolves to about thirty upstream versions. Hashing thirty values per partition is free.

Some are total: a full-table aggregate depends on every upstream partition. With 100,000 upstream partitions, every freshness check hashes 100,000 versions, and any single upstream change invalidates the aggregate. The invalidation is *correct* — the aggregate genuinely did change — but the hashing cost is not acceptable at scheduling frequency.

The intended fix is a **merkle tree over ordinal ranges**. Each asset maintains a tree whose leaves are page-level digests of its per-partition data versions and whose root summarises the whole asset. A total dependency hashes the root, not the leaves. Updating one partition updates `log(n)` nodes rather than rehashing everything.

This also makes the common "did anything at all change upstream?" check a single root comparison.

Not yet designed in detail. Page size and rebalancing on partition-space growth are open.

## Relationship to Dagster's data versions

Dagster does have `DataVersion` and code versions, and it does compute staleness from them. The difference is structural rather than conceptual: in Dagster they are metadata attached to events in a log that remains the authority, so staleness computation still walks the event graph and still requires the log to be complete and queryable. Here the provenance hash *is* the identity, stored in a fixed-size slot per partition, and the log is derived from it rather than the reverse.

The practical consequence is that losing the audit log here costs observability but never correctness. In Dagster, losing the event log loses the system's knowledge of what exists.
