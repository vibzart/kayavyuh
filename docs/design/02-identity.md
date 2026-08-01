# 02 — Materialization identity

This is the load-bearing document. Everything else is downstream of it.

## The primitives

```
AssetKey           = tuple[str, ...]     # ("warehouse", "orders")
PartitionKey       = str                 # "2026-08-01" or "us-west|2026-08-01"

CodeVersion        = str                 # hash over asset function + declared deps
SchemaFingerprint  = str                 # hash over declared input/output types

SnapshotId         = str                 # STRUCTURAL identity  -- where bytes live
ProvenanceHash     = str                 # PER-PARTITION identity -- whether bytes are fresh

DataVersion        = str                 # hash over output bytes, OR a foreign snapshot id
```

`DataVersion` is deliberately opaque. It may be a content hash the framework computed, or an identifier borrowed from the storage layer — an Iceberg snapshot id, a Delta table version. The framework never interprets it, only compares it.

## Two hashes, two jobs

The original version of this document had one hash and it did not work. The split is the most important correction in the design, and the reason for it is worth recording so it is not collapsed back later.

**`SnapshotId` is structural and recursive.**

```
SnapshotId(A) = H( code_version(A),
                   schema_fingerprint(A),
                   { SnapshotId(U) for U in upstream(A) } )
```

It answers *where do this asset's bytes live*: `(asset, snapshot_id) → Location`. It changes only when code or schema changes, in `A` or in any structural ancestor. For a stable pipeline it changes on deploys, not on data arrival.

The recursion is borrowed from SQLMesh, where fingerprints incorporate parents' fingerprints. It is not decoration — see the collision argument below.

**`ProvenanceHash` is per-partition and data-dependent.**

```
ProvenanceHash(A, P) = H( SnapshotId(A),
                          { (U, DataVersion(U, p))
                            for U in upstream(A)
                            for p in map(A, U, P) } )
```

It answers *is this partition fresh*. It changes whenever any upstream partition it depends on gets new data, which is constantly.

## Why one hash cannot do both

Two failure modes, and avoiding both is what forces the split.

**Keying physical storage by provenance hash explodes the table count.** Provenance changes on every upstream data change. A daily pipeline would create a new physical table per partition per day, forever.

**Keying physical storage by `(asset, code_version)` alone leaks between environments.** Suppose staging and production run identical code but staging is fed a different upstream dataset. Both compute asset `A`, partition `P`. Same code version, so same physical table — but different provenance hashes competing for the same slot. One silently overwrites the other.

The recursive `SnapshotId` closes both. Divergent upstream structure produces a divergent snapshot id and therefore a separate physical table. Identical structure shares one.

That gives the invariant that makes zero-copy environment sharing sound:

> **Identical `SnapshotId` implies identical logic and identical structural ancestry.**
> Therefore any partition materialized under a snapshot is valid for *every* environment pointing at that snapshot.

Sharing physical data between environments is safe not by convention but by construction. See [03-state-and-log.md](03-state-and-log.md) for the pointer store this enables.

## The freshness rule

A partition `P` of asset `A` is **fresh** if and only if:

```
stored_provenance(A, P) == ProvenanceHash(A, P)   # recomputed from current upstream versions
```

Three properties follow, and they are the reason for the whole design.

**No history is consulted.** The check reads the current snapshot id and the current upstream data versions. It never scans an event log. Dagster answers the same question by reconstructing state from recorded events; this answers it by comparing two hashes.

**Cost is proportional to fan-in, not to history or to run count.** A pipeline that has run ten million times costs exactly as much to evaluate as one that has run once.

**Reuse becomes possible.** If a provenance hash has been seen before and its output still exists, the computation can be skipped entirely — across runs, across environments, across branches, even if the last materialization was months ago. This is Nix and Bazel semantics applied to data assets, and it falls out of the identity scheme rather than being bolted on.

## Why the schema fingerprint is in there

Borrowed from Flyte, whose cache key includes a hash of the task's input and output types alongside its source-code version.

Without it, an asset whose output schema changes while its code hash stays stable reports fresh while serving data of the wrong shape. That happens more easily than it sounds: schema read from a config file, a column list supplied by an upstream contract, a `SELECT *` over a table that gained a column.

The fingerprint covers the asset's declared input and output types. What computes it depends on the type system in play — an Arrow schema, a Pydantic model, a dbt contract, an Iceberg schema id. The framework only needs a stable hash.

Adding this later would invalidate every stored hash in every deployment and force a global rebackfill, which is why it is here now rather than in the open questions.

## Where data versions come from

The freshness rule requires `DataVersion(U, P)` to be knowable without recomputing `U`. Two cases.

**Managed assets.** The framework materialized it, so it recorded the version at commit time. Trivial.

**External and source assets.** Nothing in the system produced it, so the version has to be *observed*:

```python
class VersionOracle(Protocol):
    def observe(self, asset: AssetKey, partition: PartitionKey | None,
                location: Location) -> DataVersion | None: ...
```

| storage | observed version |
| --- | --- |
| Iceberg | snapshot id, or sequence number for the partition |
| Delta Lake | table version |
| Lance | native dataset version — first-class, and the format for multimodal corpora |
| object storage | digest of (size, etag, last-modified), or content hash if cheap |
| relational table | user-supplied watermark query |
| unobservable | `None` — see below |

Returning `None` means "cannot observe." The asset is then treated as *always potentially stale*, and freshness must come from an explicit policy — a cron, a sensor, a user-declared version — rather than from content. This is an honest degradation rather than a silent lie.

Dagster has this concept as observable source assets, but as an optional add-on beside the event log. Here it is load-bearing: without observation, content-addressing has no ground truth to stand on.

## Non-determinism

Content-addressing assumes that recomputing an asset from the same inputs yields the same output. Increasingly it does not. An asset that calls an LLM, samples a random seed, reads the wall clock, or hits a third-party API produces a different `DataVersion` on every run, and every downstream provenance hash then changes, cascading unconditional staleness through the graph forever.

This document originally offered one mechanism for that. Enzyme offers three, and its middle tier reframes the problem well enough that the original single mechanism was demoted to a fallback.

**The reframing: non-determinism is usually an undeclared input, not a property of the computation.**

Enzyme handles `current_timestamp()` inside a filter predicate by capturing the function's value at the previous and current refresh and computing which rows entered and left the temporal window. The nondeterministic value is treated as an *explicit versioned input* rather than as an opaque source of chaos. Once it is named, everything downstream is deterministic again.

That generalises directly to the case this project cares most about. An LLM call is not inherently unhashable — it is a function of a model identity, a prompt template, a temperature, and a seed. Declare those and the asset becomes deterministic given its declared inputs:

```python
@asset(inputs={"model": "some-model@2026-01",
               "prompt": sha256(TEMPLATE),
               "temperature": 0.0,
               "seed": 7})
def enriched_text(ctx, raw: Table) -> Table: ...
```

Now the provenance hash covers them. Re-running with the same declared inputs is legitimately cacheable, and bumping the model version correctly invalidates every downstream asset — which is the behaviour anyone maintaining an embedding pipeline actually wants and which no amount of `nondeterministic=True` provides.

**Three tiers, in order of preference.**

**1. Declare the hidden input.** Model version, prompt hash, seed, a snapshotted clock value. Restores full content-addressing, exact reuse, and correct downstream invalidation. This should be the strongly encouraged default, and the documentation should treat reaching for tier 2 as a smell.

**2. Pin the data version to the provenance hash.** For genuinely stochastic assets — sampling at temperature above zero, an external API with no version to observe. The recorded `DataVersion` becomes the asset's own `ProvenanceHash` rather than a digest of its output bytes, so the system asserts "this output is *defined* by what produced it, whatever bytes came out." Stops the staleness cascade at the cost of no longer detecting that the output changed.

**3. Always recompute.** For assets that must reflect live external state, where a stale-but-consistent answer is worse than an expensive one. Enzyme's equivalent is its fallback to full recomputation, and it exists for the same reason: sometimes there is no honest shortcut.

The remaining open question is narrower than it was, and worth stating precisely: whether tier 2 should exist as a user-facing option at all, or whether every asset that cannot reach tier 1 should be forced to tier 3. Tier 2 trades correctness of change detection for cost, silently. Recorded in [05-non-goals.md](05-non-goals.md).

## Wide fan-in

Some partition mappings are narrow: a monthly aggregate over daily partitions resolves to about thirty upstream versions. Hashing thirty values per partition is free.

Some are total: a full-table aggregate depends on every upstream partition. With 100,000 upstream partitions, every freshness check hashes 100,000 versions, and any single upstream change invalidates the aggregate. The invalidation is *correct* — the aggregate genuinely did change — but the hashing cost is not acceptable at scheduling frequency.

The intended fix is a **merkle tree over ordinal ranges**. Each asset maintains a tree whose leaves are page-level digests of its per-partition data versions and whose root summarises the whole asset. A total dependency hashes the root, not the leaves. Updating one partition updates `log(n)` nodes rather than rehashing everything.

This also makes the common "did anything at all change upstream?" check a single root comparison.

Not yet designed in detail. Page size and rebalancing on partition-space growth are open.

## Relationship to Dagster's data versions

Dagster does have data versions and code versions, and it does compute staleness from them. The difference is structural rather than conceptual: in Dagster they are metadata attached to events in a log that remains the authority, so staleness computation still walks the event graph and still requires the log to be complete and queryable. Here the two hashes *are* the identity, stored in fixed-size slots, and the log is derived from them rather than the reverse.

The practical consequence is that losing the audit log here costs observability but never correctness. In Dagster, losing the event log loses the system's knowledge of what exists.
