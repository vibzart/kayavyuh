# 05 — Non-goals and open questions

## Deliberate refusals

Each of these is a thing a reasonable person will ask for, and the reason for saying no is recorded so the decision can be revisited on its merits rather than re-argued from scratch.

### A pluggable state store for arbitrary databases

Refused because a generic abstraction is forced to the semantics of its weakest backend, which is precisely how Dagster's row-per-event log came to exist. Full reasoning in [03-state-and-log.md](03-state-and-log.md).

Postgres and SQLite only. Postgres wire compatibility already covers Aurora, RDS, Neon, CockroachDB, and Yugabyte with no additional code.

Snowflake, BigQuery, and other OLAP warehouses are explicitly rejected as state stores — no row-level locking, DML latency in the hundreds of milliseconds, micro-partition rewrite on update. They are first-class as *audit log* sinks, which is what they are good at.

### Row-level lineage computed by the orchestrator

Refused because the orchestrator does not touch rows. It hands work to Ray, Spark, or dbt and is told the work finished. Any row lineage it claimed to compute would be inferred rather than observed.

Instead it is read from the storage layer where it genuinely exists:

```python
class RowLineageCapability(Enum):
    NONE = "none"                              # backend tracks nothing
    READ = "read"                              # readable, but writes may drop it
    PRESERVED_ON_UPDATE = "preserved_on_update" # survives updates and merges

class RowLineageSource(Protocol):
    def capability(self, location: Location) -> RowLineageCapability: ...
    def row_lineage(self, asset: AssetKey, partition: PartitionKey | None,
                    version: DataVersion) -> RowLineageRef | None: ...
```

Iceberg v3 tracks `_row_id` and `_last_updated_sequence_number` natively, and the Iceberg implementation returns a reference into that.

The capability method is not decoration. As of mid-2026, v3 row lineage support is uneven — Spark is the most complete, Snowflake reached GA in May 2026, AWS shipped it in November 2025, and Trino and Flink are partial. Whether a given *write* path preserves `_row_id` across updates depends on the engine and its Iceberg version. A user who builds a compliance workflow on row lineage and is silently running a write path that drops it has been actively misled, so the capability is reported rather than inferred.

Backends with nothing to offer report `NONE`, and the UI shows the granularity actually available rather than a fabricated one.

This means the original complaint — no lineage below a partition — is answered by *delegation*, not by implementation. Users on a storage layer with no row lineage get no row lineage, and that is the honest answer.

### Universal colocated execution

Refused because it is not achievable. Snowflake cannot hold a Python object between steps. Tier 2 is opt-in per engine and most engines will never have it. See [04-compute.md](04-compute.md).

### A general workflow engine

The unit of work is an asset — something that exists in storage and has a version. Not an arbitrary task with side effects.

This is a real limitation and it is chosen. "Send an email," "call an API," and "wait for a human" are legitimate orchestration needs that this system will handle badly. Airflow and Temporal are better answers for those, and the boundary should be stated rather than blurred.

### True incremental view maintenance

Refused, and Enzyme sharpened the reason rather than weakening it.

Materialize and Feldera maintain results incrementally through differential dataflow. Databricks' Enzyme does it for data engineering specifically, and does it well enough to report compute savings in billions of CPU seconds per day. This is unambiguously a better answer to "what changed" than partition-level recompute, wherever it applies.

**It requires owning a query planner and a table format's change feed.** Enzyme derives an internal `row_id` at every stage of the plan — base tables from Delta row tracking, joins by combining left and right input ids, aggregations from grouping keys — and detects change through Delta change data feed inside Delta transactions. Pull out Catalyst or pull out Delta and the technique stops working.

An orchestrator owns neither. It hands work to an engine and is told the work finished. Building IVM here would mean becoming a query engine, which is refused separately and permanently below.

The consequence is a genuine capability gap and it should be stated rather than spun: for relational transformations over Delta tables in Spark, Enzyme will always beat this project's granularity. The right response is to **delegate that subgraph to Spark Declarative Pipelines** rather than to compete with it, and to be better where relational structure does not exist at all — which is most AI and multimodal work, and where Enzyme itself degrades to full recomputation. Full argument in [07-differentiation.md](07-differentiation.md).

Partitions are the granularity here. Below that, the answer is [what the table format tracks](#row-level-lineage-computed-by-the-orchestrator).

### A compute or query engine of its own

Never. The project schedules other engines and stores state about them.

## Open questions

Unresolved, roughly in order of how much damage a wrong answer does.

**1. Does the tier-independence invariant survive Spark?**
Ray's object store and Spark's cached-`DataFrame` model are very different — Spark's cache lifetime is bound to a `SparkSession`, eviction is not under the caller's control, and a `Ref` wrapping a cached DataFrame has weaker guarantees than one wrapping an `ObjectRef`. If `Colocation` cannot be implemented cleanly for Spark, then Tier 2 is a Ray-specific escape hatch wearing the costume of an abstraction, and it should be renamed and rescoped accordingly. This is the single most important thing the prototype has to answer.

**2. Should the provenance-pinning tier exist as a user-facing option at all?**
Narrower than it was, after adopting Enzyme's three-tier framing in [02-identity.md](02-identity.md). Tier 1 — declare the hidden input, so an LLM asset is keyed on model version, prompt hash, temperature, and seed — is clearly right and should be the encouraged default. Tier 3, always recompute, is clearly honest. The question is tier 2: pinning `DataVersion` to the asset's own `ProvenanceHash` trades away the ability to detect that an output changed, in exchange for cost, and it does so *silently*. That may be an unacceptable thing to offer as a flag. Forcing everything that cannot reach tier 1 down to tier 3 is more expensive and easier to reason about.

**2b. Does the planner need a cost model, and can the audit log supply it?**
This design currently has none — colocated-versus-persisted edges are chosen on a hand-waved size estimate. Enzyme demonstrates both the need and a method: estimate cost as summed executor CPU across operators, and ground the estimate by matching *normalised plan shape* against structurally similar historical executions. The analogue is matching a normalised step signature — asset, snapshot, partition count, input sizes — against historical run profiles, which the audit log already records and nothing currently reads for planning.

Their harder insight is that planning must be **graph-global**: materialising an upstream asset one way can be worth it even when a different way is cheaper for that asset, because it lowers cost for everything downstream. A greedy per-asset planner gets this wrong by construction. Enzyme's own cost model is right about seven times in eight, which is a useful calibration for how much to trust one.

**3. Snapshot garbage collection.**
Introduced as a hard requirement by the environment-pointer design in [03-state-and-log.md](03-state-and-log.md), and it did not exist before that. Every deploy that changes a snapshot id leaves physical tables behind, and nothing reclaims them. The hazards are the familiar content-addressed-store ones: never collect a snapshot any environment points at, never collect one an in-flight run is writing to, and keep a retention window long enough that rollback stays possible for more than one deploy. Nix, Bazel, and container registries all have this problem and none solved it trivially. Unsolved here, and it is a correctness issue rather than a housekeeping one — collecting a live snapshot destroys data.

**4. Who owns asset identity — this project or the catalog?**
Iceberg REST catalogs, Unity Catalog, and Polaris all maintain table identity and metadata pointers, which is a large overlap with the environment pointer store. Being catalog-native would mean environments *are* catalog namespaces and pointer flips are catalog operations, which is elegant and cedes control. Being catalog-agnostic means maintaining a parallel mapping and reconciling drift. The overlap is real either way and picking late means building the wrong one first.

**5. Merkle tree design for wide fan-in.**
Page size, rebalancing as the partition space grows, and whether the root is maintained eagerly on commit or lazily on read. Sketched, not designed.

**6. Does Postgres `BYTEA` plus compare-and-swap hold at a million partitions per asset?**
Page size of 65,536 ordinals is a guess. Needs a real benchmark before it is a decision, and the benchmark is cheap to write.

**7. One leased scheduler, or scheduling sharded by asset?**
A single writer is simpler and almost certainly sufficient at first. Sharding needs to be at least *possible* without changing the state interface, or the ceiling gets built in.

**8. Sparse multi-dimensional partition spaces.**
Row-major ordinals over a dimension cross-product allocate slots that may never be used. Roaring absorbs this for status bitmaps; the version vectors do not. Unclear how bad this gets in practice.

**9. What is the CLI called?**
`kayavyuh` is eight characters and gets typed dozens of times a day. Precedent says decouple it from the project name — Kubernetes has `kubectl`, PostgreSQL has `psql`. Undecided.

**10. Apache-2.0 or BSL?**
Determines whether a cloud vendor can host the project and whether some enterprises will adopt it. Effectively irreversible once external contributions arrive, so it has to be settled before the first one is accepted.

## Prerequisites for anyone to evaluate this seriously

Not open questions — known work, listed so the gap between "the design is right" and "this is usable" stays visible.

Partition mapping across mismatched granularities. Backfill semantics under partial failure. Freshness policies and declarative auto-materialization. Sensors and event-driven triggers. Secrets and config. Kubernetes deployment. A dbt integration good enough that someone with a 500-model project does not hit a wall.

Most of these are semantic problems rather than coding problems, which means they are discovered from real usage and cannot be shortened by writing code faster.
