# 06 — Prior art

Checked August 2026. Recorded because several of these tools have already solved problems this design was about to solve badly, and one of them has already built a large part of the thesis.

## SQLMesh — virtual data environments

[Snapshots and fingerprints](https://sqlmesh.readthedocs.io/en/stable/concepts/architecture/snapshots/), [virtual data environments](https://www.tobikodata.com/blog/virtual-data-environments)

SQLMesh computes a fingerprint per model from the attributes that affect its output — query, storage format, partitioning scheme. Each snapshot writes to its own physical table. Environments are then a layer of views pointing at snapshots, so creating a new environment is instant for unchanged models and costs physical storage only for models that actually diverged.

Fingerprints are **recursive**: a model's fingerprint incorporates its parents' fingerprints. That detail is what makes environment sharing safe, and it is the part that was easy to miss.

**Adopted.** This is the natural completion of content-addressing, and the original design here did not have it. See [02-identity.md](02-identity.md) for the resulting two-hash split and [03-state-and-log.md](03-state-and-log.md) for the pointer store.

## Airflow 3 — task execution API

[Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)

Airflow 3's central architectural change was removing worker access to the metadata database. Workers now talk to an API server; only the control plane touches the database. They cite security isolation, reduced lock contention, and improved scalability, and it also makes workers language-agnostic and tolerant of version skew.

Getting there cost Airflow a major version and a long migration, because DAG authors had been importing database sessions and models directly for years.

**Adopted.** A `ControlPlaneClient` boundary is defined in [04-compute.md](04-compute.md) and is the only surface user code may touch.

## Flyte — interface hash in the cache key

[Caching](https://www.union.ai/docs/v2/flyte/user-guide/task-configuration/caching/)

Flyte's cache key is composed of the resolved inputs, the fully-qualified task name, a **hash of the task's input and output types**, and a cache version derived from source code. It also supports content-based hashing of large inputs, so a dataframe can participate in the cache key by content rather than by reference.

The interface hash is the piece this design was missing. Without it, an asset whose output schema changes while its code hash stays stable — schema read from config, an upstream contract, a `SELECT *` — reports fresh while serving data of the wrong shape.

**Adopted.** A schema fingerprint is now part of the structural hash. See [02-identity.md](02-identity.md).

## Temporal — journaled decisions and replay

[Durable execution](https://temporal.io/blog/what-is-durable-execution)

Temporal journals every step of a workflow so execution resumes exactly where it stopped after any crash, by replaying event history against deterministic workflow code.

Airflow, Dagster, and Prefect each hand-rolled scheduler crash recovery, and each has had bugs in it.

**Lesson adopted, dependency rejected.** Depending on Temporal would mean running a second stateful system to schedule the first. The discipline is what matters: the scheduler's decision loop is a pure function of state, so recovery is recomputation and decisions are replayable and testable without a cluster.

Worth noting this is structurally easier here than in Dagster. A pure `decide(state)` requires state to be small and authoritative. Dagster's state is a projection over an event log, so its decision function cannot be pure over a small input. Ours can, which is a second dividend from the identity design rather than a separate feature.

## Iceberg v3 — row lineage

[Table spec](https://iceberg.apache.org/spec/), [Snowflake v3 support](https://docs.snowflake.com/en/release-notes/2026/other/2026-03-04-iceberg-v3-support-preview), [AWS deletion vectors and row lineage](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-apache-iceberg-v3-deletion-vectors-row-lineage)

V3 requires tables to track row lineage: `_row_id` and `_last_updated_sequence_number`, assigned by inheritance when a row is first added. As of mid-2026 Spark has the most complete support, Snowflake reached GA in May 2026, AWS shipped it in November 2025, and Trino and Flink are partial.

The important caveat is that whether a write path *preserves* `_row_id` across updates depends on the engine and its Iceberg version.

**Delegation strategy validated, with a refinement.** `RowLineageSource` must report a capability rather than only optionally returning data, so a user on a write path that silently drops row lineage finds out before depending on it. See [05-non-goals.md](05-non-goals.md).

## Bauplan — the closest prior art to the thesis

[Zero-copy, scale-up FaaS for data pipelines](https://arxiv.org/abs/2410.17465)

Bauplan is a serverless lakehouse that already implements a large part of what this project set out to do: Arrow-based transfer between pipeline steps that the paper measures as orders of magnitude faster than the usual write-to-object-storage-then-read pattern, git-for-data semantics over Nessie, and Iceberg outputs that stay readable by any Iceberg engine.

Two consequences, both worth stating plainly.

The thesis is **independently validated** by a team who published on it, which is a stronger signal than any amount of internal conviction.

Zero-copy inter-step handoff can **no longer be the headline novelty.** It is prior art with a paper.

The differentiation survives but moves. Bauplan is a closed-source managed runtime that you adopt wholesale; this is an open-source orchestrator over engines you already run, and the identity and state design is where the remaining novelty lives. The paper should be read before any code is written.

## lakeFS and Nessie — data branching

[Nessie catalog](https://lakefs.io/blog/nessie-catalog/), [data version control](https://lakefs.io/data-version-control/)

Both provide zero-copy branching — lakeFS over object storage, Nessie over catalog metadata. The established orchestration pattern is to branch, run the pipeline into the branch, validate, and merge only on success.

**Not adopted as a dependency.** The environment-pointer design gives the same isolation and atomic-promotion properties without requiring either system, and remains compatible with both for users who already run them.

## Enzyme and Spark Declarative Pipelines — the most important prior art

[Enzyme paper](https://arxiv.org/abs/2603.27775) (SIGMOD 2026, Best Paper Honorable Mention, 18 authors), [Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/), [donation to Apache Spark](https://www.databricks.com/blog/bringing-declarative-pipelines-apache-spark-open-source-project)

Databricks open-sourced its declarative ETL framework — formerly Delta Live Tables — into Apache Spark as Spark Declarative Pipelines. Enzyme is the incremental view maintenance engine underneath it, and it also powers Databricks SQL materialized views. They report cumulative daily compute reduction measured in billions of CPU seconds across thousands of production pipelines.

Read the paper before writing code. The overlap with this design is substantial and the parts that differ are the parts worth understanding.

**How it decides what to recompute.** A cost model estimates total refresh cost as the sum of executor CPU times across joins, aggregates, window functions, shuffles, file scans, and file writes. Estimates are grounded in history: for each operator in a candidate plan, Enzyme finds structurally similar past executions via *normalized physical plan matching* and derives expected CPU time from their observed metrics. Reported accuracy is 87.5% on TPC-DI.

**It plans globally, not per view.** The insight worth stealing outright: an upstream view may be better refreshed incrementally *even when full recomputation would be cheaper for that view*, because incremental refresh produces a smaller change feed and that dramatically reduces cost for everything downstream. Refresh strategy for one node changes the cost of its descendants, so greedy per-node planning is wrong.

**Row lineage through the query plan.** An internal `row_id` column identifies every output tuple at every plan stage. Base tables get stable ids from Delta Lake row tracking; joins derive ids by combining left and right input ids; aggregations use the grouping keys. This is genuine row-level lineage, derived operator by operator — which is precisely the thing declared out of reach for an orchestrator in [05-non-goals.md](05-non-goals.md). They can do it because they own the query planner.

**Non-determinism, handled in three tiers rather than one.** Semantic-preserving rewrites first — an explicit local sort makes `collect_set` deterministic without a cross-node shuffle. Then specialized incrementalization — for temporal filters on `current_timestamp()`, capture the function's value at the previous and current refresh and compute which rows entered and left the window. Only if neither applies does it fall back to full recomputation.

That middle tier is the good idea, and it reframes the problem: they treat the nondeterministic value as an **explicit versioned input** rather than as an opaque property of the query. See [02-identity.md](02-identity.md), where that reframing replaced this design's original single mechanism.

**What it depends on, and this is the whole differentiation story.** Delta Lake change data feed, Delta row tracking, deletion vectors, Delta time travel, Delta transactions for atomic data-plus-provenance commits, and Spark Catalyst. It requires work expressed as trees of relational operators, and it deliberately runs on *normalized* logical plans rather than optimized ones because some Catalyst optimizations destroy the information incrementalization needs.

**Stated constraints.** Some queries cannot be refreshed incrementally at all due to unsupported operators. When more than about 95% of rows change per batch, row-level update costs exceed a full refresh. The cost model is wrong roughly one time in eight.

Full analysis of where this leaves kayavyuh is in [07-differentiation.md](07-differentiation.md). The short version: Enzyme is a refresh optimizer *inside* a relational engine, not a cross-engine scheduler, and the two should compose rather than compete.

## Daft, Lance, and the multimodal engines

[Daft and Ray Data for multimodal](https://mehulbatra.medium.com/a-field-journal-on-ray-data-and-daft-for-multimodal-data-lake-e0c26839f5b5), [Lance format](https://github.com/lance-format/lance), [DuckDB and Polars on Iceberg](https://datalakehousehub.com/blog/2026-05-duckdb-polars-iceberg/)

Daft is an Arrow-native distributed engine built for AI and multimodal work — file fetch, codec, and resample as column operations. Lance is a lakehouse format for multimodal AI with a file format, table format, catalog spec, native data versioning, and vector indexing. DuckDB 1.4 LTS added Iceberg writes and Polars added Iceberg sinks, so both now have complete read-write paths through REST catalogs.

**Daft is adopted as the intended second `ColocatedRuntime`.** This matters more than it sounds: a `Ref` wrapping an Arrow-backed Daft dataframe has far cleaner lifetime semantics than one wrapping a cached Spark DataFrame, whose eviction is not under the caller's control. Open question 1 — whether Tier 2 is a real abstraction or a Ray-only escape hatch — is better answered by Daft than by Spark. See [04-compute.md](04-compute.md).

**Lance is adopted as a `VersionOracle` and `RowLineageSource` target**, since it versions data natively and is the format for exactly the multimodal corpus workloads this project is most useful for.

## Prefect acquiring Dagster Labs

[Announcement](https://www.businesswire.com/news/home/20260713065285/en/Prefect-Acquires-Dagster-Uniting-the-Two-Leading-Modern-Orchestrators), [Dagster's post](https://dagster.io/blog/prefect-is-acquiring-dagster), [analysis](https://thenewstack.io/prefect-acquires-dagster-orchestrator/)

On 13 July 2026 Prefect acquired Dagster Labs. Around forty Dagster staff moved to Prefect; both products keep their names, open-source licences, roadmaps, and pricing; Dagster's founder and CEO became strategic advisers.

Relevant here for one reason. [01-thesis.md](01-thesis.md) argues that Dagster OSS's event-log scaling limits function as the funnel to Dagster+, so there was little incentive to fix them. One company now monetises two overlapping commercial products, which lowers that incentive further rather than raising it.

The counterweight is worth stating with equal force: two well-funded companies with good products consolidated rather than both growing independently. That is evidence the standalone-orchestrator market supports fewer players than it appeared to.

## Deliberately not pursued

**True incremental view maintenance.** See [05-non-goals.md](05-non-goals.md) for the structural argument, which Enzyme sharpened rather than weakened — IVM at this quality requires owning a query planner and a table format's change feed.

**OpenLineage as the internal event format.** Worth emitting, but it is an `AuditLog` implementation rather than an architectural commitment, and the retrofit cost is low. Deferred without prejudice.
