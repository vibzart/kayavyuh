# 07 — Differentiation from Enzyme

Enzyme is the strongest system in this space and the closest to parts of this design. This document works out where it structurally cannot reach, because that is the only honest basis for building something else.

## The question is not "how do we beat Enzyme"

Enzyme answers: *given a materialized view defined by a relational query over Delta tables in Spark, refresh it for minimum compute.*

kayavyuh answers: *given a graph of assets produced by arbitrary compute — Ray tasks, Spark jobs, dbt models, GPU embedding jobs, Python functions — decide which need recomputing, and run them.*

Those are different layers. Enzyme is a refresh optimiser inside one engine. kayavyuh is a scheduler across engines. **The correct relationship is composition, not competition**: for the subgraph of assets that are relational queries over Delta tables in Spark, kayavyuh should delegate to Spark Declarative Pipelines and let Enzyme do the incrementalisation, because it will do it better than this project ever will.

Any differentiation claim that requires beating Enzyme at incremental view maintenance is a bad claim. The four below do not.

## Enzyme's boundary conditions

Stated precisely, from the paper, because the differentiation is entirely a function of these:

- work must be expressed as **trees of relational operators** — Spark DataFrames or SQL
- base-table row identity comes from **Delta Lake row tracking**
- change detection comes from **Delta change data feed**
- atomicity comes from **Delta transactions**
- plan analysis requires **Catalyst**, on normalised rather than optimised plans
- some operators cannot be incrementalised at all, and it falls back to full recompute
- above roughly 95% row churn per batch, incremental costs more than full refresh
- the cost model is wrong about one time in eight

## 1. Relational operators are the unit, and AI work has none

This is the strongest differentiation and it is structural rather than a matter of effort.

Enzyme derives granularity from relational algebra. A join combines row ids; an aggregation uses grouping keys. That machinery has nothing to say about:

- embed this text with a model
- OCR this manuscript page
- transcribe this audio segment
- call an LLM to extract structured fields
- run inference while keeping the actor warm

To Catalyst, a Python UDF is an opaque box. Enzyme cannot derive row lineage through it, cannot incrementalise it, and its cost model — built on per-operator CPU metrics for joins, shuffles and scans — has no basis for estimating it.

So on multimodal and AI preprocessing, which is where the workload is growing, **Enzyme degrades to full recomputation**. That is exactly kayavyuh's granularity, except Enzyme arrives there with none of the partition machinery: no per-partition state, no bitmap backfill, no partition-level provenance, no warm-actor fan-out.

kayavyuh's unit of work is a partition of arbitrary compute. That is a worse unit than a relational operator when a relational operator is available, and a far better one when it is not.

## 2. Single engine, single format

Enzyme requires Delta change data feed, Delta row tracking, deletion vectors, Delta time travel, Delta transactions, and Catalyst. None of that is available for a dbt model on BigQuery, a Ray job writing Lance, a Snowflake table, or a Flink stream into Iceberg.

The paper describes a modular architecture "designed for generalization across data sources and query engines." That is architectural aspiration, not shipped capability, and even fully generalised it remains IVM over relational queries.

Heterogeneity is this project's premise rather than a roadmap item. `VersionOracle` exists so that Iceberg snapshot ids, Delta versions, Lance versions, and object-store digests are all first-class on day one.

## 3. Enzyme minimises refresh cost; it does not manage environments

Enzyme has no concept of staging versus production, no atomic promotion, no rollback of a bad deploy, no CI environment that inherits unchanged assets. It refreshes tables in place.

The environment-pointer design in [03-state-and-log.md](03-state-and-log.md) is on an axis Enzyme does not operate on, and it yields a capability Enzyme structurally lacks: **reuse across environments and branches, not merely incremental refresh within one table's history.** If a provenance hash was computed in staging, production can point at that result without recomputing anything. Enzyme has no content-addressed store to make that possible, because refreshing in place is its whole model.

Databricks has adjacent capability in Unity Catalog and Lakeflow, but it is not Enzyme's concern and it is not content-addressed.

## 4. Enzyme decides *how* to refresh; kayavyuh decides *where* and *whether*

Enzyme's decision space is incremental-versus-full for a given view on a given cluster. It cannot decide which engine should run the work, on what hardware, with what parallelism, whether a warm actor should be reused, or whether the result already exists somewhere else under the same provenance.

Those are scheduling decisions, and they are the ones that matter most for GPU-bearing pipelines where hardware placement dominates cost.

## What to steal

Two things, without reservation.

**Graph-global cost planning.** Their insight generalises well beyond IVM: refreshing an upstream node incrementally can be worth it *even when full recompute is cheaper for that node*, because it yields a smaller change feed and reduces downstream cost. Restated for this design: **how an asset is materialised changes the cost of its descendants, so planning must be graph-global rather than greedy per-asset.** This design currently has no cost model at all — the planner picks colocated versus persisted edges on a hand-waved size estimate. Recorded as an open question.

Their grounding method transfers directly. They match *normalised physical plan shape* against historical executions and derive expected CPU from observed metrics. The analogue is matching a normalised step signature — asset, snapshot, partition count, input sizes — against historical run profiles from the audit log. The audit log already exists for this; it just was not being used for planning.

**The three-tier non-determinism taxonomy.** Theirs is rewrite-to-determinism, then specialised incrementalisation, then fall back to recompute. This design had one mechanism where they have three, and the middle tier is the insight: for `current_timestamp()` in a filter they capture the function's value at the previous and current refresh and compute which rows entered and left the window.

That treats the nondeterministic value as an **explicit versioned input** rather than as an opaque property of the computation, and it generalises to LLM assets far better than this design's original hack. See [02-identity.md](02-identity.md), where it replaced that hack.

## The honest risk

Two things could compress the space this project occupies.

Spark Declarative Pipelines is now in Apache Spark, and it performs orchestration: correct execution order, maximum parallelism, retries escalating from task to flow to pipeline. For Spark-centric shops **the orchestrator is being absorbed into the engine from below**, at the same time the vendor layer consolidates from above via the Prefect–Dagster merger. Both squeeze the standalone-orchestrator category.

And if Databricks does generalise Enzyme across engines and formats as the paper says it is architected to, the delegation story becomes a dependency story.

The defence is the same in both cases, and it is the reason to build this at all: **be the layer that handles compute with no relational structure.** That is where neither Enzyme nor Spark Declarative Pipelines can follow without becoming a different kind of system, and it is where the growth in data work currently is.
