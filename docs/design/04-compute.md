# 04 — Compute

## Why a single compute interface cannot work

The obvious design is one `ComputeEngine` interface that Ray, Spark, Snowflake, dbt, and Kubernetes all implement. It is the wrong design, and the reason is the same as for the state store: the interface collapses to the intersection of its implementations.

Work out what that intersection contains. Snowflake cannot hold a Python object in memory between two steps. A Kubernetes Job cannot hand anything to the next Job except through storage. So the universal interface is necessarily `submit work → poll until done → read the result from external storage`.

That is Pipes. Building it would reproduce exactly the limitation this project exists to remove.

## Two tiers

**Tier 1 — `Launcher`.** Universal, narrow, required. Submit a unit of work, poll its state, stream its logs, cancel it, collect the materializations it reported. Inputs are resolved to storage locations and outputs are written to storage locations. Every engine implements this: Spark, Databricks, Snowflake, dbt, Kubernetes Job, local subprocess, Flink.

**Tier 2 — `ColocatedRuntime`.** Optional, rich, per-engine. Open a session that hosts several steps inside one live runtime, and pass in-memory handles between them instead of storage locations. Ray implements this fully. Snowflake will never implement it. Spark can implement a partial version and that is an open question.

Tier 1 is what makes the project usable. Tier 2 is what makes it differentiated.

## The control plane boundary

Adopted from Airflow 3, whose central architectural change was removing worker access to the metadata database. Workers there now talk to an API server; only the control plane touches the database.

Stated as a rule for this project:

> **User code and workers never touch the state store.**
> The only surface they may use is a `ControlPlaneClient`.

```python
class ControlPlaneClient(Protocol):
    def resolve_input(self, asset: AssetKey,
                      partition: PartitionKey | None) -> Location | Ref: ...
    def report_materialization(self, m: Materialization) -> None: ...
    def emit_event(self, kind: str, payload: Mapping[str, object]) -> None: ...
    def heartbeat(self, handle: LaunchHandle) -> None: ...
    def get_config(self, key: str) -> object: ...
```

Four things follow, and only the first is obvious.

**Workers can be written in any language.** An asset implemented in Rust, Go, or Java needs an HTTP client, not a Postgres driver and a Python ORM.

**The state schema can evolve without redeploying workers.** Bitmap paging, merkle roots, and the environment pointer store are all internal changes behind the client, so none of them becomes a breaking change for people who have written assets.

**Long-running work survives version skew.** A worker started before a control-plane upgrade keeps working against a versioned API rather than breaking on a schema migration mid-run.

**Credentials stop leaking outward.** Asset code never needs database credentials, which matters as soon as anyone runs untrusted or third-party assets.

Enforcement has to be structural rather than documented, because the failure mode is people importing what is reachable. State store types live in a package that user-facing code does not depend on, and the distribution that asset authors install does not contain them at all. Airflow needed a major version and a long migration precisely because that boundary was missing for years and DAG authors had already built on its absence.

## The scheduler is a pure function of state

Not a compute tier, but the same concern: what is allowed to hold state.

Airflow, Dagster, and Prefect each hand-rolled scheduler crash recovery and each has had bugs in it. Temporal's answer is to journal every decision and replay it against deterministic code. The dependency is not worth taking — it would mean running a second stateful system to schedule the first — but the discipline is:

```python
class Decider(Protocol):
    def decide(self, world: WorldState) -> Sequence[Action]: ...
```

`decide` performs no IO, holds no state between calls, and consults no clock it was not handed. Everything it needs arrives in `WorldState`, read from the state store by the caller.

Three payoffs. Crash recovery is recomputation rather than reconstruction. The loop is unit-testable against a literal state fixture, with no database and no cluster. And scheduling decisions become replayable, so "why did it not run yesterday?" is answerable by feeding yesterday's state back in.

This is structurally easier here than in Dagster, for a reason that traces back to the identity design: a pure `decide` needs state to be small and authoritative. Dagster's state is a projection over an event log, so its decision function cannot be pure over a small input. Ours can.

It is also close to unachievable as a retrofit. Once a daemon accumulates caches, in-flight bookkeeping, and incidental clock reads, purity cannot be imposed after the fact.

## Tier 1

```python
class Launcher(Protocol):
    name: str
    def submit(self, work: WorkUnit) -> LaunchHandle: ...
    def poll(self, handle: LaunchHandle) -> LaunchState: ...
    def logs(self, handle: LaunchHandle, since: Cursor | None) -> Iterator[LogRecord]: ...
    def cancel(self, handle: LaunchHandle) -> None: ...
    def results(self, handle: LaunchHandle) -> Sequence[Materialization]: ...
```

`WorkUnit` carries the steps, the resolved input locations, the intended output locations, the code version per asset, and configuration. A launcher that can host a connected subgraph may receive several steps at once; a launcher that cannot receives one.

`results` returns `Materialization` records rather than raw success or failure, because the data version has to come back from wherever the bytes were actually written. For a launcher that cannot introspect its own output, the version is supplied by the relevant `VersionOracle` after the fact.

`LaunchHandle` must be serialisable and must survive orchestrator restart. If the control plane dies mid-run, it has to be able to reattach to work that is still running rather than orphaning or duplicating it.

## Tier 2

```python
class ColocatedRuntime(Protocol):
    name: str
    def open(self, plan: SubPlan) -> Colocation: ...

class Colocation(Protocol):
    def load(self, asset: AssetKey, location: Location) -> Ref: ...
    def execute(self, step: Step, inputs: Mapping[AssetKey, Ref]) -> Ref: ...
    def fanout(self, step: Step, keys: Sequence[PartitionKey],
               inputs: Mapping[AssetKey, Ref]) -> Sequence[Ref]: ...
    def persist(self, ref: Ref, location: Location) -> DataVersion: ...
    def release(self, ref: Ref) -> None: ...
    def close(self) -> None: ...
```

`Ref` is an opaque in-cluster handle. For Ray it wraps an `ObjectRef`. For a Spark implementation it would wrap a cached `DataFrame`. The orchestrator never looks inside it.

The important signature is `execute`, which takes `Ref` and returns `Ref`. Two consecutive steps in the same colocation never touch storage. `persist` is called only where the plan says a value must become durable.

`fanout` exists because partition fan-out is the case that matters most. A 20,000-partition backfill on Ray becomes one job with 20,000 tasks scheduled against a warm cluster, rather than 20,000 processes or pods each paying full startup and import cost.

What Ray specifically buys, concretely: zero-copy Arrow handoff through the object store, actor reuse so an expensive setup like a loaded model happens once instead of per partition, GPU scheduling and placement groups, and Ray's own lineage-based task retry underneath the orchestrator's durable cross-run retry.

### Daft is the intended second implementation, not Spark

An interface with one implementation is a fiction, so Tier 2 needs a second one to be credible. Spark was the obvious candidate and it is the wrong one to attempt first.

A `Ref` wrapping a cached Spark `DataFrame` is weak: its lifetime is bound to a `SparkSession`, eviction is not under the caller's control, and the guarantees are materially worse than those of a Ray `ObjectRef`. Building against it first would produce an abstraction shaped by its weakest member — the same failure recorded for the state store in [03-state-and-log.md](03-state-and-log.md).

Daft is Arrow-native and distributed, built for AI and multimodal work, and an Arrow-backed dataframe handle has clean, explicit lifetime semantics. It is a genuinely different engine from Ray while still being able to satisfy `Colocation` honestly.

So open question 1 — is Tier 2 a real abstraction or a Ray-only escape hatch wearing a costume — should be answered with Ray plus Daft. If Spark can be added later, good; if it cannot, that is a fact about Spark's memory model rather than a refutation of the design.

### Delegate relational work rather than competing for it

For the subgraph of assets that are relational transformations over Delta tables in Spark, the right `Launcher` target is Spark Declarative Pipelines, so that Enzyme performs the incrementalisation.

Enzyme derives row-level change through the query plan and reports compute savings measured in billions of CPU seconds per day. This project will not beat that for relational work and should not try. Handing that subgraph over is not a concession — it is the reason a narrow universal `Launcher` exists. See [07-differentiation.md](07-differentiation.md).

## How the planner decides

For each edge from upstream `U` to downstream `D`, the edge is **colocated** if all of the following hold:

- `U` and `D` are assigned to the same runtime instance and the same colocation session
- `U` is not declared `durable=True` by the user
- the planner's estimate of the value's size fits the runtime's memory budget

Otherwise the edge is **persisted**: `U` writes to storage, `D` reads from storage.

Everything else about the system is unchanged by that decision. Provenance hashing, staleness, and state commits work identically — a colocated edge still produces a `DataVersion`, derived from the in-memory value rather than from written bytes.

## The invariant

> **An asset definition is written once and is byte-identical under both tiers.**
> The tier decides only whether an edge is a `Ref` or a `Location`.
> If any asset body needs to know which tier it is running under, the abstraction has failed and the design is wrong.

Asset bodies therefore receive and return *values*, never paths:

```python
@asset(partitions=daily("2024-01-01"))
def orders(ctx, raw_orders: Table) -> Table:
    return raw_orders.filter(...)
```

Dagster's IO managers already do this much. The difference is that in Dagster, materialising at every edge is a fixed property of the framework; here, whether an edge hits storage is a planner decision.

## The falsifiable test

The invariant above is a claim, not a guarantee, and it is the first thing the prototype must try to break.

Take one asset module. Run it unchanged under the subprocess `Launcher`, then unchanged under the Ray `ColocatedRuntime`. Same source, same imports, same results, differing only in wall-clock time and in how many objects were written to storage.

If passing that test requires a single `if` inside an asset body, the two-tier model does not work and needs redesigning before anything is built on it.

## Sequencing, and why both tiers must be designed now

Only Tier 1 for a couple of engines and Tier 2 for Ray will actually be implemented at first. But both have to be *designed* before either is written.

If Tier 1 ships alone, the asset API will bake in the assumption that outputs are locations. Adding handles later then becomes a breaking change to every asset anyone has written. The retrofit is not difficult so much as impossible without a major version break and a migration nobody will perform.

The general rule, applied throughout this project: an interface with one implementation is a fiction, and an interface designed only against easy implementations breaks on the first hard one. Every interface here needs at least two implementations before it is trusted, and one of them has to be the hard one.
