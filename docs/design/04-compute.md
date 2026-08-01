# 04 — Compute

## Why a single compute interface cannot work

The obvious design is one `ComputeEngine` interface that Ray, Spark, Snowflake, dbt, and Kubernetes all implement. It is the wrong design, and the reason is the same as for the state store: the interface collapses to the intersection of its implementations.

Work out what that intersection contains. Snowflake cannot hold a Python object in memory between two steps. A Kubernetes Job cannot hand anything to the next Job except through storage. So the universal interface is necessarily `submit work → poll until done → read the result from external storage`.

That is Pipes. Building it would reproduce exactly the limitation this project exists to remove.

## Two tiers

**Tier 1 — `Launcher`.** Universal, narrow, required. Submit a unit of work, poll its state, stream its logs, cancel it, collect the materializations it reported. Inputs are resolved to storage locations and outputs are written to storage locations. Every engine implements this: Spark, Databricks, Snowflake, dbt, Kubernetes Job, local subprocess, Flink.

**Tier 2 — `ColocatedRuntime`.** Optional, rich, per-engine. Open a session that hosts several steps inside one live runtime, and pass in-memory handles between them instead of storage locations. Ray implements this fully. Snowflake will never implement it. Spark can implement a partial version and that is an open question.

Tier 1 is what makes the project usable. Tier 2 is what makes it differentiated.

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
