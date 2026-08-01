"""kayavyuh -- interface sketch.

Design phase. None of this is implemented. These are the protocol boundaries
that the documents in this directory commit to, written as real Python so they
can be type-checked and argued with before any build tooling exists.

Dependency-free by intent: this module must import on a bare interpreter.

Read alongside:
    01-thesis.md          why these boundaries and not others
    02-identity.md        the provenance/freshness rule
    03-state-and-log.md   why StateStore is narrow and AuditLog is wide
    04-compute.md         why compute has two tiers
    05-non-goals.md       what is deliberately absent
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import NewType, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Scalars
# ---------------------------------------------------------------------------

AssetKey = tuple[str, ...]

PartitionKey = NewType("PartitionKey", str)
Ordinal = NewType("Ordinal", int)

CodeVersion = NewType("CodeVersion", str)
DataVersion = NewType("DataVersion", str)
ProvenanceHash = NewType("ProvenanceHash", str)

LogicalTime = NewType("LogicalTime", int)
RunId = NewType("RunId", str)
EventId = NewType("EventId", str)


@dataclass(frozen=True)
class Location:
    """Where bytes live. Never interpreted by the core, only handed to plugins."""

    scheme: str  # "s3" | "iceberg" | "delta" | "file" | "snowflake" | ...
    uri: str
    options: Mapping[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Partition space
# ---------------------------------------------------------------------------


class PartitionStatus(Enum):
    """Existence and attempt outcome. Deliberately NOT freshness.

    Freshness is never stored -- it is computed by comparing provenance hashes
    (see 02-identity.md). A partition can be MATERIALIZED and stale at the same
    time, and conflating the two is what forces a system to rewrite state on
    every upstream change.

    MISSING is implicit: absence from every bitmap. It is not stored.
    """

    REQUESTED = "requested"
    IN_PROGRESS = "in_progress"
    MATERIALIZED = "materialized"
    FAILED = "failed"
    TOMBSTONED = "tombstoned"


@runtime_checkable
class OrdinalSet(Protocol):
    """A set of ordinals. Reference implementation is a roaring bitmap.

    Abstracted so the core never imports a bitmap library, and so SQLite and
    Postgres implementations can differ in serialisation without differing in
    semantics.
    """

    def __contains__(self, ordinal: Ordinal) -> bool: ...
    def __iter__(self) -> Iterator[Ordinal]: ...
    def __len__(self) -> int: ...

    def union(self, other: OrdinalSet) -> OrdinalSet: ...
    def intersection(self, other: OrdinalSet) -> OrdinalSet: ...
    def difference(self, other: OrdinalSet) -> OrdinalSet: ...

    def to_bytes(self) -> bytes: ...


@dataclass(frozen=True)
class OrdinalRange:
    """Half-open [start, stop). Used to scope reads to the pages that matter."""

    start: Ordinal
    stop: Ordinal


class PartitionDef(Protocol):
    """Bijection between partition keys and dense integer ordinals.

    Invariants, all permanent and all unenforceable by the type system --
    see 03-state-and-log.md:

      * ordinals are stable for the lifetime of the asset; nothing is ever
        renumbered, because a renumber silently corrupts every bitmap and
        version vector at once with no way to detect it afterwards
      * ordinals are dense; sparsity degrades bitmaps and wastes vector slots
      * removal is a tombstone, never a reclaim
      * changing granularity (daily -> hourly) is a NEW ASSET, not a mutation
    """

    def ordinal(self, key: PartitionKey) -> Ordinal: ...
    def key(self, ordinal: Ordinal) -> PartitionKey: ...
    def cardinality(self) -> int: ...
    def contains(self, key: PartitionKey) -> bool: ...


class PartitionMapping(Protocol):
    """Resolves which upstream partitions feed a downstream partition."""

    def upstream(self, downstream: PartitionKey) -> Sequence[PartitionKey]: ...
    def downstream(self, upstream: PartitionKey) -> Sequence[PartitionKey]: ...

    def is_total(self) -> bool:
        """True when every downstream partition depends on the whole upstream asset.

        Lets the planner compare a single merkle root instead of hashing every
        upstream version. Open question 3 in 05-non-goals.md.
        """
        ...


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Materialization:
    """The atomic unit of "this now exists, and here is what defines it".

    provenance == H(code_version, sorted(input_versions.items()))

    For an asset declared nondeterministic, data_version is set equal to
    provenance rather than to a digest of the output bytes. See 02-identity.md.
    """

    asset: AssetKey
    partition: PartitionKey | None
    code_version: CodeVersion
    input_versions: Mapping[AssetKey, DataVersion]
    data_version: DataVersion
    provenance: ProvenanceHash
    run_id: RunId
    location: Location | None = None


class VersionOracle(Protocol):
    """Derives a DataVersion from the storage system instead of computing one.

    Iceberg returns a snapshot id, Delta a table version, object storage a
    digest of (size, etag, mtime). Returning None means "cannot observe": the
    asset is then always potentially stale and freshness must come from an
    explicit policy rather than from content.
    """

    def observe(
        self,
        asset: AssetKey,
        partition: PartitionKey | None,
        location: Location,
    ) -> DataVersion | None: ...


class RowLineageSource(Protocol):
    """Surfaces row lineage the storage layer already tracks. Never computes it.

    Iceberg v3 exposes _row_id and _last_updated_sequence_number; that
    implementation returns a reference into them. Everything else returns None,
    and the UI shows the granularity actually available. See 05-non-goals.md.
    """

    def row_lineage(
        self,
        asset: AssetKey,
        partition: PartitionKey | None,
        version: DataVersion,
    ) -> object | None: ...


# ---------------------------------------------------------------------------
# State store -- narrow, opinionated, Postgres + SQLite only
# ---------------------------------------------------------------------------


class ConcurrentModification(Exception):
    """Raised by StateStore.apply when expected logical time no longer holds."""


@dataclass(frozen=True)
class AssetState:
    """Immutable snapshot of one asset's current state, optionally page-scoped."""

    asset: AssetKey
    logical_time: LogicalTime
    scope: OrdinalRange | None  # None == whole asset
    status: Mapping[PartitionStatus, OrdinalSet]
    data_version: Mapping[Ordinal, DataVersion]
    provenance: Mapping[Ordinal, ProvenanceHash]


@dataclass(frozen=True)
class StatusDelta:
    """Bulk status transition. One of these moves 20k partitions in one write."""

    asset: AssetKey
    status: PartitionStatus
    ordinals: OrdinalSet


@dataclass(frozen=True)
class StateTransaction:
    """Atomic, optimistically concurrent. Succeeds wholly or changes nothing."""

    expected: LogicalTime
    materializations: Sequence[Materialization] = ()
    status_deltas: Sequence[StatusDelta] = ()


@dataclass(frozen=True)
class Lease:
    name: str
    holder: str
    logical_time: LogicalTime
    expires_at_epoch_s: int


class StateStore(Protocol):
    """Authoritative record of what currently exists.

    NOT pluggable beyond Postgres and SQLite, and that is a design commitment
    rather than a missing feature -- an interface that also admitted Snowflake
    would be forced down to row-per-event semantics, which is the exact defect
    this project exists to remove. Reasoning in 03-state-and-log.md.
    """

    def asset_state(
        self, asset: AssetKey, scope: OrdinalRange | None = None
    ) -> AssetState: ...

    def apply(self, txn: StateTransaction) -> LogicalTime:
        """Raises ConcurrentModification if txn.expected is stale."""
        ...

    def logical_time(self) -> LogicalTime: ...

    def merkle_root(self, asset: AssetKey) -> ProvenanceHash | None:
        """Summary over all per-partition versions, for total dependencies.

        None until open question 3 in 05-non-goals.md is resolved.
        """
        ...

    # Single-writer bookkeeping for the scheduler. Another capability an OLAP
    # backend cannot provide, and another reason this store is not pluggable.
    def acquire_lease(self, name: str, holder: str, ttl_s: int) -> Lease | None: ...
    def renew_lease(self, lease: Lease) -> Lease | None: ...
    def release_lease(self, lease: Lease) -> None: ...


# ---------------------------------------------------------------------------
# Audit log -- wide open, two methods
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Event:
    event_id: EventId
    logical_time: LogicalTime
    kind: str
    run_id: RunId | None = None
    asset: AssetKey | None = None
    partition: PartitionKey | None = None
    payload: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class EventFilter:
    assets: Sequence[AssetKey] | None = None
    runs: Sequence[RunId] | None = None
    kinds: Sequence[str] | None = None
    since: LogicalTime | None = None
    until: LogicalTime | None = None
    limit: int | None = None


class AuditLog(Protocol):
    """What happened. Derived from state, never authoritative over it.

    Invariants:
      * state commits first, the log appends second
      * append is idempotent on event_id -- delivery is at-least-once
      * a failed append is a monitoring incident, never a correctness incident

    The log cannot be rebuilt from state, so losing it does lose history
    permanently. The claim is only that it never loses knowledge of what
    exists -- the inversion of a system where the event log IS the state.

    Two methods, because everything about this should be swappable: Parquet on
    object storage, ClickHouse, Postgres, BigQuery, Snowflake.
    """

    def append(self, events: Sequence[Event]) -> None: ...
    def scan(self, filter: EventFilter) -> Iterator[Event]: ...


# ---------------------------------------------------------------------------
# Compute tier 1 -- Launcher. Universal, narrow, required.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Step:
    asset: AssetKey
    partition: PartitionKey | None
    code_version: CodeVersion
    config: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkUnit:
    """Steps whose inputs and outputs are all resolved to storage locations."""

    run_id: RunId
    steps: Sequence[Step]
    inputs: Mapping[AssetKey, Location]
    outputs: Mapping[AssetKey, Location]


class LaunchState(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class LaunchHandle:
    """Must be serialisable and must survive orchestrator restart.

    If the control plane dies mid-run it has to reattach to work still running,
    rather than orphaning it or launching it twice.
    """

    launcher: str
    external_id: str
    payload: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class LogRecord:
    cursor: str
    stream: str  # "stdout" | "stderr" | engine-specific
    line: str


class Launcher(Protocol):
    """Every engine implements this. Spark, Databricks, Snowflake, dbt,
    Kubernetes Job, local subprocess, Flink.
    """

    name: str

    def submit(self, work: WorkUnit) -> LaunchHandle: ...
    def poll(self, handle: LaunchHandle) -> LaunchState: ...
    def logs(
        self, handle: LaunchHandle, since: str | None = None
    ) -> Iterator[LogRecord]: ...
    def cancel(self, handle: LaunchHandle) -> None: ...

    def results(self, handle: LaunchHandle) -> Sequence[Materialization]:
        """Materializations rather than a bare success flag, because the data
        version has to come back from wherever the bytes were actually written.
        A launcher that cannot introspect its own output leaves data_version
        to be filled in by the relevant VersionOracle.
        """
        ...


# ---------------------------------------------------------------------------
# Compute tier 2 -- ColocatedRuntime. Optional, rich, per-engine.
# ---------------------------------------------------------------------------


@runtime_checkable
class Ref(Protocol):
    """Opaque in-cluster handle to a value that has not touched storage.

    Ray wraps an ObjectRef. A Spark implementation would wrap a cached
    DataFrame -- with materially weaker lifetime guarantees, which is open
    question 1 and the most important thing the prototype must settle.
    """

    @property
    def runtime(self) -> str: ...


@dataclass(frozen=True)
class SubPlan:
    """A connected slice of the run that one colocation session will host."""

    run_id: RunId
    steps: Sequence[Step]
    colocated_edges: Sequence[tuple[AssetKey, AssetKey]]
    persisted_inputs: Mapping[AssetKey, Location]
    persisted_outputs: Mapping[AssetKey, Location]


class Colocation(Protocol):
    """A live multi-step runtime session.

    The signature that matters is execute(): Ref in, Ref out. Two consecutive
    steps in the same colocation never touch storage. That is the capability
    a subprocess protocol structurally cannot provide.
    """

    def load(self, asset: AssetKey, location: Location) -> Ref: ...
    def execute(self, step: Step, inputs: Mapping[AssetKey, Ref]) -> Ref: ...

    def fanout(
        self,
        step: Step,
        keys: Sequence[PartitionKey],
        inputs: Mapping[AssetKey, Ref],
    ) -> Sequence[Ref]:
        """The case that matters most: a 20k-partition backfill becomes one job
        with 20k tasks against a warm cluster, not 20k processes each paying
        full startup and import cost.
        """
        ...

    def persist(self, ref: Ref, location: Location) -> DataVersion: ...
    def release(self, ref: Ref) -> None: ...
    def close(self) -> None: ...


class ColocatedRuntime(Protocol):
    """Implemented only by engines that can host several steps in one runtime.

    Ray implements this fully. Snowflake never will, and that is fine -- it
    implements Launcher and loses nothing except an optimisation.
    """

    name: str

    def open(self, plan: SubPlan) -> Colocation: ...


# ---------------------------------------------------------------------------
# The invariant
# ---------------------------------------------------------------------------

# An asset definition is written once and is byte-identical under both tiers.
# The tier decides only whether an edge is a Ref or a Location. If any asset
# body needs to know which tier it is running under, the abstraction has failed
# and the design is wrong.
#
# Asset bodies therefore receive and return values, never paths:
#
#     @asset(partitions=daily("2024-01-01"))
#     def orders(ctx, raw_orders: Table) -> Table:
#         return raw_orders.filter(...)
#
# Dagster's IO managers already get this far. The difference is that in Dagster
# materialising at every edge is a fixed property of the framework; here,
# whether an edge hits storage is a planner decision.
#
# FALSIFIABLE TEST, and the first thing the prototype must try to break:
# run one unmodified asset module under the subprocess Launcher, then under the
# Ray ColocatedRuntime. Same source, same results, differing only in wall-clock
# time and in how many objects were written to storage. If passing that test
# needs a single `if` inside an asset body, this model does not work and has to
# be redesigned before anything is built on it.
