"""kayavyuh -- interface sketch.

Design phase. None of this is implemented. These are the protocol boundaries
that the documents in this directory commit to, written as real Python so they
can be type-checked and argued with before any build tooling exists.

Dependency-free by intent: this module must import on a bare interpreter.

Read alongside:
    01-thesis.md          why these boundaries and not others
    02-identity.md        the two hashes and the freshness rule
    03-state-and-log.md   bitmap state, environments, the separated audit log
    04-compute.md         two compute tiers, the control plane boundary
    05-non-goals.md       what is deliberately absent
    06-prior-art.md       what was borrowed, from where, and what was rejected
    07-differentiation.md where Enzyme structurally cannot reach, and why that
                          is the only honest basis for building this
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
SchemaFingerprint = NewType("SchemaFingerprint", str)

# Two hashes with two jobs. Collapsing them back into one does not work --
# the argument is in 02-identity.md and it is the most important correction
# the design has taken.
#
#   SnapshotId     structural, recursive over upstream SnapshotIds.
#                  Answers WHERE bytes live. Changes on deploys.
#
#   ProvenanceHash per-partition, incorporates upstream DataVersions.
#                  Answers WHETHER a partition is fresh. Changes constantly.
SnapshotId = NewType("SnapshotId", str)
ProvenanceHash = NewType("ProvenanceHash", str)

DataVersion = NewType("DataVersion", str)

Environment = NewType("Environment", str)  # "prod" | "staging" | "dev-ydatta"

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
        upstream version. Open question 5 in 05-non-goals.md.
        """
        ...


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Materialization:
    """The atomic unit of "this now exists, and here is what defines it".

        snapshot   == H(code_version, schema_fingerprint, {upstream snapshot ids})
        provenance == H(snapshot, {(upstream, data_version) for resolved inputs})

    The schema fingerprint is inside the snapshot hash on purpose, borrowed from
    Flyte's interface hash. Without it an asset whose output schema changes but
    whose code hash is stable reports fresh while serving the wrong shape.

    For an asset declared nondeterministic, data_version is set equal to
    provenance rather than to a digest of the output bytes. See 02-identity.md.
    """

    asset: AssetKey
    partition: PartitionKey | None
    snapshot: SnapshotId
    code_version: CodeVersion
    schema_fingerprint: SchemaFingerprint
    input_versions: Mapping[AssetKey, DataVersion]
    data_version: DataVersion
    provenance: ProvenanceHash
    run_id: RunId


class NonDeterminismPolicy(Enum):
    """How an asset that is not a pure function of its declared inputs is handled.

    Three tiers, taxonomy adopted from Enzyme, in order of preference. The
    reframing that matters: non-determinism is usually an UNDECLARED INPUT
    rather than a property of the computation. See 02-identity.md.
    """

    # Tier 1, strongly preferred. Name the hidden input -- model version, prompt
    # hash, temperature, seed, a snapshotted clock value -- and the asset becomes
    # deterministic given its declared inputs. Full content-addressing, exact
    # reuse, and correct downstream invalidation when the model version bumps.
    DECLARED_INPUTS = "declared_inputs"

    # Tier 2. DataVersion is set to the asset's own ProvenanceHash rather than to
    # a digest of its bytes. Stops the staleness cascade for genuinely stochastic
    # assets, but silently gives up detecting that the output changed. Whether
    # this should be offered at all is open question 2 in 05-non-goals.md.
    PIN_TO_PROVENANCE = "pin_to_provenance"

    # Tier 3. For assets that must reflect live external state, where a stale but
    # self-consistent answer is worse than an expensive one. Enzyme's equivalent
    # fallback exists for the same reason: sometimes there is no honest shortcut.
    ALWAYS_RECOMPUTE = "always_recompute"


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


class RowLineageCapability(Enum):
    """What a backend can actually promise about row lineage.

    Reported rather than inferred, because Iceberg v3 support is uneven across
    engines and a write path that silently drops _row_id would otherwise mislead
    anyone building a compliance workflow on it. See 05-non-goals.md.
    """

    NONE = "none"
    READ = "read"  # readable, but writes may drop it
    PRESERVED_ON_UPDATE = "preserved_on_update"


class RowLineageSource(Protocol):
    """Surfaces row lineage the storage layer already tracks. Never computes it.

    Iceberg v3 exposes _row_id and _last_updated_sequence_number; that
    implementation returns a reference into them. Everything else reports NONE,
    and the UI shows the granularity actually available.
    """

    def capability(self, location: Location) -> RowLineageCapability: ...

    def row_lineage(
        self,
        asset: AssetKey,
        partition: PartitionKey | None,
        version: DataVersion,
    ) -> object | None: ...


# ---------------------------------------------------------------------------
# Physical store and environments  (adopted from SQLMesh)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StoredSnapshot:
    """Physical bytes, addressed by the structure that produced them.

    One per (asset, snapshot). Partitions live inside it; a partition
    materialising does NOT create a new StoredSnapshot, which is why physical
    storage is keyed by snapshot rather than by provenance.
    """

    asset: AssetKey
    snapshot: SnapshotId
    location: Location
    created_at_logical_time: LogicalTime


@dataclass(frozen=True)
class EnvironmentPointer:
    """Named access to a snapshot. An environment is just a set of these.

    Safe to share physical data across environments by construction: identical
    snapshot implies identical logic AND identical structural ancestry, so any
    partition materialised under it is valid for every environment pointing at
    it. The recursion in SnapshotId is what makes that true.
    """

    environment: Environment
    asset: AssetKey
    snapshot: SnapshotId


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
    """Atomic, optimistically concurrent. Succeeds wholly or changes nothing.

    Pointer moves ride in the same transaction as materializations so that
    promotion is atomic -- no reader ever observes a partially advanced
    environment.
    """

    expected: LogicalTime
    materializations: Sequence[Materialization] = ()
    status_deltas: Sequence[StatusDelta] = ()
    register_snapshots: Sequence[StoredSnapshot] = ()
    move_pointers: Sequence[EnvironmentPointer] = ()


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

    Nothing outside the control plane may reach this. Workers and user code get
    a ControlPlaneClient instead; see 04-compute.md.
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

        None until open question 5 in 05-non-goals.md is resolved.
        """
        ...

    # --- physical store and environments ---

    def resolve(self, env: Environment, asset: AssetKey) -> StoredSnapshot | None: ...
    def pointers(self, env: Environment) -> Sequence[EnvironmentPointer]: ...

    def fork_environment(self, source: Environment, target: Environment) -> None:
        """Copy a pointer set. Instant, and copies no data."""
        ...

    def unreferenced_snapshots(self) -> Sequence[StoredSnapshot]:
        """Candidates for collection -- pointed at by no environment.

        Necessary but NOT sufficient to delete: an in-flight run may be writing
        to one, and a retention window has to keep rollback possible for longer
        than a single deploy. Deleting a live snapshot destroys data, which makes
        this a correctness problem rather than housekeeping. Open question 3.
        """
        ...

    # --- single-writer bookkeeping for the scheduler ---
    # Another capability an OLAP backend cannot provide, and another reason this
    # store is not pluggable.

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
# The control plane boundary  (adopted from Airflow 3)
# ---------------------------------------------------------------------------


class ControlPlaneClient(Protocol):
    """The ONLY surface user code and workers may touch.

    Airflow 3's central architectural change was removing worker access to the
    metadata database, and it cost them a major version plus a long migration
    because DAG authors had built on its absence for years. Starting with the
    boundary is free; adding it later is not.

    What it buys beyond isolation:
      * workers in any language -- an HTTP client, not a Postgres driver
      * the state schema can evolve (paging, merkle roots, pointer store)
        without any of it being a breaking change for asset authors
      * long-running work survives control plane upgrades via a versioned API
      * asset code never holds database credentials, which matters the moment
        anyone runs third-party assets

    Enforcement is structural, not documentary: state store types live in a
    package the user-facing distribution does not depend on.
    """

    def resolve_input(
        self, asset: AssetKey, partition: PartitionKey | None
    ) -> Location | Ref: ...

    def report_materialization(self, m: Materialization) -> None: ...
    def emit_event(self, kind: str, payload: Mapping[str, object]) -> None: ...
    def heartbeat(self, handle: LaunchHandle) -> None: ...
    def get_config(self, key: str) -> object: ...


# ---------------------------------------------------------------------------
# Scheduling -- a pure function of state  (lesson from Temporal, not Temporal)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorldState:
    """Everything decide() is allowed to see.

    Read from the store by the caller. Note now_epoch_s is handed in rather than
    read from a clock, because a decider that reads the clock is not replayable.
    """

    logical_time: LogicalTime
    now_epoch_s: int
    environment: Environment
    assets: Mapping[AssetKey, AssetState]
    in_flight: Mapping[RunId, Sequence["Step"]]


class ActionKind(Enum):
    REQUEST = "request"  # materialise these partitions
    CANCEL = "cancel"
    PROMOTE = "promote"  # advance environment pointers
    COLLECT = "collect"  # release an unreferenced snapshot


@dataclass(frozen=True)
class Action:
    kind: ActionKind
    asset: AssetKey | None = None
    ordinals: OrdinalSet | None = None
    snapshot: SnapshotId | None = None
    run_id: RunId | None = None


class Decider(Protocol):
    """decide() does no IO, holds no state between calls, and reads no clock.

    Payoffs: crash recovery is recomputation rather than reconstruction; the
    loop is unit-testable against a literal fixture with no database and no
    cluster; and "why did it not run yesterday?" is answered by replaying
    yesterday's state.

    Structurally easier here than in Dagster, and for a reason that traces back
    to the identity design -- a pure decide() needs state to be small and
    authoritative, and Dagster's state is a projection over an event log.

    Close to unachievable as a retrofit, once a daemon has accumulated caches
    and incidental clock reads.
    """

    def decide(self, world: WorldState) -> Sequence[Action]: ...


@dataclass(frozen=True)
class StepSignature:
    """Normalised shape of a step, for matching against historical executions.

    The analogue of Enzyme's normalised-physical-plan matching. Deliberately
    coarse: two steps with the same signature should have had similar cost.
    """

    asset: AssetKey
    snapshot: SnapshotId
    runtime: str
    partition_count: int
    input_bytes: int


@dataclass(frozen=True)
class CostEstimate:
    cpu_seconds: float
    confidence: float  # Enzyme's own model is right ~7 times in 8. Calibrate.


class CostModel(Protocol):
    """Grounded in history, read from the AuditLog, which already records it.

    Two things this must get right, both from Enzyme:

    Estimates come from OBSERVED executions of structurally similar work, not
    from a static formula over input sizes.

    Planning is GRAPH-GLOBAL, not greedy per asset. Materialising an upstream
    asset one way can be worth it even when another way is cheaper for that
    asset alone, because it lowers cost for everything downstream. A planner
    that optimises each asset independently gets this wrong by construction.

    Open question 2b in 05-non-goals.md. Nothing implements this yet, and the
    planner currently picks colocated-vs-persisted edges on a guess.
    """

    def estimate(self, signature: StepSignature) -> CostEstimate | None: ...
    def estimate_plan(self, plan: SubPlan) -> CostEstimate | None: ...


# ---------------------------------------------------------------------------
# Compute tier 1 -- Launcher. Universal, narrow, required.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Step:
    asset: AssetKey
    partition: PartitionKey | None
    snapshot: SnapshotId
    code_version: CodeVersion
    config: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkUnit:
    """Steps whose inputs and outputs are all resolved to storage locations."""

    run_id: RunId
    environment: Environment
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
    environment: Environment
    steps: Sequence[Step]
    colocated_edges: Sequence[tuple[AssetKey, AssetKey]]
    persisted_inputs: Mapping[AssetKey, Location]
    persisted_outputs: Mapping[AssetKey, Location]


class Colocation(Protocol):
    """A live multi-step runtime session.

    The signature that matters is execute(): Ref in, Ref out. Two consecutive
    steps in the same colocation never touch storage. That is the capability
    a subprocess protocol structurally cannot provide.

    Prior art, and it has a paper: Bauplan (arXiv 2410.17465) measures Arrow
    handoff between pipeline steps as orders of magnitude faster than
    write-to-object-storage-then-read. This is no longer a novel claim.
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
# Invariants
# ---------------------------------------------------------------------------

# 1. TIER INDEPENDENCE
#    An asset definition is written once and is byte-identical under both tiers.
#    The tier decides only whether an edge is a Ref or a Location. If any asset
#    body needs to know which tier it is running under, the abstraction has
#    failed and the design is wrong.
#
#    Asset bodies therefore receive and return values, never paths:
#
#        @asset(partitions=daily("2024-01-01"))
#        def orders(ctx, raw_orders: Table) -> Table:
#            return raw_orders.filter(...)
#
#    Dagster's IO managers already get this far. The difference is that in
#    Dagster materialising at every edge is a fixed property of the framework;
#    here, whether an edge hits storage is a planner decision.
#
# 2. CONTROL PLANE ISOLATION
#    Nothing outside the control plane imports StateStore. Workers and user code
#    use ControlPlaneClient. Enforced by packaging, not by convention.
#
# 3. SHARING SAFETY
#    Identical SnapshotId implies identical logic and identical structural
#    ancestry, so a partition materialised under a snapshot is valid for every
#    environment pointing at it. This is what makes zero-copy environments sound
#    rather than merely convenient, and it depends on SnapshotId being recursive.
#
# 4. DECIDER PURITY
#    decide(WorldState) -> [Action] performs no IO and reads no clock.
#
# FALSIFIABLE TEST, and the first thing the prototype must try to break:
# run one unmodified asset module under the subprocess Launcher, then under the
# Ray ColocatedRuntime. Same source, same results, differing only in wall-clock
# time and in how many objects were written to storage. If passing that test
# needs a single `if` inside an asset body, invariant 1 does not hold and the
# model has to be redesigned before anything is built on it.
