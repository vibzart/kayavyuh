# kayavyuh

An asset-oriented data orchestrator built around content-addressed materialization, bitmap partition state, and pluggable compute.

**Status: design phase. Nothing here runs yet.** This repository currently contains design documents only.

---

## The name

*Kāya-vyūha* (काय-व्यूह) is a siddhi described in commentaries on the Yoga Sūtras — the deployment of many simultaneous bodies from a single consciousness, so that accumulated karma is exhausted in parallel rather than serially across many rebirths.

It is an unreasonably precise description of a partitioned backfill.

| concept | system |
| --- | --- |
| one consciousness, many simultaneous bodies | one asset definition, many partition executions |
| exhausting karma in parallel, not across rebirths | backfill |
| *saṃskāras* — latent impressions carried forward | stale partitions carrying unmaterialized state |
| one identity embodied across many forms | one control plane across Ray, Spark, Snowflake |

That is the last mystical paragraph in this repository.
Everything below, and every API surface, error message, and config key in this project, stays literal.

Canonical spelling in all identifiers — package, module, org, domain — is `kayavyuh`, without diacritics or hyphens.

---

## What it is

A data orchestrator in the same category as Dagster: you declare *assets* (tables, files, models) and their dependencies, and the system decides what needs recomputing and runs it.

Three things are different.

**Materialization identity is content-addressed.** An asset partition is identified by a hash of its code version and the data versions of its resolved inputs. Staleness is a hash comparison, not a walk over historical events. See [`docs/design/02-identity.md`](docs/design/02-identity.md).

**Partition state is a bitmap, not a row per event.** Marking 20,000 partitions as requested is one compare-and-swap write, not 20,000 inserts. The audit trail is a separate, append-only, pluggable log — because "what is currently true" and "what happened" are different questions with different storage requirements. See [`docs/design/03-state-and-log.md`](docs/design/03-state-and-log.md).

**Compute has two tiers.** Every engine implements a narrow `Launcher` (submit, poll, logs, cancel). Engines that can host multiple steps in one live runtime may additionally implement `ColocatedRuntime`, which passes in-memory handles between steps instead of round-tripping through object storage. Ray implements both. Snowflake implements only the first, and that is fine. Asset code is identical either way. See [`docs/design/04-compute.md`](docs/design/04-compute.md).

## What it is not

It does not invent row-level lineage; it surfaces what the table format already tracks.
It does not offer a pluggable state store for arbitrary databases.
It is not a general workflow engine — the unit of work is an asset, not a task.

The reasoning for each refusal is in [`docs/design/05-non-goals.md`](docs/design/05-non-goals.md), along with the open questions this design has not yet answered.

## Design documents

| document | contents |
| --- | --- |
| [01-thesis.md](docs/design/01-thesis.md) | The problem, and why it is one root cause rather than four complaints |
| [02-identity.md](docs/design/02-identity.md) | Content-addressed materialization identity and the freshness rule |
| [03-state-and-log.md](docs/design/03-state-and-log.md) | Bitmap partition state, ordinal allocation, the separated audit log |
| [04-compute.md](docs/design/04-compute.md) | The two-tier compute model and the tier-independence invariant |
| [05-non-goals.md](docs/design/05-non-goals.md) | Deliberate refusals, unresolved design questions |
| [interfaces.py](docs/design/interfaces.py) | Every protocol in one importable, dependency-free file |

## License

**Undecided.** No license file has been added yet, which means default copyright applies and this code is not yet open source in any usable sense.

This is deliberate rather than an oversight. The Apache-2.0 versus BSL decision determines whether a cloud vendor can host the project and whether some enterprises will adopt it, and it is effectively irreversible once external contributions arrive. It will be settled before the first contribution is accepted.
