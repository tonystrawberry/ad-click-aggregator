<!--
SYNC IMPACT REPORT
==================
Version change: 1.0.0 → 1.0.1
Bump rationale (1.0.1, PATCH): Principle III example clarified — Flink jobs may be
written in PyFlink (the stream aggregator migrated from Java to PyFlink so the
codebase is Ruby + Python, dropping the Java/Maven toolchain). No principle added,
removed, or redefined.

--- Prior: (template) → 1.0.0 ---
Bump rationale: Initial ratification of the project constitution from the template.

Modified principles: N/A (initial adoption)
Added sections:
  - Core Principles (I–VI)
  - Technology Constraints
  - Development Workflow & Quality Gates
  - Governance
Removed sections: None

Principles defined:
  I.   Reference-Architecture Fidelity
  II.  Infrastructure as Code (Terraform)
  III. Ruby-First Implementation
  IV.  Data Integrity & Idempotency
  V.   Managed AWS Services First
  VI.  Educational Clarity

Templates requiring updates:
  ✅ .specify/templates/plan-template.md   (Constitution Check pulls dynamically — no edit needed)
  ✅ .specify/templates/spec-template.md   (generic — no edit needed)
  ✅ .specify/templates/tasks-template.md  (generic — no edit needed)

Follow-up TODOs: None
-->

# Ad Click Aggregator Constitution

This project is an **educational reference implementation** of an ad click aggregator,
following the system design walkthrough captured in `REFERENCE.md` (Hello Interview /
Evan, ex-Meta staff engineer). Its purpose is learning: to build, in real AWS
infrastructure, the streaming + batch data pipeline described in that design.

## Core Principles

### I. Reference-Architecture Fidelity

The system MUST implement the architecture described in `REFERENCE.md`, not a
substitute that happens to be easier. The canonical pipeline is:

`Click Processor → Kinesis (event stream) → Flink (stream aggregator) → Aggregated OLAP store → Query Service`,
supplemented by Kinesis→S3 raw dumps and a periodic Spark reconciliation job.

Concretely, the following components MUST appear in the design and implementation:
API Gateway (click + query entry points), Kinesis Data Streams, Apache Flink (managed
via Kinesis Data Analytics / Managed Service for Apache Flink), Spark for batch
reconciliation, an OLAP store for aggregates (Redshift), DynamoDB for the ads catalog,
and Redis for impression-ID idempotency/caching.

Deviations from the reference (substituting, removing, or collapsing a named component)
are allowed ONLY when documented in the plan's Complexity Tracking table with the
learning trade-off made explicit. Rationale: the entire point of the project is to
exercise these specific technologies end to end.

### II. Infrastructure as Code (Terraform)

All AWS resources MUST be provisioned through Terraform committed to this repository.
Manual changes via the AWS Console or ad-hoc CLI mutations are PROHIBITED except for
transient debugging, which MUST be reconciled back into Terraform before the work is
considered complete. State, variables, and environment configuration MUST be checked in
(secrets excluded). Rationale: reproducibility and the ability to tear down/recreate the
full stack are core learning outcomes and prevent surprise AWS costs.

### III. Ruby-First Implementation

Application and service code MUST be written in Ruby wherever the runtime supports it —
this explicitly includes Lambda functions (click processor, query service, glue logic).
A non-Ruby language MAY be used ONLY where a component has no practical Ruby option
(e.g., Flink jobs in PyFlink/Java/Scala/SQL, Spark jobs in PySpark/Scala); each such
exception MUST be noted in the plan. Rationale: Ruby is the maintainer's preferred language and
keeping services in one language maximizes the learning value of the service tier.

### IV. Data Integrity & Idempotency

Clicks represent billable events and MUST NOT be silently lost or double-counted.
Therefore:
- Every click MUST carry an **impression ID**; the click processor MUST deduplicate
  against Redis before counting.
- Raw click events MUST be durably retained (Kinesis→S3) to enable replay.
- The Spark reconciliation job MUST be able to recompute aggregates from raw events and
  correct any drift introduced by the real-time path.
- Error handling MUST be explicit — failures surface and are logged, never swallowed
  into a path that drops events.

Rationale: billing/payout accuracy is the stated non-functional requirement that
justifies the dual (stream + batch) architecture.

### V. Managed AWS Services First

When a managed AWS service satisfies a need, it MUST be preferred over self-hosted
infrastructure (e.g., Managed Service for Apache Flink over self-run Flink; Kinesis over
self-hosted Kafka; Redshift over self-managed OLAP; ElastiCache over self-run Redis;
Lambda over standing servers). Self-hosting is permitted only when no managed option
exists or the managed option cannot demonstrate the concept being learned, with the
reason documented. Rationale: minimizes operational burden and keeps focus on the data
pipeline rather than infrastructure babysitting.

### VI. Educational Clarity

Because the audience is a learner, code and infrastructure MUST favor readability and
explicitness over cleverness. Each major component MUST be accompanied by documentation
(README or inline) explaining what part of the reference design it implements and why.
Premature optimization and abstraction not present in the reference design are
discouraged (YAGNI). Rationale: the artifact's value is in being understood, not just in
running.

## Technology Constraints

- **Cloud**: AWS only. Credentials are supplied by the maintainer when provisioning is needed.
- **IaC**: Terraform (HCL).
- **Services / compute**: AWS Lambda (Ruby runtime) for the click processor and query service.
- **Ingestion / API**: Amazon API Gateway.
- **Streaming**: Amazon Kinesis Data Streams.
- **Stream processing**: Apache Flink via Amazon Managed Service for Apache Flink.
- **Batch / reconciliation**: Apache Spark (e.g., AWS Glue or EMR) reading raw events from S3.
- **OLAP / aggregates**: Amazon Redshift.
- **Ads catalog (OLTP key lookups)**: Amazon DynamoDB.
- **Idempotency / cache**: Amazon ElastiCache for Redis.
- **Raw event archive**: Amazon S3.

Target scale (from the reference, for sizing discussions — not a hard SLA for the
educational build): ~10M ads, ~10K clicks/second peak, advertiser queries under 1 second,
1-minute aggregation granularity.

## Development Workflow & Quality Gates

- Work proceeds through the Spec Kit flow: constitution → specify → (clarify) → plan →
  tasks → implement.
- Every plan MUST pass the Constitution Check gate; any violation MUST be recorded with
  justification in the plan's Complexity Tracking table.
- Infrastructure changes MUST be expressed as Terraform diffs and be reviewable before apply.
- Components MUST be demonstrable end to end against real AWS resources before a feature
  is considered done; where full deployment is impractical, the limitation MUST be stated.
- Cost awareness: prefer the smallest provisioned capacity that demonstrates the concept,
  and ensure resources can be destroyed via Terraform when not in use.

## Governance

This constitution supersedes ad-hoc practices for this project. Amendments are made by
editing this file via the `/speckit-constitution` flow and MUST include an updated Sync
Impact Report and version bump.

Versioning policy (semantic):
- **MAJOR**: backward-incompatible governance changes or removal/redefinition of a principle.
- **MINOR**: a new principle or section, or materially expanded guidance.
- **PATCH**: clarifications, wording, or non-semantic refinements.

Compliance: plans and implementations MUST be checked against these principles at the
Constitution Check gate. Because this is a solo educational project, the maintainer is
the sole approver; the requirement is that deviations are documented and intentional,
not that they are forbidden.

**Version**: 1.0.1 | **Ratified**: 2026-06-13 | **Last Amended**: 2026-06-14
