---
title: "System Design Interview: Design an Ad Click Aggregator w/ a Ex-Meta Staff Engineer"
date: "2026-06-13"
excerpt: "Hello Interview's walkthrough of designing an ad click aggregator — infrastructure-design flow, Kinesis + Flink stream aggregation, Cassandra/OLAP query path, hot-shard mitigation, and periodic reconciliation."
author: "Tony Duong"
category: "note"
categories: ["note", "tech"]
tags: ["system-design", "ad-tech", "kinesis", "flink", "streaming", "cassandra", "interview", "distributed-systems"]
coverImage: ""
youtubeUrl: "https://www.youtube.com/watch?v=Zcv_899yqhI"
---

Notes from Hello Interview (Evan, ex-Meta staff engineer) on **designing an ad click aggregator** — a common system design question at top companies, and one he's asked many times himself.

Unlike product designs (Ticketmaster, Uber, Dropbox), this is an **infrastructure design** question: less about user-facing APIs and entities, more about data pipelines and analytics.

## Interview roadmap

1. **Requirements** (functional + non-functional)
2. **System interface & data flow** (instead of core entities + API)
3. **High-level design** (satisfy functional requirements)
4. **Deep dives** (satisfy non-functional requirements)

## What the system does

Users click ads → get redirected to the advertiser → clicks are logged → **advertisers query click metrics over time** (campaign effectiveness, click counts by period, etc.).

Minimum query granularity: **1 minute** (e.g. last week at hourly resolution, last day at minute resolution).

## Scale assumptions

- ~**10 million ads** on the platform at any time
- ~**10,000 ad clicks/second** at peak

These numbers matter because they drive scalability and aggregation design.

## Functional requirements

- User clicks an ad → **redirected to the advertiser's website**
- **Advertisers can query click metrics over time** for their campaigns

## Non-functional requirements (context-specific)

| Concern | Ad click aggregator framing |
|---------|----------------------------|
| **Scalability** | Handle peak of **10K clicks/sec** |
| **Low-latency analytics** | Advertiser queries return in **< 1 second** |
| **High data integrity** | Don't lose clicks — billing/payout accuracy depends on it |
| **Near real-time** | Metrics as fresh as possible within **1-minute granularity** |
| **Idempotency / security** | Prevent click spam / fraudulent inflation of ad metrics |

## System interface & data flow

**Input:** click events from user browsers
**Output:** aggregated click metrics queryable by advertisers

High-level pipeline:

1. User clicks ad → hit **click processor service**
2. **Validate** click data (idempotency / anti-fraud)
3. **Log** raw click data
4. **Aggregate** into read-optimized form
5. **Query service** serves advertiser dashboards

## High-level design (v1 — naive)

```
Browser → Click Processor → Click DB (Cassandra) → Query Service → Advertiser browser
```

- **Cassandra** is a common interview choice — LSM-style writes (memtable in memory, periodic flush to disk) handle high write throughput well
- Cassandra is optimized for **point lookups by key**, not **range queries and aggregations** — which is exactly what advertisers need

**Problem:** querying raw clicks over a week at minute granularity means scanning/aggregating millions of rows — too slow to meet the **< 1 second** NFR, even on Postgres or DynamoDB.

## Deep dive: pre-aggregation with batch (Spark)

Add a **Spark** batch layer:

- Periodic **map-reduce** job reads all Cassandra shards
- Aggregates clicks at **minute intervals**
- Writes pre-aggregated counts to a **read-optimized OLAP DB** (or DynamoDB / Postgres for this simpler query shape)

Query service now reads pre-aggregated minute buckets → fast enough for advertisers.

**Trade-off:** batch interval adds latency (e.g. 5-minute delay before metrics appear).

## Deep dive: stream processing (Kinesis + Flink)

Replace (or supplement) the naive write path with a **stream**:

```
Click Processor → Kinesis (click event stream) → Flink (stream aggregator) → Aggregated store → Query Service
```

- **Kinesis** (or Kafka) holds the click event stream
- **Flink** consumes events in real time, maintains in-memory aggregates per time window (e.g. minute 45, count = 12)
- Writes rolling aggregates to the read store → **near real-time** analytics without waiting for batch jobs

Managed Kinesis/Kafka can be assumed always available in interviews.

## Hot shard problem

A viral ad (e.g. Nike + LeBron) can create a **hot shard** in Kinesis — one partition overwhelmed by writes → increased latency or even data loss.

**Mitigation:** further partition the data beyond the default key (e.g. composite partition key, salting) so no single shard absorbs all traffic.

## Idempotency & click validation

**Problem:** users with ad blockers can extract the redirect URL and skip sending the click event; attackers can spam fake clicks.

**Approach:**

- Generate an **ad impression ID** when the ad is shown (retargeting: same ad on Monday and Thursday gets tracked separately)
- Pass impression ID through to click processing
- **Redis cache** stores seen impression IDs — reject duplicates / validate click legitimacy before counting

## Periodic reconciliation

The stream + Flink path is neither pure **Lambda** nor pure **Kappa**:

- **Kappa:** everything via real-time stream processing
- **Lambda:** batch layer + separate real-time layer (real-time may be approximate)

Final design adds **periodic reconciliation**:

- Enable Kinesis to **dump raw click events to S3**
- Hourly/daily batch job (Spark) re-processes raw events
- Corrects any drift or loss from the real-time path → ensures **data integrity** for billing

## Redirect flow nuance

Two ways to handle the click → redirect:

1. **Simple:** redirect immediately, send click event in parallel — ad blockers can skip the event
2. **Better:** server-side redirect through click processor — ensures click is logged before redirect (discuss trade-offs with interviewer)

## Key takeaways

- **Infrastructure design** questions use **system interface + data flow** instead of entities/API
- Raw click storage alone won't satisfy **< 1s analytics queries** at 10K CPS — you need **pre-aggregation**
- **Stream path:** Kinesis → Flink for real-time minute-level aggregates
- **Batch path:** Spark over Cassandra/S3 for backfill and reconciliation
- **Hot shards** in Kinesis need explicit partitioning strategy for viral ads
- **Idempotency** via impression IDs + Redis dedup protects metric integrity
- Specify NFRs with **numbers and context** (10K CPS, 1-minute granularity, < 1s query latency) — not generic buzzwords
