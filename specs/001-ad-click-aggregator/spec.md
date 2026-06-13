# Feature Specification: Ad Click Aggregator

**Feature Branch**: `001-ad-click-aggregator`

**Created**: 2026-06-13

**Status**: Draft

**Input**: User description: "Educational ad click aggregator implementing the REFERENCE.md design: click ingestion and redirect, advertiser metrics querying, and periodic reconciliation."

## Overview

The platform shows ads to end users. When a user clicks an ad, the system must send the
user onward to the advertiser's destination **and** record that the click happened.
Advertisers then log in to a dashboard and ask questions like "how many clicks did
campaign X get last Tuesday between 2pm and 3pm?" Because clicks drive billing, the count
an advertiser sees must eventually be exact, even though it should also appear almost
immediately.

This feature delivers that end-to-end loop in three independently valuable slices:
capturing clicks reliably, serving fast aggregated metrics, and guaranteeing the numbers
are ultimately correct.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Capture a click and redirect the user (Priority: P1)

An end user clicks an ad. The system immediately sends them to the advertiser's
destination URL, and the click is recorded for later counting. A click that has already
been recorded for the same ad impression is not counted again.

**Why this priority**: Nothing else in the system has value without trustworthy click
capture. The redirect is the user-visible behavior, and reliable, de-duplicated recording
is the foundation every downstream metric depends on. This slice alone is a demonstrable
MVP: clicks flow in and land in durable storage.

**Independent Test**: Issue a click for a known ad and confirm (a) the response redirects
to the correct advertiser destination, and (b) exactly one click record exists for that
impression even if the same click is replayed several times.

**Acceptance Scenarios**:

1. **Given** an active ad with a known destination, **When** a user clicks it, **Then** the
   user is redirected to that destination and a click is recorded.
2. **Given** a click that was already recorded for an impression, **When** the same
   impression's click is received again, **Then** no additional click is counted.
3. **Given** a click referencing an ad that does not exist, **When** it is received,
   **Then** the system rejects it without recording a click and without redirecting to an
   arbitrary location.
4. **Given** the downstream recording path is temporarily unavailable, **When** a user
   clicks, **Then** the click is not silently lost (it is retained for later processing).

---

### User Story 2 - Advertiser queries click metrics over time (Priority: P2)

An advertiser opens a dashboard and requests click counts for one of their campaigns over
a chosen time range and granularity (down to one minute), and receives results quickly.

**Why this priority**: This is the advertiser-facing payoff and the second half of the
product promise. It depends on clicks being captured (US1) but can be built and tested
against captured data independently of reconciliation.

**Independent Test**: With a known set of recorded clicks, query a campaign over a time
window at minute and hour granularity and confirm the returned counts match the known
data and arrive well within the latency target.

**Acceptance Scenarios**:

1. **Given** recorded clicks for a campaign, **When** the advertiser requests counts for a
   time window at minute granularity, **Then** per-minute counts for that window are
   returned.
2. **Given** the same data, **When** the advertiser requests a wider window at hourly
   granularity, **Then** correctly bucketed hourly counts are returned.
3. **Given** an advertiser, **When** they request metrics for a campaign they do not own,
   **Then** the request is denied.
4. **Given** a recent click, **When** the advertiser queries shortly after, **Then** the
   click is reflected in near-real-time results within the freshness target.
5. **Given** a time window with no clicks, **When** queried, **Then** zero counts are
   returned rather than an error.

---

### User Story 3 - Reconciliation guarantees correct counts (Priority: P3)

On a periodic schedule, the system recomputes click counts from the durable raw record of
every click and corrects any aggregated figures that drifted from the near-real-time path,
so the numbers advertisers are billed on are exact.

**Why this priority**: This is what makes the metrics trustworthy for billing. The system
is usable for demos without it (US1 + US2 give live approximate metrics), but data
integrity is a stated non-negotiable, so it is required for the feature to be considered
complete.

**Independent Test**: Introduce a deliberate discrepancy between the fast aggregates and
the raw click record, run reconciliation, and confirm the aggregates afterward match the
counts derived from raw clicks.

**Acceptance Scenarios**:

1. **Given** raw click records, **When** reconciliation runs, **Then** aggregated counts
   for the reconciled period match the counts derived directly from raw records.
2. **Given** the fast path under-counted or over-counted a period, **When** reconciliation
   runs, **Then** the stored aggregates for that period are corrected to the exact value.
3. **Given** reconciliation has run for a period, **When** an advertiser later queries that
   period, **Then** they see the corrected (exact) counts.

---

### Edge Cases

- A single ad goes viral and receives a disproportionate share of all clicks in a short
  window — the system must keep capturing and counting without dropping clicks or stalling.
- A click arrives with a missing or malformed impression reference — it is rejected
  cleanly rather than counted ambiguously.
- Clicks arrive out of order or are delayed — they must still be attributed to the correct
  minute bucket once reconciled.
- An advertiser requests an extremely large time range — the system returns results at an
  appropriate granularity without timing out or returning an error.
- The same physical click is delivered more than once (retries) — it is counted at most
  once per impression.
- A query arrives for a campaign during the brief window before its first click is
  aggregated — it returns zero rather than failing.

## Requirements *(mandatory)*

### Functional Requirements

**Click capture & redirect (US1)**

- **FR-001**: System MUST redirect a clicking user to the advertiser destination
  associated with the clicked ad.
- **FR-002**: System MUST record every accepted click for later aggregation in a durable
  store such that no accepted click is lost.
- **FR-003**: System MUST associate each click with the ad/campaign it belongs to and the
  time the click occurred, at a resolution of at least one minute.
- **FR-004**: System MUST treat each ad impression as the unit of de-duplication and count
  at most one click per impression.
- **FR-005**: System MUST reject clicks that reference an unknown ad without recording a
  count.
- **FR-006**: System MUST preserve a raw record of every accepted click sufficient to
  recompute all aggregates later.

**Advertiser metrics querying (US2)**

- **FR-007**: System MUST allow an advertiser to request click counts for their campaign
  over a specified time range.
- **FR-008**: System MUST support query granularities from one minute up to at least one
  day (e.g., per-minute, per-hour, per-day buckets).
- **FR-009**: System MUST restrict each advertiser's queries to campaigns they own.
- **FR-010**: System MUST return zero-valued buckets (not errors) for periods within range
  that have no clicks.
- **FR-011**: System MUST reflect newly captured clicks in query results within the defined
  freshness target.

**Reconciliation & integrity (US3)**

- **FR-012**: System MUST periodically recompute aggregates from the raw click record.
- **FR-013**: System MUST correct stored aggregates that differ from the recomputed values
  for the reconciled period.
- **FR-014**: System MUST ensure that, after reconciliation for a period, advertiser
  queries for that period return the exact reconciled counts.

**Cross-cutting**

- **FR-015**: System MUST continue accepting and counting clicks correctly when a single
  ad receives a disproportionate spike of traffic.
- **FR-016**: System MUST record enough information per click to attribute delayed or
  out-of-order clicks to the correct time bucket during reconciliation.

### Key Entities *(include if feature involves data)*

- **Ad**: A specific advertisement that can be shown and clicked. Belongs to a campaign,
  has a destination the user is sent to on click, and an owning advertiser.
- **Campaign**: A grouping of ads belonging to one advertiser; the unit advertisers query
  metrics for.
- **Advertiser**: The party that owns campaigns and queries their performance; may only
  see their own campaigns.
- **Ad Impression**: A single instance of an ad being shown to a user; identified uniquely
  so that the resulting click can be de-duplicated and (for retargeting) the same ad shown
  on different occasions is tracked separately.
- **Click Event**: A record that a user clicked an ad impression at a point in time;
  attributable to an ad/campaign and a minute-resolution timestamp; the raw unit of truth.
- **Aggregated Metric**: A click count for a campaign within a time bucket (minute and
  coarser), optimized for fast advertiser queries.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The system sustains a peak load of 10,000 clicks per second without losing
  accepted clicks.
- **SC-002**: 95% of advertiser metric queries return in under 1 second.
- **SC-003**: A newly captured click is reflected in advertiser query results within 1
  minute (near-real-time freshness).
- **SC-004**: After reconciliation, aggregated counts for any reconciled period match the
  counts derived from raw click records exactly (0% discrepancy).
- **SC-005**: Replaying the same click for an impression any number of times changes the
  counted total by at most one.
- **SC-006**: When a single ad receives up to 50% of total click traffic during a spike,
  click capture and counting continue with no dropped clicks.
- **SC-007**: Advertisers can retrieve metrics at one-minute granularity for any window
  within the supported retention period.

## Assumptions

- Ads, campaigns, and advertiser ownership already exist as reference data the system can
  read; managing the ad catalog's full lifecycle (creation, billing, payments) is out of
  scope for this feature.
- Each ad impression shown to a user is assigned a unique identifier at display time and
  that identifier is available when the click is received; impression generation by the
  ad-serving surface is assumed and not built here.
- Advertiser identity/authentication for the query path is assumed available; building a
  full identity provider is out of scope, though queries must still be scoped to the
  caller's own campaigns.
- "Near real-time" is interpreted as the 1-minute granularity from the reference design;
  sub-second freshness is not required.
- Retention of raw clicks is at least long enough to support the advertiser query windows
  and at least one reconciliation cycle; an exact retention period is a planning detail.
- This is an educational build: demonstrating each behavior end to end is the goal, and
  load targets (SC-001) are design/sizing targets rather than a contractual SLA to be
  load-tested at full 10K/sec.
- Fraud detection beyond per-impression de-duplication (e.g., behavioral bot detection) is
  out of scope for this feature.
