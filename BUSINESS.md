# BUSINESS.md: Trade8:Feed — Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Trade8:Feed **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Put a single, clean, entitlement-gated **market-data stream** behind
  every trader screen: live quotes, depth, prints, and auction imbalance —
  regardless of vendor wire format.
- **Protect data entitlements** with the same authorization plane as
  orders: a subscriber without depth rights never receives level data.
- Give traders **sub-second price alerts** with a simple declarative rule
  format — and a path for tenants to author richer alert logic.
- Give trading systems trustworthy **anchors**: prints that drive chart
  bars, halt/uptick state for the risk gate, and reference prices for
  flatten operations.

## 2. Business Scope (What the Service Provides)

| Capability | Description |
|---|---|
| Vendor adapter framework | Pluggable adapters (ITCH, IDC, crypto WS, FIX-MD, sim) behind one trait |
| Data normalization | All vendor packets → four internal event shapes (L1, L2, imbalance, print) |
| Quote consolidation | Cross-source BBO merge (best bid/ask, sizes summed at extremes — NBBO-style) |
| Market-data streaming | Per-symbol SSE: snapshot frame + live frames with sequence numbers |
| Snapshot APIs | Point-in-time L1/L2 reads for app cold start and service callers |
| Price alerts | Declarative rules (>, <, ≥, ≤ thresholds) evaluated on prints; fired-alert inbox + acknowledge |
| Feed health | Per-link status (healthy/degraded/dead), silence-based classification, admin wall |
| Vendor credential custody | Secret-reference-only link configuration |
| Admin workspace | RFW pages: link wall, quote-source catalog, alert queue with ack |
| Simulation/replay | Deterministic recorded sessions for demos, tests, and cutover rehearsal |

### What the service does NOT own (business boundary)

| Concern | Owned by |
|---|---|
| Bar aggregation/history | Trade8:Bars |
| Instrument catalog (symbols) | Trade8:Desk |
| Halt/uptick decisions | Trade8:Risk (consumes feed events) |
| Complex alert condition authoring | Sentin8:Func (`on_quote` modules) |
| Vendor connectivity credentials storage | Sentin8 secrets (Feed stores references) |
| Multicast distribution | Out of scope (venue-side concern) |

## 3. Business Capabilities

| Area | Capability | Business rule |
|---|---|---|
| Ingest | Multi-vendor adapters | Each link declares its protocol kind; adapters parse to one raw record shape |
| Ingest | Simulated feeds | Recorded sessions replay deterministically for demos/tests |
| Normalization | One event vocabulary | Four event types cover quotes, depth, imbalance, prints — consumers downstream need no vendor knowledge |
| Consolidation | NBBO-style best quote | Best bid across sources, best ask across sources; sizes summed at the extremes |
| Streaming | Snapshot-then-live | A new subscriber gets the current snapshot first, then live frames with per-topic sequence numbers |
| Streaming | Symbol-scoped delivery | A symbol's stream carries only that symbol's frames |
| Alerts | Declarative rules | Users create symbol + comparator + threshold rules; matches fire on prints within sub-seconds |
| Alerts | Acknowledge trail | Fired alerts persist and stay in an ack queue until acknowledged (who/when recorded) |
| Health | Link lifecycle | Silence classifies links (5s+ degraded, 30s+ dead); transitions emit status events |
| Entitlement | Tiered access | L1 vs L2 are separate permissions; subscription-time rejection (no partial data) |

## 4. Business Events (Outbound)

| Event | Fires when |
|---|---|
| `trade8:feed:quote:updated` | A quote packet normalizes (per-source + consolidated state) |
| `trade8:feed:depth:updated` | A level update normalizes |
| `trade8:feed:imbalance:updated` | An auction imbalance snapshot normalizes |
| `trade8:feed:trade:printed` | A consolidated print occurs — Bars's aggregation input |
| `trade8:feed:alert:fired` | An alert rule matches |
| `trade8:feed:feed:status` | A link's health state changes |

**Consumed:** tenant-authoring trigger on quotes (Sentin8:Func
`price-alerts` module).

## 5. Business Rules & Invariants

1. Every data frame leaving Feed is normalized — vendor formats never
   leak downstream.
2. Consolidated quotes dominate per-source quotes: best bid/ask logic is
   deterministic and test-pinned.
3. A subscriber receives a snapshot before any live frame.
4. Alerts fire once per trigger crossing and persist until acknowledged;
   acks record who and when.
5. Feed health transitions are evented, not just page-refreshed.
6. Vendor credentials are stored only as secret references.
7. Depth data requires its own entitlement — quote access is not implicit
   depth access.

## 6. Target-User Roles & Use Cases

| Persona | Use cases |
|---|---|
| Trader | Watch ladder/tape, set price alerts, see NBBO in the ticket |
| Risk (system) | Halt + uptick mirror from prints/quotes |
| Bars (system) | Aggregate prints into chart bars |
| Desk (system) | BBO snapshot for flatten pricing |
| Trading ops | Link wall monitoring; degraded/dead feeds; restart cycles |
| Compliance/entitlements admin | Control L1/L2/depth scope assignment |
| Tenant power users | Author complex alert conditions as Func modules |

## 7. KPIs / Success Criteria

- Ingest+fan-out throughput meets the latency budget (harness-pinned;
  100k quotes/s design target).
- Alert latency: a triggering print fires a persisted alert within the
  ingest budget (sub-second).
- Zero entitlement leakage (depth frames to non-entitled subscribers).
- Feed health accuracy: stale links classified within one health interval.
- Adapter restart storms bounded (backoff cap 30s).
- Snapshot-first delivery on every stream (cold start correct).

## 8. Dependencies & Reliance on Other Services

| Dependency | Why the business relies on it |
|---|---|
| Vendors | The raw market-data streams |
| Trade8:Bars | The primary print consumer (charts) |
| Trade8:Risk | The halt/uptick mirror consumer |
| Trade8:Desk | Symbol catalog context (via directory wiring) |
| Sentin8:Func | Tenant-authored alert complexity |
| Sentin8:Bus | Event fan-out to vertical consumers |
| Sentin8:Auth + Shield | Stream/read entitlement decisions |
| Sentin8:Sys | Audit delivery |
| Sentin8:DB | Links/rules/alerts/status persistence (schema `feed`) |

## 9. Acceptance Criteria

1. A trader sees live quotes/tape immediately after opening a workspace
   (snapshot-then-live).
2. Depth access is denied cleanly when the entitlement is absent.
3. A price alert fires within a second of the triggering print and sits in
   the admin ack queue until handled.
4. A vendor outage is visible on the health wall (degraded/dead) without
   operator polling.
5. Vendor onboarding requires only a new adapter + link row (no engine
   changes).
6. Simulated sessions reproduce recorded sequences deterministically.

## 10. Open Business Questions

- Which vendors demand the FIX-MD adapter path first?
- Should alert rules support sustained-condition windows at launch
  (`window_ms` confirm) or remain instant-trigger?
- Does the business need imbalance data in trader screens at launch (API
  exists; app surface pending)?
- Market-data redistribution policies (per-office vs per-trader quotas) —
  enforced where: Shield attributes vs Feed throttles?

---

**Design attribution:** trade8:feed and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
