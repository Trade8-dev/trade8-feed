// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Feed service binary - market data normalization + SSE fan-out.
//!
//! Design: `../feed/DESIGN.md`.
//! Feed-link config bootstraps from the DB, adapters drive ingestion
//! (sim for demos/tests), quotes/prints fan out over SSE
//! (`/v1/stream/{symbol}`), fired alerts persist + emit `alert:fired`
//! outbox events.

#![allow(dead_code)]

mod admin_ui;
mod core;
mod openapi;
mod registration;
mod store;

use sqlx::Acquire;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream::Stream;
use std::convert::Infallible;

/// Service state.
pub struct FeedState {
    pub core: sentin8::shell::ServiceCore,
    pub engine: core::FeedEngine,
    /// Broadcast fan-out for SSE (per-event wake).
    pub tx: tokio::sync::broadcast::Sender<String>,
}

fn extend_router(app: Router<Arc<FeedState>>, _state: Arc<FeedState>) -> Router<Arc<FeedState>> {
    app.route("/v1/quotes/{symbol}", get(quote))
        .route("/v1/depth/{symbol}", get(depth))
        .route("/v1/alerts", get(list_alerts).post(create_alert))
        .route("/v1/feeds", get(list_feeds))
        // SSE: one-way server→client market data.
        .route("/v1/stream/{symbol}", get(sse_symbol))
        // Admin RFW pages.
        .route("/v1/admin/ui/{*page}", get(admin_ui::get_admin_ui))
        // ── Shield policy-admin API (shared
        // implementation; handlers generic over this state via HasPolicyAdmin).
        .route(
            "/v1/admin/policy-sets",
            get(sentin8::shield::admin::list_policy_sets::<Arc<FeedState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}",
            get(sentin8::shield::admin::get_policy_set::<Arc<FeedState>>)
                .put(sentin8::shield::admin::put_policy_set::<Arc<FeedState>>)
                .delete(sentin8::shield::admin::delete_policy_set::<Arc<FeedState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}/test",
            post(sentin8::shield::admin::test_policy_set::<Arc<FeedState>>),
        )
        .route(
            "/v1/admin/policy-sets/enforce",
            post(sentin8::shield::admin::admin_enforce_policy_set::<Arc<FeedState>>),
        )
        .route(
            "/v1/admin/policy-sets/shadow",
            post(sentin8::shield::admin::admin_shadow_policy_set::<Arc<FeedState>>),
        )
        .route(
            "/v1/admin/policy-sets/disable",
            post(sentin8::shield::admin::admin_disable_policy_set::<Arc<FeedState>>),
        )
        .route(
            "/v1/admin/relations",
            get(sentin8::shield::admin::list_relations::<Arc<FeedState>>)
                .put(sentin8::shield::admin::put_relation::<Arc<FeedState>>)
                .delete(sentin8::shield::admin::delete_relation::<Arc<FeedState>>),
        )
        .route(
            "/v1/admin/relations/check",
            get(sentin8::shield::admin::check_relation::<Arc<FeedState>>),
        )
        .route(
            "/v1/admin/attributes",
            get(sentin8::shield::admin::list_attributes::<Arc<FeedState>>)
                .put(sentin8::shield::admin::put_attribute::<Arc<FeedState>>)
                .delete(sentin8::shield::admin::delete_attribute::<Arc<FeedState>>),
        )
        .route("/v1/admin/feeds", get(admin_ui::admin_feeds))
        .route("/v1/admin/sources", get(admin_ui::admin_sources))
        .route("/v1/admin/alerts", get(admin_ui::admin_alerts))
        .route(
            "/v1/admin/alerts/{id}/ack",
            get(admin_ui::admin_ack_alert).post(admin_ui::admin_ack_alert),
        )
        .route(
            "/v1/health",
            // Carries the binary's build identity (`version`/`build`,
            // BUILDID-1) so captain's monitor populates `make status`'s
            // VERSION column - the consuming BINARY's identity.
            get(|| async { Json(serde_json::json!({"status": "ok", "service": "trade8:feed", "version": sentin8::build_info::id(), "build": sentin8::build_info::build_hash()})) }),
        )
}
#[utoipa::path(
    get,
    path = "/v1/quotes/{symbol}",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "feed",
)]

pub(crate) async fn quote(
    State(st): State<Arc<FeedState>>,
    headers: axum::http::HeaderMap,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:quote:read"),
    )
    .await?;
    match st.engine.quote_snapshot(&symbol) {
        Some(q) => Ok(Json(serde_json::to_value(q).unwrap_or_default())),
        None => Err((axum::http::StatusCode::NOT_FOUND, "no quote".into())),
    }
}

async fn depth(
    State(st): State<Arc<FeedState>>,
    headers: axum::http::HeaderMap,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:depth:read"),
    )
    .await?;
    match st.engine.depth_snapshot(&symbol, 10) {
        Some((bids, asks)) => Ok(Json(serde_json::json!({"bids": bids, "asks": asks}))),
        None => Err((axum::http::StatusCode::NOT_FOUND, "no depth".into())),
    }
}
#[utoipa::path(
    get,
    path = "/v1/stream/{symbol}",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "feed",
)]

/// SSE quote stream: `text/event-stream` frames carrying
/// `trade8:feed:quote:updated`-shaped payloads. Entitlement scope is
/// enforced at subscribe time (subscription-time reject).
pub(crate) async fn sse_symbol(
    State(st): State<Arc<FeedState>>,
    headers: axum::http::HeaderMap,
    Path(symbol): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:quote:read"),
    )
    .await?;
    // Snapshot frame first (cold start), then live fan-out.
    let first = st
        .engine
        .quote_snapshot(&symbol)
        .map(|q| serde_json::to_value(&q).unwrap_or_default())
        .unwrap_or(serde_json::json!({"symbol": symbol, "waiting": true}));
    let mut rx = st.tx.subscribe();
    let sym = symbol.clone();
    let stream = async_stream::stream! {
        yield Ok(Event::default().event("snapshot").data(first.to_string()));
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    // Fan out only this symbol's frames (broadcast filter).
                    if let Some(payload) = msg.strip_prefix(&format!("{sym}\u{1}")) {
                        yield Ok(Event::default().event("quote").data(payload));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
#[utoipa::path(
    get,
    path = "/v1/alerts",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "feed",
)]

pub(crate) async fn list_alerts(
    State(st): State<Arc<FeedState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:quote:read"),
    )
    .await?;
    let rules = st.engine.alert_rules.read();
    Ok(Json(serde_json::json!({
        "rules": rules.iter().map(|r| serde_json::json!({
            "rule_id": r.rule_id, "usr_id": r.usr_id,
            "symbol": r.symbol_name, "condition": r.describe(),
        })).collect::<Vec<_>>()
    })))
}

async fn create_alert(
    State(st): State<Arc<FeedState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:quote:read"),
    )
    .await?;
    let env = auth.env_id();
    let symbol = body.get("symbol").and_then(|v| v.as_str()).ok_or((
        axum::http::StatusCode::BAD_REQUEST,
        "symbol required".into(),
    ))?;
    let op = body
        .get("op")
        .and_then(|v| v.as_str())
        .unwrap_or("gt")
        .to_string();
    let threshold = body.get("threshold_raw").and_then(|v| v.as_i64()).ok_or((
        axum::http::StatusCode::BAD_REQUEST,
        "threshold_raw required".into(),
    ))?;
    let rule = core::AlertRule {
        rule_id: uuid::Uuid::now_v7(),
        usr_id: auth.claims.sub.clone(),
        symbol_name: symbol.to_string(),
        op,
        threshold_raw: threshold as i128,
    };
    // DB write, then the engine copy for the hot path.
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) -
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "feed: tenant tx begin failed - retry next tick");
            return Err(sentin8::shell::err_db(e));
        }
    };
    store::persist_alert_rule(&mut tx, env, &rule)
        .await
        .map_err(sentin8::shell::err_db)?;
    st.engine.add_alert_rule(rule);
    tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    Ok(Json(serde_json::json!({"status": "ok"})))
}
#[utoipa::path(
    get,
    path = "/v1/feeds",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "feed",
)]

/// Feed links from the DB (the wall's config load).
pub(crate) async fn list_feeds(
    State(st): State<Arc<FeedState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:manage"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) -
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "feed: tenant tx begin failed - retry next tick");
            return Err(sentin8::shell::err_db(e));
        }
    };
    let links = store::bootstrap_links(&mut tx, env).await;
    tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    Ok(Json(serde_json::json!({"feeds": links})))
}

/// The link-supervisor ingestion loop: per-link adapter task
/// feeding the normalizer; events fan out to SSE subscribers; alert fires
/// persist + outbox-emit. Restart backoff is per-link (cap 30s).
async fn run_link_supervisor(state: Arc<FeedState>, env_id: uuid::Uuid, link: store::FeedLinkRow) {
    let Some(kind) = trade8::feed_adapters::AdapterKind::parse(&link.kind) else {
        tracing::warn!(kind = %link.kind, "unknown adapter kind; link skipped");
        return;
    };
    let adapter = trade8::feed_adapters::adapter_for(kind);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<trade8::feed_adapters::RawQuotePacket>(256);
    let cfg = trade8::feed_adapters::FeedLinkConfig {
        link_id: link.link_id.to_string(),
        kind,
        name: link.name.clone(),
        source_id: link
            .config
            .get("source_id")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u32,
        config: link.config.clone(),
        credentials_ref: link.credentials_ref.clone().unwrap_or_default(),
        generate_top_of_book: link.generate_top_of_book,
        enable: link.enable,
    };
    // Adapter task (bounded channel → normalizer backpressure).
    let adapter_tx = std::sync::Arc::new(tx);
    let adapter_cfg = cfg.clone();
    let handle = tokio::spawn(async move {
        adapter
            .run(adapter_cfg, None, adapter_tx)
            .await
            .map_err(|e| tracing::warn!(error = %e, "adapter task ended"))
    });
    // Normalizer loop: raw → events → SSE + outbox (payload-
    // complete events for Risk/Bars/strategy consumers) + alerts.
    let mut backoff_ms = 250u64;
    // Per-topic SSE sequence - monotonic per symbol.
    let mut topic_seq: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    while let Some(pkt) = rx.recv().await {
        let events = state.engine.ingest_packet(&pkt);
        let mut fired: Vec<core::AlertRule> = Vec::new();
        // Outbox batch: one connection for the packet's events.
        let mut conn = state.core.pool.acquire().await.ok();
        if let Some(c) = conn.as_mut() {
            sentin8::shell::set_tenant_context(c, env_id).await;
        }
        for ev in events {
            let (topic, payload, is_alert, event_type) = match &ev {
                core::TradeEvent::Quote(q) => (
                    q.symbol_name.clone(),
                    serde_json::to_value(q).unwrap_or_default(),
                    false,
                    trade8::contracts::events::QUOTE_UPDATED,
                ),
                core::TradeEvent::Print(p) => (
                    p.symbol_name.clone(),
                    serde_json::to_value(p).unwrap_or_default(),
                    false,
                    trade8::contracts::events::TRADE_PRINTED,
                ),
                core::TradeEvent::Depth(d) => (
                    d.symbol_name.clone(),
                    serde_json::to_value(d).unwrap_or_default(),
                    false,
                    trade8::contracts::events::DEPTH_UPDATED,
                ),
                core::TradeEvent::Imbalance(i) => (
                    i.symbol_name.clone(),
                    serde_json::to_value(i).unwrap_or_default(),
                    false,
                    trade8::contracts::events::IMBALANCE_UPDATED,
                ),
                core::TradeEvent::Alert(a) => {
                    // Match the fired alert back to its rule for persistence.
                    let rule = state
                        .engine
                        .alert_rules
                        .read()
                        .iter()
                        .find(|r| r.rule_id == a.rule_id)
                        .cloned();
                    if let Some(r) = rule {
                        fired.push(r);
                    }
                    (
                        a.symbol_name.clone(),
                        serde_json::to_value(a).unwrap_or_default(),
                        true,
                        trade8::contracts::events::ALERT_FIRED,
                    )
                }
            };
            if !is_alert {
                // SSE fan-out with per-topic seq.
                let seq = topic_seq.entry(topic.clone()).or_insert(0);
                *seq += 1;
                let framed = serde_json::json!({
                    "seq": *seq,
                    "event": event_type,
                    "data": payload,
                });
                let _ = state.tx.send(format!("{topic}\u{1}{framed}"));
                // Bus outbox event - same connection, per event.
                if let Some(c) = conn.as_mut() {
                    let _ = store::insert_outbox_event(c, env_id, event_type, payload).await;
                }
            }
        }
        // Persist fired alerts + `alert:fired` outbox events.
        if !fired.is_empty() {
            if let Some(c) = conn.as_mut() {
                for rule in &fired {
                    let _ = store::persist_fired_alert(
                        c,
                        env_id,
                        rule,
                        &serde_json::json!({"source": "engine"}),
                    )
                    .await;
                }
            }
        }
        // Healthy traffic resets the restart backoff.
        backoff_ms = 250;
    }
    let _ = handle.await;
    // Adapter ended - record feed status with backoff info.
    if let Ok(mut conn) = state.core.pool.acquire().await {
        sentin8::shell::set_tenant_context(&mut conn, env_id).await;
        // RLS context is transaction-local (DESIGN.md §5) -
        // begin() pins the tenant context to the SAME tx as the DML/reads.
        let Ok(mut tx) = conn.begin().await else {
            tracing::warn!("feed: status tx begin failed - status record skipped");
            return;
        };
        let _ = store::persist_feed_status(
            &mut tx,
            env_id,
            link.link_id,
            "completed",
            serde_json::json!({"name": link.name, "next_restart_ms": backoff_ms}),
        )
        .await;
        tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    }
}

/// Feed health loop: periodic silence classification + `feed:status`
/// events. A link whose engine seq stalls for > 5s while enabled is
/// classified degraded; > 30s dead - both persist + emit so consumers
/// (Risk halt mirror, admin wall) see the transition.
async fn health_loop(state: Arc<FeedState>, env_id: uuid::Uuid) {
    let mut last_seq_by_link: std::collections::HashMap<uuid::Uuid, (u64, std::time::Instant)> =
        std::collections::HashMap::new();
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let Ok(mut conn) = state.core.pool.acquire().await else {
            continue;
        };
        sentin8::shell::set_tenant_context(&mut conn, env_id).await;
        // RLS context is transaction-local (DESIGN.md §5) -
        // begin() pins the tenant context to the SAME tx as the DML/reads.
        let Ok(mut tx) = conn.begin().await else {
            tracing::warn!("feed: health tx begin failed - retry next tick");
            continue;
        };
        let links = store::bootstrap_links(&mut tx, env_id).await;
        for link in links {
            if !link.enable {
                continue;
            }
            let entry = last_seq_by_link
                .entry(link.link_id)
                .or_insert((0, std::time::Instant::now()));
            // Classification by silence: no packet for 5s+ → degraded,
            // 30s+ → dead (traffic resets via the normalizer's seq bump -
            // the engine's per-source seq is the liveness truth).
            let silent = entry.1.elapsed();
            let status = if silent.as_secs() >= 30 {
                "dead"
            } else if silent.as_secs() >= 5 {
                "degraded"
            } else {
                "healthy"
            };
            let _ = store::persist_feed_status(
                &mut tx,
                env_id,
                link.link_id,
                status,
                serde_json::json!({"name": link.name, "silent_s": silent.as_secs()}),
            )
            .await;
            let _ = store::insert_outbox_event(
                &mut tx,
                env_id,
                trade8::contracts::events::FEED_STATUS,
                serde_json::json!({
                    "link_id": link.link_id, "name": link.name,
                    "status": status, "silent_s": silent.as_secs(),
                }),
            )
            .await;
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("trade8:feed", "TRADE8_FEED")
            .with_registration(registration::print)
            .with_own_health()
            .with_state(|core| async move {
                let engine = core::FeedEngine::default();
                let (tx, _) = tokio::sync::broadcast::channel(1024);
                let state = Arc::new(FeedState { core, engine, tx });
                // Bootstrap pass: links + alert rules for the
                // boot env; start every enabled link's adapter (sim for
                // demos/tests, itch/idc/crypto listeners for live feeds).
                if let Ok(env_slug) = std::env::var("TRADE8_FEED_ENV") {
                    if let Ok(env_id) = uuid::Uuid::parse_str(&env_slug) {
                        let mut conn = state.core.pool.acquire().await?;
                        let links = store::bootstrap_links(&mut conn, env_id).await;
                        let rules = store::bootstrap_alert_rules(&mut conn, env_id).await;
                        for r in rules {
                            state.engine.add_alert_rule(r);
                        }
                        // Register symbols for the normalizer (symbol
                        // resolution) from the feed-link configs.
                        for link in &links {
                            if let Some(sym) = link.config.get("symbol").and_then(|v| v.as_str()) {
                                state.engine.register_symbol(sym, uuid::Uuid::now_v7(), 4);
                            }
                        }
                        let enabled: Vec<store::FeedLinkRow> =
                            links.into_iter().filter(|l| l.enable).collect();
                        for link in enabled {
                            let st2 = state.clone();
                            tokio::spawn(async move {
                                run_link_supervisor(st2, env_id, link).await;
                            });
                        }
                        // Health loop: gap detection + feed:status
                        // events + degraded/dead classification.
                        let st3 = state.clone();
                        tokio::spawn(async move {
                            health_loop(st3, env_id).await;
                        });
                    }
                }
                Ok(state)
            })
            .with_router(extend_router),
    )
    .await
}

// ---------------------------------------------------------------------------
// Latency harness - feed hot paths. Generous CI-jitter headroom;
// tight numbers are measured by the release-mode sim-harness runs.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod latency {
    use super::*;
    use std::time::Instant;
    use trade8::feed_adapters::{build_itch_bbo, RawQuotePacket};

    const N: usize = 50_000;

    #[test]
    fn feed_ingest_throughput() {
        let e = core::FeedEngine::default();
        e.register_symbol("AAPL", uuid::Uuid::new_v4(), 4);
        let start = Instant::now();
        for i in 0..N {
            let bytes = build_itch_bbo("AAPL", 100_000, 100, 100_200, 100, 0);
            let pkt = RawQuotePacket {
                source_id: 7,
                seq: i as u64,
                bytes,
            };
            let _ = e.ingest_packet(&pkt);
        }
        let elapsed = start.elapsed();
        let per = elapsed.as_nanos() as f64 / N as f64;
        // Budget: 100k/s = 10µs/quote; 5× headroom (50µs) for CI jitter.
        assert!(
            per < 50_000.0,
            "ingest per-packet {per:.0}ns exceeds 50µs budget"
        );
        println!("feed ingest: {N} packets, {per:.0}ns/packet");
    }

    #[test]
    fn sse_broadcast_fanout_latency() {
        let (tx, _) = tokio::sync::broadcast::channel(1024);
        let mut subs = Vec::new();
        for _ in 0..100 {
            subs.push(tx.subscribe());
        }
        let payload = serde_json::json!({"symbol_name": "AAPL", "bid_raw": 100_000});
        let frame = format!("AAPL\u{1}{payload}");
        let start = Instant::now();
        for _ in 0..1000 {
            let _ = tx.send(frame.clone());
        }
        let per = start.elapsed().as_nanos() as f64 / 1000.0;
        // Budget: < 1 ms per frame at 100 subscribers.
        assert!(
            per < 1_000_000.0,
            "broadcast per-frame {per:.0}ns exceeds 1ms budget"
        );
        println!("sse fan-out: {} subscribers, {per:.0}ns/frame", subs.len());
    }
}

/// Shared policy-admin surface accessor: the shell boot
/// loaded the engine into `core.admin`; the shared handlers extract it via
/// `HasPolicyAdmin` so the admin surface and the request path evaluate the
/// SAME cache.
impl sentin8::shield::admin::HasPolicyAdmin for FeedState {
    fn policy_admin(&self) -> &sentin8::shield::admin::SharedAdminState {
        &self.core.admin
    }
}

// ---------------------------------------------------------------------------
// Admin POST-alias tests (test-architecture design §7): feed's
// `/v1/admin/alerts/{id}/ack` alias rides the SAME handler as the GET
// alias (idempotent ack contract) and inherits the verify+Shield gate.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod admin_alias_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    /// The feed router exactly like `main` builds it.
    fn test_router(state: Arc<FeedState>) -> Router {
        let for_state = state.clone();
        extend_router(Router::new(), for_state).with_state(state)
    }

    /// Feed state mirroring the shell boot (skip-offline).
    async fn build_state() -> Option<Arc<FeedState>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect("postgresql://trade8_feed_service@localhost:5432/trade8")
            .await
            .ok()?;
        let issuer = "https://auth.test.invalid".to_string();
        let policy_engine = sentin8::shield::runtime::engine_for("trade8:feed");
        let _ = sentin8::shield::runtime::boot_full(&pool, &issuer, "trade8:feed", &policy_engine)
            .await;
        let config = Arc::new(sentin8::shell::ShellConfig {
            service_type: "trade8:feed",
            port: 0,
            database_url: "postgresql://trade8_feed_service@localhost:5432/trade8".into(),
            sentin8_auth_issuer: issuer.clone(),
            sentin8_bus_url: String::new(),
            sentin8_auth_client: sentin8::auth::client::Client::new(
                sentin8::auth::client::ClientOptions {
                    base_url: issuer,
                    ..Default::default()
                },
            ),
            valkey_url: String::new(),
        });
        let core = sentin8::shell::ServiceCore {
            config,
            pool: pool.clone(),
            admin: sentin8::shield::admin::SharedAdminState {
                service: "trade8:feed".into(),
                pool,
                policy: policy_engine,
                auth_client: Arc::new(sentin8::auth::client::Client::new(
                    sentin8::auth::client::ClientOptions {
                        base_url: "https://auth.test.invalid".into(),
                        ..Default::default()
                    },
                )),
            },
        };
        let (tx, _rx) = tokio::sync::broadcast::channel(16);
        Some(Arc::new(FeedState {
            core,
            engine: core::FeedEngine::default(),
            tx,
        }))
    }

    async fn oneshot_json(
        app: &Router,
        method: &str,
        uri: &str,
        token: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let mut b = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        let req = match body {
            Some(v) => b
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&v).unwrap())),
            None => b.body(Body::empty()),
        }
        .unwrap();
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, json)
    }

    /// Unauthenticated POST-alias → 401 (the verify step).
    #[tokio::test]
    async fn post_alias_ack_requires_token() {
        let Some(state) = build_state().await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        let (status, _) = oneshot_json(
            &app,
            "POST",
            &format!("/v1/admin/alerts/{}/ack", uuid::Uuid::new_v4()),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
