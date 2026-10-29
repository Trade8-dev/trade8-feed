// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Feed SQL integration tests (test-architecture-design §5.6).
//!
//! Real `trade8_feed_service` NOBYPASSRLS role: link bootstrap with the
//! enable flag, alert rule → fired → ack lifecycle, and the RLS matrix.
//!
//! Design: `../../DESIGN.md §9 (testing)` §5.6.

#[path = "../src/core.rs"]
#[allow(dead_code)]
mod core;
#[path = "../src/store.rs"]
#[allow(dead_code)]
mod store;

use core::AlertRule;
use sqlx::postgres::PgPoolOptions;
use sqlx::Acquire;
use trade8::contracts::events;
use uuid::Uuid;

const FEED_URL: &str = "postgresql://trade8_feed_service@localhost:5432/trade8";
const PROBE_URL: &str = "postgresql://trade8_probe@localhost:5432/trade8";

fn env_a() -> Uuid {
    Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
}
fn env_b() -> Uuid {
    Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
}

async fn feed_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(FEED_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

async fn probe_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(PROBE_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

async fn cleanup(conn: &mut sqlx::PgConnection, env: Uuid, ids: &[Uuid]) {
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    for id in ids {
        let _ = sqlx::query(r#"DELETE FROM feed.feed_link WHERE env_id=$1 AND link_id=$2"#)
            .bind(env)
            .bind(id)
            .execute(&mut *tx)
            .await;
        let _ = sqlx::query(r#"DELETE FROM feed.quote_source WHERE env_id=$1 AND source_id=$2"#)
            .bind(env)
            .bind(id)
            .execute(&mut *tx)
            .await;
        let _ = sqlx::query(r#"DELETE FROM feed.alert WHERE env_id=$1 AND rule_id=$2"#)
            .bind(env)
            .bind(id)
            .execute(&mut *tx)
            .await;
        let _ = sqlx::query(r#"DELETE FROM feed.alert_rule WHERE env_id=$1 AND rule_id=$2"#)
            .bind(env)
            .bind(id)
            .execute(&mut *tx)
            .await;
    }
    let _ = sqlx::query(r#"DELETE FROM feed.outbox WHERE env_id=$1 AND stream_family='feed' AND created_at > now() - interval '1 minute'"#)
        .bind(env)
        .execute(&mut *tx)
        .await;
    tx.commit().await.unwrap();
}

// ------------------------------------------------------------------------
// RLS matrix
// ------------------------------------------------------------------------

#[tokio::test]
async fn rls_matrix_no_context_returns_zero() {
    let Some(mut conn) = probe_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM feed.feed_link")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(n, 0, "no tenant context must see zero rows");
}

#[tokio::test]
async fn rls_matrix_cross_env_returns_zero() {
    let Some(mut conn) = feed_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let link = Uuid::new_v4();
    let src = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        sqlx::query(
            r#"INSERT INTO feed.quote_source (source_id, env_id, name)
               VALUES ($1,$2,'src-x')"#,
        )
        .bind(src)
        .bind(env_a())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO feed.feed_link (link_id, env_id, kind, name, quote_source_id, config, enable)
               VALUES ($1,$2,'sim','link-x',$3,'{}',true)"#,
        )
        .bind(link)
        .bind(env_a())
        .bind(src)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_b()).await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM feed.feed_link WHERE link_id = $1")
        .bind(link)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(n, 0, "env-b must not see env-a's feed links");
    cleanup(&mut conn, env_a(), &[link, src]).await;
}

// ------------------------------------------------------------------------
// Link bootstrap with enable flag (design §5.6)
// ------------------------------------------------------------------------

#[tokio::test]
async fn bootstrap_links_respects_enable_flag() {
    let Some(mut conn) = feed_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let enabled = Uuid::new_v4();
    let disabled = Uuid::new_v4();
    let src = Uuid::new_v4();
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        sqlx::query(
            r#"INSERT INTO feed.quote_source (source_id, env_id, name) VALUES ($1,$2,'src-b')"#,
        )
        .bind(src)
        .bind(env_a())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO feed.feed_link (link_id, env_id, kind, name, quote_source_id, config, credentials_ref, enable)
               VALUES ($1,$2,'sim','link-on',$3,$4,'ref://x',true)"#,
        )
        .bind(enabled)
        .bind(env_a())
        .bind(src)
        .bind(serde_json::json!({"endpoints": ["udp://239.1.1.1:10001"]}))
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO feed.feed_link (link_id, env_id, kind, name, quote_source_id, config, enable)
               VALUES ($1,$2,'sim','link-off',$3,'{}',false)"#,
        )
        .bind(disabled)
        .bind(env_a())
        .bind(src)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    // bootstrap_links sets its own context (tx-local) - call in a tx (DESIGN.md §5).
    let links = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::bootstrap_links(&mut tx, env_a()).await;
        tx.commit().await.unwrap();
        v
    };
    let mine: Vec<_> = links.iter().filter(|l| l.link_id == enabled).collect();
    assert_eq!(mine.len(), 1, "enabled link must load");
    assert!(
        links.iter().all(|l| l.link_id != disabled),
        "disabled link must be excluded"
    );
    assert_eq!(
        mine[0].config["endpoints"][0], "udp://239.1.1.1:10001",
        "config JSONB round-trips"
    );
    assert_eq!(
        mine[0].credentials_ref.as_deref(),
        Some("ref://x"),
        "credentials stay by-reference"
    );
    cleanup(&mut conn, env_a(), &[enabled, disabled, src]).await;
}

// ------------------------------------------------------------------------
// Alert rule → fired → ack lifecycle (design §5.6)
// ------------------------------------------------------------------------

#[tokio::test]
async fn alert_rule_roundtrip_and_fired_persist() {
    let Some(mut conn) = feed_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let rule_id = Uuid::new_v4();
    let rule = AlertRule {
        rule_id,
        usr_id: "u-sql".into(),
        symbol_name: "AAPL".into(),
        op: "lte".into(),
        threshold_raw: 99_500, // 9.95 ×10⁴
    };
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_alert_rule(&mut tx, env_a(), &rule)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    // The bootstrap parses the condition JSONB back into the engine shape.
    let loaded = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::bootstrap_alert_rules(&mut tx, env_a()).await;
        tx.rollback().await.unwrap();
        v
    };
    let mine: Vec<_> = loaded
        .into_iter()
        .filter(|r| r.rule_id == rule_id)
        .collect();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].op, "lte");
    assert_eq!(mine[0].threshold_raw, 99_500);

    // Fire → persist + outbox event.
    let quote = serde_json::json!({"symbol": "AAPL", "bid": 99.4});
    let alert_id = {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        let id = store::persist_fired_alert(&mut tx, env_a(), &rule, &quote_val(&quote))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    };
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let (ack_by, condition): (Option<String>, Option<String>) = sqlx::query_as(
        r#"SELECT ack_by, condition_text FROM feed.alert WHERE alert_id = $1 AND env_id = $2"#,
    )
    .bind(alert_id)
    .bind(env_a())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert!(ack_by.is_none(), "fired alert starts unacked");
    assert!(condition.is_some());
    let events_: i64 = sqlx::query_scalar(
        r#"SELECT count(*) FROM feed.outbox WHERE env_id=$1 AND event_type=$2 AND payload->>'alert_id'=$3"#,
    )
    .bind(env_a())
    .bind(events::ALERT_FIRED)
    .bind(alert_id.to_string())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert!(events_ >= 1, "fired alert must emit the outbox event");
    tx.commit().await.unwrap();

    // Ack once; a second ack is a no-op (already-acked predicate).
    let first = {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        let v = store::ack_alert(&mut tx, env_a(), alert_id, "u-sql")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        v
    };
    let second = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::ack_alert(&mut tx, env_a(), alert_id, "u-sql")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        v
    };
    assert!(first, "first ack must stamp the row");
    assert!(!second, "second ack must be a no-op");
    cleanup(&mut conn, env_a(), &[rule_id]).await;
}

/// Quote JSON value for the fired-alert persist (kept simple for the
/// fixture; the engine's real quote shape is covered by core unit tests).
fn quote_val(v: &serde_json::Value) -> serde_json::Value {
    v.clone()
}
