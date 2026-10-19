// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! Feed DB persistence - link/source bootstrap, alert rule
//! CRUD, fired-alert persistence + `alert:fired` events (outbox-first).

use sqlx::PgConnection;
use trade8::contracts::events;
use uuid::Uuid;

use crate::core::AlertRule;

/// Feed link config row (the link supervisor's boot input).
#[derive(Debug, Clone, PartialEq, serde::Serialize, utoipa::ToSchema)]
pub struct FeedLinkRow {
    pub link_id: Uuid,
    pub kind: String,
    pub name: String,
    pub quote_source_id: Uuid,
    pub config: serde_json::Value,
    pub credentials_ref: Option<String>,
    pub generate_top_of_book: bool,
    pub enable: bool,
}

/// Load enabled feed links for one env (config load).
pub async fn bootstrap_links(conn: &mut PgConnection, env_id: Uuid) -> Vec<FeedLinkRow> {
    match bootstrap_links_inner(conn, env_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "trade8:feed: link bootstrap failed (no links)");
            Vec::new()
        }
    }
}

async fn bootstrap_links_inner(
    conn: &mut PgConnection,
    env_id: Uuid,
) -> anyhow::Result<Vec<FeedLinkRow>> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    type LinkRowTuple = (
        Uuid,
        String,
        String,
        Uuid,
        serde_json::Value,
        Option<String>,
        bool,
        bool,
    );
    let rows: Vec<LinkRowTuple> = sqlx::query_as(
        r#"SELECT link_id, kind, name, quote_source_id, config,
                      credentials_ref, generate_top_of_book, enable
               FROM feed.feed_link WHERE env_id = $1 AND enable
               ORDER BY name"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                link_id,
                kind,
                name,
                quote_source_id,
                config,
                credentials_ref,
                generate_top_of_book,
                enable,
            )| {
                FeedLinkRow {
                    link_id,
                    kind,
                    name,
                    quote_source_id,
                    config,
                    credentials_ref,
                    generate_top_of_book,
                    enable,
                }
            },
        )
        .collect())
}

/// Load active alert rules into the engine.
pub async fn bootstrap_alert_rules(conn: &mut PgConnection, env_id: Uuid) -> Vec<AlertRule> {
    match bootstrap_rules_inner(conn, env_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "trade8:feed: alert rule bootstrap failed");
            Vec::new()
        }
    }
}

async fn bootstrap_rules_inner(
    conn: &mut PgConnection,
    env_id: Uuid,
) -> anyhow::Result<Vec<AlertRule>> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let rows: Vec<(Uuid, String, String, serde_json::Value)> = sqlx::query_as(
        r#"SELECT rule_id, usr_id, symbol_name, condition
           FROM feed.alert_rule WHERE env_id = $1 AND active"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(rule_id, usr_id, symbol_name, condition)| {
            Some(AlertRule {
                rule_id,
                usr_id,
                symbol_name,
                op: condition.get("op")?.as_str()?.to_string(),
                threshold_raw: condition.get("threshold")?.as_i64()? as i128,
            })
        })
        .collect())
}

/// Persist a new alert rule (CRUD).
pub async fn persist_alert_rule(
    conn: &mut PgConnection,
    env_id: Uuid,
    rule: &AlertRule,
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    sqlx::query(
        r#"INSERT INTO feed.alert_rule
           (rule_id, env_id, usr_id, symbol_name, condition, active)
           VALUES ($1,$2,$3,$4,$5,true)
           ON CONFLICT (rule_id) DO UPDATE SET
             condition = EXCLUDED.condition, active = true"#,
    )
    .bind(rule.rule_id)
    .bind(env_id)
    .bind(&rule.usr_id)
    .bind(&rule.symbol_name)
    .bind(serde_json::json!({"op": rule.op, "threshold": rule.threshold_raw as i64}))
    .execute(&mut *conn)
    .await
    .map(|_| ())
}

/// Persist a fired alert + `alert:fired` outbox event (same tx).
#[allow(clippy::too_many_arguments)]
pub async fn persist_fired_alert(
    conn: &mut PgConnection,
    env_id: Uuid,
    rule: &AlertRule,
    quote: &serde_json::Value,
) -> Result<Uuid, sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let alert_id = Uuid::now_v7();
    sqlx::query(
        r#"INSERT INTO feed.alert
           (alert_id, env_id, rule_id, usr_id, symbol_name, quote, condition_text)
           VALUES ($1,$2,$3,$4,$5,$6,$7)"#,
    )
    .bind(alert_id)
    .bind(env_id)
    .bind(rule.rule_id)
    .bind(&rule.usr_id)
    .bind(&rule.symbol_name)
    .bind(quote)
    .bind(rule.describe())
    .execute(&mut *conn)
    .await?;
    insert_outbox_event(
        conn,
        env_id,
        events::ALERT_FIRED,
        serde_json::json!({
            "alert_id": alert_id, "rule_id": rule.rule_id,
            "usr_id": rule.usr_id, "symbol_name": rule.symbol_name,
            "condition": rule.describe(), "quote": quote,
        }),
    )
    .await?;
    Ok(alert_id)
}

/// Ack a fired alert (the alerts-panel ack action).
pub async fn ack_alert(
    conn: &mut PgConnection,
    env_id: Uuid,
    alert_id: Uuid,
    ack_by: &str,
) -> Result<bool, sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let res = sqlx::query(
        r#"UPDATE feed.alert SET ack_by = $3, ack_at = now()
           WHERE alert_id = $1 AND env_id = $2 AND ack_by IS NULL"#,
    )
    .bind(alert_id)
    .bind(env_id)
    .bind(ack_by)
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Feed-status event helper (gap/dead classification events).
pub async fn persist_feed_status(
    conn: &mut PgConnection,
    env_id: Uuid,
    link_id: Uuid,
    status: &str,
    detail: serde_json::Value,
) -> Result<(), sqlx::Error> {
    insert_outbox_event(
        conn,
        env_id,
        events::FEED_STATUS,
        serde_json::json!({"link_id": link_id, "status": status, "detail": detail}),
    )
    .await
}

/// Outbox insert - drained by the shell's boot-step-8b relay to Bus.
pub async fn insert_outbox_event(
    conn: &mut PgConnection,
    env_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), sqlx::Error> {
    let row_id =
        sentin8::bus::outbox::insert_outbox_row(&mut *conn, env_id, "feed", event_type, &payload)
            .await
            .map_err(|e| match e {
                sentin8::bus::outbox::OutboxError::Db(db) => db,
                other => sqlx::Error::Configuration(other.to_string().into()),
            })?;
    let _ = row_id;
    Ok(())
}
