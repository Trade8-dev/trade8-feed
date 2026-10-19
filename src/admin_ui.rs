// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Feed admin UI - RFW pages + data endpoints.

use crate::AckResponse;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use sentin8::rfw::page::AdminPage;
use sentin8::rfw::response::AdminUiResponse;
use sentin8::rfw::widget::*;

use crate::FeedState;

fn cols(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: true,
        width: None,
        format: None,
    }
}
fn col(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: false,
        width: None,
        format: None,
    }
}
fn table(columns: Vec<WidgetDataTableColumn>, endpoint: &str) -> RemoteWidget {
    RemoteWidget::DataTable(WidgetDataTable {
        columns,
        data_endpoint: endpoint.into(),
        sortable: true,
        filterable: true,
        page_size: 25,
        empty_message: Some("No data.".into()),
        row_link: None,
        row_actions: vec![],
    })
}
fn card(title: &str, body: &str) -> RemoteWidget {
    RemoteWidget::Card(WidgetCard {
        title: Some(title.into()),
        subtitle: None,
        children: vec![RemoteWidget::Text(WidgetText {
            text: body.into(),
            text_type: WidgetTextType::Body,
            align: TextAlign::Left,
            max_lines: None,
        })],
        actions: vec![],
    })
}
#[utoipa::path(
    get,
    path = "/v1/admin/ui/{page}",
    responses(
        (status = 200, description = "OK", body = sentin8::rfw::AdminUiResponse),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/ui/{page}` - RFW definitions.
pub async fn get_admin_ui(
    State(_st): State<std::sync::Arc<FeedState>>,
    headers: HeaderMap,
    Path(page): Path<String>,
) -> Result<Json<AdminUiResponse>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&_st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &_st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:quote:read"),
    )
    .await?;
    let def = match page.as_str() {
        "feeds" => AdminPage::new("Feed Links")
            .widget(table(
                vec![
                    cols("name", "Link"),
                    cols("kind", "Adapter"),
                    cols("quote_source_id", "Source"),
                    cols("generate_top_of_book", "Top-of-Book"),
                    cols("enable", "Enabled"),
                ],
                "/v1/admin/feeds",
            ))
            .build(),
        "sources" => AdminPage::new("Quote Sources")
            .widget(table(
                vec![
                    cols("name", "Source"),
                    cols("display_name", "Display"),
                    cols("is_book", "Is Book"),
                    cols("create_tos", "Creates TOs"),
                ],
                "/v1/admin/sources",
            ))
            .build(),
        "alerts" => AdminPage::new("Alerts")
            .widget(card(
                "Alert Queue",
                "Fired alerts with ack state. Ack via the POST alias endpoint.",
            ))
            .widget(table(
                vec![
                    cols("alert_id", "Alert ID"),
                    cols("symbol_name", "Symbol"),
                    cols("condition_text", "Condition"),
                    cols("usr_id", "Trader"),
                    cols("at", "Fired At"),
                    col("ack_by", "Acked By"),
                ],
                "/v1/admin/alerts",
            ))
            .build(),
        _ => {
            return Err((
                axum::http::StatusCode::NOT_FOUND,
                format!("Unknown admin UI page: {page}"),
            ))
        }
    };
    Ok(Json(
        AdminUiResponse::page(def)
    ))
}

type Qp = std::collections::HashMap<String, String>;
#[utoipa::path(
    get,
    path = "/v1/admin/feeds",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/feeds` - link rows.
pub async fn admin_feeds(
    State(st): State<std::sync::Arc<FeedState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
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
    let rows: Vec<(uuid::Uuid, String, String, uuid::Uuid, bool, bool)> = sqlx::query_as(
        r#"SELECT link_id, kind, name, quote_source_id, generate_top_of_book, enable
           FROM feed.feed_link WHERE env_id = $1 ORDER BY name"#,
    )
    .bind(env)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(rows
        .into_iter()
        .map(|(link_id, kind, name, quote_source_id, gtob, enable)| {
            serde_json::json!({
                "link_id": link_id, "kind": kind, "name": name,
                "quote_source_id": quote_source_id,
                "generate_top_of_book": gtob, "enable": enable,
            })
        })
        .collect::<Vec<_>>()))
}
#[utoipa::path(
    get,
    path = "/v1/admin/sources",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/sources` - quote source rows.
pub async fn admin_sources(
    State(st): State<std::sync::Arc<FeedState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
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
    let rows: Vec<(String, Option<String>, bool, bool)> = sqlx::query_as(
        r#"SELECT name, display_name, is_book, create_tos
           FROM feed.quote_source WHERE env_id = $1 ORDER BY name"#,
    )
    .bind(env)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(rows
        .into_iter()
        .map(|(name, display_name, is_book, create_tos)| {
            serde_json::json!({
                "name": name, "display_name": display_name,
                "is_book": is_book, "create_tos": create_tos,
            })
        })
        .collect::<Vec<_>>()))
}
#[utoipa::path(
    get,
    path = "/v1/admin/alerts",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/alerts` - fired alerts (latest first).
pub async fn admin_alerts(
    State(st): State<std::sync::Arc<FeedState>>,
    headers: HeaderMap,
    Query(q): Query<Qp>,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:quote:read"),
    )
    .await?;
    let env = a.env_id();
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(100)
        .min(500);
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    type AdminAlertRow = (
        uuid::Uuid,
        String,
        String,
        String,
        chrono::DateTime<chrono::Utc>,
        Option<String>,
    );
    let rows: Vec<AdminAlertRow> = sqlx::query_as(
        r#"SELECT alert_id, symbol_name, condition_text, usr_id, at, ack_by
               FROM feed.alert WHERE env_id = $1
               ORDER BY at DESC LIMIT $2"#,
    )
    .bind(env)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(rows
        .into_iter()
        .map(
            |(alert_id, symbol_name, condition_text, usr_id, at, ack_by)| {
                serde_json::json!({
                    "alert_id": alert_id, "symbol_name": symbol_name,
                    "condition_text": condition_text, "usr_id": usr_id,
                    "at": at, "ack_by": ack_by,
                })
            }
        )
        .collect::<Vec<_>>()))
}
#[utoipa::path(
    get,
    path = "/v1/admin/alerts/{id}/ack",
    responses(
        (status = 200, description = "Alert acked", body = AckResponse),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `POST /v1/admin/alerts/{id}/ack` - RFW POST-alias ack.
pub async fn admin_ack_alert(
    State(st): State<std::sync::Arc<FeedState>>,
    headers: HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<AckResponse>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:feed:quote:read"),
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
    let ok = crate::store::ack_alert(&mut conn, env, id, &a.claims.sub)
        .await
        .map_err(sentin8::shell::err_db)?;
    if ok {
        Ok(Json(AckResponse {
        status: "acked".into(),
    }))
    } else {
        Err((
            axum::http::StatusCode::NOT_FOUND,
            "alert not found or already acked".into(),
        ))
    }
}
