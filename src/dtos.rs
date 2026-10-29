// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! Typed OpenAPI bodies . Shared between the spec module and the
//! binary handlers.

/// Liveness/build identity (typed body).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct HealthInfo {
    /// Always `ok` while serving.
    pub status: String,
    /// The service identifier.
    pub service: String,
    /// The build version.
    pub version: String,
    /// The build hash.
    pub build: String,
}

/// Depth ladder envelope (one level = `[px_i128-as-text, qty]`).
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct DepthResponse {
    /// Bid ladder (best first).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bids: Option<Vec<Vec<serde_json::Value>>>,
    /// Ask ladder (best first).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asks: Option<Vec<Vec<serde_json::Value>>>,
}

/// Alert-rule row.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AlertRuleRow {
    /// The rule id.
    pub rule_id: uuid::Uuid,
    /// The owning principal.
    pub usr_id: String,
    /// The watched symbol.
    pub symbol: String,
    /// Human-readable condition.
    pub condition: String,
}

/// Alert-rules list envelope.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AlertRulesResponse {
    /// The configured rules.
    pub rules: Vec<AlertRuleRow>,
}

/// Alert creation request.
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct CreateAlertRequest {
    /// The symbol to watch.
    pub symbol: String,
    /// Comparison op (defaults to `gt`).
    #[serde(default)]
    pub op: Option<String>,
    /// The raw threshold (scaled i128, parsed as i64 for the engine hot path).
    pub threshold_raw: String,
}

/// Ack envelope.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AckResponse {
    /// The action status (e.g. `ok`, `acked`).
    pub status: String,
}

/// Feed links list envelope.
#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct FeedLinksResponse {
    /// The configured feed links.
    pub feeds: Vec<crate::store::FeedLinkRow>,
}
