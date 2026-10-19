// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! OpenAPI spec for Trade8:Feed.
//!
//! The routed handlers live in `main.rs` (binary crate); the spec describes
//! them through spec-only wrappers (`crate::w`, same method + path - OAPEC
//! §2). The shared Shield policy-admin surface is spec'd through the
//! `sentin8::shell::policy_*` wrappers. The service exposes
//! `GET /openapi.json` via the shell's OpenAPI route.

use utoipa::OpenApi;

/// Spec-only wrappers for the binary's route handlers (OAPEC §2).
pub mod w {
    /// Spec wrapper for `quote`.
    #[utoipa::path(
        get,
        path = "/v1/quotes/{symbol}",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "feed",
    )]
    pub async fn quote() {}

    /// Spec wrapper for `depth`.
    #[utoipa::path(
        get,
        path = "/v1/depth/{symbol}",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "feed",
    )]
    pub async fn depth() {}

    /// Spec wrapper for `list_alerts`.
    #[utoipa::path(
        get,
        path = "/v1/alerts",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "feed",
    )]
    pub async fn list_alerts() {}

    /// Spec wrapper for `create_alert`.
    #[utoipa::path(
        post,
        path = "/v1/alerts",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "feed",
    )]
    pub async fn create_alert() {}

    /// Spec wrapper for `list_feeds`.
    #[utoipa::path(
        get,
        path = "/v1/feeds",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "feed",
    )]
    pub async fn list_feeds() {}

    /// Spec wrapper for `sse_symbol`.
    #[utoipa::path(
        get,
        path = "/v1/stream/{symbol}",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "feed",
    )]
    pub async fn sse_symbol() {}

    /// Spec wrapper for `get_admin_ui`.
    #[utoipa::path(
        get,
        path = "/v1/admin/ui/{page}",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 404, description = "Unknown page"),
        ),
        security(),
        tag = "trade8-admin",
    )]
    pub async fn get_admin_ui() {}

    /// Spec wrapper for `admin_feeds`.
    #[utoipa::path(
        get,
        path = "/v1/admin/feeds",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "trade8-admin",
    )]
    pub async fn admin_feeds() {}

    /// Spec wrapper for `admin_sources`.
    #[utoipa::path(
        get,
        path = "/v1/admin/sources",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "trade8-admin",
    )]
    pub async fn admin_sources() {}

    /// Spec wrapper for `admin_alerts`.
    #[utoipa::path(
        get,
        path = "/v1/admin/alerts",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "trade8-admin",
    )]
    pub async fn admin_alerts() {}

    /// Spec wrapper for `admin_ack_alert` (GET probe).
    #[utoipa::path(
        get,
        path = "/v1/admin/alerts/{id}/ack",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "trade8-admin",
    )]
    pub async fn admin_ack_alert() {}

    /// Spec wrapper for the POST binding of `admin_ack_alert` - utoipa
    /// allows one served path per annotated fn, so the second binding gets
    /// its own.
    #[utoipa::path(
        post,
        path = "/v1/admin/alerts/{id}/ack",
        responses(
            (status = 200, description = "OK", body = serde_json::Value),
            (status = 403, description = "Forbidden"),
        ),
        security(),
        tag = "trade8-admin",
    )]
    pub async fn admin_ack_alert_post() {}

    /// Spec wrapper for the inline health closure.
    #[utoipa::path(
        get,
        path = "/v1/health",
        responses(
            (status = 200, description = "Service is healthy", body = serde_json::Value),
        ),
        security(),
        tag = "feed",
    )]
    pub async fn health() {}
}

/// OpenAPI spec for the feed API.
#[derive(OpenApi)]
#[openapi(
    paths(
        crate::openapi::w::quote,
        crate::openapi::w::depth,
        crate::openapi::w::list_alerts,
        crate::openapi::w::create_alert,
        crate::openapi::w::list_feeds,
        crate::openapi::w::sse_symbol,
        crate::openapi::w::get_admin_ui,
        crate::openapi::w::admin_feeds,
        crate::openapi::w::admin_sources,
        crate::openapi::w::admin_alerts,
        crate::openapi::w::admin_ack_alert,
        crate::openapi::w::admin_ack_alert_post,
        crate::openapi::w::health,
        sentin8::shell::policy_list_sets,
        sentin8::shell::policy_get_set,
        sentin8::shell::policy_put_set,
        sentin8::shell::policy_delete_set,
        sentin8::shell::policy_test_set,
        sentin8::shell::policy_mode_enforce,
        sentin8::shell::policy_mode_shadow,
        sentin8::shell::policy_mode_disable,
        sentin8::shell::policy_list_relations,
        sentin8::shell::policy_put_relation,
        sentin8::shell::policy_delete_relation,
        sentin8::shell::policy_check_relation,
        sentin8::shell::policy_list_attributes,
        sentin8::shell::policy_put_attribute,
        sentin8::shell::policy_delete_attribute,
    ),
    components(schemas()),
    security(),
    tags(
        (name = "feed", description = "Trade8:Feed - market data + alerts"),
        (name = "trade8-admin", description = "Trade8 admin surface (RFW)"),
        (name = "policy", description = "Shield policy administration (shared surface)"),
    ),
)]
pub struct FeedOpenApi;