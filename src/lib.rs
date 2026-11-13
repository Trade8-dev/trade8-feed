// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! trade8-feed library surface - the engine + store modules shared by
//! the service binary and the latency harness integration test.
//! `admin_ui` (RFW pages) and the handlers live in the binary only -
//! they need `FeedState` which is bin-local.

pub mod core;
pub mod dtos;
pub mod openapi;

// The utoipa dump for registration::print - mirrored from main.rs; the lib
// crate compiles registration::print too (harnesses embedding via #[path]
// never invoke it, so the pointer is referenced, not run, there).
pub static OPENAPI_DUMP: fn() -> serde_json::Value = || {
    use utoipa::OpenApi as _;
    serde_json::to_value(openapi::FeedOpenApi::openapi()).unwrap_or(serde_json::Value::Null)
};

pub mod registration;
pub mod store;
