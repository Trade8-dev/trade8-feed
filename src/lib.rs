// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! trade8-feed library surface — the engine + store modules shared by
//! the service binary and the latency harness integration test.
//! `admin_ui` (RFW pages) and the handlers live in the binary only —
//! they need `FeedState` which is bin-local.

pub mod core;
pub mod registration;
pub mod store;
