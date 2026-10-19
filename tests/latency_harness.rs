// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! Latency harness - Feed hot paths:
//!   - Normalizer: ingest → events at 100k quotes/s sustained
//!   - SSE fan-out: broadcast send < 1 ms at 100 subscribers
//!
//! Wall-clock assertions carry generous CI-jitter headroom (5× the
//! design budgets); the tight numbers are measured by release-mode
//! sim-harness runs. (The bars roll-up + pnl fill→summary halves of the
//! The other services' budgets live in their own test modules.)

use std::time::Instant;

use trade8::feed_adapters::{build_itch_bbo, RawQuotePacket};

use trade8_feed_server::core::FeedEngine;

const N: usize = 50_000;

fn bbo_pkt(source: u32, seq: u64, symbol: &str) -> RawQuotePacket {
    let bytes = build_itch_bbo(symbol, 100_000, 100, 100_200, 100, 0);
    RawQuotePacket {
        source_id: source,
        seq,
        bytes,
    }
}

#[test]
fn feed_ingest_throughput() {
    let e = FeedEngine::default();
    e.register_symbol("AAPL", uuid::Uuid::new_v4(), 4);
    let start = Instant::now();
    for i in 0..N {
        let pkt = bbo_pkt(7, i as u64, "AAPL");
        let _ = e.ingest_packet(&pkt);
    }
    let elapsed = start.elapsed();
    let per = elapsed.as_nanos() as f64 / N as f64;
    // Budget: 100k/s = 10µs/quote; assert 5× headroom (50µs) for CI jitter.
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
    // Budget: < 1 ms per frame at 100 subscribers; generous headroom.
    assert!(
        per < 1_000_000.0,
        "broadcast per-frame {per:.0}ns exceeds 1ms budget"
    );
    println!("sse fan-out: {} subscribers, {per:.0}ns/frame", subs.len());
}
