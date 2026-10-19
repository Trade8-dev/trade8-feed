// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
// SPDX-License-Identifier: Apache-2.0

//! Feed engine - normalization, consolidation,
//! alert evaluation, snapshot reads.
//!
//! Design: `../feed/DESIGN.md` §4.
//! Adapter tasks + SSE fan-out live in main.rs; normalization + BBO/depth
//! state + alerts are complete here.

use std::collections::HashMap;

use parking_lot::RwLock;
use trade8::contracts::{
    AlertFired, DepthUpdate, FeedType, ImbalanceUpdate, QuoteUpdate, TradePrint,
};
use trade8::feed_adapters::{parse_itch_record, RawQuotePacket, RawRecord, SymbolDirectory};
use uuid::Uuid;

/// One side's ladder: (price_raw, volume), ordered best-first by the
/// caller's sort.
pub type Ladder = Vec<(i128, u64)>;

/// A depth snapshot: (bids desc, asks asc).
pub type DepthSnapshot = (Ladder, Ladder);

/// Consolidated per-symbol quote state.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SymbolQuotes {
    pub l1: Option<QuoteUpdate>,
    /// Per-source L1 (cross-source consolidation): source → BBO.
    /// The consolidated `l1` is the best bid/ask across sources.
    pub l1_by_source: HashMap<Uuid, QuoteUpdate>,
    pub prints: Vec<TradePrint>,
    /// L2 levels per source: (source_id → (bid, ask) level maps).
    pub depth: HashMap<Uuid, DepthState>,
    pub imbalance: Option<ImbalanceUpdate>,
}

impl SymbolQuotes {
    /// Cross-source consolidated BBO: best bid = max bid across
    /// sources, best ask = min ask; sizes summed at the extremes. When
    /// only one source has quoted, its BBO is the consolidation.
    pub fn consolidated_bbo(&self) -> QuoteUpdate {
        let mut it = self.l1_by_source.values();
        let Some(first) = it.next() else {
            return QuoteUpdate {
                symbol_id: Uuid::nil(),
                symbol_name: String::new(),
                venue_ts_ms: 0,
                bid_price: trade8::fixed::Price::from_scaled(0, 4),
                ask_price: trade8::fixed::Price::from_scaled(0, 4),
                bid_size: 0,
                ask_size: 0,
            };
        };
        let mut best = first.clone();
        for q in it {
            if q.bid_price.raw > best.bid_price.raw {
                best.bid_price = q.bid_price;
                best.bid_size = q.bid_size;
            } else if q.bid_price.raw == best.bid_price.raw {
                best.bid_size += q.bid_size;
            }
            if q.ask_price.raw < best.ask_price.raw {
                best.ask_price = q.ask_price;
                best.ask_size = q.ask_size;
            } else if q.ask_price.raw == best.ask_price.raw {
                best.ask_size += q.ask_size;
            }
            if q.venue_ts_ms > best.venue_ts_ms {
                best.venue_ts_ms = q.venue_ts_ms;
            }
        }
        best
    }
}

/// One source's book side.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DepthState {
    /// price_raw → volume (descending on read).
    pub bids: HashMap<i128, u64>,
    pub asks: HashMap<i128, u64>,
}

impl DepthState {
    pub fn top_n_bids(&self, n: usize) -> Vec<(i128, u64)> {
        let mut v: Vec<_> = self.bids.iter().map(|(p, q)| (*p, *q)).collect();
        v.sort_by_key(|(p, _)| std::cmp::Reverse(*p)); // best (highest) first
        v.truncate(n);
        v
    }
    pub fn top_n_asks(&self, n: usize) -> Vec<(i128, u64)> {
        let mut v: Vec<_> = self.asks.iter().map(|(p, q)| (*p, *q)).collect();
        v.sort_by_key(|(p, _)| *p); // best (lowest) first
        v.truncate(n);
        v
    }
}

/// Alert rule.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertRule {
    pub rule_id: Uuid,
    pub usr_id: String,
    pub symbol_name: String,
    /// Operator on last trade price: `gt` / `lt` / `gte` / `lte`.
    pub op: String,
    pub threshold_raw: i128,
}

impl AlertRule {
    pub fn matches(&self, last_print_raw: Option<i128>) -> bool {
        let Some(p) = last_print_raw else {
            return false;
        };
        match self.op.as_str() {
            "gt" => p > self.threshold_raw,
            "lt" => p < self.threshold_raw,
            "gte" => p >= self.threshold_raw,
            "lte" => p <= self.threshold_raw,
            _ => false,
        }
    }
    pub fn describe(&self) -> String {
        let op = match self.op.as_str() {
            "gt" => ">",
            "lt" => "<",
            "gte" => "≥",
            "lte" => "≤",
            _ => "?",
        };
        format!("last {} {}", op, self.threshold_raw)
    }
}

/// The Feed engine: symbol state + alert rules.
#[derive(Default)]
pub struct FeedEngine {
    /// vendor symbol → engine symbol state.
    pub symbols: RwLock<HashMap<String, SymbolQuotes>>,
    /// vendor symbol → (symbol_id, price_scale) directory.
    pub directory: RwLock<SymbolDirectory>,
    /// source UUID per vendor source id (wire parity: u32 → UUID).
    pub sources: RwLock<HashMap<u32, Uuid>>,
    pub alert_rules: RwLock<Vec<AlertRule>>,
}

impl FeedEngine {
    /// Normalize one raw packet into internal events - records the
    /// state and returns the derived event set for the bus/SSE pass.
    pub fn ingest_packet(&self, pkt: &RawQuotePacket) -> Vec<TradeEvent> {
        let Ok(records) = parse_itch_record(pkt) else {
            return vec![]; // codec gap - feed health event in the shell pass
        };
        let mut out = Vec::new();
        for rec in records {
            match rec {
                RawRecord::Bbo {
                    symbol,
                    bid_px,
                    bid_size,
                    ask_px,
                    ask_size,
                    venue_ts_ms,
                } => {
                    let Some(&(sym_id, scale)) = self.directory.read().map.get(&symbol) else {
                        continue;
                    };
                    let source = self
                        .sources
                        .read()
                        .get(&pkt.source_id)
                        .copied()
                        .unwrap_or_default();
                    let q = QuoteUpdate {
                        symbol_id: sym_id,
                        symbol_name: symbol.clone(),
                        venue_ts_ms,
                        bid_price: trade8::fixed::Price::from_scaled(bid_px, scale),
                        ask_price: trade8::fixed::Price::from_scaled(ask_px, scale),
                        bid_size,
                        ask_size,
                    };
                    let mut symbols = self.symbols.write();
                    let state = symbols.entry(symbol.clone()).or_default();
                    // Per-source BBO: the consolidated quote is the
                    // best bid (max) and best ask (min) across sources -
                    // semantics (NBBO-style).
                    state.l1_by_source.insert(source, q.clone());
                    let consolidated = state.consolidated_bbo();
                    state.l1 = Some(consolidated.clone());
                    out.push(TradeEvent::Quote(consolidated));
                }
                RawRecord::Level {
                    symbol,
                    is_bid,
                    price,
                    volume,
                    depth: _,
                    mmid: _,
                    venue_ts_ms,
                } => {
                    let Some(&(sym_id, scale)) = self.directory.read().map.get(&symbol) else {
                        continue;
                    };
                    // depth rides the consolidated source (first configured)
                    let source = self
                        .sources
                        .read()
                        .get(&pkt.source_id)
                        .copied()
                        .unwrap_or_default();
                    let d = DepthUpdate {
                        symbol_id: sym_id,
                        symbol_name: symbol.clone(),
                        feed_type: FeedType::ConsolidatedBook,
                        source_id: source,
                        venue_ts_ms,
                        is_bid,
                        price: trade8::fixed::Price::from_scaled(price, scale),
                        volume,
                        depth: None,
                        mmid: None,
                        sequence: pkt.seq,
                    };
                    let mut symbols = self.symbols.write();
                    let state = symbols.entry(symbol.clone()).or_default();
                    let ds = state.depth.entry(source).or_default();
                    let side = if is_bid { &mut ds.bids } else { &mut ds.asks };
                    if volume == 0 {
                        side.remove(&price);
                    } else {
                        side.insert(price, volume);
                    }
                    out.push(TradeEvent::Depth(d));
                }
                RawRecord::Print {
                    symbol,
                    price,
                    qty,
                    seq,
                    venue_ts_ms,
                } => {
                    let Some(&(sym_id, scale)) = self.directory.read().map.get(&symbol) else {
                        continue;
                    };
                    let p = TradePrint {
                        symbol_id: sym_id,
                        symbol_name: symbol.clone(),
                        venue_ts_ms,
                        price: trade8::fixed::Price::from_scaled(price, scale),
                        qty,
                        seq,
                    };
                    let mut symbols = self.symbols.write();
                    let state = symbols.entry(symbol.clone()).or_default();
                    state.prints.push(p.clone());
                    state.prints.truncate(1000); // bounded tail
                    out.push(TradeEvent::Print(p));
                    // alert evaluation on the new last print
                    let last = Some(price);
                    let rules = self.alert_rules.read().clone();
                    for rule in rules {
                        if rule.symbol_name == symbol && rule.matches(last) {
                            let q = state.l1.clone();
                            out.push(TradeEvent::Alert(AlertFired {
                                alert_id: Uuid::now_v7(),
                                rule_id: rule.rule_id,
                                usr_id: rule.usr_id.clone(),
                                symbol_name: symbol.clone(),
                                quote: q.unwrap_or(QuoteUpdate {
                                    symbol_id: sym_id,
                                    symbol_name: symbol.clone(),
                                    venue_ts_ms,
                                    bid_price: trade8::fixed::Price::from_scaled(price, scale),
                                    ask_price: trade8::fixed::Price::from_scaled(price, scale),
                                    bid_size: 0,
                                    ask_size: 0,
                                }),
                                condition: rule.describe(),
                                at_ms: venue_ts_ms,
                            }));
                        }
                    }
                }
                RawRecord::Imbalance {
                    symbol,
                    side,
                    imbalance_type,
                    status,
                    price,
                    auction_px,
                    continuous_px,
                    volume,
                    paired,
                    venue_ts_ms,
                } => {
                    let Some(&(sym_id, scale)) = self.directory.read().map.get(&symbol) else {
                        continue;
                    };
                    let imb = ImbalanceUpdate {
                        symbol_id: sym_id,
                        symbol_name: symbol.clone(),
                        side,
                        imbalance_type,
                        status,
                        venue_ts_ms,
                        price: trade8::fixed::Price::from_scaled(price, scale),
                        auction_price: trade8::fixed::Price::from_scaled(auction_px, scale),
                        continuous_price: trade8::fixed::Price::from_scaled(continuous_px, scale),
                        volume,
                        paired_volume: paired,
                    };
                    let mut symbols = self.symbols.write();
                    symbols.entry(symbol.clone()).or_default().imbalance = Some(imb.clone());
                    out.push(TradeEvent::Imbalance(imb));
                }
            }
        }
        out
    }

    /// L1 snapshot.
    pub fn quote_snapshot(&self, symbol: &str) -> Option<QuoteUpdate> {
        self.symbols.read().get(symbol)?.l1.clone()
    }

    /// Depth snapshot (top N per side, consolidated across sources).
    pub fn depth_snapshot(&self, symbol: &str, levels: usize) -> Option<DepthSnapshot> {
        let symbols = self.symbols.read();
        let state = symbols.get(symbol)?;
        let mut bids: HashMap<i128, u64> = HashMap::new();
        let mut asks: HashMap<i128, u64> = HashMap::new();
        for ds in state.depth.values() {
            for (p, q) in &ds.bids {
                *bids.entry(*p).or_insert(0) += q;
            }
            for (p, q) in &ds.asks {
                *asks.entry(*p).or_insert(0) += q;
            }
        }
        let mut b: Vec<_> = bids.into_iter().collect();
        b.sort_by_key(|(p, _)| std::cmp::Reverse(*p)); // best (highest) first
        b.truncate(levels);
        let mut a: Vec<_> = asks.into_iter().collect();
        a.sort_by_key(|(p, _)| *p); // best (lowest) first
        a.truncate(levels);
        Some((b, a))
    }

    /// Last print (alert/uptick/marks input).
    pub fn last_print(&self, symbol: &str) -> Option<TradePrint> {
        self.symbols.read().get(symbol)?.prints.last().cloned()
    }

    pub fn register_symbol(&self, vendor_symbol: &str, symbol_id: Uuid, scale: u8) {
        self.directory
            .write()
            .map
            .insert(vendor_symbol.to_string(), (symbol_id, scale));
    }

    pub fn register_source(&self, wire_id: u32, source_uuid: Uuid) {
        self.sources.write().insert(wire_id, source_uuid);
    }

    pub fn add_alert_rule(&self, rule: AlertRule) {
        self.alert_rules.write().push(rule);
    }
}

/// Internal normalized event set (one per inbound record).
#[derive(Debug, Clone, PartialEq)]
pub enum TradeEvent {
    Quote(QuoteUpdate),
    Depth(DepthUpdate),
    Print(TradePrint),
    Imbalance(ImbalanceUpdate),
    Alert(AlertFired),
}

#[cfg(test)]
mod tests {
    use super::*;
    use trade8::feed_adapters::{build_itch_bbo, build_itch_print, RawQuotePacket};

    fn engine_with_aapl() -> FeedEngine {
        let e = FeedEngine::default();
        e.register_symbol("AAPL", Uuid::new_v4(), 4);
        e.register_source(7, Uuid::new_v4());
        e
    }

    fn pkt(source: u32, seq: u64, bytes: Vec<u8>) -> RawQuotePacket {
        RawQuotePacket {
            source_id: source,
            seq,
            bytes,
        }
    }

    #[test]
    fn bbo_ingest_updates_state() {
        let e = engine_with_aapl();
        let bytes = build_itch_bbo("AAPL", 100_000, 10, 100_400, 8, 1);
        let events = e.ingest_packet(&pkt(7, 1, bytes));
        assert_eq!(events.len(), 1);
        match &events[0] {
            TradeEvent::Quote(q) => {
                assert_eq!(q.bid_price.raw, 100_000);
                assert_eq!(q.ask_size, 8);
            }
            other => panic!("wrong event {other:?}"),
        }
        let snap = e.quote_snapshot("AAPL").unwrap();
        assert_eq!(snap.bid_price.raw, 100_000);
    }

    #[test]
    fn depth_consolidation_top_n() {
        let e = engine_with_aapl();
        let mut bytes = vec![b'D', 0];
        bytes.extend_from_slice(b"AAPL    ");
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.push(b'B');
        bytes.extend_from_slice(&100_000i64.to_be_bytes());
        bytes.extend_from_slice(&10u32.to_be_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(b"    ");
        let _ = e.ingest_packet(&pkt(7, 1, bytes));
        let (bids, asks) = e.depth_snapshot("AAPL", 5).unwrap();
        assert_eq!(bids, vec![(100_000, 10)]);
        assert!(asks.is_empty());
    }

    /// Cross-source BBO consolidation: two
    /// sources quote; the consolidated BBO takes the max bid + min ask.
    #[test]
    fn cross_source_bbo_consolidation() {
        let e = engine_with_aapl();
        // Source 7: 100.00 × 100.20.
        let s7 = build_itch_bbo("AAPL", 100_000, 10, 100_200, 10, 1);
        let _ = e.ingest_packet(&pkt(7, 1, s7));
        // Source 9: better bid 100.10, worse ask 100.30.
        let s9 = build_itch_bbo("AAPL", 100_100, 5, 100_300, 5, 2);
        let events = e.ingest_packet(&pkt(9, 1, s9));
        // The consolidated quote: bid 100.10 (source 9), ask 100.20
        // (source 7).
        match events.last() {
            Some(TradeEvent::Quote(q)) => {
                assert_eq!(q.bid_price.raw, 100_100);
                assert_eq!(q.bid_size, 5);
                assert_eq!(q.ask_price.raw, 100_200);
                assert_eq!(q.ask_size, 10);
            }
            other => panic!("wrong event {other:?}"),
        }
        let snap = e.quote_snapshot("AAPL").unwrap();
        assert_eq!(snap.bid_price.raw, 100_100);
        assert_eq!(snap.ask_price.raw, 100_200);
        // Same-price sides sum sizes across sources.
        let s9b = build_itch_bbo("AAPL", 100_100, 7, 100_200, 3, 3);
        let _ = e.ingest_packet(&pkt(9, 2, s9b));
        let snap = e.quote_snapshot("AAPL").unwrap();
        // bid 100.10 = source9 (5) + source... wait source 7's bid is
        // 100.00 - not at the extreme. bid at 100.10 is source 9 only → 7
        // (its own latest). ask 100.20: source 7 (10) + source 9 (3) = 13.
        assert_eq!(snap.bid_size, 7);
        assert_eq!(snap.ask_size, 13);
    }

    #[test]
    fn alert_fires_on_threshold_cross() {
        let e = engine_with_aapl();
        e.add_alert_rule(AlertRule {
            rule_id: Uuid::new_v4(),
            usr_id: "u1".into(),
            symbol_name: "AAPL".into(),
            op: "gt".into(),
            threshold_raw: 100_500,
        });
        // print below threshold → no alert
        let below = build_itch_print("AAPL", 100_000, 10, 1, 1);
        let events = e.ingest_packet(&pkt(7, 1, below));
        assert!(!events.iter().any(|ev| matches!(ev, TradeEvent::Alert(_))));
        // print above → alert
        let above = build_itch_print("AAPL", 100_600, 5, 2, 2);
        let events = e.ingest_packet(&pkt(7, 2, above));
        assert!(events.iter().any(|ev| matches!(ev, TradeEvent::Alert(_))));
        assert_eq!(e.last_print("AAPL").unwrap().price.raw, 100_600);
    }

    #[test]
    fn unknown_symbol_dropped() {
        let e = engine_with_aapl();
        let bytes = build_itch_bbo("ZZZZ", 1, 1, 2, 1, 0);
        assert!(e.ingest_packet(&pkt(7, 1, bytes)).is_empty());
    }
}
