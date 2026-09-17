use crate::types::{
    ExecutionId, LevelQuote, MarketDepth, Order, OrderId, OrderStatus, OrderType, Side,
    TimeInForce, Trade,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchResult {
    pub order: Order,
    pub trades: Vec<Trade>,
    pub cancelled_quantity: f64,
}

#[derive(Debug)]
pub struct OrderBook {
    pub symbol: String,
    pub tick_size: f64,
    pub lot_size: f64,
    // Bids sorted by price tick: highest tick is best bid (accessed with .iter().rev())
    bids: BTreeMap<i64, VecDeque<Order>>,
    // Asks sorted by price tick: lowest tick is best ask (accessed with .iter())
    asks: BTreeMap<i64, VecDeque<Order>>,
    // Fast lookup: order_id -> (Side, price_tick)
    order_map: HashMap<OrderId, (Side, i64)>,
    execution_seq: AtomicU64,
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>, tick_size: f64, lot_size: f64) -> Self {
        Self {
            symbol: symbol.into(),
            tick_size: if tick_size <= 0.0 { 0.01 } else { tick_size },
            lot_size: if lot_size <= 0.0 { 0.0001 } else { lot_size },
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_map: HashMap::new(),
            execution_seq: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn price_to_tick(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    #[inline]
    pub fn tick_to_price(&self, tick: i64) -> f64 {
        (tick as f64 * self.tick_size * 1e8).round() / 1e8
    }

    pub fn next_exec_id(&self) -> ExecutionId {
        self.execution_seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.iter().next_back().map(|(&tick, queue)| {
            let total_qty: f64 = queue.iter().map(|o| o.remaining_quantity).sum();
            (self.tick_to_price(tick), total_qty)
        })
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(&tick, queue)| {
            let total_qty: f64 = queue.iter().map(|o| o.remaining_quantity).sum();
            (self.tick_to_price(tick), total_qty)
        })
    }

    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some((bid + ask) / 2.0),
            (Some((bid, _)), None) => Some(bid),
            (None, Some((ask, _))) => Some(ask),
            (None, None) => None,
        }
    }

    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some(ask - bid),
            _ => None,
        }
    }

    pub fn get_depth(&self, levels: usize) -> MarketDepth {
        let max_levels = if levels == 0 { 10 } else { levels };

        let bids: Vec<LevelQuote> = self
            .bids
            .iter()
            .rev()
            .take(max_levels)
            .map(|(&tick, queue)| LevelQuote {
                price: self.tick_to_price(tick),
                quantity: queue.iter().map(|o| o.remaining_quantity).sum(),
                order_count: queue.len(),
            })
            .collect();

        let asks: Vec<LevelQuote> = self
            .asks
            .iter()
            .take(max_levels)
            .map(|(&tick, queue)| LevelQuote {
                price: self.tick_to_price(tick),
                quantity: queue.iter().map(|o| o.remaining_quantity).sum(),
                order_count: queue.len(),
            })
            .collect();

        MarketDepth {
            symbol: self.symbol.clone(),
            bids,
            asks,
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0),
        }
    }

    pub fn process_order(&mut self, mut order: Order) -> MatchResult {
        let mut trades = Vec::new();

        // Validate FOK: Check if order can be filled in full immediately
        if order.time_in_force == TimeInForce::FOK {
            let available_qty = self.calculate_available_liquidity(order.side, order.price);
            if available_qty < order.remaining_quantity {
                order.status = OrderStatus::Rejected;
                return MatchResult {
                    order,
                    trades,
                    cancelled_quantity: 0.0,
                };
            }
        }

        match order.order_type {
            OrderType::Market => {
                self.match_market_order(&mut order, &mut trades);
                // Any remaining quantity of market order is cancelled
                let cancelled = order.remaining_quantity;
                if cancelled > 1e-9 {
                    order.status = if order.filled_quantity > 0.0 {
                        OrderStatus::PartiallyFilled
                    } else {
                        OrderStatus::Cancelled
                    };
                    order.remaining_quantity = 0.0;
                }
                MatchResult {
                    order,
                    trades,
                    cancelled_quantity: cancelled,
                }
            }
            OrderType::Limit => {
                self.match_limit_order(&mut order, &mut trades);

                let mut cancelled = 0.0;
                if order.remaining_quantity > 1e-9 {
                    match order.time_in_force {
                        TimeInForce::IOC => {
                            cancelled = order.remaining_quantity;
                            order.remaining_quantity = 0.0;
                            if order.filled_quantity > 0.0 {
                                order.status = OrderStatus::PartiallyFilled;
                            } else {
                                order.status = OrderStatus::Cancelled;
                            }
                        }
                        TimeInForce::GTC | TimeInForce::FOK => {
                            // Place remaining order on resting book
                            let tick = self.price_to_tick(order.price);
                            let order_id = order.id;
                            let side = order.side;

                            match side {
                                Side::Buy => {
                                    self.bids
                                        .entry(tick)
                                        .or_default()
                                        .push_back(order.clone());
                                }
                                Side::Sell => {
                                    self.asks
                                        .entry(tick)
                                        .or_default()
                                        .push_back(order.clone());
                                }
                            }
                            self.order_map.insert(order_id, (side, tick));
                        }
                    }
                }

                MatchResult {
                    order,
                    trades,
                    cancelled_quantity: cancelled,
                }
            }
        }
    }

    fn match_market_order(&mut self, taker: &mut Order, trades: &mut Vec<Trade>) {
        while taker.remaining_quantity > 1e-9 {
            let next_level = match taker.side {
                Side::Buy => self.asks.keys().next().copied(),
                Side::Sell => self.bids.keys().next_back().copied(),
            };

            let Some(tick) = next_level else {
                break;
            };

            self.match_at_level(tick, taker, trades);
        }
    }

    fn match_limit_order(&mut self, taker: &mut Order, trades: &mut Vec<Trade>) {
        let taker_tick = self.price_to_tick(taker.price);

        while taker.remaining_quantity > 1e-9 {
            let next_level = match taker.side {
                Side::Buy => {
                    if let Some(&ask_tick) = self.asks.keys().next() {
                        if taker_tick >= ask_tick {
                            Some(ask_tick)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                Side::Sell => {
                    if let Some(&bid_tick) = self.bids.keys().next_back() {
                        if taker_tick <= bid_tick {
                            Some(bid_tick)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            };

            let Some(tick) = next_level else {
                break;
            };

            self.match_at_level(tick, taker, trades);
        }
    }

    fn match_at_level(&mut self, tick: i64, taker: &mut Order, trades: &mut Vec<Trade>) {
        let is_buy = taker.side == Side::Buy;
        let exec_price = self.tick_to_price(tick);
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let exec_seq = &self.execution_seq;

        let queue = if is_buy {
            self.asks.get_mut(&tick)
        } else {
            self.bids.get_mut(&tick)
        };

        let Some(queue) = queue else {
            return;
        };

        while let Some(maker) = queue.front_mut() {
            let match_qty = taker.remaining_quantity.min(maker.remaining_quantity);
            if match_qty <= 1e-9 {
                break;
            }

            maker.apply_fill(match_qty);
            taker.apply_fill(match_qty);

            let exec_id = exec_seq.fetch_add(1, Ordering::SeqCst) + 1;
            let trade = Trade {
                execution_id: exec_id,
                maker_order_id: maker.id,
                taker_order_id: taker.id,
                maker_account_id: maker.account_id.clone(),
                taker_account_id: taker.account_id.clone(),
                symbol: self.symbol.clone(),
                side: taker.side,
                price: exec_price,
                quantity: match_qty,
                fee: 0.0,
                timestamp: now,
            };
            trades.push(trade);

            if maker.remaining_quantity <= 1e-9 {
                let maker_id = maker.id;
                queue.pop_front();
                self.order_map.remove(&maker_id);
            }

            if taker.remaining_quantity <= 1e-9 {
                break;
            }
        }

        // Clean up empty price level
        if queue.is_empty() {
            if is_buy {
                self.asks.remove(&tick);
            } else {
                self.bids.remove(&tick);
            }
        }
    }

    fn calculate_available_liquidity(&self, taker_side: Side, limit_price: f64) -> f64 {
        let mut total = 0.0;
        let taker_tick = self.price_to_tick(limit_price);

        match taker_side {
            Side::Buy => {
                for (&tick, queue) in &self.asks {
                    if tick <= taker_tick {
                        total += queue.iter().map(|o| o.remaining_quantity).sum::<f64>();
                    } else {
                        break;
                    }
                }
            }
            Side::Sell => {
                for (&tick, queue) in self.bids.iter().rev() {
                    if tick >= taker_tick {
                        total += queue.iter().map(|o| o.remaining_quantity).sum::<f64>();
                    } else {
                        break;
                    }
                }
            }
        }

        total
    }

    pub fn cancel_order(&mut self, order_id: OrderId) -> Option<Order> {
        let (side, tick) = self.order_map.remove(&order_id)?;

        let queue = match side {
            Side::Buy => self.bids.get_mut(&tick),
            Side::Sell => self.asks.get_mut(&tick),
        }?;

        let mut removed_order = None;
        if let Some(pos) = queue.iter().position(|o| o.id == order_id) {
            let mut order = queue.remove(pos)?;
            order.status = OrderStatus::Cancelled;
            order.updated_at = Utc::now().timestamp_nanos_opt().unwrap_or(0);
            removed_order = Some(order);
        }

        if queue.is_empty() {
            match side {
                Side::Buy => {
                    self.bids.remove(&tick);
                }
                Side::Sell => {
                    self.asks.remove(&tick);
                }
            }
        }

        removed_order
    }

    pub fn get_order(&self, order_id: OrderId) -> Option<&Order> {
        let (side, tick) = self.order_map.get(&order_id)?;
        let queue = match side {
            Side::Buy => self.bids.get(tick),
            Side::Sell => self.asks.get(tick),
        }?;
        queue.iter().find(|o| o.id == order_id)
    }

    pub fn total_orders(&self) -> usize {
        self.order_map.len()
    }
}
