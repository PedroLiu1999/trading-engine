use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fmt;

pub type OrderId = u64;
pub type ExecutionId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(&self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderType::Limit => write!(f, "LIMIT"),
            OrderType::Market => write!(f, "MARKET"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum TimeInForce {
    #[default]
    GTC, // Good 'Til Cancelled
    IOC, // Immediate Or Cancel
    FOK, // Fill Or Kill
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderStatus::New => write!(f, "NEW"),
            OrderStatus::PartiallyFilled => write!(f, "PARTIALLY_FILLED"),
            OrderStatus::Filled => write!(f, "FILLED"),
            OrderStatus::Cancelled => write!(f, "CANCELLED"),
            OrderStatus::Rejected => write!(f, "REJECTED"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub client_order_id: Option<String>,
    pub account_id: String,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: f64,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub status: OrderStatus,
    pub time_in_force: TimeInForce,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Order {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: OrderId,
        client_order_id: Option<String>,
        symbol: impl Into<String>,
        side: Side,
        order_type: OrderType,
        price: f64,
        quantity: f64,
        time_in_force: TimeInForce,
    ) -> Self {
        Self::new_with_account(
            id,
            client_order_id,
            "DEFAULT",
            symbol,
            side,
            order_type,
            price,
            quantity,
            time_in_force,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_account(
        id: OrderId,
        client_order_id: Option<String>,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
        side: Side,
        order_type: OrderType,
        price: f64,
        quantity: f64,
        time_in_force: TimeInForce,
    ) -> Self {
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Self {
            id,
            client_order_id,
            account_id: account_id.into(),
            symbol: symbol.into(),
            side,
            order_type,
            price,
            quantity,
            filled_quantity: 0.0,
            remaining_quantity: quantity,
            time_in_force,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, OrderStatus::New | OrderStatus::PartiallyFilled)
    }

    pub fn apply_fill(&mut self, fill_qty: f64) {
        self.filled_quantity += fill_qty;
        self.remaining_quantity = (self.quantity - self.filled_quantity).max(0.0);
        if self.remaining_quantity <= 1e-9 {
            self.status = OrderStatus::Filled;
            self.remaining_quantity = 0.0;
        } else {
            self.status = OrderStatus::PartiallyFilled;
        }
        self.updated_at = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub execution_id: ExecutionId,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub maker_account_id: String,
    pub taker_account_id: String,
    pub symbol: String,
    pub side: Side, // Taker side
    pub price: f64,
    pub quantity: f64,
    pub fee: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LevelQuote {
    pub price: f64,
    pub quantity: f64,
    pub order_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDepth {
    pub symbol: String,
    pub bids: Vec<LevelQuote>,
    pub asks: Vec<LevelQuote>,
    pub timestamp: i64,
}

impl MarketDepth {
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|b| b.price)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|a| a.price)
    }

    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            (Some(bid), None) => Some(bid),
            (None, Some(ask)) => Some(ask),
            (None, None) => None,
        }
    }

    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: String,
    pub bid_price: f64,
    pub bid_qty: f64,
    pub ask_price: f64,
    pub ask_qty: f64,
    pub last_price: f64,
    pub last_qty: f64,
    pub timestamp: i64,
}
