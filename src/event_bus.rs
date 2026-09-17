use crate::types::{Order, OrderId, Side, Trade};
use crossbeam_channel::{unbounded, Receiver, Sender};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EngineEvent {
    OrderSubmitted(Order),
    OrderAccepted(Order),
    OrderRejected {
        order_id: OrderId,
        client_order_id: Option<String>,
        symbol: String,
        reason: String,
    },
    OrderCancelled {
        order_id: OrderId,
        client_order_id: Option<String>,
        symbol: String,
        side: Side,
        remaining_qty: f64,
    },
    OrderFilled {
        order_id: OrderId,
        client_order_id: Option<String>,
        symbol: String,
        trade: Trade,
        remaining_qty: f64,
    },
    TradeExecuted(Trade),
    BookUpdated {
        symbol: String,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        timestamp: i64,
    },
    PositionUpdated {
        symbol: String,
        quantity: f64,
        avg_entry_price: f64,
        unrealized_pnl: f64,
        realized_pnl: f64,
    },
    AccountUpdated {
        cash_balance: f64,
        equity: f64,
        realized_pnl: f64,
        unrealized_pnl: f64,
    },
    RiskBreached {
        rule: String,
        reason: String,
        order_id: Option<OrderId>,
    },
    SystemAlert {
        level: String,
        message: String,
    },
}

impl EngineEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            EngineEvent::OrderSubmitted(_) => "ORDER_SUBMITTED",
            EngineEvent::OrderAccepted(_) => "ORDER_ACCEPTED",
            EngineEvent::OrderRejected { .. } => "ORDER_REJECTED",
            EngineEvent::OrderCancelled { .. } => "ORDER_CANCELLED",
            EngineEvent::OrderFilled { .. } => "ORDER_FILLED",
            EngineEvent::TradeExecuted(_) => "TRADE_EXECUTED",
            EngineEvent::BookUpdated { .. } => "BOOK_UPDATED",
            EngineEvent::PositionUpdated { .. } => "POSITION_UPDATED",
            EngineEvent::AccountUpdated { .. } => "ACCOUNT_UPDATED",
            EngineEvent::RiskBreached { .. } => "RISK_BREACHED",
            EngineEvent::SystemAlert { .. } => "SYSTEM_ALERT",
        }
    }
}

struct EventBusInner {
    subscribers: RwLock<Vec<Sender<EngineEvent>>>,
    history: RwLock<Vec<EngineEvent>>,
    max_history: usize,
}

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<EventBusInner>,
}

impl EventBus {
    pub fn new(max_history: usize) -> Self {
        Self {
            inner: Arc::new(EventBusInner {
                subscribers: RwLock::new(Vec::new()),
                history: RwLock::new(Vec::with_capacity(max_history.min(10_000))),
                max_history,
            }),
        }
    }

    pub fn subscribe(&self) -> Receiver<EngineEvent> {
        let (tx, rx) = unbounded();
        self.inner.subscribers.write().push(tx);
        rx
    }

    pub fn publish(&self, event: EngineEvent) {
        // Broadcast to all active subscribers
        let mut subscribers = self.inner.subscribers.write();
        subscribers.retain(|tx| tx.send(event.clone()).is_ok());

        // Store into history ring buffer
        if self.inner.max_history > 0 {
            let mut hist = self.inner.history.write();
            if hist.len() >= self.inner.max_history {
                hist.remove(0);
            }
            hist.push(event);
        }
    }

    pub fn history(&self) -> Vec<EngineEvent> {
        self.inner.history.read().clone()
    }

    pub fn clear_history(&self) {
        self.inner.history.write().clear();
    }

    pub fn subscriber_count(&self) -> usize {
        self.inner.subscribers.read().len()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(10_000)
    }
}
