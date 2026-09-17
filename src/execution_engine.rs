use crate::event_bus::{EngineEvent, EventBus};
use crate::order_book::OrderBook;
use crate::position_manager::{Account, Position, PositionManager};
use crate::risk_manager::{RiskConfig, RiskManager, RiskRejection};
use crate::types::{MarketDepth, Order, OrderId, OrderStatus, OrderType, Side, TimeInForce};
use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionError {
    #[error("Symbol '{0}' not registered")]
    SymbolNotRegistered(String),

    #[error("Order rejected by risk management: {0}")]
    RiskRejected(#[from] RiskRejection),

    #[error("Order not found: {0}")]
    OrderNotFound(OrderId),

    #[error("Engine halted: {0}")]
    EngineHalted(String),
}

pub struct ExecutionEngine {
    books: RwLock<HashMap<String, OrderBook>>,
    position_manager: RwLock<PositionManager>,
    risk_manager: RwLock<RiskManager>,
    event_bus: EventBus,
    order_seq: AtomicU64,
}

impl ExecutionEngine {
    pub fn new(initial_balance: f64, leverage: f64, risk_config: RiskConfig) -> Self {
        let event_bus = EventBus::new(10_000);
        let risk_manager = RiskManager::new(risk_config, initial_balance);
        let position_manager = PositionManager::new(initial_balance, leverage);

        Self {
            books: RwLock::new(HashMap::new()),
            position_manager: RwLock::new(position_manager),
            risk_manager: RwLock::new(risk_manager),
            event_bus,
            order_seq: AtomicU64::new(1),
        }
    }

    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    pub fn register_symbol(&self, symbol: &str, tick_size: f64, lot_size: f64) {
        let mut books = self.books.write();
        if !books.contains_key(symbol) {
            books.insert(
                symbol.to_string(),
                OrderBook::new(symbol, tick_size, lot_size),
            );
        }
    }

    pub fn next_order_id(&self) -> OrderId {
        self.order_seq.fetch_add(1, Ordering::SeqCst)
    }

    pub fn submit_order(
        &self,
        client_order_id: Option<String>,
        symbol: &str,
        side: Side,
        order_type: OrderType,
        price: f64,
        quantity: f64,
        time_in_force: TimeInForce,
    ) -> Result<Order, ExecutionError> {
        self.submit_order_with_account(
            client_order_id,
            Some("DEFAULT".to_string()),
            symbol,
            side,
            order_type,
            price,
            quantity,
            time_in_force,
        )
    }

    pub fn submit_order_with_account(
        &self,
        client_order_id: Option<String>,
        account_id: Option<String>,
        symbol: &str,
        side: Side,
        order_type: OrderType,
        price: f64,
        quantity: f64,
        time_in_force: TimeInForce,
    ) -> Result<Order, ExecutionError> {
        let mut books = self.books.write();
        let book = books
            .get_mut(symbol)
            .ok_or_else(|| ExecutionError::SymbolNotRegistered(symbol.to_string()))?;

        let acct_id = account_id.unwrap_or_else(|| "DEFAULT".to_string());
        let order_id = self.next_order_id();
        let mut order = Order::new_with_account(
            order_id,
            client_order_id.clone(),
            &acct_id,
            symbol,
            side,
            order_type,
            price,
            quantity,
            time_in_force,
        );

        // Notify Order Submitted
        self.event_bus
            .publish(EngineEvent::OrderSubmitted(order.clone()));

        // Pre-trade Risk Check
        let mid_price = book.mid_price();
        let unrealized_pnl = self.position_manager.read().total_unrealized_pnl(&acct_id);

        {
            let mut risk = self.risk_manager.write();
            let mut pos_mgr = self.position_manager.write();
            let current_pos = pos_mgr.get_position(&acct_id, symbol).cloned();
            let account = pos_mgr.get_or_create_account(&acct_id).clone();

            if let Err(rejection) =
                risk.check_order(&order, &account, current_pos.as_ref(), mid_price, unrealized_pnl)
            {
                order.status = OrderStatus::Rejected;
                self.event_bus.publish(EngineEvent::RiskBreached {
                    rule: "PreTradeCheck".to_string(),
                    reason: rejection.to_string(),
                    order_id: Some(order.id),
                });
                self.event_bus.publish(EngineEvent::OrderRejected {
                    order_id: order.id,
                    client_order_id: order.client_order_id.clone(),
                    symbol: symbol.to_string(),
                    reason: rejection.to_string(),
                });
                return Err(ExecutionError::RiskRejected(rejection));
            }
        }

        // Accept Order
        self.event_bus
            .publish(EngineEvent::OrderAccepted(order.clone()));

        // Execute against Order Book
        let match_result = book.process_order(order);
        let final_order = match_result.order.clone();

        // Process Trades and update Position Manager
        if !match_result.trades.is_empty() {
            let mut pos_mgr = self.position_manager.write();

            for trade in &match_result.trades {
                // Update position for taker
                pos_mgr.on_trade(&trade.taker_account_id, trade, trade.side);
                // Update position for maker
                pos_mgr.on_trade(&trade.maker_account_id, trade, trade.side.opposite());

                // Publish trade events
                self.event_bus
                    .publish(EngineEvent::TradeExecuted(trade.clone()));
                self.event_bus.publish(EngineEvent::OrderFilled {
                    order_id: trade.taker_order_id,
                    client_order_id: final_order.client_order_id.clone(),
                    symbol: symbol.to_string(),
                    trade: trade.clone(),
                    remaining_qty: final_order.remaining_quantity,
                });
            }

            // Mark to market with last trade price
            if let Some(last_trade) = match_result.trades.last() {
                pos_mgr.mark_to_market(symbol, last_trade.price);
            }

            if let Some(pos) = pos_mgr.get_position(&acct_id, symbol) {
                self.event_bus.publish(EngineEvent::PositionUpdated {
                    symbol: symbol.to_string(),
                    quantity: pos.quantity,
                    avg_entry_price: pos.avg_entry_price,
                    unrealized_pnl: pos.unrealized_pnl,
                    realized_pnl: pos.realized_pnl,
                });
            }

            if let Some(acct) = pos_mgr.get_account(&acct_id) {
                let un_pnl = pos_mgr.total_unrealized_pnl(&acct_id);
                self.event_bus.publish(EngineEvent::AccountUpdated {
                    cash_balance: acct.cash_balance,
                    equity: acct.equity(un_pnl),
                    realized_pnl: acct.realized_pnl,
                    unrealized_pnl: un_pnl,
                });

                // Post-trade Risk: Check portfolio drawdown
                let mut risk = self.risk_manager.write();
                if risk.check_drawdown(acct, un_pnl) {
                    self.event_bus.publish(EngineEvent::SystemAlert {
                        level: "CRITICAL".to_string(),
                        message: format!(
                            "Kill switch triggered by maximum drawdown breach on account '{}'",
                            acct_id
                        ),
                    });
                }
            }
        }

        // Publish cancellations if any unfilled qty was cancelled (IOC / Market)
        if match_result.cancelled_quantity > 1e-9 {
            self.event_bus.publish(EngineEvent::OrderCancelled {
                order_id: final_order.id,
                client_order_id: final_order.client_order_id.clone(),
                symbol: symbol.to_string(),
                side: final_order.side,
                remaining_qty: match_result.cancelled_quantity,
            });
        }

        // Publish Order Book Snapshot / Update
        let (bid_p, _) = book.best_bid().unwrap_or((0.0, 0.0));
        let (ask_p, _) = book.best_ask().unwrap_or((0.0, 0.0));
        self.event_bus.publish(EngineEvent::BookUpdated {
            symbol: symbol.to_string(),
            best_bid: if bid_p > 0.0 { Some(bid_p) } else { None },
            best_ask: if ask_p > 0.0 { Some(ask_p) } else { None },
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0),
        });

        Ok(final_order)
    }

    pub fn cancel_order(&self, symbol: &str, order_id: OrderId) -> Result<Order, ExecutionError> {
        let mut books = self.books.write();
        let book = books
            .get_mut(symbol)
            .ok_or_else(|| ExecutionError::SymbolNotRegistered(symbol.to_string()))?;

        if let Some(order) = book.cancel_order(order_id) {
            self.event_bus.publish(EngineEvent::OrderCancelled {
                order_id: order.id,
                client_order_id: order.client_order_id.clone(),
                symbol: symbol.to_string(),
                side: order.side,
                remaining_qty: order.remaining_quantity,
            });

            // Update book metrics
            let (bid_p, _) = book.best_bid().unwrap_or((0.0, 0.0));
            let (ask_p, _) = book.best_ask().unwrap_or((0.0, 0.0));
            self.event_bus.publish(EngineEvent::BookUpdated {
                symbol: symbol.to_string(),
                best_bid: if bid_p > 0.0 { Some(bid_p) } else { None },
                best_ask: if ask_p > 0.0 { Some(ask_p) } else { None },
                timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            });

            Ok(order)
        } else {
            Err(ExecutionError::OrderNotFound(order_id))
        }
    }

    pub fn get_depth(&self, symbol: &str, levels: usize) -> Option<MarketDepth> {
        self.books.read().get(symbol).map(|b| b.get_depth(levels))
    }

    pub fn get_position(&self, symbol: &str) -> Option<Position> {
        self.get_position_by_account("DEFAULT", symbol)
    }

    pub fn get_position_by_account(&self, account_id: &str, symbol: &str) -> Option<Position> {
        self.position_manager
            .read()
            .get_position(account_id, symbol)
            .cloned()
    }

    pub fn get_positions(&self) -> Vec<Position> {
        self.get_positions_by_account("DEFAULT")
    }

    pub fn get_positions_by_account(&self, account_id: &str) -> Vec<Position> {
        self.position_manager
            .read()
            .get_positions_by_account(account_id)
    }

    pub fn get_all_positions(&self) -> Vec<Position> {
        self.position_manager.read().get_all_positions()
    }

    pub fn get_account(&self) -> Account {
        self.get_account_by_id("DEFAULT")
            .unwrap_or_else(|| self.position_manager.read().default_account().clone())
    }

    pub fn get_account_by_id(&self, account_id: &str) -> Option<Account> {
        self.position_manager
            .read()
            .get_account(account_id)
            .cloned()
    }

    pub fn get_all_account_ids(&self) -> Vec<String> {
        self.position_manager.read().get_all_account_ids()
    }

    pub fn get_risk_config(&self) -> RiskConfig {
        self.risk_manager.read().config.clone()
    }

    pub fn set_risk_config(&self, config: RiskConfig) {
        self.risk_manager.write().config = config;
    }

    pub fn is_kill_switch_active(&self) -> bool {
        self.risk_manager.read().is_kill_switch_active
    }

    pub fn trip_kill_switch(&self, reason: &str) {
        self.risk_manager.write().trip_kill_switch(reason);
        self.event_bus.publish(EngineEvent::SystemAlert {
            level: "CRITICAL".to_string(),
            message: format!("Kill switch manually tripped: {}", reason),
        });
    }

    pub fn reset_kill_switch(&self) {
        self.risk_manager.write().reset_kill_switch();
        self.event_bus.publish(EngineEvent::SystemAlert {
            level: "INFO".to_string(),
            message: "Kill switch reset".to_string(),
        });
    }

    pub fn mark_to_market(&self, symbol: &str, price: f64) {
        self.position_manager.write().mark_to_market(symbol, price);
    }
}
