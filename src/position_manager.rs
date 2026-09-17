use crate::types::{Side, Trade};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub quantity: f64, // positive = Long, negative = Short, 0 = Flat
    pub avg_entry_price: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub total_trades: usize,
    pub total_volume: f64,
    pub fees_paid: f64,
}

impl Position {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            quantity: 0.0,
            avg_entry_price: 0.0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            total_trades: 0,
            total_volume: 0.0,
            fees_paid: 0.0,
        }
    }

    pub fn is_long(&self) -> bool {
        self.quantity > 1e-9
    }

    pub fn is_short(&self) -> bool {
        self.quantity < -1e-9
    }

    pub fn is_flat(&self) -> bool {
        self.quantity.abs() <= 1e-9
    }

    pub fn apply_trade(&mut self, trade_side: Side, price: f64, qty: f64, fee: f64) {
        self.total_trades += 1;
        self.total_volume += price * qty;
        self.fees_paid += fee;
        self.realized_pnl -= fee;

        let trade_qty = match trade_side {
            Side::Buy => qty,
            Side::Sell => -qty,
        };

        if self.is_flat() {
            // Opening fresh position
            self.quantity = trade_qty;
            self.avg_entry_price = price;
        } else if (self.quantity > 0.0 && trade_qty > 0.0)
            || (self.quantity < 0.0 && trade_qty < 0.0)
        {
            // Adding to existing position (scale in)
            let current_notional = self.quantity.abs() * self.avg_entry_price;
            let added_notional = qty * price;
            let new_qty = self.quantity + trade_qty;
            self.avg_entry_price = (current_notional + added_notional) / new_qty.abs();
            self.quantity = new_qty;
        } else {
            // Reducing, closing, or flipping position
            let current_abs = self.quantity.abs();

            if qty <= current_abs {
                // Partial reduction or complete close
                let pnl = if self.is_long() {
                    qty * (price - self.avg_entry_price)
                } else {
                    qty * (self.avg_entry_price - price)
                };
                self.realized_pnl += pnl;
                self.quantity += trade_qty;

                if self.is_flat() {
                    self.quantity = 0.0;
                    self.avg_entry_price = 0.0;
                    self.unrealized_pnl = 0.0;
                }
            } else {
                // Position flip (close existing + open reverse)
                let closed_qty = current_abs;
                let close_pnl = if self.is_long() {
                    closed_qty * (price - self.avg_entry_price)
                } else {
                    closed_qty * (self.avg_entry_price - price)
                };
                self.realized_pnl += close_pnl;

                let remaining_new_qty = qty - closed_qty;
                self.quantity = match trade_side {
                    Side::Buy => remaining_new_qty,
                    Side::Sell => -remaining_new_qty,
                };
                self.avg_entry_price = price;
            }
        }
    }

    pub fn mark_to_market(&mut self, current_price: f64) {
        if self.is_flat() {
            self.unrealized_pnl = 0.0;
            return;
        }

        if self.is_long() {
            self.unrealized_pnl = self.quantity * (current_price - self.avg_entry_price);
        } else if self.is_short() {
            self.unrealized_pnl = self.quantity.abs() * (self.avg_entry_price - current_price);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub cash_balance: f64,
    pub initial_balance: f64,
    pub realized_pnl: f64,
    pub margin_used: f64,
    pub leverage: f64,
}

impl Account {
    pub fn new(initial_balance: f64, leverage: f64) -> Self {
        Self {
            cash_balance: initial_balance,
            initial_balance,
            realized_pnl: 0.0,
            margin_used: 0.0,
            leverage: if leverage <= 0.0 { 1.0 } else { leverage },
        }
    }

    pub fn equity(&self, unrealized_pnl: f64) -> f64 {
        self.cash_balance + unrealized_pnl
    }

    pub fn free_margin(&self, unrealized_pnl: f64) -> f64 {
        (self.equity(unrealized_pnl) - self.margin_used).max(0.0)
    }

    pub fn margin_ratio(&self, unrealized_pnl: f64) -> f64 {
        let eq = self.equity(unrealized_pnl);
        if self.margin_used <= 0.0 {
            0.0
        } else if eq <= 0.0 {
            1.0
        } else {
            (self.margin_used / eq).min(1.0)
        }
    }
}

pub struct PositionManager {
    positions: HashMap<String, Position>,
    account: Account,
}

impl PositionManager {
    pub fn new(initial_balance: f64, leverage: f64) -> Self {
        Self {
            positions: HashMap::new(),
            account: Account::new(initial_balance, leverage),
        }
    }

    pub fn get_position(&self, symbol: &str) -> Option<&Position> {
        self.positions.get(symbol)
    }

    pub fn get_or_create_position(&mut self, symbol: &str) -> &mut Position {
        self.positions
            .entry(symbol.to_string())
            .or_insert_with(|| Position::new(symbol))
    }

    pub fn get_all_positions(&self) -> Vec<Position> {
        self.positions.values().cloned().collect()
    }

    pub fn account(&self) -> &Account {
        &self.account
    }

    pub fn account_mut(&mut self) -> &mut Account {
        &mut self.account
    }

    pub fn total_unrealized_pnl(&self) -> f64 {
        self.positions.values().map(|p| p.unrealized_pnl).sum()
    }

    pub fn total_realized_pnl(&self) -> f64 {
        self.positions.values().map(|p| p.realized_pnl).sum()
    }

    pub fn on_trade(&mut self, trade: &Trade, side: Side) {
        let prev_realized = self
            .positions
            .get(&trade.symbol)
            .map(|p| p.realized_pnl)
            .unwrap_or(0.0);

        let pos = self.get_or_create_position(&trade.symbol);
        pos.apply_trade(side, trade.price, trade.quantity, trade.fee);

        let new_realized = pos.realized_pnl;
        let pnl_diff = new_realized - prev_realized;

        self.account.cash_balance += pnl_diff;
        self.account.realized_pnl += pnl_diff;

        self.recalculate_margin();
    }

    pub fn mark_to_market(&mut self, symbol: &str, current_price: f64) {
        if let Some(pos) = self.positions.get_mut(symbol) {
            pos.mark_to_market(current_price);
        }
        self.recalculate_margin();
    }

    pub fn recalculate_margin(&mut self) {
        let mut total_margin = 0.0;
        for pos in self.positions.values() {
            let notional = pos.quantity.abs() * pos.avg_entry_price;
            total_margin += notional / self.account.leverage;
        }
        self.account.margin_used = total_margin;
    }
}
