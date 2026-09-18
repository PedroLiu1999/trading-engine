use crate::position_manager::{Account, Position};
use crate::types::{Order, OrderType, Side};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_order_qty: f64,
    pub max_order_notional: f64,
    pub max_position_notional: f64,
    pub price_collar_pct: f64, // e.g. 0.05 for 5% max deviation from mid
    pub max_drawdown_pct: f64, // e.g. 0.20 for 20% max portfolio drawdown
    pub max_orders_per_sec: usize,
    pub require_margin: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_order_qty: 1_000_000.0,
            max_order_notional: 10_000_000.0,
            max_position_notional: 20_000_000.0,
            price_collar_pct: 0.10, // 10% collar
            max_drawdown_pct: 0.25, // 25% max drawdown
            max_orders_per_sec: 1000,
            require_margin: true,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Serialize, Deserialize)]
pub enum RiskRejection {
    #[error("Kill switch is active: {0}")]
    KillSwitchActive(String),

    #[error("Rate limit exceeded: {0} orders in last second")]
    RateLimitExceeded(usize),

    #[error("Order quantity {qty} exceeds max allowed {max_qty}")]
    OrderQtyTooLarge { qty: f64, max_qty: f64 },

    #[error("Order notional {notional} exceeds max allowed {max_notional}")]
    OrderNotionalTooLarge { notional: f64, max_notional: f64 },

    #[error("Projected position notional {projected} exceeds max allowed {max}")]
    PositionNotionalTooLarge { projected: f64, max: f64 },

    #[error(
        "Limit price {price} deviates {diff_pct:.2}% from mid {mid}, exceeding collar {collar:.2}%"
    )]
    PriceCollarBreached {
        price: f64,
        mid: f64,
        diff_pct: f64,
        collar: f64,
    },

    #[error("Insufficient margin: required {required}, available {available}")]
    InsufficientMargin { required: f64, available: f64 },

    #[error("Invalid order parameters: {0}")]
    InvalidParameters(String),
}

pub struct RiskManager {
    pub config: RiskConfig,
    pub is_kill_switch_active: bool,
    pub kill_switch_reason: Option<String>,
    order_timestamps: VecDeque<i64>, // Milliseconds of orders in current sliding window
    peak_equity: f64,
}

impl RiskManager {
    pub fn new(config: RiskConfig, initial_balance: f64) -> Self {
        Self {
            config,
            is_kill_switch_active: false,
            kill_switch_reason: None,
            order_timestamps: VecDeque::new(),
            peak_equity: initial_balance,
        }
    }

    pub fn trip_kill_switch(&mut self, reason: impl Into<String>) {
        self.is_kill_switch_active = true;
        self.kill_switch_reason = Some(reason.into());
    }

    pub fn reset_kill_switch(&mut self) {
        self.is_kill_switch_active = false;
        self.kill_switch_reason = None;
    }

    pub fn check_order(
        &mut self,
        order: &Order,
        account: &Account,
        position: Option<&Position>,
        mid_price: Option<f64>,
        unrealized_pnl: f64,
    ) -> Result<(), RiskRejection> {
        // 1. Check Kill Switch
        if self.is_kill_switch_active {
            let reason = self
                .kill_switch_reason
                .clone()
                .unwrap_or_else(|| "Manual trip".to_string());
            return Err(RiskRejection::KillSwitchActive(reason));
        }

        // 2. Validate Order Basic Parameters
        if order.quantity <= 0.0 || order.quantity.is_nan() || order.quantity.is_infinite() {
            return Err(RiskRejection::InvalidParameters(
                "Order quantity must be positive and finite".to_string(),
            ));
        }

        if order.order_type == OrderType::Limit
            && (order.price <= 0.0 || order.price.is_nan() || order.price.is_infinite())
        {
            return Err(RiskRejection::InvalidParameters(
                "Limit order price must be positive and finite".to_string(),
            ));
        }

        // 3. Rate Limiting (Sliding 1-second window)
        let now_ms = Utc::now().timestamp_millis();
        while let Some(&t) = self.order_timestamps.front() {
            if now_ms - t > 1000 {
                self.order_timestamps.pop_front();
            } else {
                break;
            }
        }

        if self.config.max_orders_per_sec > 0
            && self.order_timestamps.len() >= self.config.max_orders_per_sec
        {
            return Err(RiskRejection::RateLimitExceeded(
                self.order_timestamps.len(),
            ));
        }

        // 4. Max Order Quantity
        if order.quantity > self.config.max_order_qty {
            return Err(RiskRejection::OrderQtyTooLarge {
                qty: order.quantity,
                max_qty: self.config.max_order_qty,
            });
        }

        // Effective valuation price
        let valuation_price = if order.price > 0.0 {
            order.price
        } else if let Some(mid) = mid_price {
            mid
        } else {
            return Err(RiskRejection::InvalidParameters(
                "Market order cannot be evaluated without market mid price".to_string(),
            ));
        };

        // 5. Max Order Notional
        let notional = order.quantity * valuation_price;
        if notional > self.config.max_order_notional {
            return Err(RiskRejection::OrderNotionalTooLarge {
                notional,
                max_notional: self.config.max_order_notional,
            });
        }

        // 6. Max Position Notional
        let current_qty = position.map(|p| p.quantity).unwrap_or(0.0);
        let projected_qty = match order.side {
            Side::Buy => current_qty + order.quantity,
            Side::Sell => current_qty - order.quantity,
        };
        let projected_notional = projected_qty.abs() * valuation_price;
        if projected_notional > self.config.max_position_notional {
            return Err(RiskRejection::PositionNotionalTooLarge {
                projected: projected_notional,
                max: self.config.max_position_notional,
            });
        }

        // 7. Price Collar (fat-finger protection for limit orders)
        if order.order_type == OrderType::Limit {
            if let Some(mid) = mid_price {
                if mid > 0.0 {
                    let diff_pct = (order.price - mid).abs() / mid;
                    if diff_pct > self.config.price_collar_pct {
                        return Err(RiskRejection::PriceCollarBreached {
                            price: order.price,
                            mid,
                            diff_pct: diff_pct * 100.0,
                            collar: self.config.price_collar_pct * 100.0,
                        });
                    }
                }
            }
        }

        // 8. Margin Requirement Check
        if self.config.require_margin {
            let required_margin = notional / account.leverage;
            let available_margin = account.free_margin(unrealized_pnl);
            // Only require margin if expanding risk (not reducing existing opposite position)
            let is_expanding = (current_qty >= 0.0 && order.side == Side::Buy)
                || (current_qty <= 0.0 && order.side == Side::Sell);

            if is_expanding && required_margin > available_margin {
                return Err(RiskRejection::InsufficientMargin {
                    required: required_margin,
                    available: available_margin,
                });
            }
        }

        // Record timestamp for rate limiter
        self.order_timestamps.push_back(now_ms);

        Ok(())
    }

    pub fn check_drawdown(&mut self, account: &Account, unrealized_pnl: f64) -> bool {
        let equity = account.equity(unrealized_pnl);
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }

        if self.peak_equity > 0.0 {
            let drawdown = (self.peak_equity - equity) / self.peak_equity;
            if drawdown >= self.config.max_drawdown_pct {
                self.trip_kill_switch(format!(
                    "Maximum drawdown breached: {:.2}% >= {:.2}%",
                    drawdown * 100.0,
                    self.config.max_drawdown_pct * 100.0
                ));
                return true;
            }
        }

        false
    }
}
