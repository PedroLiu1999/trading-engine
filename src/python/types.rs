use crate::position_manager::{Account, Position};
use crate::risk_manager::RiskConfig;
use crate::types::{
    LevelQuote, MarketDepth, Order, OrderId, OrderStatus, OrderType, Side, TimeInForce, Trade,
};
use pyo3::prelude::*;

#[pyclass(eq, eq_int, name = "Side")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PySide {
    Buy,
    Sell,
}

impl From<Side> for PySide {
    fn from(s: Side) -> Self {
        match s {
            Side::Buy => PySide::Buy,
            Side::Sell => PySide::Sell,
        }
    }
}

impl From<PySide> for Side {
    fn from(s: PySide) -> Self {
        match s {
            PySide::Buy => Side::Buy,
            PySide::Sell => Side::Sell,
        }
    }
}

#[pymethods]
impl PySide {
    fn __repr__(&self) -> &'static str {
        match self {
            PySide::Buy => "Side.Buy",
            PySide::Sell => "Side.Sell",
        }
    }
}

#[pyclass(eq, eq_int, name = "OrderType")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyOrderType {
    Limit,
    Market,
}

impl From<OrderType> for PyOrderType {
    fn from(ot: OrderType) -> Self {
        match ot {
            OrderType::Limit => PyOrderType::Limit,
            OrderType::Market => PyOrderType::Market,
        }
    }
}

impl From<PyOrderType> for OrderType {
    fn from(ot: PyOrderType) -> Self {
        match ot {
            PyOrderType::Limit => OrderType::Limit,
            PyOrderType::Market => OrderType::Market,
        }
    }
}

#[pymethods]
impl PyOrderType {
    fn __repr__(&self) -> &'static str {
        match self {
            PyOrderType::Limit => "OrderType.Limit",
            PyOrderType::Market => "OrderType.Market",
        }
    }
}

#[pyclass(eq, eq_int, name = "TimeInForce")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyTimeInForce {
    GTC,
    IOC,
    FOK,
}

impl From<TimeInForce> for PyTimeInForce {
    fn from(tif: TimeInForce) -> Self {
        match tif {
            TimeInForce::GTC => PyTimeInForce::GTC,
            TimeInForce::IOC => PyTimeInForce::IOC,
            TimeInForce::FOK => PyTimeInForce::FOK,
        }
    }
}

impl From<PyTimeInForce> for TimeInForce {
    fn from(tif: PyTimeInForce) -> Self {
        match tif {
            PyTimeInForce::GTC => TimeInForce::GTC,
            PyTimeInForce::IOC => TimeInForce::IOC,
            PyTimeInForce::FOK => TimeInForce::FOK,
        }
    }
}

#[pymethods]
impl PyTimeInForce {
    fn __repr__(&self) -> &'static str {
        match self {
            PyTimeInForce::GTC => "TimeInForce.GTC",
            PyTimeInForce::IOC => "TimeInForce.IOC",
            PyTimeInForce::FOK => "TimeInForce.FOK",
        }
    }
}

#[pyclass(eq, eq_int, name = "OrderStatus")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyOrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

impl From<OrderStatus> for PyOrderStatus {
    fn from(os: OrderStatus) -> Self {
        match os {
            OrderStatus::New => PyOrderStatus::New,
            OrderStatus::PartiallyFilled => PyOrderStatus::PartiallyFilled,
            OrderStatus::Filled => PyOrderStatus::Filled,
            OrderStatus::Cancelled => PyOrderStatus::Cancelled,
            OrderStatus::Rejected => PyOrderStatus::Rejected,
        }
    }
}

#[pymethods]
impl PyOrderStatus {
    fn __repr__(&self) -> &'static str {
        match self {
            PyOrderStatus::New => "OrderStatus.New",
            PyOrderStatus::PartiallyFilled => "OrderStatus.PartiallyFilled",
            PyOrderStatus::Filled => "OrderStatus.Filled",
            PyOrderStatus::Cancelled => "OrderStatus.Cancelled",
            PyOrderStatus::Rejected => "OrderStatus.Rejected",
        }
    }
}

#[pyclass(name = "Order")]
#[derive(Clone)]
pub struct PyOrder {
    #[pyo3(get)]
    pub id: OrderId,
    #[pyo3(get)]
    pub client_order_id: Option<String>,
    #[pyo3(get)]
    pub account_id: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub side: PySide,
    #[pyo3(get)]
    pub order_type: PyOrderType,
    #[pyo3(get)]
    pub price: f64,
    #[pyo3(get)]
    pub quantity: f64,
    #[pyo3(get)]
    pub filled_quantity: f64,
    #[pyo3(get)]
    pub remaining_quantity: f64,
    #[pyo3(get)]
    pub time_in_force: PyTimeInForce,
    #[pyo3(get)]
    pub status: PyOrderStatus,
    #[pyo3(get)]
    pub created_at: i64,
    #[pyo3(get)]
    pub updated_at: i64,
}

impl From<Order> for PyOrder {
    fn from(o: Order) -> Self {
        Self {
            id: o.id,
            client_order_id: o.client_order_id,
            account_id: o.account_id,
            symbol: o.symbol,
            side: o.side.into(),
            order_type: o.order_type.into(),
            price: o.price,
            quantity: o.quantity,
            filled_quantity: o.filled_quantity,
            remaining_quantity: o.remaining_quantity,
            time_in_force: o.time_in_force.into(),
            status: o.status.into(),
            created_at: o.created_at,
            updated_at: o.updated_at,
        }
    }
}

#[pymethods]
impl PyOrder {
    fn is_active(&self) -> bool {
        matches!(
            self.status,
            PyOrderStatus::New | PyOrderStatus::PartiallyFilled
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "Order(id={}, acct='{}', symbol='{}', side={:?}, price={}, qty={}, filled={}, remaining={}, status={:?})",
            self.id,
            self.account_id,
            self.symbol,
            self.side.__repr__(),
            self.price,
            self.quantity,
            self.filled_quantity,
            self.remaining_quantity,
            self.status.__repr__()
        )
    }
}

#[pyclass(name = "Trade")]
#[derive(Clone)]
pub struct PyTrade {
    #[pyo3(get)]
    pub execution_id: u64,
    #[pyo3(get)]
    pub maker_order_id: u64,
    #[pyo3(get)]
    pub taker_order_id: u64,
    #[pyo3(get)]
    pub maker_account_id: String,
    #[pyo3(get)]
    pub taker_account_id: String,
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub side: PySide,
    #[pyo3(get)]
    pub price: f64,
    #[pyo3(get)]
    pub quantity: f64,
    #[pyo3(get)]
    pub fee: f64,
    #[pyo3(get)]
    pub timestamp: i64,
}

impl From<Trade> for PyTrade {
    fn from(t: Trade) -> Self {
        Self {
            execution_id: t.execution_id,
            maker_order_id: t.maker_order_id,
            taker_order_id: t.taker_order_id,
            maker_account_id: t.maker_account_id,
            taker_account_id: t.taker_account_id,
            symbol: t.symbol,
            side: t.side.into(),
            price: t.price,
            quantity: t.quantity,
            fee: t.fee,
            timestamp: t.timestamp,
        }
    }
}

#[pymethods]
impl PyTrade {
    fn __repr__(&self) -> String {
        format!(
            "Trade(exec_id={}, symbol='{}', side={:?}, price={}, qty={})",
            self.execution_id,
            self.symbol,
            self.side.__repr__(),
            self.price,
            self.quantity
        )
    }
}

#[pyclass(name = "LevelQuote")]
#[derive(Clone)]
pub struct PyLevelQuote {
    #[pyo3(get)]
    pub price: f64,
    #[pyo3(get)]
    pub quantity: f64,
    #[pyo3(get)]
    pub order_count: usize,
}

impl From<LevelQuote> for PyLevelQuote {
    fn from(l: LevelQuote) -> Self {
        Self {
            price: l.price,
            quantity: l.quantity,
            order_count: l.order_count,
        }
    }
}

#[pymethods]
impl PyLevelQuote {
    fn __repr__(&self) -> String {
        format!(
            "LevelQuote(price={}, qty={}, orders={})",
            self.price, self.quantity, self.order_count
        )
    }
}

#[pyclass(name = "MarketDepth")]
#[derive(Clone)]
pub struct PyMarketDepth {
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub bids: Vec<PyLevelQuote>,
    #[pyo3(get)]
    pub asks: Vec<PyLevelQuote>,
    #[pyo3(get)]
    pub timestamp: i64,
}

impl From<MarketDepth> for PyMarketDepth {
    fn from(md: MarketDepth) -> Self {
        Self {
            symbol: md.symbol,
            bids: md.bids.into_iter().map(Into::into).collect(),
            asks: md.asks.into_iter().map(Into::into).collect(),
            timestamp: md.timestamp,
        }
    }
}

#[pymethods]
impl PyMarketDepth {
    fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|b| b.price)
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|a| a.price)
    }

    fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            (Some(bid), None) => Some(bid),
            (None, Some(ask)) => Some(ask),
            (None, None) => None,
        }
    }

    fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "MarketDepth(symbol='{}', best_bid={:?}, best_ask={:?}, bid_levels={}, ask_levels={})",
            self.symbol,
            self.best_bid(),
            self.best_ask(),
            self.bids.len(),
            self.asks.len()
        )
    }
}

#[pyclass(name = "Position")]
#[derive(Clone)]
pub struct PyPosition {
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub quantity: f64,
    #[pyo3(get)]
    pub avg_entry_price: f64,
    #[pyo3(get)]
    pub realized_pnl: f64,
    #[pyo3(get)]
    pub unrealized_pnl: f64,
    #[pyo3(get)]
    pub total_trades: usize,
    #[pyo3(get)]
    pub total_volume: f64,
    #[pyo3(get)]
    pub fees_paid: f64,
}

impl From<Position> for PyPosition {
    fn from(p: Position) -> Self {
        Self {
            symbol: p.symbol,
            quantity: p.quantity,
            avg_entry_price: p.avg_entry_price,
            realized_pnl: p.realized_pnl,
            unrealized_pnl: p.unrealized_pnl,
            total_trades: p.total_trades,
            total_volume: p.total_volume,
            fees_paid: p.fees_paid,
        }
    }
}

#[pymethods]
impl PyPosition {
    fn is_long(&self) -> bool {
        self.quantity > 1e-9
    }

    fn is_short(&self) -> bool {
        self.quantity < -1e-9
    }

    fn is_flat(&self) -> bool {
        self.quantity.abs() <= 1e-9
    }

    fn __repr__(&self) -> String {
        format!(
            "Position(symbol='{}', qty={}, avg_entry={}, realized_pnl={:.2}, unrealized_pnl={:.2})",
            self.symbol,
            self.quantity,
            self.avg_entry_price,
            self.realized_pnl,
            self.unrealized_pnl
        )
    }
}

#[pyclass(name = "Account")]
#[derive(Clone)]
pub struct PyAccount {
    #[pyo3(get)]
    pub cash_balance: f64,
    #[pyo3(get)]
    pub initial_balance: f64,
    #[pyo3(get)]
    pub realized_pnl: f64,
    #[pyo3(get)]
    pub margin_used: f64,
    #[pyo3(get)]
    pub leverage: f64,
}

impl From<Account> for PyAccount {
    fn from(a: Account) -> Self {
        Self {
            cash_balance: a.cash_balance,
            initial_balance: a.initial_balance,
            realized_pnl: a.realized_pnl,
            margin_used: a.margin_used,
            leverage: a.leverage,
        }
    }
}

#[pymethods]
impl PyAccount {
    fn equity(&self, unrealized_pnl: f64) -> f64 {
        self.cash_balance + unrealized_pnl
    }

    fn free_margin(&self, unrealized_pnl: f64) -> f64 {
        (self.equity(unrealized_pnl) - self.margin_used).max(0.0)
    }

    fn __repr__(&self) -> String {
        format!(
            "Account(cash={:.2}, realized_pnl={:.2}, margin_used={:.2}, leverage={:.1}x)",
            self.cash_balance, self.realized_pnl, self.margin_used, self.leverage
        )
    }
}

#[pyclass(name = "RiskConfig")]
#[derive(Clone)]
pub struct PyRiskConfig {
    #[pyo3(get, set)]
    pub max_order_qty: f64,
    #[pyo3(get, set)]
    pub max_order_notional: f64,
    #[pyo3(get, set)]
    pub max_position_notional: f64,
    #[pyo3(get, set)]
    pub price_collar_pct: f64,
    #[pyo3(get, set)]
    pub max_drawdown_pct: f64,
    #[pyo3(get, set)]
    pub max_orders_per_sec: usize,
    #[pyo3(get, set)]
    pub require_margin: bool,
}

#[pymethods]
impl PyRiskConfig {
    #[new]
    #[pyo3(signature = (
        max_order_qty=1_000_000.0,
        max_order_notional=10_000_000.0,
        max_position_notional=20_000_000.0,
        price_collar_pct=0.10,
        max_drawdown_pct=0.25,
        max_orders_per_sec=1000,
        require_margin=true
    ))]
    fn new(
        max_order_qty: f64,
        max_order_notional: f64,
        max_position_notional: f64,
        price_collar_pct: f64,
        max_drawdown_pct: f64,
        max_orders_per_sec: usize,
        require_margin: bool,
    ) -> Self {
        Self {
            max_order_qty,
            max_order_notional,
            max_position_notional,
            price_collar_pct,
            max_drawdown_pct,
            max_orders_per_sec,
            require_margin,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RiskConfig(max_qty={}, max_notional={}, collar={:.1}%, max_dd={:.1}%)",
            self.max_order_qty,
            self.max_order_notional,
            self.price_collar_pct * 100.0,
            self.max_drawdown_pct * 100.0
        )
    }
}

impl From<RiskConfig> for PyRiskConfig {
    fn from(c: RiskConfig) -> Self {
        Self {
            max_order_qty: c.max_order_qty,
            max_order_notional: c.max_order_notional,
            max_position_notional: c.max_position_notional,
            price_collar_pct: c.price_collar_pct,
            max_drawdown_pct: c.max_drawdown_pct,
            max_orders_per_sec: c.max_orders_per_sec,
            require_margin: c.require_margin,
        }
    }
}

impl From<PyRiskConfig> for RiskConfig {
    fn from(c: PyRiskConfig) -> Self {
        Self {
            max_order_qty: c.max_order_qty,
            max_order_notional: c.max_order_notional,
            max_position_notional: c.max_position_notional,
            price_collar_pct: c.price_collar_pct,
            max_drawdown_pct: c.max_drawdown_pct,
            max_orders_per_sec: c.max_orders_per_sec,
            require_margin: c.require_margin,
        }
    }
}
