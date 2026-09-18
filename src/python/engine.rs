use crate::event_bus::EngineEvent;
use crate::execution_engine::ExecutionEngine;
use crate::python::types::{PyAccount, PyMarketDepth, PyOrder, PyPosition, PyRiskConfig};
use crate::risk_manager::RiskConfig;
use crate::types::{OrderType, Side, TimeInForce};
use crossbeam_channel::Receiver;
use parking_lot::Mutex;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(name = "Engine")]
pub struct PyEngine {
    engine: Arc<ExecutionEngine>,
    event_rx: Mutex<Receiver<EngineEvent>>,
}

impl PyEngine {
    pub fn inner(&self) -> Arc<ExecutionEngine> {
        Arc::clone(&self.engine)
    }
}

#[pymethods]
impl PyEngine {
    #[new]
    #[pyo3(signature = (initial_balance=100_000.0, leverage=1.0, risk_config=None))]
    fn new(
        initial_balance: f64,
        leverage: f64,
        risk_config: Option<PyRiskConfig>,
    ) -> PyResult<Self> {
        let config: RiskConfig = risk_config.map(Into::into).unwrap_or_default();
        let engine = Arc::new(ExecutionEngine::new(initial_balance, leverage, config));
        let event_rx = Mutex::new(engine.event_bus().subscribe());

        Ok(Self { engine, event_rx })
    }

    #[pyo3(signature = (symbol, tick_size=0.01, lot_size=0.0001))]
    fn register_symbol(&self, symbol: &str, tick_size: f64, lot_size: f64) {
        self.engine.register_symbol(symbol, tick_size, lot_size);
    }

    #[pyo3(signature = (
        symbol,
        side,
        order_type,
        price,
        quantity,
        time_in_force="GTC",
        client_order_id=None,
        account_id=None
    ))]
    fn submit_order(
        &self,
        symbol: &str,
        side: &str,
        order_type: &str,
        price: f64,
        quantity: f64,
        time_in_force: &str,
        client_order_id: Option<String>,
        account_id: Option<String>,
    ) -> PyResult<PyOrder> {
        let parsed_side = match side.to_uppercase().as_str() {
            "BUY" | "B" => Side::Buy,
            "SELL" | "S" => Side::Sell,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid side '{}', expected 'BUY' or 'SELL'",
                    side
                )))
            }
        };

        let parsed_type = match order_type.to_uppercase().as_str() {
            "LIMIT" | "L" => OrderType::Limit,
            "MARKET" | "M" => OrderType::Market,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid order_type '{}', expected 'LIMIT' or 'MARKET'",
                    order_type
                )))
            }
        };

        let parsed_tif = match time_in_force.to_uppercase().as_str() {
            "GTC" => TimeInForce::GTC,
            "IOC" => TimeInForce::IOC,
            "FOK" => TimeInForce::FOK,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid time_in_force '{}', expected 'GTC', 'IOC', or 'FOK'",
                    time_in_force
                )))
            }
        };

        self.engine
            .submit_order_with_account(
                client_order_id,
                account_id,
                symbol,
                parsed_side,
                parsed_type,
                price,
                quantity,
                parsed_tif,
            )
            .map(PyOrder::from)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn cancel_order(&self, symbol: &str, order_id: u64) -> PyResult<PyOrder> {
        self.engine
            .cancel_order(symbol, order_id)
            .map(PyOrder::from)
            .map_err(|e| PyKeyError::new_err(e.to_string()))
    }

    #[pyo3(signature = (symbol, levels=10))]
    fn get_depth(&self, symbol: &str, levels: usize) -> Option<PyMarketDepth> {
        self.engine.get_depth(symbol, levels).map(Into::into)
    }

    #[pyo3(signature = (symbol, account_id="DEFAULT"))]
    fn get_position(&self, symbol: &str, account_id: &str) -> Option<PyPosition> {
        self.engine
            .get_position_by_account(account_id, symbol)
            .map(Into::into)
    }

    #[pyo3(signature = (account_id="DEFAULT"))]
    fn get_positions(&self, account_id: &str) -> Vec<PyPosition> {
        self.engine
            .get_positions_by_account(account_id)
            .into_iter()
            .map(Into::into)
            .collect()
    }

    fn get_all_positions(&self) -> Vec<PyPosition> {
        self.engine
            .get_all_positions()
            .into_iter()
            .map(Into::into)
            .collect()
    }

    #[pyo3(signature = (account_id="DEFAULT"))]
    fn get_account(&self, account_id: &str) -> Option<PyAccount> {
        self.engine.get_account_by_id(account_id).map(Into::into)
    }

    fn get_all_account_ids(&self) -> Vec<String> {
        self.engine.get_all_account_ids()
    }

    fn get_risk_config(&self) -> PyRiskConfig {
        self.engine.get_risk_config().into()
    }

    fn set_risk_config(&self, config: PyRiskConfig) {
        self.engine.set_risk_config(config.into());
    }

    #[pyo3(signature = (reason="Manual trip via Python API"))]
    fn trip_kill_switch(&self, reason: &str) {
        self.engine.trip_kill_switch(reason);
    }

    fn reset_kill_switch(&self) {
        self.engine.reset_kill_switch();
    }

    fn is_kill_switch_active(&self) -> bool {
        self.engine.is_kill_switch_active()
    }

    fn mark_to_market(&self, symbol: &str, price: f64) {
        self.engine.mark_to_market(symbol, price);
    }

    #[pyo3(signature = (max_count=100))]
    fn poll_events(&self, max_count: usize) -> Vec<String> {
        let rx = self.event_rx.lock();
        let mut events = Vec::new();
        while events.len() < max_count {
            match rx.try_recv() {
                Ok(ev) => {
                    if let Ok(json) = serde_json::to_string(&ev) {
                        events.push(json);
                    }
                }
                Err(_) => break,
            }
        }
        events
    }

    fn event_history(&self) -> Vec<String> {
        self.engine
            .event_bus()
            .history()
            .into_iter()
            .filter_map(|ev| serde_json::to_string(&ev).ok())
            .collect()
    }
}
