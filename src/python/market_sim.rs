use crate::market_sim::{AssetConfig, MultiAssetMarketSim};
use crate::python::engine::PyEngine;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass(name = "AssetConfig")]
#[derive(Clone)]
pub struct PyAssetConfig {
    #[pyo3(get, set)]
    pub symbol: String,
    #[pyo3(get, set)]
    pub initial_price: f64,
    #[pyo3(get, set)]
    pub drift: f64,
    #[pyo3(get, set)]
    pub volatility: f64,
    #[pyo3(get, set)]
    pub tick_size: f64,
    #[pyo3(get, set)]
    pub lot_size: f64,
    #[pyo3(get, set)]
    pub quote_levels: usize,
    #[pyo3(get, set)]
    pub base_spread_bps: f64,
    #[pyo3(get, set)]
    pub arrival_rate: f64,
    #[pyo3(get, set)]
    pub avg_order_qty: f64,
}

#[pymethods]
impl PyAssetConfig {
    #[new]
    #[pyo3(signature = (
        symbol,
        initial_price,
        drift=0.05,
        volatility=0.30,
        tick_size=0.01,
        lot_size=0.001,
        quote_levels=3,
        base_spread_bps=5.0,
        arrival_rate=10.0,
        avg_order_qty=1.0
    ))]
    fn new(
        symbol: String,
        initial_price: f64,
        drift: f64,
        volatility: f64,
        tick_size: f64,
        lot_size: f64,
        quote_levels: usize,
        base_spread_bps: f64,
        arrival_rate: f64,
        avg_order_qty: f64,
    ) -> Self {
        Self {
            symbol,
            initial_price,
            drift,
            volatility,
            tick_size,
            lot_size,
            quote_levels,
            base_spread_bps,
            arrival_rate,
            avg_order_qty,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "AssetConfig(symbol='{}', price={}, vol={:.1}%, spread_bps={:.1})",
            self.symbol,
            self.initial_price,
            self.volatility * 100.0,
            self.base_spread_bps
        )
    }
}

impl From<PyAssetConfig> for AssetConfig {
    fn from(c: PyAssetConfig) -> Self {
        AssetConfig {
            symbol: c.symbol,
            initial_price: c.initial_price,
            drift: c.drift,
            volatility: c.volatility,
            tick_size: c.tick_size,
            lot_size: c.lot_size,
            quote_levels: c.quote_levels,
            base_spread_bps: c.base_spread_bps,
            arrival_rate: c.arrival_rate,
            avg_order_qty: c.avg_order_qty,
        }
    }
}

impl From<AssetConfig> for PyAssetConfig {
    fn from(c: AssetConfig) -> Self {
        PyAssetConfig {
            symbol: c.symbol,
            initial_price: c.initial_price,
            drift: c.drift,
            volatility: c.volatility,
            tick_size: c.tick_size,
            lot_size: c.lot_size,
            quote_levels: c.quote_levels,
            base_spread_bps: c.base_spread_bps,
            arrival_rate: c.arrival_rate,
            avg_order_qty: c.avg_order_qty,
        }
    }
}

#[pyclass(name = "MultiAssetMarketSim")]
pub struct PyMultiAssetMarketSim {
    sim: MultiAssetMarketSim,
}

#[pymethods]
impl PyMultiAssetMarketSim {
    #[new]
    #[pyo3(signature = (engine, assets, correlation_matrix, seed=42))]
    fn new(
        engine: &PyEngine,
        assets: Vec<PyAssetConfig>,
        correlation_matrix: Vec<Vec<f64>>,
        seed: u64,
    ) -> PyResult<Self> {
        let rust_assets: Vec<AssetConfig> = assets.into_iter().map(Into::into).collect();
        let sim = MultiAssetMarketSim::new(
            engine.inner(),
            rust_assets,
            correlation_matrix,
            seed,
        )
        .map_err(|e| PyValueError::new_err(e))?;

        Ok(Self { sim })
    }

    #[pyo3(signature = (dt=1.0))]
    fn step(&mut self, dt: f64) -> HashMap<String, f64> {
        self.sim.step(dt)
    }

    #[pyo3(signature = (count=10, dt=1.0))]
    fn run_steps(&mut self, count: usize, dt: f64) {
        self.sim.run_steps(count, dt);
    }

    fn get_prices(&self) -> HashMap<String, f64> {
        self.sim.get_prices()
    }

    fn get_fair_price(&self, symbol: &str) -> Option<f64> {
        self.sim.get_fair_price(symbol)
    }

    fn step_count(&self) -> usize {
        self.sim.step_count()
    }
}
