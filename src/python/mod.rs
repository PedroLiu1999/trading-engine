pub mod engine;
pub mod market_sim;
pub mod types;

use pyo3::prelude::*;

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<engine::PyEngine>()?;
    m.add_class::<types::PySide>()?;
    m.add_class::<types::PyOrderType>()?;
    m.add_class::<types::PyTimeInForce>()?;
    m.add_class::<types::PyOrderStatus>()?;
    m.add_class::<types::PyOrder>()?;
    m.add_class::<types::PyTrade>()?;
    m.add_class::<types::PyLevelQuote>()?;
    m.add_class::<types::PyMarketDepth>()?;
    m.add_class::<types::PyPosition>()?;
    m.add_class::<types::PyAccount>()?;
    m.add_class::<types::PyRiskConfig>()?;
    m.add_class::<market_sim::PyAssetConfig>()?;
    m.add_class::<market_sim::PyMultiAssetMarketSim>()?;
    Ok(())
}
