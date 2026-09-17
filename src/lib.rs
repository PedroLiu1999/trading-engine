pub mod event_bus;
pub mod execution_engine;
pub mod order_book;
pub mod position_manager;
pub mod risk_manager;
pub mod types;

#[cfg(feature = "python")]
pub mod python;

// Re-exports for Rust library consumers
pub use event_bus::{EngineEvent, EventBus};
pub use execution_engine::{ExecutionEngine, ExecutionError};
pub use order_book::{MatchResult, OrderBook};
pub use position_manager::{Account, Position, PositionManager};
pub use risk_manager::{RiskConfig, RiskManager, RiskRejection};
pub use types::*;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymodule]
fn _trading_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register_module(m)
}
