# High-Performance Rust Trading Engine with Python API

[![CI](https://github.com/PedroLiu1999/trading-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/PedroLiu1999/trading-engine/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![PyO3](https://img.shields.io/badge/pyo3-0.22-green.svg)](https://pyo3.rs/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

A modular, thread-safe trading and matching engine built in Rust with Python bindings via PyO3, real-time risk controls, and multi-asset market simulation.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph ClientLayer["Client & Strategy Layer"]
        PythonAPI["Python API (PyEngine)"]
        RustAPI["Native Rust API (ExecutionEngine)"]
    end

    subgraph CoreEngine["Trading Engine Core"]
        Risk["Risk Manager\n- Max Order Size & Notional\n- Price Collar / Fat-Finger Check\n- Rate Limiting\n- Margin Requirement\n- Max Drawdown Kill-Switch"]
        
        OB["Limit Order Book\n- Price-Time Priority (FIFO)\n- Level 2 Aggregate Depth\n- Level 3 Order Queues\n- Market Order Book Walker"]
        
        PM["Position Manager\n- Real-Time Position Tracking\n- Moving Avg Entry Price\n- Realized & Unrealized PnL\n- Leverage & Collateral Tracking"]
        
        EB["Event Bus\n- Crossbeam Lock-Free Ring Channels\n- Multi-Subscriber Dispatch\n- Complete Audit History Log"]
    end

    PythonAPI -->|Submit Order| Risk
    RustAPI -->|Submit Order| Risk
    Risk -->|Approved| OB
    OB -->|Execution / Fills| PM
    OB -->|Trades & Book Updates| EB
    Risk -->|Risk Breaches| EB
    PM -->|Position & PnL Updates| EB
    EB -->|Streams Events| PythonAPI
    EB -->|Streams Events| RustAPI
```

---

## Core Components

### 1. Limit Order Book (`src/order_book.rs`)
- **Price-Time Priority (FIFO)** matching engine.
- Integer price-tick indexing to eliminate IEEE-754 floating-point comparison hazards.
- $O(1)$ order lookup and cancellation via hash index mapping.
- Level 2 market depth generation (top $N$ bids & asks, cumulative quantities, and order counts).
- Supports Limit, Market, GTC (Good 'Til Cancelled), IOC (Immediate Or Cancel), and FOK (Fill Or Kill) orders.

### 2. Event Bus (`src/event_bus.rs`)
- High-throughput asynchronous multi-producer multi-consumer publish/subscribe bus built on `crossbeam-channel`.
- Strongly-typed events: `OrderSubmitted`, `OrderAccepted`, `OrderRejected`, `OrderCancelled`, `OrderFilled`, `TradeExecuted`, `BookUpdated`, `PositionUpdated`, `AccountUpdated`, `RiskBreached`, `SystemAlert`.
- In-memory ring buffer capturing chronological event audit logs.

### 3. Position Manager (`src/position_manager.rs`)
- Multi-asset position tracking per symbol.
- Correct average entry price calculation across scale-ins, partial reductions, and position flips (Long $\leftrightarrow$ Short).
- Real-time realized PnL and Mark-to-Market unrealized PnL.
- Margin accounting (used margin, free collateral, leverage support).

### 4. Risk Manager (`src/risk_manager.rs`)
- **Pre-trade risk pipeline**:
  - `MaxOrderQuantity` and `MaxOrderNotional` safeguards.
  - `MaxPositionNotional` aggregate exposure limits.
  - `PriceCollar`: rejects limit orders that deviate by more than $X\%$ from the prevailing mid price.
  - Rate limiting (sliding-window orders-per-second throttling).
  - Margin verification against available free capital.
- **Post-trade circuit breaker**:
  - Automatically trips engine kill-switch if portfolio drawdown breaches the configured risk limit.
  - Supports manual emergency halts (`trip_kill_switch` / `reset_kill_switch`).

### 5. Execution Engine (`src/execution_engine.rs`)
- Coordinates the complete order lifecycle state machine.
- Routes submissions through risk checks $\rightarrow$ order book matching $\rightarrow$ position updates $\rightarrow$ event broadcasting.

### 6. Correlated Multi-Asset Market Simulator (`src/market_sim.rs`)
- **Correlated Geometric Brownian Motion (GBM)**: Simulates $N$ correlated price trajectories using Cholesky decomposition ($\mathbf{\Sigma} = \mathbf{L} \mathbf{L}^T$) and Box-Muller Gaussian transforms.
- **Market Microstructure**:
  - *Automated Market Makers*: Multi-level quoting ladders dynamically adjusted around prevailing theoretical fair prices.
  - *Noise / Flow Traders*: Poisson arrival process simulating aggressive liquidity-taking market order flow.
- Seamlessly accessible from Python via `MultiAssetMarketSim` and `AssetConfig`.

### 7. Python API (`src/python/` & `trading_engine`)
- Native Python C-extension built with PyO3.
- Idiomatic Python classes: `Engine`, `Order`, `Trade`, `Position`, `Account`, `MarketDepth`, `RiskConfig`.
- Fully typed with PEP 561 marker and clean docstrings.

---

## Quickstart

### Prerequisites
- [Rust](https://www.rust-lang.org/) (stable toolchain)
- [uv](https://docs.astral.sh/uv/) (recommended) or Python 3.11+

### Installation & Build

1. **Install dependencies and compile Python extension module:**
   ```bash
   uv venv
   uv run maturin develop
   ```

2. **Run Python Examples:**
   ```bash
   uv run python examples/basic_trading.py
   uv run python examples/market_maker.py
   uv run python examples/multi_asset_stat_arb.py
   uv run python examples/order_book_imbalance.py
   ```

3. **Run Native Rust Standalone CLI Demo:**
   ```bash
   cargo run --bin trading-engine-cli --no-default-features
   ```

---

## Python Usage Example

```python
from trading_engine import Engine, RiskConfig

# Configure risk limits
risk = RiskConfig(
    max_order_qty=100.0,
    max_order_notional=500_000.0,
    max_position_notional=1_000_000.0,
    price_collar_pct=0.05,  # 5% price collar
    max_drawdown_pct=0.20,
)

# Initialize engine with $100,000 cash balance and 2x leverage
engine = Engine(initial_balance=100_000.0, leverage=2.0, risk_config=risk)
engine.register_symbol("BTC-USDT", tick_size=0.50, lot_size=0.001)

# Submit resting limit orders
engine.submit_order("BTC-USDT", "BUY", "LIMIT", 60_000.0, 1.0)
engine.submit_order("BTC-USDT", "SELL", "LIMIT", 60_100.0, 1.0)

# Inspect Order Book depth
depth = engine.get_depth("BTC-USDT", levels=5)
print(f"Mid Price: ${depth.mid_price():,.2f} | Spread: ${depth.spread():,.2f}")

# Submit market taker order
order = engine.submit_order("BTC-USDT", "BUY", "MARKET", 0.0, 0.5, "IOC")
print(f"Filled: {order.filled_quantity} BTC")

# Inspect Position and Account
pos = engine.get_position("BTC-USDT")
print(f"Position: {pos.quantity} BTC @ avg ${pos.avg_entry_price:,.2f}")

# Poll real-time events
events = engine.poll_events()
```

---

## Correlated Multi-Asset Simulation Example

```python
from trading_engine import AssetConfig, Engine, MultiAssetMarketSim

engine = Engine(initial_balance=500_000.0)

# Configure correlated asset universe
btc = AssetConfig("BTC-USDT", initial_price=60_000.0, drift=0.03, volatility=0.45)
eth = AssetConfig("ETH-USDT", initial_price=3_000.0, drift=0.03, volatility=0.55)

# Correlation matrix
correlation = [
    [1.0, 0.85],
    [0.85, 1.0],
]

# Initialize simulator
sim = MultiAssetMarketSim(engine, [btc, eth], correlation, seed=42)

# Step market 100 times (1 second intervals)
for _ in range(100):
    prices = sim.step(dt=1.0)
    btc_depth = engine.get_depth("BTC-USDT")
    eth_depth = engine.get_depth("ETH-USDT")
```

---

## Testing & Quality Assurance

### Rust Unit & Integration Tests
```bash
cargo test --no-default-features --all-targets
```

### Python Linting & Formatting (Ruff)
```bash
uv run ruff check .
uv run ruff format --check .
```

### Python Integration Tests (Pytest)
```bash
uv run pytest tests/
```

---

## Real-Market Kraken Order Book Backtesting

Backtest strategies against authentic exchange order book depth and historical public market taker executions stored in compressed Apache Parquet format:

```bash
# Run backtest on cached Kraken ETH/USD Parquet dataset (OBI Scalper vs. Random Baseline)
uv run python examples/kraken_backtest.py

# Download 5,000 (or 10,000) historical market trades from Kraken API and save to Parquet
uv run python examples/kraken_backtest.py --fetch-history 5000

# Force refresh order book depth snapshot and trade history from Kraken
uv run python examples/kraken_backtest.py --refresh

# Continuously stream live market trades and execute in real-time until Ctrl+C
uv run python examples/kraken_backtest.py --live --pair ETHUSD
```

Features:
- **No API Key Required**: Ingests public L2 depth snapshots and trade streams directly from Kraken REST API.
- **Ultra-Fast Parquet Storage**: Uses Apache Parquet (`pyarrow`) for columnar, compressed, zero-copy tick reading across thousands of market trades.
- **Realistic Queue Position**: Passive strategy orders join the authentic exchange queue at price levels with price-time priority behind resting market maker liquidity.
- **True Order-Flow Toxicity**: Real market taker trades walk the book, subjecting passive limit orders to authentic adverse selection and sweep dynamics.

---

## Performance & Benchmarks

All figures below are measured using the automated benchmark suite ([examples/benchmark.py](file:///home/peter/quant/trading-engine/examples/benchmark.py)) across the Python API boundary (including PyO3 type conversion, pre-trade risk validation, FIFO queue operations, dual-sided ledger balance updates, and event bus publication).

### Measured Figures (x86_64 Linux, Python 3.11, Rust Core)

| Benchmark Subsystem | Samples | Throughput | Mean Latency | Median (p50) | p90 | p99 | Max |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **L2 Depth & Top-of-Book Read** | 50,000 | **227,916 op/s** | 4.32 µs | 4.14 µs | 4.21 µs | 10.42 µs | 119.88 µs |
| **Order Cancellation** | 25,000 | **24,151 op/s** | 41.20 µs | 38.99 µs | 45.67 µs | 70.36 µs | 2,039.73 µs |
| **Limit Order Placement** | 25,000 | **17,303 op/s** | 56.91 µs | 60.27 µs | 73.04 µs | 113.85 µs | 2,275.90 µs |
| **Trade Execution & Matching** | 25,000 | **6,926 op/s** | 144.01 µs | 135.89 µs | 165.58 µs | 227.33 µs | 2,305.18 µs |
| **Multi-Account Routing (10 accts)** | 25,000 | **6,365 op/s** | 156.69 µs | 151.79 µs | 179.60 µs | 227.07 µs | 2,136.70 µs |
| **3-Asset Correlated Sim (GBM+MM+Noise)** | 500 | **577 step/s** | 1.73 ms | 2.07 ms | 2.15 ms | 2.67 ms | 2.99 ms |

*Note: Latency is end-to-end Python round-trip measured using `time.perf_counter_ns()`. Pure internal Rust core operations without Python FFI boundaries execute with substantially lower latency.*

### Running the Benchmark Suite

```bash
uv run python examples/benchmark.py
```

CLI Options:
- `--orders N`: Number of orders to benchmark for insertion, matching, and cancellation (default: `25,000`)
- `--depth-queries N`: Number of L2 order book depth snapshots (default: `50,000`)
- `--sim-steps N`: Number of multi-asset simulation steps (default: `500`)
- `--quick`: Fast iteration benchmark with smaller sample sizes

---

## License
MIT
