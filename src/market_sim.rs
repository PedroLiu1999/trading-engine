use crate::execution_engine::ExecutionEngine;
use crate::types::{OrderId, OrderType, Side, TimeInForce};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration parameters for an asset in the market simulation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetConfig {
    pub symbol: String,
    pub initial_price: f64,
    pub drift: f64,           // Annualized drift (e.g. 0.05 for 5%)
    pub volatility: f64,      // Annualized volatility (e.g. 0.30 for 30%)
    pub tick_size: f64,       // Minimum price increment
    pub lot_size: f64,        // Minimum order quantity
    pub quote_levels: usize,  // Depth levels quoted by market maker (e.g. 3)
    pub base_spread_bps: f64, // Half-spread in basis points (e.g. 5.0 for 0.05%)
    pub arrival_rate: f64,    // Poisson market order arrival rate (orders per second)
    pub avg_order_qty: f64,   // Average quantity for noise market orders
}

impl AssetConfig {
    pub fn new(
        symbol: impl Into<String>,
        initial_price: f64,
        drift: f64,
        volatility: f64,
        tick_size: f64,
        lot_size: f64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            initial_price,
            drift,
            volatility,
            tick_size: if tick_size <= 0.0 { 0.01 } else { tick_size },
            lot_size: if lot_size <= 0.0 { 0.001 } else { lot_size },
            quote_levels: 3,
            base_spread_bps: 5.0,
            arrival_rate: 10.0,
            avg_order_qty: 1.0,
        }
    }
}

/// Lightweight, deterministic pseudo-random number generator (XorShift64*).
pub struct FastRng {
    state: u64,
}

impl FastRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x853c49e6748fea9b } else { seed },
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    /// Generates uniform float in [0, 1)
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / 9007199254740992.0)
    }

    /// Generates a standard normal random variable N(0, 1) using Box-Muller transform
    pub fn next_normal(&mut self) -> f64 {
        let u1 = self.next_f64().max(1e-15);
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}

/// Performs Cholesky decomposition of a symmetric positive-definite matrix: Sigma = L * L^T.
pub fn cholesky(matrix: &[Vec<f64>]) -> Result<Vec<Vec<f64>>, String> {
    let n = matrix.len();
    if n == 0 {
        return Err("Matrix is empty".to_string());
    }
    for row in matrix {
        if row.len() != n {
            return Err("Matrix is not square".to_string());
        }
    }

    let mut l = vec![vec![0.0; n]; n];

    for i in 0..n {
        for j in 0..=i {
            let mut sum = 0.0;
            for k in 0..j {
                sum += l[i][k] * l[j][k];
            }

            if i == j {
                let val = matrix[i][i] - sum;
                if val <= 0.0 {
                    // Apply slight regularization to ensure positive definiteness
                    let regularized = (val.abs() + 1e-6).sqrt();
                    l[i][j] = regularized;
                } else {
                    l[i][j] = val.sqrt();
                }
            } else {
                if l[j][j].abs() < 1e-12 {
                    l[i][j] = 0.0;
                } else {
                    l[i][j] = (matrix[i][j] - sum) / l[j][j];
                }
            }
        }
    }

    Ok(l)
}

/// Correlated Multi-Asset Market Simulator.
pub struct MultiAssetMarketSim {
    engine: Arc<ExecutionEngine>,
    assets: Vec<AssetConfig>,
    current_prices: Vec<f64>,
    cholesky_l: Vec<Vec<f64>>,
    rng: FastRng,
    resting_quote_ids: HashMap<String, Vec<OrderId>>,
    step_count: usize,
}

impl MultiAssetMarketSim {
    pub fn new(
        engine: Arc<ExecutionEngine>,
        assets: Vec<AssetConfig>,
        correlation_matrix: Vec<Vec<f64>>,
        seed: u64,
    ) -> Result<Self, String> {
        let n = assets.len();
        if n == 0 {
            return Err("Must provide at least one asset config".to_string());
        }
        if correlation_matrix.len() != n || correlation_matrix.iter().any(|r| r.len() != n) {
            return Err(format!(
                "Correlation matrix dimension mismatch: expected {}x{}, got {}x{}",
                n,
                n,
                correlation_matrix.len(),
                correlation_matrix.first().map(|r| r.len()).unwrap_or(0)
            ));
        }

        let cholesky_l = cholesky(&correlation_matrix)?;
        let current_prices: Vec<f64> = assets.iter().map(|a| a.initial_price).collect();

        // Register each asset in the ExecutionEngine
        for asset in &assets {
            engine.register_symbol(&asset.symbol, asset.tick_size, asset.lot_size);
        }

        Ok(Self {
            engine,
            assets,
            current_prices,
            cholesky_l,
            rng: FastRng::new(seed),
            resting_quote_ids: HashMap::new(),
            step_count: 0,
        })
    }

    /// Advances the simulation by dt (seconds, e.g. 0.1 for 100ms or 1.0 for 1s).
    pub fn step(&mut self, dt: f64) -> HashMap<String, f64> {
        let dt = if dt <= 0.0 { 1.0 } else { dt };
        let n = self.assets.len();
        self.step_count += 1;

        // 1. Generate independent standard normals
        let z: Vec<f64> = (0..n).map(|_| self.rng.next_normal()).collect();

        // 2. Correlate normals: epsilon = L * Z
        let mut epsilon = vec![0.0; n];
        for i in 0..n {
            let mut sum = 0.0;
            for j in 0..=i {
                sum += self.cholesky_l[i][j] * z[j];
            }
            epsilon[i] = sum;
        }

        // Annualized scaling: dt in seconds -> dt_years = dt / (365.25 * 86400)
        let dt_years = dt / (365.25 * 86400.0);
        let sqrt_dt = dt_years.sqrt();

        // 3. Update theoretical fair prices via Geometric Brownian Motion
        for i in 0..n {
            let asset = &self.assets[i];
            let drift_term = (asset.drift - 0.5 * asset.volatility * asset.volatility) * dt_years;
            let diffusion_term = asset.volatility * sqrt_dt * epsilon[i];
            let new_price = self.current_prices[i] * (drift_term + diffusion_term).exp();
            self.current_prices[i] = (new_price / asset.tick_size).round() * asset.tick_size;

            // Update mark-to-market in engine
            self.engine
                .mark_to_market(&asset.symbol, self.current_prices[i]);
        }

        // 4. Cancel previous resting market maker quotes
        for (sym, order_ids) in &self.resting_quote_ids {
            for &oid in order_ids {
                let _ = self.engine.cancel_order(sym, oid);
            }
        }
        self.resting_quote_ids.clear();

        // 5. Place fresh Market Maker quote ladders around new fair prices
        for i in 0..n {
            let asset = &self.assets[i];
            let fair_price = self.current_prices[i];
            let spread_half = fair_price * (asset.base_spread_bps / 10_000.0);
            let mut new_ids = Vec::new();

            for level in 1..=asset.quote_levels {
                let level_offset = (level as f64) * asset.tick_size * 2.0;
                let raw_bid = (fair_price - spread_half - level_offset).max(asset.tick_size);
                let raw_ask = fair_price + spread_half + level_offset;

                let bid_price = (raw_bid / asset.tick_size).round() * asset.tick_size;
                let ask_price = (raw_ask / asset.tick_size).round() * asset.tick_size;

                let qty = (asset.avg_order_qty * (1.0 + 0.5 * (level as f64)) / asset.lot_size)
                    .round()
                    * asset.lot_size;

                if let Ok(bid) = self.engine.submit_order_with_account(
                    Some(format!("mm_bid_{}_{}", asset.symbol, level)),
                    Some("SIM_MM".to_string()),
                    &asset.symbol,
                    Side::Buy,
                    OrderType::Limit,
                    bid_price,
                    qty,
                    TimeInForce::GTC,
                ) {
                    new_ids.push(bid.id);
                }

                if let Ok(ask) = self.engine.submit_order_with_account(
                    Some(format!("mm_ask_{}_{}", asset.symbol, level)),
                    Some("SIM_MM".to_string()),
                    &asset.symbol,
                    Side::Sell,
                    OrderType::Limit,
                    ask_price,
                    qty,
                    TimeInForce::GTC,
                ) {
                    new_ids.push(ask.id);
                }
            }

            self.resting_quote_ids.insert(asset.symbol.clone(), new_ids);
        }

        // 6. Simulate Noise Traders / Aggressive Market Orders (Poisson arrivals)
        for asset in &self.assets {
            let expected_arrivals = asset.arrival_rate * dt;
            // Generate arrivals based on Poisson / Bernoulli sampling
            let p_arrival = (1.0 - (-expected_arrivals).exp()).min(0.95);

            if self.rng.next_f64() < p_arrival {
                let is_buy = self.rng.next_f64() < 0.5;
                let size_factor = 0.5 + self.rng.next_f64(); // 0.5x to 1.5x avg qty
                let qty =
                    ((asset.avg_order_qty * size_factor) / asset.lot_size).round() * asset.lot_size;

                if qty > 0.0 {
                    let side = if is_buy { Side::Buy } else { Side::Sell };
                    let _ = self.engine.submit_order_with_account(
                        Some(format!("noise_{}_{}", asset.symbol, self.step_count)),
                        Some("SIM_NOISE".to_string()),
                        &asset.symbol,
                        side,
                        OrderType::Market,
                        0.0,
                        qty,
                        TimeInForce::IOC,
                    );
                }
            }
        }

        self.get_prices()
    }

    /// Advances simulation by `count` steps.
    pub fn run_steps(&mut self, count: usize, dt: f64) {
        for _ in 0..count {
            self.step(dt);
        }
    }

    /// Returns the current theoretical fair prices for all assets.
    pub fn get_prices(&self) -> HashMap<String, f64> {
        let mut map = HashMap::new();
        for (i, asset) in self.assets.iter().enumerate() {
            map.insert(asset.symbol.clone(), self.current_prices[i]);
        }
        map
    }

    pub fn get_fair_price(&self, symbol: &str) -> Option<f64> {
        self.assets
            .iter()
            .position(|a| a.symbol == symbol)
            .map(|idx| self.current_prices[idx])
    }

    pub fn step_count(&self) -> usize {
        self.step_count
    }
}
