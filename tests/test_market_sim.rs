use std::sync::Arc;
use trading_engine::market_sim::{cholesky, AssetConfig, MultiAssetMarketSim};
use trading_engine::{ExecutionEngine, RiskConfig};

#[test]
fn test_cholesky_decomposition_2x2() {
    let corr = vec![vec![1.0, 0.6], vec![0.6, 1.0]];
    let l = cholesky(&corr).unwrap();

    // L * L^T should equal corr
    let l00 = l[0][0];
    let l10 = l[1][0];
    let l11 = l[1][1];

    assert!((l00 * l00 - 1.0).abs() < 1e-6);
    assert!((l10 * l00 - 0.6).abs() < 1e-6);
    assert!((l10 * l10 + l11 * l11 - 1.0).abs() < 1e-6);
}

#[test]
fn test_cholesky_decomposition_3x3() {
    let corr = vec![
        vec![1.0, 0.8, 0.5],
        vec![0.8, 1.0, 0.4],
        vec![0.5, 0.4, 1.0],
    ];
    let l = cholesky(&corr).unwrap();

    // Reconstruct and verify
    for i in 0..3 {
        for j in 0..3 {
            let mut sum = 0.0;
            for k in 0..3 {
                sum += l[i][k] * l[j][k];
            }
            assert!((sum - corr[i][j]).abs() < 1e-5);
        }
    }
}

#[test]
fn test_multi_asset_market_sim_flow() {
    let risk_config = RiskConfig {
        max_order_qty: 1000.0,
        max_order_notional: 10_000_000.0,
        max_position_notional: 20_000_000.0,
        price_collar_pct: 0.20,
        max_drawdown_pct: 0.30,
        max_orders_per_sec: 100_000,
        require_margin: false,
    };

    let engine = Arc::new(ExecutionEngine::new(1_000_000.0, 1.0, risk_config));

    let assets = vec![
        AssetConfig::new("BTC-USDT", 50_000.0, 0.05, 0.40, 0.50, 0.001),
        AssetConfig::new("ETH-USDT", 3_000.0, 0.05, 0.50, 0.10, 0.01),
    ];

    let corr = vec![vec![1.0, 0.85], vec![0.85, 1.0]];

    let mut sim = MultiAssetMarketSim::new(engine.clone(), assets, corr, 12345).unwrap();

    // Step the simulation 5 times
    for _ in 0..5 {
        sim.step(1.0);
    }

    assert_eq!(sim.step_count(), 5);

    // Verify order books are seeded with active quotes
    let btc_depth = engine.get_depth("BTC-USDT", 5).unwrap();
    let eth_depth = engine.get_depth("ETH-USDT", 5).unwrap();

    assert!(btc_depth.best_bid().is_some());
    assert!(btc_depth.best_ask().is_some());
    assert!(btc_depth.best_bid().unwrap() < btc_depth.best_ask().unwrap());

    assert!(eth_depth.best_bid().is_some());
    assert!(eth_depth.best_ask().is_some());
    assert!(eth_depth.best_bid().unwrap() < eth_depth.best_ask().unwrap());

    let prices = sim.get_prices();
    assert!(prices.contains_key("BTC-USDT"));
    assert!(prices.contains_key("ETH-USDT"));
}
