import pytest

try:
    from trading_engine import AssetConfig, Engine, MultiAssetMarketSim, RiskConfig
except ImportError:
    MultiAssetMarketSim = None


@pytest.fixture
def sim_setup():
    if MultiAssetMarketSim is None:
        pytest.skip("trading_engine extension is not built")

    risk = RiskConfig(
        max_order_qty=1000.0,
        max_order_notional=10_000_000.0,
        max_position_notional=20_000_000.0,
        price_collar_pct=0.25,
        max_drawdown_pct=0.30,
        max_orders_per_sec=100_000,
        require_margin=False,
    )

    engine = Engine(initial_balance=500_000.0, leverage=2.0, risk_config=risk)

    btc = AssetConfig("BTC-USDT", initial_price=60_000.0, drift=0.05, volatility=0.40)
    eth = AssetConfig("ETH-USDT", initial_price=3_200.0, drift=0.05, volatility=0.50)
    sol = AssetConfig("SOL-USDT", initial_price=150.0, drift=0.08, volatility=0.65)

    corr = [
        [1.0, 0.85, 0.70],
        [0.85, 1.0, 0.75],
        [0.70, 0.75, 1.0],
    ]

    sim = MultiAssetMarketSim(engine, [btc, eth, sol], corr, seed=123)
    return engine, sim


def test_multi_asset_sim_stepping(sim_setup):
    engine, sim = sim_setup

    prices = sim.step(dt=1.0)
    assert len(prices) == 3
    assert "BTC-USDT" in prices
    assert "ETH-USDT" in prices
    assert "SOL-USDT" in prices

    sim.run_steps(count=5, dt=1.0)
    assert sim.step_count() == 6

    # Verify order books are active and populated
    for sym in ["BTC-USDT", "ETH-USDT", "SOL-USDT"]:
        depth = engine.get_depth(sym, levels=3)
        assert depth is not None
        assert depth.best_bid() is not None
        assert depth.best_ask() is not None
        assert depth.best_bid() < depth.best_ask()
        assert depth.spread() > 0.0


def test_strategy_trading_against_simulated_market(sim_setup):
    engine, sim = sim_setup

    # Run initial steps to create market liquidity
    sim.run_steps(count=3, dt=1.0)

    initial_pos = engine.get_position("BTC-USDT")
    initial_qty = initial_pos.quantity if initial_pos else 0.0

    # Strategy buys 0.1 BTC using a market order
    order = engine.submit_order(
        symbol="BTC-USDT",
        side="BUY",
        order_type="MARKET",
        price=0.0,
        quantity=0.1,
        time_in_force="IOC",
    )
    assert order.filled_quantity == 0.1

    pos = engine.get_position("BTC-USDT")
    assert pos is not None
    assert pos.quantity == pytest.approx(initial_qty + 0.1)
