"""Correlated Multi-Asset Market Simulation & Statistical Arbitrage (Pairs Trading)

Demonstrates:
1. Setting up a 3-asset correlated universe (BTC, ETH, SOL) using Cholesky GBM in Rust
2. Simulating market maker ladders and noise flow
3. Running a statistical arbitrage (ETH/BTC ratio mean-reversion) strategy
4. Tracking real-time portfolio equity, positions, and risk metrics
"""

from trading_engine import AssetConfig, Engine, MultiAssetMarketSim, RiskConfig


def run_stat_arb_simulation():
    print("=" * 70)
    print("Correlated Multi-Asset Market Simulation & Statistical Arbitrage")
    print("=" * 70)

    # 1. Initialize Engine
    risk = RiskConfig(
        max_order_qty=100.0,
        max_order_notional=1_000_000.0,
        max_position_notional=2_000_000.0,
        price_collar_pct=0.15,
        max_drawdown_pct=0.20,
        max_orders_per_sec=10_000,
        require_margin=False,
    )

    engine = Engine(initial_balance=500_000.0, leverage=2.0, risk_config=risk)

    # 2. Configure 3 Correlated Assets
    btc = AssetConfig(
        symbol="BTC-USDT",
        initial_price=60_000.0,
        drift=0.03,
        volatility=0.45,
        tick_size=0.50,
        lot_size=0.001,
        quote_levels=3,
        base_spread_bps=4.0,
        arrival_rate=8.0,
        avg_order_qty=0.5,
    )

    eth = AssetConfig(
        symbol="ETH-USDT",
        initial_price=3_000.0,
        drift=0.03,
        volatility=0.55,
        tick_size=0.10,
        lot_size=0.01,
        quote_levels=3,
        base_spread_bps=5.0,
        arrival_rate=12.0,
        avg_order_qty=5.0,
    )

    sol = AssetConfig(
        symbol="SOL-USDT",
        initial_price=150.0,
        drift=0.05,
        volatility=0.70,
        tick_size=0.05,
        lot_size=0.1,
        quote_levels=3,
        base_spread_bps=8.0,
        arrival_rate=15.0,
        avg_order_qty=20.0,
    )

    # 3x3 Correlation Matrix: high correlation between BTC, ETH, and SOL
    correlation_matrix = [
        [1.0, 0.85, 0.72],
        [0.85, 1.0, 0.78],
        [0.72, 0.78, 1.0],
    ]

    sim = MultiAssetMarketSim(engine, [btc, eth, sol], correlation_matrix, seed=777)
    print("Multi-Asset Market Simulator initialized with BTC-USDT, ETH-USDT, and SOL-USDT.")
    print("Correlation(BTC, ETH) = 0.85 | Correlation(ETH, SOL) = 0.78\n")

    # 3. Warm up the simulator to seed initial liquidity
    print("Seeding initial order books...")
    sim.run_steps(count=10, dt=1.0)

    # Initial prices
    prices = sim.get_prices()
    for sym, p in sorted(prices.items()):
        depth = engine.get_depth(sym, levels=1)
        best_bid = depth.best_bid() or 0.0
        best_ask = depth.best_ask() or 0.0
        print(f"  {sym:<8}: Mid=${p:,.2f} | BestBid=${best_bid:,.2f} | BestAsk=${best_ask:,.2f}")

    # 4. Statistical Arbitrage Loop on ETH/BTC price ratio
    print("\n--- Running Statistical Arbitrage Strategy (ETH / BTC Pairs Trading) ---")
    ratio_history = []
    target_ratio = 3_000.0 / 60_000.0  # ~ 0.050

    for step in range(1, 16):
        # Advance simulation by 1 second
        sim.step(dt=1.0)

        btc_depth = engine.get_depth("BTC-USDT", levels=1)
        eth_depth = engine.get_depth("ETH-USDT", levels=1)

        btc_mid = btc_depth.mid_price() or prices["BTC-USDT"]
        eth_mid = eth_depth.mid_price() or prices["ETH-USDT"]

        current_ratio = eth_mid / btc_mid
        ratio_history.append(current_ratio)

        # Deviation from theoretical target ratio
        deviation_pct = (current_ratio - target_ratio) / target_ratio * 100.0

        eth_pos = engine.get_position("ETH-USDT")
        eth_qty = eth_pos.quantity if eth_pos else 0.0

        action = "HOLD"
        # If ETH is relatively underpriced, buy ETH and sell equivalent BTC
        if deviation_pct < -0.005 and eth_qty <= 0.0:
            action = "LONG ETH / SHORT BTC (Arbitrage Entry)"
            engine.submit_order("ETH-USDT", "BUY", "MARKET", 0.0, 5.0, "IOC")
            engine.submit_order("BTC-USDT", "SELL", "MARKET", 0.0, 0.25, "IOC")
        elif deviation_pct > 0.015 and eth_qty >= 0.0:
            action = "SHORT ETH / LONG BTC (Arbitrage Entry)"
            engine.submit_order("ETH-USDT", "SELL", "MARKET", 0.0, 5.0, "IOC")
            engine.submit_order("BTC-USDT", "BUY", "MARKET", 0.0, 0.25, "IOC")
        elif abs(deviation_pct) < 0.005 and abs(eth_qty) > 0.0:
            action = "CLOSE ARB (Ratio Reverted)"
            if eth_qty > 0.0:
                engine.submit_order("ETH-USDT", "SELL", "MARKET", 0.0, abs(eth_qty), "IOC")
                engine.submit_order("BTC-USDT", "BUY", "MARKET", 0.0, 0.25, "IOC")
            else:
                engine.submit_order("ETH-USDT", "BUY", "MARKET", 0.0, abs(eth_qty), "IOC")
                engine.submit_order("BTC-USDT", "SELL", "MARKET", 0.0, 0.25, "IOC")

        print(
            f"[Step {step:02d}] BTC: ${btc_mid:>8.2f} | ETH: ${eth_mid:>7.2f} | "
            f"Ratio: {current_ratio:.5f} (dev: {deviation_pct:>+5.2f}%) -> {action}"
        )

    # 5. Final Portfolio Summary
    acct = engine.get_account("DEFAULT")
    positions = engine.get_positions("DEFAULT")
    print("\n" + "=" * 70)
    print("Simulation Complete - Strategy Account Summary (Isolated):")
    print(
        f"Strategy Cash Balance: ${acct.cash_balance:,.2f} | "
        f"Realized PnL: ${acct.realized_pnl:>+8.2f}"
    )
    if positions:
        for pos in positions:
            print(
                f"  {pos.symbol:<8}: Qty={pos.quantity:>6.2f} | "
                f"Realized PnL=${pos.realized_pnl:>+8.2f} | "
                f"Unrealized PnL=${pos.unrealized_pnl:>+8.2f}"
            )
    else:
        print("  (All arbitrage positions closed flat)")

    print("\nSimulator Internal Accounts (Isolated):")
    for acct_id in sorted(engine.get_all_account_ids()):
        if acct_id != "DEFAULT":
            sim_acct = engine.get_account(acct_id)
            print(
                f"  Account [{acct_id:<9}]: Cash=${sim_acct.cash_balance:,.2f} | "
                f"Realized PnL=${sim_acct.realized_pnl:>+8.2f}"
            )
    print("=" * 70)


if __name__ == "__main__":
    run_stat_arb_simulation()
