"""Correlated Multi-Asset Market Simulation & Statistical Arbitrage (Pairs Trading)

Demonstrates:
1. Setting up a 3-asset correlated universe (BTC, ETH, SOL) using Cholesky GBM in Rust
2. Simulating market maker quote ladders and noise trader flow
3. Running a professional statistical arbitrage (ETH / BTC Pairs Trading) strategy:
   - Dynamic rolling mean & standard deviation (Z-score)
   - Cost-aware execution filter (only trade when expected alpha > round-trip spread friction)
   - Market-neutral dollar hedging (dollar-weighted sizing for both legs)
   - Mean-reversion exit & blowout stop-loss protection
4. Tracking isolated multi-account positions, cash balance, and PnL
"""

import math
from collections import deque

from trading_engine import AssetConfig, Engine, MultiAssetMarketSim, RiskConfig


def flatten_positions(engine: Engine) -> None:
    """Closes all strategy positions flat with market orders."""
    eth_pos = engine.get_position("ETH-USDT")
    btc_pos = engine.get_position("BTC-USDT")
    eth_qty = eth_pos.quantity if eth_pos else 0.0
    btc_qty = btc_pos.quantity if btc_pos else 0.0

    if abs(eth_qty) >= 0.01:
        side = "SELL" if eth_qty > 0 else "BUY"
        engine.submit_order("ETH-USDT", side, "MARKET", 0.0, round(abs(eth_qty), 2), "IOC")

    if abs(btc_qty) >= 0.001:
        side = "SELL" if btc_qty > 0 else "BUY"
        engine.submit_order("BTC-USDT", side, "MARKET", 0.0, round(abs(btc_qty), 3), "IOC")


def run_stat_arb_simulation():
    print("=" * 70)
    print("Correlated Multi-Asset Market Simulation & Statistical Arbitrage")
    print("=" * 70)

    # 1. Initialize Engine with risk parameters suitable for high-frequency simulation
    risk = RiskConfig(
        max_order_qty=100.0,
        max_order_notional=1_000_000.0,
        max_position_notional=2_000_000.0,
        price_collar_pct=0.15,
        max_drawdown_pct=0.20,
        max_orders_per_sec=100_000,  # High limit to accommodate fast-forward backtest loops
        require_margin=False,
    )

    engine = Engine(initial_balance=500_000.0, leverage=2.0, risk_config=risk)

    # 2. Configure 3 Correlated Assets
    # Microstructure flow causes transient order book dislocations
    btc = AssetConfig(
        symbol="BTC-USDT",
        initial_price=60_000.0,
        drift=0.01,
        volatility=0.40,
        tick_size=0.50,
        lot_size=0.001,
        quote_levels=3,
        base_spread_bps=1.0,  # Realistic tier-1 exchange spread (0.01%)
        arrival_rate=15.0,
        avg_order_qty=0.5,
    )

    eth = AssetConfig(
        symbol="ETH-USDT",
        initial_price=3_000.0,
        drift=0.01,
        volatility=0.50,
        tick_size=0.10,
        lot_size=0.01,
        quote_levels=3,
        base_spread_bps=1.2,  # Realistic tier-1 exchange spread (0.012%)
        arrival_rate=20.0,
        avg_order_qty=5.0,
    )

    sol = AssetConfig(
        symbol="SOL-USDT",
        initial_price=150.0,
        drift=0.02,
        volatility=0.65,
        tick_size=0.05,
        lot_size=0.1,
        quote_levels=3,
        base_spread_bps=3.0,
        arrival_rate=20.0,
        avg_order_qty=20.0,
    )

    # 3x3 Correlation Matrix: Strong correlation between BTC, ETH, and SOL
    correlation_matrix = [
        [1.00, 0.88, 0.75],
        [0.88, 1.00, 0.80],
        [0.75, 0.80, 1.00],
    ]

    sim = MultiAssetMarketSim(engine, [btc, eth, sol], correlation_matrix, seed=777)
    print("Multi-Asset Market Simulator initialized with BTC-USDT, ETH-USDT, and SOL-USDT.")
    print("Correlation(BTC, ETH) = 0.88 | Correlation(ETH, SOL) = 0.80\n")

    # 3. Warm up the simulator to seed initial liquidity
    print("Seeding initial order books...")
    sim.run_steps(count=10, dt=1.0)

    prices = sim.get_prices()
    for sym, p in sorted(prices.items()):
        depth = engine.get_depth(sym, levels=1)
        best_bid = depth.best_bid() or 0.0
        best_ask = depth.best_ask() or 0.0
        print(f"  {sym:<8}: Mid=${p:,.2f} | BestBid=${best_bid:,.2f} | BestAsk=${best_ask:,.2f}")

    # 4. Statistical Arbitrage Parameters
    # Calibrated for high-frequency pairs trading with realistic tier-1 spreads:
    WINDOW_SIZE = 25
    Z_ENTRY = 1.20  # Enter when ratio diverges by >= 1.20 std devs
    Z_EXIT = 0.20  # Exit when ratio reverts to within 0.20 std devs of mean
    Z_STOP = 3.50  # Stop loss if correlation breaks down
    MIN_DEV_PCT = 0.012  # Minimum deviation % required to trigger entry (1.2 bps)
    TARGET_NOTIONAL = 30_000.0  # $30,000 per leg for dollar-neutral exposure

    ratio_window = deque(maxlen=WINDOW_SIZE)
    in_trade = False
    trade_side = None  # "LONG_ETH" or "SHORT_ETH"

    print("\n--- Running Statistical Arbitrage Strategy (ETH / BTC Pairs Trading) ---")
    print(
        f"Strategy: Rolling Z-Score (Window={WINDOW_SIZE}), "
        f"Z_Entry={Z_ENTRY}, MinDev={MIN_DEV_PCT}%\n"
    )

    TOTAL_STEPS = 800
    for step in range(1, TOTAL_STEPS + 1):
        # Advance market simulation by 1 second
        sim.step(dt=1.0)

        btc_depth = engine.get_depth("BTC-USDT", levels=1)
        eth_depth = engine.get_depth("ETH-USDT", levels=1)

        btc_mid = btc_depth.mid_price() or 60_000.0
        eth_mid = eth_depth.mid_price() or 3_000.0

        current_ratio = eth_mid / btc_mid
        ratio_window.append(current_ratio)

        # Skip trading during initial rolling window warm-up
        if len(ratio_window) < WINDOW_SIZE:
            print(
                f"[Step {step:02d}] Warming up ratio window ({len(ratio_window)}/{WINDOW_SIZE})..."
            )
            continue

        mean_ratio = sum(ratio_window) / len(ratio_window)
        variance = sum((r - mean_ratio) ** 2 for r in ratio_window) / (len(ratio_window) - 1)
        std_ratio = math.sqrt(variance) if variance > 1e-12 else 1e-6
        z_score = (current_ratio - mean_ratio) / std_ratio
        dev_pct = (current_ratio - mean_ratio) / mean_ratio * 100.0

        # Calculate exact dollar-neutral order sizes
        eth_qty = round((TARGET_NOTIONAL / eth_mid) / 0.01) * 0.01
        btc_qty = round((TARGET_NOTIONAL / btc_mid) / 0.001) * 0.001

        action = "HOLD"

        if not in_trade:
            # Check entry condition: Z-score breached AND deviation exceeds spread friction
            if z_score <= -Z_ENTRY and abs(dev_pct) >= MIN_DEV_PCT:
                # ETH is undervalued relative to BTC -> Long ETH / Short BTC
                action = f"ENTER LONG ETH ({eth_qty:.2f}) / SHORT BTC ({btc_qty:.3f})"
                engine.submit_order("ETH-USDT", "BUY", "MARKET", 0.0, eth_qty, "IOC")
                engine.submit_order("BTC-USDT", "SELL", "MARKET", 0.0, btc_qty, "IOC")
                in_trade = True
                trade_side = "LONG_ETH"

            elif z_score >= Z_ENTRY and abs(dev_pct) >= MIN_DEV_PCT:
                # ETH is overvalued relative to BTC -> Short ETH / Long BTC
                action = f"ENTER SHORT ETH ({eth_qty:.2f}) / LONG BTC ({btc_qty:.3f})"
                engine.submit_order("ETH-USDT", "SELL", "MARKET", 0.0, eth_qty, "IOC")
                engine.submit_order("BTC-USDT", "BUY", "MARKET", 0.0, btc_qty, "IOC")
                in_trade = True
                trade_side = "SHORT_ETH"

        else:
            # Check exit conditions for open trade
            should_exit = False
            exit_reason = ""

            # 1. Mean Reversion Exit: Z-score reverted toward 0
            if (trade_side == "LONG_ETH" and z_score >= -Z_EXIT) or (
                trade_side == "SHORT_ETH" and z_score <= Z_EXIT
            ):
                should_exit = True
                exit_reason = "PROFIT EXIT (Reverted to Mean)"

            # 2. Stop-Loss Exit: Extreme dislocation beyond tolerance
            elif abs(z_score) >= Z_STOP:
                should_exit = True
                exit_reason = "STOP LOSS (Divergence Blowout)"

            if should_exit:
                flatten_positions(engine)
                action = exit_reason
                in_trade = False
                trade_side = None

        print(
            f"[Step {step:02d}] BTC: ${btc_mid:>8.2f} | ETH: ${eth_mid:>7.2f} | "
            f"Ratio: {current_ratio:.5f} | Z: {z_score:>+5.2f} (dev: {dev_pct:>+5.2f}%) -> {action}"
        )

    # 5. Flatten any remaining inventory at end of simulation for clean accounting
    if in_trade:
        print("\n[End of Sim] Flattening open arbitrage positions...")
        flatten_positions(engine)

    # 6. Final Portfolio Summary
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
        print("  (All arbitrage positions closed flat - 0 residual inventory)")

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
