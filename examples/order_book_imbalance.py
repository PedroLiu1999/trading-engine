"""Order Book Imbalance (OBI) & Micro-Price Scalping Strategy

Demonstrates:
1. Multi-level Weighted Order Book Imbalance (WOBI) across L2 depth queues
2. Stoikov Micro-Price calculation and deviation from mid-price
3. High-frequency tick scalping with rapid inventory turnover
4. Dynamic position sizing, take-profit, stop-loss, and queue-depletion exits
5. Multi-account isolation tracking strategy alpha against internal simulator flow
"""

from collections import deque

from trading_engine import (
    AssetConfig,
    Engine,
    MarketDepth,
    MultiAssetMarketSim,
    RiskConfig,
)


def compute_order_book_metrics(depth: MarketDepth, max_levels: int = 4) -> dict[str, float]:
    """Calculates top-of-book and multi-level weighted order book imbalance metrics."""
    bids = depth.bids[:max_levels]
    asks = depth.asks[:max_levels]

    if not bids or not asks:
        return {
            "best_bid": 0.0,
            "best_ask": 0.0,
            "mid_price": 0.0,
            "spread": 0.0,
            "top_obi": 0.0,
            "wobi": 0.0,
            "micro_price": 0.0,
            "micro_dev_bps": 0.0,
        }

    best_bid = bids[0].price
    best_ask = asks[0].price
    mid_price = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid

    # 1. Top-of-Book Simple Imbalance
    top_bid_qty = bids[0].quantity
    top_ask_qty = asks[0].quantity
    top_vol_sum = top_bid_qty + top_ask_qty
    top_obi = (top_bid_qty - top_ask_qty) / top_vol_sum if top_vol_sum > 0 else 0.0

    # 2. Multi-Level Weighted Order Book Imbalance (decaying weights: 1.0, 0.5, 0.33, 0.25)
    weighted_bid_vol = sum(b.quantity / (idx + 1.0) for idx, b in enumerate(bids))
    weighted_ask_vol = sum(a.quantity / (idx + 1.0) for idx, a in enumerate(asks))
    total_weighted_vol = weighted_bid_vol + weighted_ask_vol
    wobi = (
        (weighted_bid_vol - weighted_ask_vol) / total_weighted_vol
        if total_weighted_vol > 0
        else 0.0
    )

    # 3. Stoikov Micro-Price
    # P_micro = (V_bid * P_ask + V_ask * P_bid) / (V_bid + V_ask)
    micro_price = (
        (top_bid_qty * best_ask + top_ask_qty * best_bid) / top_vol_sum
        if top_vol_sum > 0
        else mid_price
    )
    micro_dev_bps = ((micro_price - mid_price) / mid_price) * 10_000.0 if mid_price > 0 else 0.0

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid_price,
        "spread": spread,
        "top_obi": top_obi,
        "wobi": wobi,
        "micro_price": micro_price,
        "micro_dev_bps": micro_dev_bps,
    }


def run_obi_strategy():
    print("=" * 75)
    print("Order Book Imbalance (OBI) & Micro-Price Scalping Strategy")
    print("=" * 75)

    STRATEGY_ACCOUNT = "OBI_SCALPER"

    # 1. Initialize Engine with risk parameters suitable for high-frequency scalping
    risk = RiskConfig(
        max_order_qty=50.0,
        max_order_notional=500_000.0,
        max_position_notional=1_000_000.0,
        price_collar_pct=0.10,
        max_drawdown_pct=0.15,
        max_orders_per_sec=100_000,
        require_margin=False,
    )

    engine = Engine(initial_balance=250_000.0, leverage=2.0, risk_config=risk)

    # 2. Configure Liquid Market with Active Noise Flow
    # Realistic tight spread allows OBI scalpers to capture price jumps
    eth = AssetConfig(
        symbol="ETH-USDT",
        initial_price=3_000.0,
        drift=0.00,
        volatility=0.40,
        tick_size=0.10,
        lot_size=0.01,
        quote_levels=5,
        base_spread_bps=0.8,  # ~ $0.24 tight spread
        arrival_rate=30.0,  # Active noise order flow hitting book
        avg_order_qty=4.0,
    )

    sim = MultiAssetMarketSim(engine, [eth], [[1.0]], seed=101)

    print("Seeding initial order book depth...")
    sim.run_steps(count=15, dt=0.5)

    initial_depth = engine.get_depth("ETH-USDT", levels=4)
    print(
        f"Initial Order Book: BestBid=${initial_depth.best_bid():,.2f} | "
        f"BestAsk=${initial_depth.best_ask():,.2f} | "
        f"Spread=${initial_depth.spread():,.2f}"
    )

    # 3. Strategy Configuration Parameters
    WOBI_ENTRY_THRESH = 0.22  # Strong directional volume imbalance
    WOBI_EXIT_THRESH = 0.04  # Imbalance normalized
    MAX_HOLD_STEPS = 12  # Holding window for momentum follow-through
    ORDER_QTY = 3.0  # 3.0 ETH per trade (~ $9,000 notional)
    TAKE_PROFIT_PTS = 0.80  # Take profit (+8 ticks)
    STOP_LOSS_PTS = 1.00  # Stop loss (-10 ticks)

    # Strategy State Tracking
    in_position = False
    pos_side = None  # "LONG" or "SHORT"
    entry_step = 0
    trades_executed = 0
    profitable_trades = 0
    recent_wobi = deque(maxlen=5)

    print("\n--- Running High-Frequency Order Book Imbalance Scalper ---")
    print(
        f"Signal Parameters: Entry WOBI >= ±{WOBI_ENTRY_THRESH:.2f} | "
        f"Exit WOBI <= ±{WOBI_EXIT_THRESH:.2f} | Max Hold = {MAX_HOLD_STEPS} steps\n"
    )

    TOTAL_SIM_STEPS = 180
    for step in range(1, TOTAL_SIM_STEPS + 1):
        # Advance simulation by 500ms
        sim.step(dt=0.5)

        depth = engine.get_depth("ETH-USDT", levels=4)
        metrics = compute_order_book_metrics(depth, max_levels=4)
        wobi = metrics["wobi"]
        mid = metrics["mid_price"]
        recent_wobi.append(wobi)

        action = "HOLD"

        # Check Position Status
        pos = engine.get_position("ETH-USDT", account_id=STRATEGY_ACCOUNT)
        curr_qty = pos.quantity if pos else 0.0
        avg_entry = pos.avg_entry_price if pos else 0.0

        if not in_position:
            # Entry Logic: High buying pressure vs selling pressure
            if wobi >= WOBI_ENTRY_THRESH:
                # Heavy bid support -> Go LONG
                order = engine.submit_order(
                    symbol="ETH-USDT",
                    side="BUY",
                    order_type="MARKET",
                    price=0.0,
                    quantity=ORDER_QTY,
                    time_in_force="IOC",
                    account_id=STRATEGY_ACCOUNT,
                )
                if order.filled_quantity > 0:
                    in_position = True
                    pos_side = "LONG"
                    entry_step = step
                    trades_executed += 1
                    action = (
                        f"BUY {ORDER_QTY:.1f} ETH "
                        f"(WOBI: {wobi:+.2f}, Dev: {metrics['micro_dev_bps']:+.1f}bps)"
                    )

            elif wobi <= -WOBI_ENTRY_THRESH:
                # Heavy ask pressure -> Go SHORT
                order = engine.submit_order(
                    symbol="ETH-USDT",
                    side="SELL",
                    order_type="MARKET",
                    price=0.0,
                    quantity=ORDER_QTY,
                    time_in_force="IOC",
                    account_id=STRATEGY_ACCOUNT,
                )
                if order.filled_quantity > 0:
                    in_position = True
                    pos_side = "SHORT"
                    entry_step = step
                    trades_executed += 1
                    action = (
                        f"SELL {ORDER_QTY:.1f} ETH "
                        f"(WOBI: {wobi:+.2f}, Dev: {metrics['micro_dev_bps']:+.1f}bps)"
                    )

        else:
            # Exit Logic for Open Scalp based on true liquidation price
            hold_duration = step - entry_step
            if pos_side == "LONG":
                pnl_pts = metrics["best_bid"] - avg_entry
            else:
                pnl_pts = avg_entry - metrics["best_ask"]

            should_close = False
            close_reason = ""

            # Condition A: Take-Profit reached
            if pnl_pts >= TAKE_PROFIT_PTS:
                should_close = True
                close_reason = f"TP HIT (+${pnl_pts:.2f})"
            # Condition B: Stop-Loss reached
            elif pnl_pts <= -STOP_LOSS_PTS:
                should_close = True
                close_reason = f"SL HIT (-${abs(pnl_pts):.2f})"
            # Condition C: Imbalance dissipates / normalizes
            elif (pos_side == "LONG" and wobi <= WOBI_EXIT_THRESH) or (
                pos_side == "SHORT" and wobi >= -WOBI_EXIT_THRESH
            ):
                should_close = True
                close_reason = f"WOBI NORMALIZED ({wobi:+.2f})"
            # Condition D: Max holding period elapsed
            elif hold_duration >= MAX_HOLD_STEPS:
                should_close = True
                close_reason = f"TIMEOUT ({hold_duration} steps)"

            if should_close and abs(curr_qty) > 0:
                close_side = "SELL" if pos_side == "LONG" else "BUY"
                engine.submit_order(
                    symbol="ETH-USDT",
                    side=close_side,
                    order_type="MARKET",
                    price=0.0,
                    quantity=abs(curr_qty),
                    time_in_force="IOC",
                    account_id=STRATEGY_ACCOUNT,
                )
                if pnl_pts > 0:
                    profitable_trades += 1
                pnl_dollars = pnl_pts * ORDER_QTY
                action = (
                    f"CLOSE {pos_side} @ ${mid:.2f} [{close_reason}] (Pnl: ${pnl_dollars:>+6.2f})"
                )
                in_position = False
                pos_side = None

        if step % 2 == 0 or action != "HOLD":
            dev_bps = metrics["micro_dev_bps"]
            print(
                f"[Step {step:03d}] Mid: ${mid:>7.2f} | WOBI: {wobi:>+5.2f} | "
                f"Dev: {dev_bps:>+5.1f}bps | Pos: {curr_qty:>+4.1f} -> {action}"
            )

    # 4. Final Position Clean-Up (Ensure 0 residual overnight inventory)
    final_pos = engine.get_position("ETH-USDT", account_id=STRATEGY_ACCOUNT)
    if final_pos and abs(final_pos.quantity) >= 0.01:
        close_side = "SELL" if final_pos.quantity > 0 else "BUY"
        engine.submit_order(
            symbol="ETH-USDT",
            side=close_side,
            order_type="MARKET",
            price=0.0,
            quantity=abs(final_pos.quantity),
            time_in_force="IOC",
            account_id=STRATEGY_ACCOUNT,
        )

    # 5. Performance Report
    strat_acct = engine.get_account(STRATEGY_ACCOUNT)
    realized_pnl = strat_acct.realized_pnl if strat_acct else 0.0
    cash_balance = strat_acct.cash_balance if strat_acct else 250_000.0
    win_rate = (profitable_trades / trades_executed * 100.0) if trades_executed > 0 else 0.0

    print("\n" + "=" * 75)
    print("Order Book Imbalance Strategy Performance Summary (Isolated):")
    print(f"  Account ID              : {STRATEGY_ACCOUNT}")
    print(f"  Total Trades Executed   : {trades_executed}")
    print(f"  Win Rate                : {win_rate:.1f}% ({profitable_trades}/{trades_executed})")
    print(f"  Realized PnL            : ${realized_pnl:>+10.2f}")
    print(f"  Final Cash Balance      : ${cash_balance:,.2f}")

    print("\nSimulator Internal Accounts:")
    for acct_id in sorted(engine.get_all_account_ids()):
        if acct_id != STRATEGY_ACCOUNT:
            acct = engine.get_account(acct_id)
            print(
                f"  Account [{acct_id:<12}]: Cash=${acct.cash_balance:,.2f} | "
                f"Realized PnL=${acct.realized_pnl:>+10.2f}"
            )
    print("=" * 75)


if __name__ == "__main__":
    run_obi_strategy()
