"""Order Book Imbalance (OBI) Passive Queue Scalping Strategy with Baseline.

Demonstrates:
1. Multi-level Weighted Order Book Imbalance (WOBI) across L2 depth queues
2. Passive queue joining with LIMIT orders (earning the spread instead of paying it)
3. Bid/ask queue shielding: posting resting limit orders on the side supported by volume
4. Opportunistic spread capture when noise traders cross our resting orders
5. Random-entry, same-exit baseline comparison
"""

import random

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


def run_single_simulation(
    mode: str = "OBI",
    sim_seed: int = 101,
    total_steps: int = 800,
    verbose: bool = True,
) -> tuple[int, float]:
    """Runs a single simulation pass for either OBI strategy or Random baseline."""
    is_random = mode.upper() == "RANDOM"
    strat_account = "RANDOM_BASELINE" if is_random else "OBI_SCALPER"

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
    eth = AssetConfig(
        symbol="ETH-USDT",
        initial_price=3_000.0,
        drift=0.00,
        volatility=0.40,
        tick_size=0.10,
        lot_size=0.01,
        quote_levels=5,
        base_spread_bps=1.0,  # ~ $0.30 spread
        arrival_rate=25.0,  # Active noise flow crossing book
        avg_order_qty=4.0,
    )

    sim = MultiAssetMarketSim(engine, [eth], [[1.0]], seed=sim_seed)

    if verbose:
        print("Seeding initial order book depth...")
    sim.run_steps(count=15, dt=0.5)

    if verbose:
        initial_depth = engine.get_depth("ETH-USDT", levels=4)
        print(
            f"Initial Order Book: BestBid=${initial_depth.best_bid():,.2f} | "
            f"BestAsk=${initial_depth.best_ask():,.2f} | "
            f"Spread=${initial_depth.spread():,.2f}"
        )

    # 3. Strategy Configuration Parameters
    WOBI_ENTRY_THRESH = 0.15  # Enter queue when volume imbalance exceeds 15%
    ORDER_QTY = 3.0  # 3.0 ETH per quote
    MAX_HOLD_STEPS = 10  # Maximum steps before emergency flatten
    STOP_LOSS_PTS = 1.00  # Stop loss in dollars

    # Strategy State Tracking
    resting_order_id = None
    entry_step = 0
    trades_executed = 0
    prev_qty = 0.0

    # Random generator for baseline entry
    rng = random.Random(sim_seed + 777)
    random_desired_side = None

    peak_equity = 250_000.0
    max_drawdown = 0.0

    if verbose:
        print("\n--- Running Passive Order Book Imbalance Scalper ---")
        print(
            f"Signal Parameters: Entry WOBI >= ±{WOBI_ENTRY_THRESH:.2f} | "
            f"Passive Limit Quoting | Max Hold = {MAX_HOLD_STEPS} steps\n"
        )

    for step in range(1, total_steps + 1):
        # Advance simulation by 500ms
        sim.step(dt=0.5)

        depth = engine.get_depth("ETH-USDT", levels=4)
        metrics = compute_order_book_metrics(depth, max_levels=4)
        wobi = metrics["wobi"]
        mid = metrics["mid_price"]

        pos = engine.get_position("ETH-USDT", account_id=strat_account)
        curr_qty = pos.quantity if pos else 0.0
        avg_entry = pos.avg_entry_price if pos else 0.0

        acct = engine.get_account(strat_account)
        current_cash = acct.cash_balance if acct else 250_000.0
        unrealized = pos.unrealized_pnl if pos else 0.0
        equity = current_cash + unrealized
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)

        action = "HOLD"

        # Detect new position fill from previous resting order
        if abs(curr_qty) > 0.0 and abs(prev_qty) < 0.01:
            trades_executed += 1
            entry_step = step
            resting_order_id = None
            random_desired_side = None

        # Case 1: We are flat (no inventory)
        if abs(curr_qty) < 0.01:
            # Cancel old resting entry quote if price/signal changed
            if resting_order_id is not None:
                try:
                    engine.cancel_order("ETH-USDT", resting_order_id)
                except Exception:
                    pass
                resting_order_id = None

            if not is_random:
                # OBI Signal Entry
                if wobi >= WOBI_ENTRY_THRESH:
                    order = engine.submit_order(
                        symbol="ETH-USDT",
                        side="BUY",
                        order_type="LIMIT",
                        price=metrics["best_bid"],
                        quantity=ORDER_QTY,
                        time_in_force="GTC",
                        account_id=strat_account,
                    )
                    resting_order_id = order.id
                    action = (
                        f"JOIN BID QUEUE: Limit Buy {ORDER_QTY:.1f} @ ${metrics['best_bid']:.2f} "
                        f"(WOBI: {wobi:+.2f})"
                    )

                elif wobi <= -WOBI_ENTRY_THRESH:
                    order = engine.submit_order(
                        symbol="ETH-USDT",
                        side="SELL",
                        order_type="LIMIT",
                        price=metrics["best_ask"],
                        quantity=ORDER_QTY,
                        time_in_force="GTC",
                        account_id=strat_account,
                    )
                    resting_order_id = order.id
                    action = (
                        f"JOIN ASK QUEUE: Limit Sell {ORDER_QTY:.1f} @ ${metrics['best_ask']:.2f} "
                        f"(WOBI: {wobi:+.2f})"
                    )
            else:
                # Random Entry
                if random_desired_side is None:
                    if rng.random() < 0.40:
                        random_desired_side = "BUY" if rng.random() < 0.50 else "SELL"

                if random_desired_side == "BUY":
                    order = engine.submit_order(
                        symbol="ETH-USDT",
                        side="BUY",
                        order_type="LIMIT",
                        price=metrics["best_bid"],
                        quantity=ORDER_QTY,
                        time_in_force="GTC",
                        account_id=strat_account,
                    )
                    resting_order_id = order.id
                    action = (
                        f"RANDOM BID QUEUE: Limit Buy {ORDER_QTY:.1f} @ ${metrics['best_bid']:.2f}"
                    )
                elif random_desired_side == "SELL":
                    order = engine.submit_order(
                        symbol="ETH-USDT",
                        side="SELL",
                        order_type="LIMIT",
                        price=metrics["best_ask"],
                        quantity=ORDER_QTY,
                        time_in_force="GTC",
                        account_id=strat_account,
                    )
                    resting_order_id = order.id
                    action = (
                        f"RANDOM ASK QUEUE: Limit Sell {ORDER_QTY:.1f} @ ${metrics['best_ask']:.2f}"
                    )

        # Case 2: We hold inventory -> Quote exit passively on opposite side (Same Exit)
        else:
            hold_duration = step - entry_step
            pnl_pts = (
                (metrics["best_bid"] - avg_entry)
                if curr_qty > 0
                else (avg_entry - metrics["best_ask"])
            )

            # Check Stop-Loss or Timeout
            should_emergency_exit = (pnl_pts <= -STOP_LOSS_PTS) or (hold_duration >= MAX_HOLD_STEPS)

            if should_emergency_exit:
                # Cancel resting quote and flatten with IOC
                if resting_order_id is not None:
                    try:
                        engine.cancel_order("ETH-USDT", resting_order_id)
                    except Exception:
                        pass
                    resting_order_id = None

                exit_side = "SELL" if curr_qty > 0 else "BUY"
                engine.submit_order(
                    symbol="ETH-USDT",
                    side=exit_side,
                    order_type="MARKET",
                    price=0.0,
                    quantity=abs(curr_qty),
                    time_in_force="IOC",
                    account_id=strat_account,
                )
                pnl_dollars = pnl_pts * abs(curr_qty)
                action = f"EMERGENCY FLATTEN ({curr_qty:+.1f} ETH) [Pnl: ${pnl_dollars:>+6.2f}]"
            else:
                # Quote passively on the exit side to capture the spread
                if curr_qty > 0:
                    exit_price = metrics["best_ask"]
                    exit_side = "SELL"
                else:
                    exit_price = metrics["best_bid"]
                    exit_side = "BUY"

                # Update resting exit quote
                if resting_order_id is not None:
                    try:
                        engine.cancel_order("ETH-USDT", resting_order_id)
                    except Exception:
                        pass
                order = engine.submit_order(
                    symbol="ETH-USDT",
                    side=exit_side,
                    order_type="LIMIT",
                    price=exit_price,
                    quantity=abs(curr_qty),
                    time_in_force="GTC",
                    account_id=strat_account,
                )
                resting_order_id = order.id
                target_pnl = (exit_price - avg_entry) if curr_qty > 0 else (avg_entry - exit_price)
                action = (
                    f"PASSIVE EXIT QUOTE: {exit_side} {abs(curr_qty):.1f} @ ${exit_price:.2f} "
                    f"(Target Spread: ${target_pnl:>+5.2f})"
                )

        prev_qty = curr_qty

        if verbose and (step % 2 == 0 or action != "HOLD"):
            dev_bps = metrics["micro_dev_bps"]
            print(
                f"[Step {step:03d}] Mid: ${mid:>7.2f} | WOBI: {wobi:>+5.2f} | "
                f"Dev: {dev_bps:>+5.1f}bps | Pos: {curr_qty:>+4.1f} -> {action}"
            )

    # 4. Final Position Clean-Up
    if resting_order_id is not None:
        try:
            engine.cancel_order("ETH-USDT", resting_order_id)
        except Exception:
            pass

    final_pos = engine.get_position("ETH-USDT", account_id=strat_account)
    if final_pos and abs(final_pos.quantity) >= 0.01:
        close_side = "SELL" if final_pos.quantity > 0 else "BUY"
        engine.submit_order(
            symbol="ETH-USDT",
            side=close_side,
            order_type="MARKET",
            price=0.0,
            quantity=abs(final_pos.quantity),
            time_in_force="IOC",
            account_id=strat_account,
        )

    # 5. Performance Report
    strat_acct = engine.get_account(strat_account)
    realized_pnl = strat_acct.realized_pnl if strat_acct else 0.0
    cash_balance = strat_acct.cash_balance if strat_acct else 250_000.0

    title = (
        "Passive Order Book Imbalance Strategy Performance Summary (Isolated):"
        if not is_random
        else "Random-Entry Same-Exit Baseline Performance Summary (Isolated):"
    )

    print("\n" + "=" * 75)
    print(title)
    print(f"  Account ID              : {strat_account}")
    print(f"  Inventory Entries       : {trades_executed}")
    print(f"  Realized PnL            : ${realized_pnl:>+10.2f}")
    print(f"  Max Drawdown            : ${max_drawdown:>10.2f}")
    print(f"  Final Cash Balance      : ${cash_balance:,.2f}")

    print("\nSimulator Internal Accounts:")
    for acct_id in sorted(engine.get_all_account_ids()):
        if acct_id == strat_account:
            continue
        if acct_id == "DEFAULT":
            def_acct = engine.get_account("DEFAULT")
            def_pos = engine.get_positions("DEFAULT")
            if def_acct.realized_pnl == 0.0 and not def_pos:
                continue
        acct = engine.get_account(acct_id)
        print(
            f"  Account [{acct_id:<12}]: Cash=${acct.cash_balance:,.2f} | "
            f"Realized PnL=${acct.realized_pnl:>+10.2f}"
        )
    print("=" * 75)

    return trades_executed, realized_pnl, max_drawdown


def run_obi_strategy():
    print("=" * 75)
    print("Order Book Imbalance (OBI) - Passive Queue Scalping Strategy")
    print("=" * 75)

    # 1. Run Strategy with Step-by-Step logs
    obi_trades, obi_pnl, obi_dd = run_single_simulation(
        mode="OBI", sim_seed=101, total_steps=800, verbose=True
    )

    # 2. Run Random-Entry Baseline (silent during simulation, outputting final summary)
    print("\n--- Running Random-Entry Same-Exit Baseline (Null Hypothesis) ---")
    rand_trades, rand_pnl, rand_dd = run_single_simulation(
        mode="RANDOM", sim_seed=101, total_steps=800, verbose=False
    )

    # 3. Concise Comparison Summary
    print("\n" + "=" * 75)
    print("Performance Comparison:")
    print(
        f"  OBI Scalper (Signal)   : Realized PnL = ${obi_pnl:>+10.2f} | "
        f"Max DD = ${obi_dd:>6.2f} | Trades = {obi_trades}"
    )
    print(
        f"  Random Baseline (Null) : Realized PnL = ${rand_pnl:>+10.2f} | "
        f"Max DD = ${rand_dd:>6.2f} | Trades = {rand_trades}"
    )
    print("=" * 75)


if __name__ == "__main__":
    run_obi_strategy()
