"""Real-Market Kraken Order Book Backtest & Live Execution.

Replays authentic Kraken exchange L2 order book depth and historical public
market taker trades stored in Apache Parquet format, or streams live market
updates continuously in real-time until stopped (Ctrl+C).
"""

import argparse
import os
import random
import time
from pathlib import Path

from trading_engine import (
    Engine,
    KrakenClient,
    KrakenMarketSession,
    KrakenOrderBookReplayer,
    MarketDepth,
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

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid_price,
        "spread": spread,
        "top_obi": top_obi,
        "wobi": wobi,
    }


def run_single_kraken_backtest(
    session: KrakenMarketSession,
    mode: str = "OBI",
    symbol: str = "ETH-USDT",
    max_trades: int | None = None,
    seed: int = 101,
    verbose: bool = False,
) -> tuple[int, float, float]:
    """Runs a backtest of either OBI or Random baseline on a real Kraken market session."""
    is_random = mode.upper() == "RANDOM"
    strat_account = "RANDOM_BASELINE" if is_random else "OBI_SCALPER"

    risk = RiskConfig(
        max_order_qty=10_000.0,
        max_order_notional=50_000_000.0,
        max_position_notional=100_000_000.0,
        price_collar_pct=0.50,
        max_drawdown_pct=0.99,
        max_orders_per_sec=0,  # No artificial throttling during backtest
        require_margin=False,
    )

    engine = Engine(initial_balance=250_000.0, leverage=2.0, risk_config=risk)
    engine.register_symbol(symbol, tick_size=0.01, lot_size=0.001)

    replayer = KrakenOrderBookReplayer(
        engine=engine,
        session=session,
        symbol=symbol,
        maker_account="KRAKEN_MAKER",
        taker_account="KRAKEN_TAKER",
    )

    # 1. Seed resting order book with real Kraken bid/ask depth
    replayer.seed_initial_book()

    # 2. Strategy Parameters
    WOBI_ENTRY_THRESH = 0.15
    ORDER_QTY = 1.0  # 1.0 ETH per quote
    MAX_HOLD_TRADES = 20  # Exit if held across 20 trades without profit target fill
    STOP_LOSS_PTS = 2.00  # $2.00 stop loss

    resting_order_id = None
    trades_executed = 0
    prev_qty = 0.0
    entry_trade_idx = 0

    peak_equity = 250_000.0
    max_drawdown = 0.0

    rng = random.Random(seed + 777)
    random_desired_side = None

    trade_limit = max_trades if max_trades is not None else len(session.trades)
    trade_limit = min(trade_limit, len(session.trades))

    for trade_idx in range(trade_limit):
        depth = engine.get_depth(symbol, levels=4)
        if not depth:
            replayer.replay_next_trade()
            continue

        metrics = compute_order_book_metrics(depth, max_levels=4)
        wobi = metrics["wobi"]

        pos = engine.get_position(symbol, account_id=strat_account)
        curr_qty = pos.quantity if pos else 0.0
        avg_entry = pos.avg_entry_price if pos else 0.0

        acct = engine.get_account(strat_account)
        current_cash = acct.cash_balance if acct else 250_000.0
        unrealized = pos.unrealized_pnl if pos else 0.0
        equity = current_cash + unrealized
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)

        # Detect new fill
        if abs(curr_qty) > 0.0 and abs(prev_qty) < 0.001:
            trades_executed += 1
            entry_trade_idx = trade_idx
            resting_order_id = None
            random_desired_side = None

        # Case 1: Flat -> Look for entry
        if abs(curr_qty) < 0.001:
            if resting_order_id is not None:
                try:
                    engine.cancel_order(symbol, resting_order_id)
                except Exception:
                    pass
                resting_order_id = None

            if not is_random:
                if wobi >= WOBI_ENTRY_THRESH and metrics["best_bid"] > 0:
                    order = engine.submit_order(
                        symbol=symbol,
                        side="BUY",
                        order_type="LIMIT",
                        price=metrics["best_bid"],
                        quantity=ORDER_QTY,
                        time_in_force="GTC",
                        account_id=strat_account,
                    )
                    resting_order_id = order.id
                elif wobi <= -WOBI_ENTRY_THRESH and metrics["best_ask"] > 0:
                    order = engine.submit_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="LIMIT",
                        price=metrics["best_ask"],
                        quantity=ORDER_QTY,
                        time_in_force="GTC",
                        account_id=strat_account,
                    )
                    resting_order_id = order.id
            else:
                # Random entry attempt
                if random_desired_side is None:
                    if rng.random() < 0.35:
                        random_desired_side = "BUY" if rng.random() < 0.50 else "SELL"

                if random_desired_side == "BUY" and metrics["best_bid"] > 0:
                    order = engine.submit_order(
                        symbol=symbol,
                        side="BUY",
                        order_type="LIMIT",
                        price=metrics["best_bid"],
                        quantity=ORDER_QTY,
                        time_in_force="GTC",
                        account_id=strat_account,
                    )
                    resting_order_id = order.id
                elif random_desired_side == "SELL" and metrics["best_ask"] > 0:
                    order = engine.submit_order(
                        symbol=symbol,
                        side="SELL",
                        order_type="LIMIT",
                        price=metrics["best_ask"],
                        quantity=ORDER_QTY,
                        time_in_force="GTC",
                        account_id=strat_account,
                    )
                    resting_order_id = order.id

        # Case 2: In position -> Quote opposite side passively
        else:
            hold_trades = trade_idx - entry_trade_idx
            pnl_pts = (
                (metrics["best_bid"] - avg_entry)
                if curr_qty > 0
                else (avg_entry - metrics["best_ask"])
            )

            should_emergency = (pnl_pts <= -STOP_LOSS_PTS) or (hold_trades >= MAX_HOLD_TRADES)

            if should_emergency:
                if resting_order_id is not None:
                    try:
                        engine.cancel_order(symbol, resting_order_id)
                    except Exception:
                        pass
                    resting_order_id = None

                exit_side = "SELL" if curr_qty > 0 else "BUY"
                try:
                    engine.submit_order(
                        symbol=symbol,
                        side=exit_side,
                        order_type="MARKET",
                        price=0.0,
                        quantity=abs(curr_qty),
                        time_in_force="IOC",
                        account_id=strat_account,
                    )
                except Exception:
                    pass
            else:
                exit_price = metrics["best_ask"] if curr_qty > 0 else metrics["best_bid"]
                exit_side = "SELL" if curr_qty > 0 else "BUY"

                if resting_order_id is not None:
                    try:
                        engine.cancel_order(symbol, resting_order_id)
                    except Exception:
                        pass
                    resting_order_id = None

                if exit_price > 0.0:
                    try:
                        order = engine.submit_order(
                            symbol=symbol,
                            side=exit_side,
                            order_type="LIMIT",
                            price=exit_price,
                            quantity=abs(curr_qty),
                            time_in_force="GTC",
                            account_id=strat_account,
                        )
                        resting_order_id = order.id
                    except Exception:
                        pass
                else:
                    # Depleted book side; safely close via market IOC
                    try:
                        engine.submit_order(
                            symbol=symbol,
                            side=exit_side,
                            order_type="MARKET",
                            price=0.0,
                            quantity=abs(curr_qty),
                            time_in_force="IOC",
                            account_id=strat_account,
                        )
                    except Exception:
                        pass

        prev_qty = curr_qty

        # Replay real market taker trade against the book
        replayer.replay_next_trade()

    # Clean up
    if resting_order_id is not None:
        try:
            engine.cancel_order(symbol, resting_order_id)
        except Exception:
            pass

    final_pos = engine.get_position(symbol, account_id=strat_account)
    if final_pos and abs(final_pos.quantity) >= 0.001:
        close_side = "SELL" if final_pos.quantity > 0 else "BUY"
        try:
            engine.submit_order(
                symbol=symbol,
                side=close_side,
                order_type="MARKET",
                price=0.0,
                quantity=abs(final_pos.quantity),
                time_in_force="IOC",
                account_id=strat_account,
            )
        except Exception:
            pass

    strat_acct = engine.get_account(strat_account)
    realized_pnl = strat_acct.realized_pnl if strat_acct else 0.0
    cash_balance = strat_acct.cash_balance if strat_acct else 250_000.0

    title = (
        "Real-Market OBI Scalper Performance Summary (Kraken ETH/USD):"
        if not is_random
        else "Real-Market Random-Entry Baseline Performance Summary (Kraken ETH/USD):"
    )

    print("\n" + "=" * 75)
    print(title)
    print(f"  Account ID              : {strat_account}")
    print(f"  Inventory Entries       : {trades_executed}")
    print(f"  Realized PnL            : ${realized_pnl:>+10.2f}")
    print(f"  Max Drawdown            : ${max_drawdown:>10.2f}")
    print(f"  Final Cash Balance      : ${cash_balance:,.2f}")

    print("\nKraken Market Accounts:")
    for acct_id in sorted(engine.get_all_account_ids()):
        if acct_id == strat_account or acct_id == "DEFAULT":
            continue
        acct = engine.get_account(acct_id)
        print(
            f"  Account [{acct_id:<12}]: Cash=${acct.cash_balance:,.2f} | "
            f"Realized PnL=${acct.realized_pnl:>+10.2f}"
        )
    print("=" * 75)

    return trades_executed, realized_pnl, max_drawdown


def run_kraken_live_stream(
    client: KrakenClient,
    pair: str = "ETHUSD",
    symbol: str = "ETH-USDT",
    poll_interval: float = 1.0,
) -> None:
    """Continuously streams live Kraken market data in real-time until stopped (Ctrl+C)."""
    print("=" * 75)
    print(f"Kraken Real-Time Live Market Execution ({pair}) - Running until Ctrl+C")
    print("=" * 75)

    risk = RiskConfig(
        max_order_qty=10_000.0,
        max_order_notional=50_000_000.0,
        max_position_notional=100_000_000.0,
        price_collar_pct=0.50,
        max_drawdown_pct=0.99,
        max_orders_per_sec=0,
        require_margin=False,
    )
    engine = Engine(initial_balance=250_000.0, leverage=2.0, risk_config=risk)
    engine.register_symbol(symbol, tick_size=0.01, lot_size=0.001)

    print("Fetching live order book depth from Kraken...")
    raw_depth = client.fetch_depth(pair=pair, count=50)
    for p, q, *_ in reversed(raw_depth["bids"]):
        if float(q) > 0.0:
            engine.submit_order(
                symbol, "BUY", "LIMIT", float(p), float(q), "GTC", account_id="KRAKEN_MAKER"
            )
    for p, q, *_ in reversed(raw_depth["asks"]):
        if float(q) > 0.0:
            engine.submit_order(
                symbol, "SELL", "LIMIT", float(p), float(q), "GTC", account_id="KRAKEN_MAKER"
            )

    depth = engine.get_depth(symbol, levels=2)
    best_bid = depth.best_bid() if depth else 0.0
    best_ask = depth.best_ask() if depth else 0.0
    print(f"Live Book Initialized: BestBid=${best_bid:,.2f} | BestAsk=${best_ask:,.2f}")

    raw_t = client._get("Trades", {"pair": pair})
    last_cursor = raw_t.get("last")
    print(f"Initial Trade Cursor: {last_cursor}")
    print("Streaming live market trades in real-time... (Press Ctrl+C to stop)\n")

    strat_account = "OBI_SCALPER"
    ORDER_QTY = 1.0
    WOBI_ENTRY_THRESH = 0.15
    STOP_LOSS_PTS = 2.00
    MAX_HOLD_TRADES = 30

    resting_order_id = None
    trades_processed = 0
    strategy_entries = 0
    prev_qty = 0.0
    entry_trade_idx = 0
    peak_equity = 250_000.0
    max_drawdown = 0.0

    try:
        while True:
            time.sleep(poll_interval)

            try:
                t_res = client._get("Trades", {"pair": pair, "since": last_cursor})
                pair_key = next(k for k in t_res.keys() if k != "last")
                new_trades = t_res[pair_key]
                next_cursor = t_res.get("last")
                if next_cursor:
                    last_cursor = next_cursor
            except Exception:
                continue

            if not new_trades:
                continue

            for item in new_trades:
                trades_processed += 1
                qty = float(item[1])
                trade_side = "BUY" if item[3] == "b" else "SELL"

                cur_depth = engine.get_depth(symbol, levels=4)
                if not cur_depth:
                    continue
                metrics = compute_order_book_metrics(cur_depth, max_levels=4)
                wobi = metrics["wobi"]

                pos = engine.get_position(symbol, account_id=strat_account)
                curr_qty = pos.quantity if pos else 0.0
                avg_entry = pos.avg_entry_price if pos else 0.0

                acct = engine.get_account(strat_account)
                current_cash = acct.cash_balance if acct else 250_000.0
                unrealized = pos.unrealized_pnl if pos else 0.0
                equity = current_cash + unrealized
                peak_equity = max(peak_equity, equity)
                max_drawdown = max(max_drawdown, peak_equity - equity)

                if abs(curr_qty) > 0.0 and abs(prev_qty) < 0.001:
                    strategy_entries += 1
                    entry_trade_idx = trades_processed
                    resting_order_id = None
                    print(
                        f"[{time.strftime('%H:%M:%S')}] >>> STRATEGY FILLED: "
                        f"{curr_qty:+.2f} ETH @ ${avg_entry:.2f}"
                    )

                # Strategy order decision
                if abs(curr_qty) < 0.001:
                    if resting_order_id is not None:
                        try:
                            engine.cancel_order(symbol, resting_order_id)
                        except Exception:
                            pass
                        resting_order_id = None

                    if wobi >= WOBI_ENTRY_THRESH and metrics["best_bid"] > 0:
                        order = engine.submit_order(
                            symbol=symbol,
                            side="BUY",
                            order_type="LIMIT",
                            price=metrics["best_bid"],
                            quantity=ORDER_QTY,
                            time_in_force="GTC",
                            account_id=strat_account,
                        )
                        resting_order_id = order.id
                    elif wobi <= -WOBI_ENTRY_THRESH and metrics["best_ask"] > 0:
                        order = engine.submit_order(
                            symbol=symbol,
                            side="SELL",
                            order_type="LIMIT",
                            price=metrics["best_ask"],
                            quantity=ORDER_QTY,
                            time_in_force="GTC",
                            account_id=strat_account,
                        )
                        resting_order_id = order.id
                else:
                    hold_trades = trades_processed - entry_trade_idx
                    pnl_pts = (
                        (metrics["best_bid"] - avg_entry)
                        if curr_qty > 0
                        else (avg_entry - metrics["best_ask"])
                    )
                    should_emergency = (pnl_pts <= -STOP_LOSS_PTS) or (
                        hold_trades >= MAX_HOLD_TRADES
                    )

                    if should_emergency:
                        if resting_order_id is not None:
                            try:
                                engine.cancel_order(symbol, resting_order_id)
                            except Exception:
                                pass
                            resting_order_id = None
                        exit_side = "SELL" if curr_qty > 0 else "BUY"
                        try:
                            engine.submit_order(
                                symbol=symbol,
                                side=exit_side,
                                order_type="MARKET",
                                price=0.0,
                                quantity=abs(curr_qty),
                                time_in_force="IOC",
                                account_id=strat_account,
                            )
                        except Exception:
                            pass
                    else:
                        exit_price = metrics["best_ask"] if curr_qty > 0 else metrics["best_bid"]
                        exit_side = "SELL" if curr_qty > 0 else "BUY"
                        if resting_order_id is not None:
                            try:
                                engine.cancel_order(symbol, resting_order_id)
                            except Exception:
                                pass
                            resting_order_id = None

                        if exit_price > 0.0:
                            try:
                                order = engine.submit_order(
                                    symbol=symbol,
                                    side=exit_side,
                                    order_type="LIMIT",
                                    price=exit_price,
                                    quantity=abs(curr_qty),
                                    time_in_force="GTC",
                                    account_id=strat_account,
                                )
                                resting_order_id = order.id
                            except Exception:
                                pass

                prev_qty = curr_qty

                # Execute real market taker trade against the book
                try:
                    engine.submit_order(
                        symbol=symbol,
                        side=trade_side,
                        order_type="MARKET",
                        price=0.0,
                        quantity=qty,
                        time_in_force="IOC",
                        account_id="KRAKEN_TAKER",
                    )
                except Exception:
                    pass

            acct = engine.get_account(strat_account)
            pnl = acct.realized_pnl if acct else 0.0
            print(
                f"[{time.strftime('%H:%M:%S')}] Live Trades: {trades_processed} | "
                f"Mid: ${metrics['mid_price']:>7.2f} | WOBI: {wobi:>+5.2f} | "
                f"Pos: {curr_qty:>+4.1f} | PnL: ${pnl:>+6.2f}"
            )

    except KeyboardInterrupt:
        print("\n[Stop Signal Received] Closing open positions and finalizing summary...")

    # Cleanup
    if resting_order_id is not None:
        try:
            engine.cancel_order(symbol, resting_order_id)
        except Exception:
            pass

    final_pos = engine.get_position(symbol, account_id=strat_account)
    if final_pos and abs(final_pos.quantity) >= 0.001:
        close_side = "SELL" if final_pos.quantity > 0 else "BUY"
        try:
            engine.submit_order(
                symbol=symbol,
                side=close_side,
                order_type="MARKET",
                price=0.0,
                quantity=abs(final_pos.quantity),
                time_in_force="IOC",
                account_id=strat_account,
            )
        except Exception:
            pass

    strat_acct = engine.get_account(strat_account)
    realized_pnl = strat_acct.realized_pnl if strat_acct else 0.0
    cash_balance = strat_acct.cash_balance if strat_acct else 250_000.0

    print("\n" + "=" * 75)
    print(f"Kraken Live Session Performance Summary ({pair}):")
    print(f"  Account ID              : {strat_account}")
    print(f"  Live Trades Processed   : {trades_processed}")
    print(f"  Inventory Entries       : {strategy_entries}")
    print(f"  Realized PnL            : ${realized_pnl:>+10.2f}")
    print(f"  Max Drawdown            : ${max_drawdown:>10.2f}")
    print(f"  Final Cash Balance      : ${cash_balance:,.2f}")
    print("=" * 75)


def main():
    parser = argparse.ArgumentParser(description="Real-Market Kraken Order Book Backtest & Live")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Continuously stream live market trades and execute in real-time until Ctrl+C",
    )
    parser.add_argument(
        "--pair",
        type=str,
        default="ETHUSD",
        help="Kraken trading pair (default: ETHUSD)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds for live market streaming (default: 1.0)",
    )
    parser.add_argument(
        "--max-trades",
        type=int,
        default=None,
        help="Limit replay to N trades for backtest mode (default: all recorded trades)",
    )
    parser.add_argument(
        "--fetch-history",
        type=int,
        default=None,
        metavar="N",
        help="Download N historical trades from Kraken API and save to Parquet",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force re-fetching fresh trades and depth from Kraken, overwriting cached Parquet",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="examples/data",
        help="Directory containing Kraken Parquet files (default: examples/data)",
    )
    args = parser.parse_args()

    client = KrakenClient()

    if args.live:
        # Live streaming mode: runs continuously until stopped
        run_kraken_live_stream(
            client=client,
            pair=args.pair,
            symbol="ETH-USDT",
            poll_interval=args.poll_interval,
        )
        return

    # Backtest Mode (Parquet Replay)
    pair_clean = args.pair.lower().replace("/", "")
    depth_pq = os.path.join(args.data_dir, f"kraken_{pair_clean}_depth.parquet")
    trades_pq = os.path.join(args.data_dir, f"kraken_{pair_clean}_trades.parquet")

    should_fetch = (
        args.refresh
        or (args.fetch_history is not None)
        or not (Path(depth_pq).exists() and Path(trades_pq).exists())
    )

    if should_fetch:
        target_trades = args.fetch_history or 5000
        print(f"Fetching {target_trades} historical trades for {args.pair} from Kraken API...")
        session = client.record_session(
            pair=args.pair,
            depth_count=100,
            max_trades=target_trades,
            output_format="parquet",
            output_dir=args.data_dir,
        )
    else:
        print(f"Loading cached Kraken Parquet dataset ({args.pair})...")
        session = client.load_from_parquet(depth_pq, trades_pq, pair=args.pair)

    print(
        f"Session loaded: {len(session.bids)} bids, {len(session.asks)} asks, "
        f"{len(session.trades)} historical market trades."
    )

    print("=" * 75)
    print("Kraken Real-Market Order Book Backtest (OBI vs. Random Baseline)")
    print("=" * 75)

    # 1. Run OBI Strategy on real Kraken flow
    obi_trades, obi_pnl, obi_dd = run_single_kraken_backtest(
        session=session,
        mode="OBI",
        symbol="ETH-USDT",
        max_trades=args.max_trades,
        seed=101,
        verbose=False,
    )

    # 2. Run Random Baseline on exact same real Kraken flow
    print("\n--- Running Random-Entry Same-Exit Baseline on Kraken Market Flow ---")
    rand_trades, rand_pnl, rand_dd = run_single_kraken_backtest(
        session=session,
        mode="RANDOM",
        symbol="ETH-USDT",
        max_trades=args.max_trades,
        seed=101,
        verbose=False,
    )

    # 3. Side-by-side comparison
    print("\n" + "=" * 75)
    print("Kraken Real-Market Performance Comparison:")
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
    main()
