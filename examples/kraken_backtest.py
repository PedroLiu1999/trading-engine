"""Real-Market Kraken Order Book Backtest & Live Execution.

Replays authentic Kraken exchange L2 order book depth and historical public
market taker trades stored in Apache Parquet format, or streams live market
updates continuously in real-time until stopped (Ctrl+C).
"""

import argparse
import asyncio
import copy
import json
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
    normalize_kraken_rest_pair,
    normalize_kraken_ws_pair,
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
) -> tuple[int, float, float, list[dict[str, float]]]:
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
    in_position = False
    trades_executed = 0
    entry_trade_idx = 0

    peak_equity = 250_000.0
    max_drawdown = 0.0

    pnl_history: list[dict[str, float]] = []

    rng = random.Random(seed + 777)
    random_desired_side = None

    trade_limit = max_trades if max_trades is not None else len(session.trades)
    trade_limit = min(trade_limit, len(session.trades))

    for trade_idx in range(trade_limit):
        trade = session.trades[trade_idx]

        # 1. Apply any real-time book deltas strictly BEFORE this trade (strict < and seq tie-break)
        replayer.apply_deltas_until(trade.timestamp, trade.seq)

        depth = engine.get_depth(symbol, levels=4)
        if not depth:
            if resting_order_id is not None:
                replayer.try_fill_resting(trade, resting_order_id)
            else:
                replayer.advance_trade()
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

        pnl_history.append(
            {
                "trade_idx": trade_idx,
                "timestamp": trade.timestamp,
                "realized_pnl": acct.realized_pnl if acct else 0.0,
                "unrealized_pnl": unrealized,
                "total_pnl": (acct.realized_pnl if acct else 0.0) + unrealized,
                "drawdown": peak_equity - equity,
                "mid_price": metrics["mid_price"],
                "position": curr_qty,
            }
        )

        # State transition: check entry completion or exit completion
        if not in_position:
            # Gate entry detection on abs(curr_qty) >= ORDER_QTY * 0.99
            if abs(curr_qty) >= ORDER_QTY * 0.99:
                in_position = True
                entry_trade_idx = trade_idx
                trades_executed += 1
                random_desired_side = None
                # Cancel any remaining unfulfilled portion of the entry quote
                if resting_order_id is not None:
                    try:
                        engine.cancel_order(symbol, resting_order_id)
                    except Exception:
                        pass
                    resting_order_id = None
        else:
            # Gate exit detection on position returning flat
            if abs(curr_qty) < 0.001:
                in_position = False
                if resting_order_id is not None:
                    try:
                        engine.cancel_order(symbol, resting_order_id)
                    except Exception:
                        pass
                    resting_order_id = None

        # Verify active status of resting order
        if resting_order_id is not None:
            order_info = engine.get_order(symbol, resting_order_id)
            if order_info is None or not order_info.is_active():
                resting_order_id = None

        # Quoting Decisions
        if not in_position:
            # Flat: look for new entry quote. (If partially filled, keep resting order alive)
            if abs(curr_qty) < 0.001:
                if not is_random:
                    desired_side = None
                    desired_price = 0.0
                    if wobi >= WOBI_ENTRY_THRESH and metrics["best_bid"] > 0:
                        desired_side = "BUY"
                        desired_price = metrics["best_bid"]
                    elif wobi <= -WOBI_ENTRY_THRESH and metrics["best_ask"] > 0:
                        desired_side = "SELL"
                        desired_price = metrics["best_ask"]

                    if desired_side:
                        if resting_order_id is not None:
                            curr_order = engine.get_order(symbol, resting_order_id)
                            if (
                                curr_order is None
                                or str(curr_order.side).upper() != desired_side
                                or abs(curr_order.price - desired_price) > 0.001
                            ):
                                try:
                                    engine.cancel_order(symbol, resting_order_id)
                                except Exception:
                                    pass
                                resting_order_id = None

                        if resting_order_id is None:
                            try:
                                order = engine.submit_order(
                                    symbol=symbol,
                                    side=desired_side,
                                    order_type="LIMIT",
                                    price=desired_price,
                                    quantity=ORDER_QTY,
                                    time_in_force="GTC",
                                    account_id=strat_account,
                                )
                                resting_order_id = order.id
                            except Exception:
                                pass
                    else:
                        if resting_order_id is not None:
                            try:
                                engine.cancel_order(symbol, resting_order_id)
                            except Exception:
                                pass
                            resting_order_id = None
                else:
                    # Random baseline entry
                    if random_desired_side is None:
                        if rng.random() < 0.35:
                            random_desired_side = "BUY" if rng.random() < 0.50 else "SELL"

                    desired_price = (
                        metrics["best_bid"] if random_desired_side == "BUY" else metrics["best_ask"]
                    )
                    if random_desired_side and desired_price > 0:
                        if resting_order_id is not None:
                            curr_order = engine.get_order(symbol, resting_order_id)
                            if (
                                curr_order is None
                                or str(curr_order.side).upper() != random_desired_side
                                or abs(curr_order.price - desired_price) > 0.001
                            ):
                                try:
                                    engine.cancel_order(symbol, resting_order_id)
                                except Exception:
                                    pass
                                resting_order_id = None

                        if resting_order_id is None:
                            try:
                                order = engine.submit_order(
                                    symbol=symbol,
                                    side=random_desired_side,
                                    order_type="LIMIT",
                                    price=desired_price,
                                    quantity=ORDER_QTY,
                                    time_in_force="GTC",
                                    account_id=strat_account,
                                )
                                resting_order_id = order.id
                            except Exception:
                                pass
                    elif resting_order_id is not None:
                        try:
                            engine.cancel_order(symbol, resting_order_id)
                        except Exception:
                            pass
                        resting_order_id = None
        else:
            # In position: quote passive exit on opposite side or execute emergency stop
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
                    curr_order = engine.get_order(symbol, resting_order_id)
                    if (
                        curr_order is None
                        or abs(curr_order.price - exit_price) > 0.001
                        or abs(curr_order.remaining_quantity - abs(curr_qty)) > 0.001
                    ):
                        try:
                            engine.cancel_order(symbol, resting_order_id)
                        except Exception:
                            pass
                        resting_order_id = None

                if resting_order_id is None and exit_price > 0.0:
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

        # Try fill strategy resting order against the market trade (without touching KRAKEN_MAKER)
        if resting_order_id is not None:
            replayer.try_fill_resting(trade, resting_order_id)
        else:
            replayer.advance_trade()

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

    return trades_executed, realized_pnl, max_drawdown, pnl_history


def plot_pnl_over_time(
    obi_history: list[dict[str, float]],
    rand_history: list[dict[str, float]],
    symbol: str = "ETHUSD",
    output_file: str = "examples/charts/kraken_pnl_chart.png",
) -> None:
    """Generates a high-resolution dark-themed performance chart comparing OBI vs Random."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as ticker
    except ImportError:
        print("[Warning] Matplotlib not installed; skipping plot generation.")
        return

    if not obi_history:
        return

    trade_indices = [h["trade_idx"] for h in obi_history]
    obi_pnl = [h["realized_pnl"] for h in obi_history]
    obi_total_pnl = [h["total_pnl"] for h in obi_history]
    obi_dd = [-h["drawdown"] for h in obi_history]
    mid_prices = [h["mid_price"] for h in obi_history]

    rand_indices = [h["trade_idx"] for h in rand_history]
    rand_pnl = [h["realized_pnl"] for h in rand_history]
    rand_total_pnl = [h["total_pnl"] for h in rand_history]
    rand_dd = [-h["drawdown"] for h in rand_history]

    plt.style.use("dark_background")
    fig, (ax_pnl, ax_dd, ax_price) = plt.subplots(
        3, 1, figsize=(14, 10), sharex=True, gridspec_kw={"height_ratios": [3, 1.5, 1.5]}
    )
    fig.patch.set_facecolor("#0f172a")

    for ax in (ax_pnl, ax_dd, ax_price):
        ax.set_facecolor("#1e293b")
        ax.grid(True, linestyle="--", alpha=0.25, color="#94a3b8")
        ax.tick_params(colors="#cbd5e1", labelsize=10)
        for spine in ax.spines.values():
            spine.set_color("#334155")

    final_obi_pnl = obi_pnl[-1] if obi_pnl else 0.0
    final_rand_pnl = rand_pnl[-1] if rand_pnl else 0.0

    # Panel 1: Cumulative PnL ($)
    ax_pnl.axhline(0, color="#64748b", linestyle=":", linewidth=1.2, alpha=0.7)
    ax_pnl.plot(
        trade_indices,
        obi_pnl,
        color="#38bdf8",
        linewidth=2.0,
        label=f"OBI Scalper Realized (${final_obi_pnl:>+6.2f})",
    )
    ax_pnl.plot(
        trade_indices,
        obi_total_pnl,
        color="#06b6d4",
        linewidth=1.0,
        linestyle="--",
        alpha=0.6,
        label="OBI Scalper Total Equity",
    )
    ax_pnl.plot(
        rand_indices,
        rand_pnl,
        color="#f97316",
        linewidth=1.8,
        label=f"Random Baseline Realized (${final_rand_pnl:>+6.2f})",
    )
    ax_pnl.plot(
        rand_indices,
        rand_total_pnl,
        color="#fb923c",
        linewidth=1.0,
        linestyle="--",
        alpha=0.6,
        label="Random Baseline Total Equity",
    )

    ax_pnl.set_title(
        f"Kraken Real-Market Order Book Backtest: {symbol} ({len(trade_indices):,} Market Trades)",
        fontsize=14,
        fontweight="bold",
        color="#f8fafc",
        pad=12,
    )
    ax_pnl.set_ylabel("Cumulative PnL ($)", fontsize=11, color="#f1f5f9", fontweight="bold")
    ax_pnl.legend(loc="upper left", framealpha=0.8, facecolor="#0f172a", edgecolor="#475569")
    ax_pnl.yaxis.set_major_formatter(ticker.FormatStrFormatter("$%.2f"))

    # Panel 2: Drawdown Underwater Curve ($)
    ax_dd.plot(trade_indices, obi_dd, color="#38bdf8", linewidth=1.2, label="OBI Drawdown")
    ax_dd.fill_between(trade_indices, obi_dd, 0, color="#38bdf8", alpha=0.15)
    ax_dd.plot(rand_indices, rand_dd, color="#f97316", linewidth=1.2, label="Random Drawdown")
    ax_dd.fill_between(rand_indices, rand_dd, 0, color="#f97316", alpha=0.15)
    ax_dd.set_ylabel("Drawdown ($)", fontsize=11, color="#f1f5f9", fontweight="bold")
    ax_dd.legend(loc="lower left", framealpha=0.8, facecolor="#0f172a", edgecolor="#475569")
    ax_dd.yaxis.set_major_formatter(ticker.FormatStrFormatter("$%.2f"))

    # Panel 3: Underlying Market Price Trajectory
    ax_price.plot(
        trade_indices,
        mid_prices,
        color="#a855f7",
        linewidth=1.5,
        label=f"{symbol} Mid Price",
    )
    ax_price.set_ylabel("Price ($)", fontsize=11, color="#f1f5f9", fontweight="bold")
    ax_price.set_xlabel(
        "Historical Market Trade Index", fontsize=11, color="#f1f5f9", fontweight="bold"
    )
    ax_price.legend(loc="upper left", framealpha=0.8, facecolor="#0f172a", edgecolor="#475569")
    ax_price.yaxis.set_major_formatter(ticker.FormatStrFormatter("$%.2f"))
    ax_price.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

    plt.tight_layout()
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    plt.savefig(output_file, dpi=200, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"\n[Chart Saved] Performance plot written to: {output_file}")


async def _async_run_kraken_live_stream(
    pair: str = "ETH/USD",
    symbol: str = "ETH-USDT",
    depth_limit: int = 100,
) -> None:
    import websockets

    pair_ws = normalize_kraken_ws_pair(pair)
    print("=" * 75)
    print(f"Kraken Real-Time Live Market Execution ({pair_ws}) - WebSocket v2 Feed")
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

    maker_account = "KRAKEN_MAKER"
    taker_account = "KRAKEN_TAKER"
    strat_account = "OBI_SCALPER"

    ORDER_QTY = 1.0
    WOBI_ENTRY_THRESH = 0.15
    STOP_LOSS_PTS = 2.00
    MAX_HOLD_TRADES = 30

    maker_orders: dict[tuple[str, float], int] = {}
    resting_order_id: int | None = None
    in_position = False
    entry_trade_idx = 0
    trades_processed = 0
    strategy_entries = 0
    peak_equity = 250_000.0
    max_drawdown = 0.0

    print("Connecting to Kraken WebSocket API v2 (wss://ws.kraken.com/v2)...")
    try:
        async with websockets.connect(
            "wss://ws.kraken.com/v2", ping_interval=20, ping_timeout=10
        ) as ws:
            # Subscribe to book and trade
            await ws.send(
                json.dumps(
                    {
                        "method": "subscribe",
                        "params": {"channel": "book", "symbol": [pair_ws], "depth": depth_limit},
                    }
                )
            )
            await ws.send(
                json.dumps(
                    {
                        "method": "subscribe",
                        "params": {"channel": "trade", "symbol": [pair_ws]},
                    }
                )
            )
            print(f"Subscribed to book (depth={depth_limit}) and trade channels for {pair_ws}...")
            print("Streaming live market feed... (Press Ctrl+C to stop)\n")

            while True:
                raw_msg = await ws.recv()
                msg = json.loads(raw_msg)
                if msg.get("method") == "subscribe":
                    if not msg.get("success", True):
                        print(f"\n[Kraken WS Error] Subscription rejected: {msg.get('error')}")
                    continue

                channel = msg.get("channel")
                msg_type = msg.get("type")
                data_list = msg.get("data", [])

                if channel == "book":
                    if msg_type == "snapshot" and data_list:
                        snap = data_list[0]
                        for oid in maker_orders.values():
                            try:
                                engine.cancel_order(symbol, oid)
                            except Exception:
                                pass
                        maker_orders.clear()

                        for b in reversed(snap.get("bids", [])):
                            p, q = float(b["price"]), float(b["qty"])
                            if q > 0 and p > 0:
                                try:
                                    ord_obj = engine.submit_order(
                                        symbol, "BUY", "LIMIT", p, q, "GTC", maker_account
                                    )
                                    maker_orders[("BUY", p)] = ord_obj.id
                                except Exception:
                                    pass
                        for a in reversed(snap.get("asks", [])):
                            p, q = float(a["price"]), float(a["qty"])
                            if q > 0 and p > 0:
                                try:
                                    ord_obj = engine.submit_order(
                                        symbol, "SELL", "LIMIT", p, q, "GTC", maker_account
                                    )
                                    maker_orders[("SELL", p)] = ord_obj.id
                                except Exception:
                                    pass
                        d = engine.get_depth(symbol, levels=1)
                        bb = d.best_bid() if d else 0.0
                        ba = d.best_ask() if d else 0.0
                        print(
                            f"[{time.strftime('%H:%M:%S')}] Live book initialized: "
                            f"BestBid=${bb:,.2f} | BestAsk=${ba:,.2f}"
                        )

                    elif msg_type == "update" and data_list:
                        upd = data_list[0]
                        for b in upd.get("bids", []):
                            p, q = float(b["price"]), float(b["qty"])
                            prior_id = maker_orders.pop(("BUY", p), None)
                            if prior_id is not None:
                                try:
                                    engine.cancel_order(symbol, prior_id)
                                except Exception:
                                    pass
                            if q > 0:
                                try:
                                    ord_obj = engine.submit_order(
                                        symbol, "BUY", "LIMIT", p, q, "GTC", maker_account
                                    )
                                    maker_orders[("BUY", p)] = ord_obj.id
                                except Exception:
                                    pass
                        for a in upd.get("asks", []):
                            p, q = float(a["price"]), float(a["qty"])
                            prior_id = maker_orders.pop(("SELL", p), None)
                            if prior_id is not None:
                                try:
                                    engine.cancel_order(symbol, prior_id)
                                except Exception:
                                    pass
                            if q > 0:
                                try:
                                    ord_obj = engine.submit_order(
                                        symbol, "SELL", "LIMIT", p, q, "GTC", maker_account
                                    )
                                    maker_orders[("SELL", p)] = ord_obj.id
                                except Exception:
                                    pass

                elif channel == "trade" and data_list:
                    for t in data_list:
                        trades_processed += 1
                        t_price = float(t["price"])
                        t_qty = float(t["qty"])
                        t_side = "BUY" if t.get("side", "").lower() == "buy" else "SELL"

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

                        # State machine & Orphaned-order fix:
                        if not in_position:
                            if abs(curr_qty) >= ORDER_QTY * 0.99:
                                in_position = True
                                entry_trade_idx = trades_processed
                                strategy_entries += 1
                                if resting_order_id is not None:
                                    try:
                                        engine.cancel_order(symbol, resting_order_id)
                                    except Exception:
                                        pass
                                    resting_order_id = None
                                print(
                                    f"[{time.strftime('%H:%M:%S')}] >>> STRATEGY FILLED ENTRY: "
                                    f"{curr_qty:+.2f} ETH @ ${avg_entry:.2f}"
                                )
                        else:
                            if abs(curr_qty) < 0.001:
                                in_position = False
                                if resting_order_id is not None:
                                    try:
                                        engine.cancel_order(symbol, resting_order_id)
                                    except Exception:
                                        pass
                                    resting_order_id = None
                                pnl_val = acct.realized_pnl if acct else 0.0
                                print(
                                    f"[{time.strftime('%H:%M:%S')}] >>> STRATEGY CLOSED OUT: "
                                    f"Flat | Realized PnL: ${pnl_val:+.2f}"
                                )

                        # Order quoting decisions
                        if resting_order_id is not None:
                            order_info = engine.get_order(symbol, resting_order_id)
                            if order_info is None or not order_info.is_active():
                                resting_order_id = None

                        if not in_position:
                            if abs(curr_qty) < 0.001:
                                desired_side = None
                                desired_price = 0.0
                                if wobi >= WOBI_ENTRY_THRESH and metrics["best_bid"] > 0:
                                    desired_side = "BUY"
                                    desired_price = metrics["best_bid"]
                                elif wobi <= -WOBI_ENTRY_THRESH and metrics["best_ask"] > 0:
                                    desired_side = "SELL"
                                    desired_price = metrics["best_ask"]

                                if desired_side:
                                    if resting_order_id is not None:
                                        curr_order = engine.get_order(symbol, resting_order_id)
                                        if (
                                            curr_order is None
                                            or str(curr_order.side).upper() != desired_side
                                            or abs(curr_order.price - desired_price) > 0.001
                                        ):
                                            try:
                                                engine.cancel_order(symbol, resting_order_id)
                                            except Exception:
                                                pass
                                            resting_order_id = None

                                    if resting_order_id is None:
                                        try:
                                            order = engine.submit_order(
                                                symbol=symbol,
                                                side=desired_side,
                                                order_type="LIMIT",
                                                price=desired_price,
                                                quantity=ORDER_QTY,
                                                time_in_force="GTC",
                                                account_id=strat_account,
                                            )
                                            resting_order_id = order.id
                                        except Exception:
                                            pass
                                elif resting_order_id is not None:
                                    try:
                                        engine.cancel_order(symbol, resting_order_id)
                                    except Exception:
                                        pass
                                    resting_order_id = None
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
                                exit_price = (
                                    metrics["best_ask"] if curr_qty > 0 else metrics["best_bid"]
                                )
                                exit_side = "SELL" if curr_qty > 0 else "BUY"

                                if resting_order_id is not None:
                                    curr_order = engine.get_order(symbol, resting_order_id)
                                    if (
                                        curr_order is None
                                        or abs(curr_order.price - exit_price) > 0.001
                                        or abs(curr_order.remaining_quantity - abs(curr_qty))
                                        > 0.001
                                    ):
                                        try:
                                            engine.cancel_order(symbol, resting_order_id)
                                        except Exception:
                                            pass
                                        resting_order_id = None

                                if resting_order_id is None and exit_price > 0.0:
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

                        # Try fill resting order from the trade without touching KRAKEN_MAKER
                        if resting_order_id is not None:
                            order = engine.get_order(symbol, resting_order_id)
                            if order is not None and order.is_active():
                                order_side = str(order.side).upper()
                                crossed = False
                                if (
                                    "BUY" in order_side
                                    and t_side == "SELL"
                                    and t_price <= order.price
                                ):
                                    crossed = True
                                elif (
                                    "SELL" in order_side
                                    and t_side == "BUY"
                                    and t_price >= order.price
                                ):
                                    crossed = True

                                if crossed:
                                    fill_qty = min(t_qty, order.remaining_quantity)
                                    if fill_qty > 1e-9:
                                        try:
                                            engine.fill_resting_order(
                                                symbol=symbol,
                                                order_id=resting_order_id,
                                                fill_price=order.price,
                                                fill_quantity=fill_qty,
                                                taker_account_id=taker_account,
                                            )
                                        except Exception:
                                            pass

                        # Periodic live log
                        if trades_processed % 5 == 0:
                            acct = engine.get_account(strat_account)
                            pnl = acct.realized_pnl if acct else 0.0
                            print(
                                f"[{time.strftime('%H:%M:%S')}] Live Trades: {trades_processed} | "
                                f"Mid: ${metrics['mid_price']:>7.2f} | WOBI: {wobi:>+5.2f} | "
                                f"Pos: {curr_qty:>+4.1f} | PnL: ${pnl:>+6.2f}"
                            )

    except (KeyboardInterrupt, asyncio.CancelledError):
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
    print(f"Kraken Live Session Performance Summary ({pair_ws}):")
    print(f"  Account ID              : {strat_account}")
    print(f"  Live Trades Processed   : {trades_processed}")
    print(f"  Inventory Entries       : {strategy_entries}")
    print(f"  Realized PnL            : ${realized_pnl:>+10.2f}")
    print(f"  Max Drawdown            : ${max_drawdown:>10.2f}")
    print(f"  Final Cash Balance      : ${cash_balance:,.2f}")
    print("=" * 75)


def run_kraken_live_stream(
    client: KrakenClient | None = None,
    pair: str = "ETH/USD",
    symbol: str = "ETH-USDT",
    poll_interval: float = 1.0,
) -> None:
    """Continuously streams live Kraken market data in real-time until stopped (Ctrl+C)."""
    try:
        asyncio.run(_async_run_kraken_live_stream(pair=pair, symbol=symbol))
    except KeyboardInterrupt:
        pass


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
        default="ETH/USD",
        help="Kraken trading pair (default: ETH/USD)",
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
        "--record",
        "--record-ws",
        dest="record",
        nargs="?",
        const=60.0,
        type=float,
        default=None,
        metavar="SECONDS",
        help="Record live L2 book depth, deltas, and trades via WebSocket (default: 60s)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force re-recording a fresh WebSocket L2 session, overwriting cached Parquet",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="examples/data",
        help="Directory containing Kraken Parquet files (default: examples/data)",
    )
    parser.add_argument(
        "--plot-file",
        type=str,
        default="examples/charts/kraken_pnl_chart.png",
        help="Path to save performance PnL chart PNG (default: examples/charts/...)",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Disable automatic PnL chart generation after backtest",
    )
    args = parser.parse_args()

    client = KrakenClient()
    pair_ws = normalize_kraken_ws_pair(args.pair)
    pair_rest = normalize_kraken_rest_pair(args.pair)

    if args.live:
        # Live streaming mode: runs continuously until stopped
        run_kraken_live_stream(
            client=client,
            pair=pair_ws,
            symbol="ETH-USDT",
            poll_interval=args.poll_interval,
        )
        return

    # Backtest Mode (Parquet Replay with Full L2 Depth & Deltas)
    pair_clean = pair_rest.lower()
    depth_pq = os.path.join(args.data_dir, f"kraken_{pair_clean}_depth.parquet")
    trades_pq = os.path.join(args.data_dir, f"kraken_{pair_clean}_trades.parquet")
    deltas_pq = os.path.join(args.data_dir, f"kraken_{pair_clean}_deltas.parquet")

    has_full_book = (
        Path(depth_pq).exists() and Path(trades_pq).exists() and Path(deltas_pq).exists()
    )

    if args.record is not None or args.refresh or not has_full_book:
        record_secs = args.record if args.record is not None else 60.0
        from trading_engine import KrakenWebSocketRecorder

        if not has_full_book and args.record is None:
            print(
                f"No full L2 order book dataset (with deltas) found in {args.data_dir}.\n"
                f"Recording {record_secs:.0f}s of live L2 book snapshots, deltas, and trades "
                f"via WebSocket v2..."
            )
        else:
            print(f"Recording Kraken WebSocket v2 live feed for {record_secs} seconds...")

        recorder = KrakenWebSocketRecorder(pair=pair_ws)
        session = recorder.record(duration_seconds=record_secs, output_dir=args.data_dir)
    else:
        print(f"Loading cached Kraken L2 Parquet dataset ({pair_ws})...")
        session = client.load_from_parquet(
            depth_pq,
            trades_pq,
            pair=pair_rest,
            deltas_file=deltas_pq,
        )

    print(
        f"Session loaded: {len(session.bids)} bids, {len(session.asks)} asks, "
        f"{len(session.deltas or [])} L2 book deltas, {len(session.trades)} market trades."
    )

    print("=" * 75)
    print("Kraken Real-Market Order Book Backtest (OBI vs. Random Baseline)")
    print("=" * 75)

    n_deltas_orig = len(session.deltas) if session.deltas else 0
    n_trades_orig = len(session.trades)
    n_bids_orig = len(session.bids)
    n_asks_orig = len(session.asks)

    # 1. Run OBI Strategy on deep-copied session
    session_obi = copy.deepcopy(session)
    obi_trades, obi_pnl, obi_dd, obi_history = run_single_kraken_backtest(
        session=session_obi,
        mode="OBI",
        symbol="ETH-USDT",
        max_trades=args.max_trades,
        seed=101,
        verbose=False,
    )
    assert len(session.deltas or []) == n_deltas_orig, "Session deltas mutated during OBI run!"
    assert len(session.trades) == n_trades_orig, "Session trades mutated during OBI run!"
    assert len(session.bids) == n_bids_orig, "Session bids mutated during OBI run!"
    assert len(session.asks) == n_asks_orig, "Session asks mutated during OBI run!"

    # 2. Run Random Baseline on deep-copied session
    print("\n--- Running Random-Entry Same-Exit Baseline on Kraken Market Flow ---")
    session_rand = copy.deepcopy(session)
    rand_trades, rand_pnl, rand_dd, rand_history = run_single_kraken_backtest(
        session=session_rand,
        mode="RANDOM",
        symbol="ETH-USDT",
        max_trades=args.max_trades,
        seed=101,
        verbose=False,
    )
    assert len(session.deltas or []) == n_deltas_orig, "Session deltas mutated during RANDOM run!"
    assert len(session.trades) == n_trades_orig, "Session trades mutated during RANDOM run!"
    assert len(session.bids) == n_bids_orig, "Session bids mutated during RANDOM run!"
    assert len(session.asks) == n_asks_orig, "Session asks mutated during RANDOM run!"

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

    # 4. Plot PnL curves over time
    if not args.no_plot:
        plot_pnl_over_time(
            obi_history=obi_history,
            rand_history=rand_history,
            symbol=pair_ws,
            output_file=args.plot_file,
        )


if __name__ == "__main__":
    main()
