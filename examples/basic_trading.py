"""Basic Trading Engine Example

Demonstrates:
1. Initializing Engine with custom RiskConfig
2. Registering instruments
3. Seeding order book liquidity
4. Executing market and limit orders
5. Tracking positions, PnL, and account margin
6. Polling event bus
"""

import json

from trading_engine import Engine, RiskConfig


def main():
    print("=" * 60)
    print("Rust Trading Engine - Python API Example")
    print("=" * 60)

    # 1. Initialize Engine
    risk = RiskConfig(
        max_order_qty=100.0,
        max_order_notional=500_000.0,
        max_position_notional=1_000_000.0,
        price_collar_pct=0.10,  # 10% price collar
        max_drawdown_pct=0.20,
        max_orders_per_sec=1000,
        require_margin=True,
    )

    engine = Engine(initial_balance=100_000.0, leverage=2.0, risk_config=risk)
    symbol = "BTC-USDT"
    engine.register_symbol(symbol, tick_size=0.50, lot_size=0.001)
    print(f"Initialized engine with $100,000 balance and registered '{symbol}'\n")

    # 2. Provide resting liquidity
    print("--- 1. Seeding Order Book ---")
    engine.submit_order(symbol, "BUY", "LIMIT", 64_900.0, 1.0)
    engine.submit_order(symbol, "BUY", "LIMIT", 65_000.0, 2.0)
    engine.submit_order(symbol, "SELL", "LIMIT", 65_100.0, 1.5)
    engine.submit_order(symbol, "SELL", "LIMIT", 65_200.0, 3.0)

    depth = engine.get_depth(symbol, levels=5)
    print(f"Best Bid: ${depth.best_bid():,.2f} | Best Ask: ${depth.best_ask():,.2f}")
    print(f"Mid Price: ${depth.mid_price():,.2f} | Spread: ${depth.spread():,.2f}\n")

    # 3. Submit Market Order (Taker)
    print("--- 2. Submitting Market Buy Order (1.0 BTC) ---")
    order = engine.submit_order(
        symbol=symbol,
        side="BUY",
        order_type="MARKET",
        price=0.0,
        quantity=1.0,
        time_in_force="IOC",
        client_order_id="algo_buy_001",
    )
    print(f"Order status: {order.status} | Filled: {order.filled_quantity} / {order.quantity}")

    # 4. Check Position & Account
    pos = engine.get_position(symbol)
    acct = engine.get_account()
    print("\n--- 3. Position & Account Summary ---")
    print(f"Position: {pos.quantity} {symbol} @ Avg Entry ${pos.avg_entry_price:,.2f}")
    print(f"Realized PnL: ${pos.realized_pnl:,.2f} | Unrealized PnL: ${pos.unrealized_pnl:,.2f}")
    print(f"Cash Balance: ${acct.cash_balance:,.2f} | Margin Used: ${acct.margin_used:,.2f}")

    # 5. Mark to market at new price
    print("\n--- 4. Price moves up to $66,000 ---")
    engine.mark_to_market(symbol, 66_000.0)
    pos = engine.get_position(symbol)
    print(f"New Unrealized PnL: ${pos.unrealized_pnl:,.2f}")
    print(f"Account Equity: ${acct.equity(pos.unrealized_pnl):,.2f}")

    # 6. Event Bus Inspection
    print("\n--- 5. Draining Event Bus ---")
    events = engine.poll_events(max_count=50)
    print(f"Received {len(events)} events from Rust event bus:")
    for idx, raw in enumerate(events[:5], 1):
        parsed = json.loads(raw)
        print(f"  [{idx}] {list(parsed.keys())[0]}")


if __name__ == "__main__":
    main()
