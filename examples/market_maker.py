"""Simple Two-Sided Market Making Bot

Demonstrates:
- Continuous quoting on both sides of the book
- Inventory skew (adjusting bid/ask spread based on current inventory)
- Handling fills and tracking realized PnL
"""

from trading_engine import Engine, RiskConfig


def run_market_maker():
    print("Starting Market Maker Simulation...")
    risk = RiskConfig(
        max_order_qty=50.0,
        max_order_notional=500_000.0,
        max_position_notional=1_000_000.0,
        price_collar_pct=0.10,
        max_drawdown_pct=0.15,
        max_orders_per_sec=1000,
        require_margin=True,
    )

    engine = Engine(initial_balance=200_000.0, leverage=3.0, risk_config=risk)
    symbol = "ETH-USDT"
    engine.register_symbol(symbol, tick_size=0.10, lot_size=0.01)

    fair_price = 3000.0
    spread = 4.0
    quote_size = 2.0

    print(f"Fair Price: ${fair_price:.2f}, Target Spread: ${spread:.2f}")

    # Quote 5 cycles simulating market activity
    for cycle in range(1, 6):
        pos = engine.get_position(symbol)
        inv = pos.quantity if pos else 0.0

        # Inventory skew: if long, lower quotes to sell; if short, raise quotes to buy
        skew = -0.2 * inv
        bid_price = round(fair_price - (spread / 2.0) + skew, 1)
        ask_price = round(fair_price + (spread / 2.0) + skew, 1)

        print(f"\n[Cycle {cycle}] Current Inventory: {inv:.2f} ETH (Skew: {skew:.2f})")
        print(
            f"  Quoting: BID {quote_size} @ ${bid_price:.2f} | ASK {quote_size} @ ${ask_price:.2f}"
        )

        bid = engine.submit_order(symbol, "BUY", "LIMIT", bid_price, quote_size)
        ask = engine.submit_order(symbol, "SELL", "LIMIT", ask_price, quote_size)

        # Simulate incoming market order from external taker
        if cycle % 2 == 1:
            # External buyer lifts our ask
            engine.submit_order(symbol, "BUY", "MARKET", 0.0, quote_size, "IOC")
            print(f"  -> External market buy filled our resting ask @ ${ask_price:.2f}!")
        else:
            # External seller hits our bid
            engine.submit_order(symbol, "SELL", "MARKET", 0.0, quote_size, "IOC")
            print(f"  -> External market sell filled our resting bid @ ${bid_price:.2f}!")

        # Clean up unexecuted orders
        try:
            engine.cancel_order(symbol, bid.id)
        except KeyError:
            pass
        try:
            engine.cancel_order(symbol, ask.id)
        except KeyError:
            pass

    pos = engine.get_position(symbol)
    acct = engine.get_account()
    print("\n" + "=" * 50)
    print("Market Maker Summary:")
    print(f"Final Inventory: {pos.quantity:.2f} {symbol}")
    print(f"Realized PnL: ${pos.realized_pnl:,.2f}")
    print(f"Account Balance: ${acct.cash_balance:,.2f}")
    print("=" * 50)


if __name__ == "__main__":
    run_market_maker()
