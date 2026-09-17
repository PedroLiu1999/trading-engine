import json

import pytest

try:
    import trading_engine
    from trading_engine import (
        Engine,
        Order,
        OrderStatus,
        OrderType,
        Position,
        RiskConfig,
        Side,
        TimeInForce,
    )
except ImportError:
    trading_engine = None


def test_module_exports():
    if trading_engine is None:
        pytest.skip("trading_engine extension module is not built")
    assert Order is not None
    assert OrderStatus is not None
    assert OrderType is not None
    assert Position is not None
    assert Side is not None
    assert TimeInForce is not None
    assert RiskConfig is not None
    assert Engine is not None


@pytest.fixture
def engine():
    if trading_engine is None:
        pytest.skip("trading_engine extension module is not built")
    config = RiskConfig(
        max_order_qty=100.0,
        max_order_notional=500_000.0,
        max_position_notional=1_000_000.0,
        price_collar_pct=0.10,
        max_drawdown_pct=0.20,
        max_orders_per_sec=1000,
        require_margin=True,
    )
    eng = Engine(initial_balance=100_000.0, leverage=1.0, risk_config=config)
    eng.register_symbol("BTC-USDT", tick_size=0.50, lot_size=0.001)
    return eng


def test_order_submission_and_depth(engine):
    # Seed ask
    ask = engine.submit_order(
        symbol="BTC-USDT",
        side="SELL",
        order_type="LIMIT",
        price=50_100.0,
        quantity=1.0,
        time_in_force="GTC",
        client_order_id="ask_1",
    )
    assert ask.symbol == "BTC-USDT"
    assert ask.price == 50_100.0
    assert ask.quantity == 1.0

    # Seed bid
    bid = engine.submit_order(
        symbol="BTC-USDT",
        side="BUY",
        order_type="LIMIT",
        price=50_000.0,
        quantity=2.0,
        time_in_force="GTC",
        client_order_id="bid_1",
    )
    assert bid.price == 50_000.0

    depth = engine.get_depth("BTC-USDT", levels=5)
    assert depth is not None
    assert depth.best_bid() == 50_000.0
    assert depth.best_ask() == 50_100.0
    assert depth.mid_price() == 50_050.0
    assert depth.spread() == 100.0


def test_market_order_fill_and_position(engine):
    # Provide resting ask liquidity
    engine.submit_order(
        symbol="BTC-USDT",
        side="SELL",
        order_type="LIMIT",
        price=50_000.0,
        quantity=0.5,
        time_in_force="GTC",
    )

    # Execute market buy
    taker = engine.submit_order(
        symbol="BTC-USDT",
        side="BUY",
        order_type="MARKET",
        price=0.0,
        quantity=0.5,
        time_in_force="IOC",
    )
    assert taker.status == OrderStatus.Filled
    assert taker.filled_quantity == 0.5

    pos = engine.get_position("BTC-USDT")
    assert pos is not None
    assert pos.quantity == 0.5
    assert pos.avg_entry_price == 50_000.0
    assert pos.is_long() is True


def test_price_collar_risk_rejection(engine):
    # Seed bid and ask to establish mid price ~ 50,000
    engine.submit_order(
        symbol="BTC-USDT",
        side="BUY",
        order_type="LIMIT",
        price=49_990.0,
        quantity=1.0,
    )
    engine.submit_order(
        symbol="BTC-USDT",
        side="SELL",
        order_type="LIMIT",
        price=50_010.0,
        quantity=1.0,
    )

    # Attempt to place order 20% above mid (collar is 10%)
    with pytest.raises(RuntimeError) as exc_info:
        engine.submit_order(
            symbol="BTC-USDT",
            side="BUY",
            order_type="LIMIT",
            price=60_000.0,
            quantity=0.1,
        )
    assert "PriceCollarBreached" in str(exc_info.value) or "price" in str(exc_info.value).lower()


def test_kill_switch(engine):
    assert engine.is_kill_switch_active() is False
    engine.trip_kill_switch("Emergency halt test")
    assert engine.is_kill_switch_active() is True

    with pytest.raises(RuntimeError) as exc:
        engine.submit_order(
            symbol="BTC-USDT",
            side="BUY",
            order_type="LIMIT",
            price=50_000.0,
            quantity=0.1,
        )
    assert "Kill switch is active" in str(exc.value)

    engine.reset_kill_switch()
    assert engine.is_kill_switch_active() is False


def test_event_bus_polling(engine):
    engine.submit_order(
        symbol="BTC-USDT",
        side="BUY",
        order_type="LIMIT",
        price=49_500.0,
        quantity=1.0,
    )
    events = engine.poll_events(max_count=20)
    assert len(events) > 0
    parsed = [json.loads(e) for e in events]
    assert any("OrderSubmitted" in e for e in parsed)
