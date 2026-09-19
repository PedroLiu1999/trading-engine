"""Unit and integration tests for Kraken Order Book replay and Parquet I/O."""

import tempfile
from pathlib import Path

from trading_engine import (
    Engine,
    KrakenBookDelta,
    KrakenClient,
    KrakenMarketSession,
    KrakenOrderBookReplayer,
    KrakenTrade,
    RiskConfig,
)


def test_kraken_session_parquet_roundtrip():
    """Verifies that KrakenMarketSession saves and loads from Parquet identically."""
    session = KrakenMarketSession(
        pair="ETHUSD",
        captured_at=1700000000.0,
        bids=[(2600.0, 1.5), (2599.0, 3.0), (2598.0, 5.0)],
        asks=[(2601.0, 2.0), (2602.0, 4.0), (2603.0, 6.0)],
        trades=[
            KrakenTrade(
                price=2601.0,
                quantity=0.5,
                timestamp=1700000001.0,
                side="BUY",
                order_type="MARKET",
                trade_id=101,
            ),
            KrakenTrade(
                price=2600.0,
                quantity=1.0,
                timestamp=1700000002.0,
                side="SELL",
                order_type="MARKET",
                trade_id=102,
            ),
        ],
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        depth_path = str(Path(tmpdir) / "depth.parquet")
        trades_path = str(Path(tmpdir) / "trades.parquet")

        KrakenClient.save_to_parquet(session, depth_path, trades_path)
        loaded = KrakenClient.load_from_parquet(depth_path, trades_path, pair="ETHUSD")

        assert loaded.pair == "ETHUSD"
        assert len(loaded.bids) == 3
        assert len(loaded.asks) == 3
        assert loaded.bids[0] == (2600.0, 1.5)
        assert loaded.asks[0] == (2601.0, 2.0)
        assert len(loaded.trades) == 2
        assert loaded.trades[0].price == 2601.0
        assert loaded.trades[0].quantity == 0.5
        assert loaded.trades[0].side == "BUY"
        assert loaded.trades[0].trade_id == 101


def test_kraken_order_book_replayer_try_fill_resting():
    """Verifies that try_fill_resting fills resting orders without mutating
    KRAKEN_MAKER liquidity.
    """
    risk = RiskConfig(
        max_order_qty=100.0,
        max_order_notional=1_000_000.0,
        max_position_notional=2_000_000.0,
        price_collar_pct=0.50,
        max_drawdown_pct=0.99,
        max_orders_per_sec=0,
        require_margin=False,
    )
    engine = Engine(initial_balance=100_000.0, leverage=2.0, risk_config=risk)
    engine.register_symbol("ETH-USDT", tick_size=0.10, lot_size=0.01)

    session = KrakenMarketSession(
        pair="ETHUSD",
        captured_at=1700000000.0,
        bids=[(3000.0, 2.0), (2999.0, 5.0)],
        asks=[(3001.0, 3.0), (3002.0, 6.0)],
        trades=[
            # Market sell 1.5 ETH @ 3000.0 -> crosses resting buy @ 3000.0
            KrakenTrade(
                price=3000.0,
                quantity=1.5,
                timestamp=1700000001.0,
                seq=5,
                side="SELL",
                order_type="MARKET",
                trade_id=1,
            ),
        ],
    )

    replayer = KrakenOrderBookReplayer(
        engine=engine,
        session=session,
        symbol="ETH-USDT",
        maker_account="KRAKEN_MAKER",
        taker_account="KRAKEN_TAKER",
    )

    # 1. Seed resting book
    replayer.seed_initial_book()

    depth = engine.get_depth("ETH-USDT", levels=2)
    assert depth is not None
    assert depth.best_bid() == 3000.0
    assert depth.best_ask() == 3001.0
    assert depth.bids[0].quantity == 2.0

    # 2. Strategy submits a resting BUY order at 3000.0
    strat_order = engine.submit_order(
        symbol="ETH-USDT",
        side="BUY",
        order_type="LIMIT",
        price=3000.0,
        quantity=1.0,
        time_in_force="GTC",
        account_id="STRAT_ACCT",
    )
    assert strat_order.id > 0

    # 3. Market taker sell trade crosses strategy's resting order
    assert replayer.has_next_trade()
    trade = session.trades[0]
    fill = replayer.try_fill_resting(trade, strat_order.id)
    assert fill is not None
    assert fill.quantity == 1.0
    assert fill.price == 3000.0

    # Strategy position is updated
    strat_pos = engine.get_position("ETH-USDT", account_id="STRAT_ACCT")
    assert strat_pos is not None
    assert abs(strat_pos.quantity - 1.0) < 1e-4

    # Crucial P0 test: KRAKEN_MAKER liquidity is NOT double-counted / depleted by the taker trade!
    depth_after = engine.get_depth("ETH-USDT", levels=2)
    assert depth_after is not None
    # Maker bids at 3000.0 remain 2.0 (only deltas mutate maker liquidity)
    maker_bid_qty = next(b.quantity for b in depth_after.bids if b.price == 3000.0)
    assert abs(maker_bid_qty - 2.0) < 1e-4


def test_kraken_session_deltas_roundtrip():
    """Verifies that KrakenBookDelta saves and loads from Parquet correctly with seq."""
    session = KrakenMarketSession(
        pair="ETHUSD",
        captured_at=1700000000.0,
        bids=[(2600.0, 1.0)],
        asks=[(2601.0, 1.0)],
        trades=[
            KrakenTrade(
                price=2601.0,
                quantity=0.5,
                timestamp=1700000005.0,
                seq=10,
                side="BUY",
                order_type="MARKET",
                trade_id=1,
            )
        ],
        deltas=[
            KrakenBookDelta(timestamp=1700000002.0, seq=1, side="BUY", price=2599.5, quantity=3.0),
            KrakenBookDelta(timestamp=1700000003.0, seq=2, side="SELL", price=2602.0, quantity=4.5),
        ],
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        depth_path = str(Path(tmpdir) / "depth.parquet")
        trades_path = str(Path(tmpdir) / "trades.parquet")
        deltas_path = str(Path(tmpdir) / "deltas.parquet")

        KrakenClient.save_to_parquet(session, depth_path, trades_path, deltas_file=deltas_path)
        loaded = KrakenClient.load_from_parquet(
            depth_path, trades_path, pair="ETHUSD", deltas_file=deltas_path
        )

        assert loaded.deltas is not None
        assert len(loaded.deltas) == 2
        assert loaded.deltas[0].price == 2599.5
        assert loaded.deltas[0].quantity == 3.0
        assert loaded.deltas[0].seq == 1
        assert loaded.deltas[0].side == "BUY"
        assert loaded.deltas[1].price == 2602.0
        assert loaded.deltas[1].quantity == 4.5
        assert loaded.deltas[1].seq == 2
        assert loaded.deltas[1].side == "SELL"


def test_kraken_order_book_replayer_strict_deltas():
    """Verifies that replayer applies real-time deltas strictly before trades with
    seq tie-breaking.
    """
    engine = Engine(initial_balance=100_000.0, leverage=2.0)
    engine.register_symbol("ETH-USDT", tick_size=0.10, lot_size=0.01)

    session = KrakenMarketSession(
        pair="ETHUSD",
        captured_at=1700000000.0,
        bids=[(3000.0, 2.0)],
        asks=[(3005.0, 3.0)],
        trades=[
            KrakenTrade(
                price=3002.0,
                quantity=1.0,
                timestamp=1700000010.0,
                seq=10,
                side="BUY",
                order_type="MARKET",
                trade_id=1,
            )
        ],
        deltas=[
            # Delta 1: strictly before (timestamp < trade.timestamp)
            KrakenBookDelta(timestamp=1700000005.0, seq=5, side="SELL", price=3002.0, quantity=2.0),
            # Delta 2: same timestamp but before trade (seq=8 < trade.seq=10)
            KrakenBookDelta(timestamp=1700000010.0, seq=8, side="BUY", price=3001.0, quantity=1.0),
            # Delta 3: same timestamp but AFTER trade (seq=12 >= trade.seq=10) -> must NOT apply
            KrakenBookDelta(
                timestamp=1700000010.0, seq=12, side="SELL", price=3001.5, quantity=4.0
            ),
            # Delta 4: strictly after (timestamp > trade.timestamp) -> must NOT apply
            KrakenBookDelta(
                timestamp=1700000015.0, seq=15, side="SELL", price=3003.0, quantity=5.0
            ),
        ],
    )

    replayer = KrakenOrderBookReplayer(
        engine=engine,
        session=session,
        symbol="ETH-USDT",
    )
    replayer.seed_initial_book()

    # Initial state: best ask 3005.0, best bid 3000.0
    depth0 = engine.get_depth("ETH-USDT", levels=2)
    assert depth0 is not None
    assert depth0.best_ask() == 3005.0
    assert depth0.best_bid() == 3000.0

    # Apply deltas strictly prior to trade (timestamp 1700000010.0, seq 10)
    applied = replayer.apply_deltas_until(1700000010.0, seq=10)
    # Delta 1 and Delta 2 should apply; Delta 3 and Delta 4 must NOT apply
    assert applied == 2

    depth1 = engine.get_depth("ETH-USDT", levels=2)
    assert depth1 is not None
    # Best ask should be 3002.0 from Delta 1
    assert depth1.best_ask() == 3002.0
    # Best bid should be 3001.0 from Delta 2
    assert depth1.best_bid() == 3001.0
    # Delta 3 (ask @ 3001.5) must NOT be present (no lookahead!)
    assert all(a.price != 3001.5 for a in depth1.asks)

    # Now apply deltas after trade (e.g. timestamp 1700000020.0)
    applied2 = replayer.apply_deltas_until(1700000020.0, seq=20)
    assert applied2 == 2  # Remaining 2 deltas applied

    depth2 = engine.get_depth("ETH-USDT", levels=3)
    assert depth2 is not None
    assert any(a.price == 3001.5 for a in depth2.asks)
