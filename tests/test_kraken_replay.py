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


def test_kraken_order_book_replayer():
    """Verifies that KrakenOrderBookReplayer seeds the book and replays trades correctly."""
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
            # Market buy 1.5 ETH -> should consume 1.5 from ask @ 3001.0
            KrakenTrade(
                price=3001.0,
                quantity=1.5,
                timestamp=1700000001.0,
                side="BUY",
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

    # 2. Replay real taker trade
    assert replayer.has_next_trade()
    replayed = replayer.replay_next_trade()
    assert replayed is not None
    assert replayed.quantity == 1.5

    # 3. Check remaining ask depth
    depth_after = engine.get_depth("ETH-USDT", levels=2)
    assert depth_after is not None
    assert depth_after.best_ask() == 3001.0
    # Remaining ask quantity at 3001.0 should be 3.0 - 1.5 = 1.5
    assert abs(depth_after.asks[0].quantity - 1.5) < 1e-4

    # 4. Check account ledger attribution
    maker_acct = engine.get_account("KRAKEN_MAKER")
    taker_acct = engine.get_account("KRAKEN_TAKER")
    assert maker_acct is not None
    assert taker_acct is not None
    # Zero-sum check
    assert abs(maker_acct.realized_pnl + taker_acct.realized_pnl) < 1e-4


def test_kraken_session_deltas_roundtrip():
    """Verifies that KrakenBookDelta saves and loads from Parquet correctly."""
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
                side="BUY",
                order_type="MARKET",
                trade_id=1,
            )
        ],
        deltas=[
            KrakenBookDelta(timestamp=1700000002.0, side="BUY", price=2599.5, quantity=3.0),
            KrakenBookDelta(timestamp=1700000003.0, side="SELL", price=2602.0, quantity=4.5),
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
        assert loaded.deltas[0].side == "BUY"
        assert loaded.deltas[1].price == 2602.0
        assert loaded.deltas[1].quantity == 4.5
        assert loaded.deltas[1].side == "SELL"


def test_kraken_order_book_replayer_with_deltas():
    """Verifies that replayer applies real-time order book deltas in chronological order."""
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
                side="BUY",
                order_type="MARKET",
                trade_id=1,
            )
        ],
        deltas=[
            # Maker introduces tighter ask @ 3002.0 before the trade
            KrakenBookDelta(timestamp=1700000005.0, side="SELL", price=3002.0, quantity=2.0),
            # Maker introduces ask @ 3003.0 AFTER the trade
            KrakenBookDelta(timestamp=1700000015.0, side="SELL", price=3003.0, quantity=5.0),
        ],
    )

    replayer = KrakenOrderBookReplayer(
        engine=engine,
        session=session,
        symbol="ETH-USDT",
    )
    replayer.seed_initial_book()

    # Before applying deltas up to 1700000010, best ask is 3005.0
    depth0 = engine.get_depth("ETH-USDT", levels=2)
    assert depth0 is not None
    assert depth0.best_ask() == 3005.0

    # Apply deltas up to trade timestamp 1700000010.0
    applied = replayer.apply_deltas_until(1700000010.0)
    assert applied == 1

    # Now best ask should be 3002.0 from the applied delta
    depth1 = engine.get_depth("ETH-USDT", levels=2)
    assert depth1 is not None
    assert depth1.best_ask() == 3002.0

    # Trade executes against new best ask 3002.0
    replayed = replayer.replay_next_trade()
    assert replayed is not None

    depth2 = engine.get_depth("ETH-USDT", levels=2)
    assert depth2 is not None
    # 2.0 - 1.0 = 1.0 remaining at 3002.0
    assert depth2.best_ask() == 3002.0
    assert abs(depth2.asks[0].quantity - 1.0) < 1e-4
