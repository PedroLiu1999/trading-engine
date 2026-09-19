"""Kraken Public Market Data Ingestion & Order Book Replay Module.

Provides:
1. KrakenClient: Fetch live L2 order book depth and paginated public trade streams
2. KrakenMarketSession: Dataclass storing initial L2 depth + chronological trade events
3. Parquet I/O: Ultra-fast columnar storage for long historical tick sessions
4. KrakenOrderBookReplayer: Replays authentic Kraken market liquidity and taker executions
   through the Rust trading engine FIFO matching queues.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from trading_engine import Engine

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


@dataclass
class KrakenTrade:
    price: float
    quantity: float
    timestamp: float
    side: str  # "BUY" or "SELL"
    order_type: str  # "MARKET" or "LIMIT"
    trade_id: int


@dataclass
class KrakenBookDelta:
    timestamp: float
    side: str  # "BUY" or "SELL"
    price: float
    quantity: float  # 0.0 indicates level was removed/cancelled


@dataclass
class KrakenMarketSession:
    pair: str
    captured_at: float
    bids: list[tuple[float, float]]  # (price, quantity)
    asks: list[tuple[float, float]]  # (price, quantity)
    trades: list[KrakenTrade]
    deltas: list[KrakenBookDelta] | None = None


class KrakenClient:
    """Client for Kraken's public REST market data endpoints (no API key required)."""

    BASE_URL = "https://api.kraken.com/0/public"

    def __init__(self, user_agent: str = "trading-engine/1.0", timeout: float = 10.0):
        self.user_agent = user_agent
        self.timeout = timeout

    def _get(
        self, endpoint: str, params: dict[str, Any] | None = None, max_retries: int = 5
    ) -> dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"

        req = urllib.request.Request(url, headers={"User-Agent": self.user_agent})

        for attempt in range(max_retries):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < max_retries - 1:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                raise ConnectionError(f"Failed to connect to Kraken API ({url}): {e}") from e
            except urllib.error.URLError as e:
                if attempt < max_retries - 1:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                raise ConnectionError(f"Failed to connect to Kraken API ({url}): {e}") from e

            errors = data.get("error", [])
            if any("Too many requests" in str(err) for err in errors):
                if attempt < max_retries - 1:
                    time.sleep(2.5 * (attempt + 1))
                    continue
                raise ValueError(f"Kraken API rate limit exceeded: {errors}")

            if errors:
                raise ValueError(f"Kraken API error: {errors}")

            return data["result"]

        raise ConnectionError(f"Exceeded max retries for Kraken API ({url})")

    def fetch_depth(self, pair: str = "ETHUSD", count: int = 100) -> dict[str, Any]:
        """Fetches current L2 order book depth (top `count` bids and asks)."""
        res = self._get("Depth", {"pair": pair, "count": count})
        pair_key = next(k for k in res.keys() if k != "last")
        return res[pair_key]

    def fetch_trades(self, pair: str = "ETHUSD", since: int | None = None) -> list[Any]:
        """Fetches recent public trades (single page up to 1,000 trades)."""
        params: dict[str, Any] = {"pair": pair}
        if since is not None:
            params["since"] = since
        res = self._get("Trades", params)
        pair_key = next(k for k in res.keys() if k != "last")
        return res[pair_key]

    @staticmethod
    def save_to_parquet(
        session: KrakenMarketSession,
        depth_file: str,
        trades_file: str,
        deltas_file: str | None = None,
    ) -> None:
        """Saves session L2 depth, trade history, and optional deltas to compressed Parquet."""
        if not HAS_PYARROW:
            raise ImportError("pyarrow is required for Parquet export. Run `uv add pyarrow`.")

        # 1. Depth Table
        depth_sides = ["bid"] * len(session.bids) + ["ask"] * len(session.asks)
        depth_prices = [p for p, _ in session.bids] + [p for p, _ in session.asks]
        depth_qtys = [q for _, q in session.bids] + [q for _, q in session.asks]

        depth_table = pa.Table.from_arrays(
            [
                pa.array(depth_sides, type=pa.string()),
                pa.array(depth_prices, type=pa.float64()),
                pa.array(depth_qtys, type=pa.float64()),
            ],
            names=["side", "price", "quantity"],
        )
        pq.write_table(depth_table, depth_file, compression="snappy")

        # 2. Trades Table
        trade_prices = [t.price for t in session.trades]
        trade_qtys = [t.quantity for t in session.trades]
        trade_ts = [t.timestamp for t in session.trades]
        trade_sides = [t.side for t in session.trades]
        trade_types = [t.order_type for t in session.trades]
        trade_ids = [t.trade_id for t in session.trades]

        trades_table = pa.Table.from_arrays(
            [
                pa.array(trade_prices, type=pa.float64()),
                pa.array(trade_qtys, type=pa.float64()),
                pa.array(trade_ts, type=pa.float64()),
                pa.array(trade_sides, type=pa.string()),
                pa.array(trade_types, type=pa.string()),
                pa.array(trade_ids, type=pa.int64()),
            ],
            names=["price", "quantity", "timestamp", "side", "order_type", "trade_id"],
        )
        pq.write_table(trades_table, trades_file, compression="snappy")

        # 3. Optional Deltas Table
        if session.deltas and deltas_file:
            d_ts = [d.timestamp for d in session.deltas]
            d_sides = [d.side for d in session.deltas]
            d_prices = [d.price for d in session.deltas]
            d_qtys = [d.quantity for d in session.deltas]
            deltas_table = pa.Table.from_arrays(
                [
                    pa.array(d_ts, type=pa.float64()),
                    pa.array(d_sides, type=pa.string()),
                    pa.array(d_prices, type=pa.float64()),
                    pa.array(d_qtys, type=pa.float64()),
                ],
                names=["timestamp", "side", "price", "quantity"],
            )
            pq.write_table(deltas_table, deltas_file, compression="snappy")

    @staticmethod
    def load_from_parquet(
        depth_file: str,
        trades_file: str,
        pair: str = "ETHUSD",
        deltas_file: str | None = None,
    ) -> KrakenMarketSession:
        """Fast zero-copy loader from Apache Parquet files."""
        if not HAS_PYARROW:
            raise ImportError("pyarrow is required for Parquet import. Run `uv add pyarrow`.")

        depth_table = pq.read_table(depth_file)
        trades_table = pq.read_table(trades_file)

        sides = depth_table["side"].to_pylist()
        prices = depth_table["price"].to_pylist()
        qtys = depth_table["quantity"].to_pylist()

        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []
        for s, p, q in zip(sides, prices, qtys):
            if s == "bid":
                bids.append((p, q))
            else:
                asks.append((p, q))

        t_prices = trades_table["price"].to_pylist()
        t_qtys = trades_table["quantity"].to_pylist()
        t_ts = trades_table["timestamp"].to_pylist()
        t_sides = trades_table["side"].to_pylist()
        t_types = trades_table["order_type"].to_pylist()
        t_ids = trades_table["trade_id"].to_pylist()

        trades: list[KrakenTrade] = []
        for p, q, ts, s, ot, tid in zip(t_prices, t_qtys, t_ts, t_sides, t_types, t_ids):
            trades.append(
                KrakenTrade(
                    price=p,
                    quantity=q,
                    timestamp=ts,
                    side=s,
                    order_type=ot,
                    trade_id=tid,
                )
            )

        deltas: list[KrakenBookDelta] | None = None
        if deltas_file and Path(deltas_file).exists():
            deltas_table = pq.read_table(deltas_file)
            d_ts = deltas_table["timestamp"].to_pylist()
            d_sides = deltas_table["side"].to_pylist()
            d_prices = deltas_table["price"].to_pylist()
            d_qtys = deltas_table["quantity"].to_pylist()
            deltas = [
                KrakenBookDelta(timestamp=ts, side=s, price=p, quantity=q)
                for ts, s, p, q in zip(d_ts, d_sides, d_prices, d_qtys)
            ]

        captured_at = trades[0].timestamp if trades else time.time()
        return KrakenMarketSession(
            pair=pair,
            captured_at=captured_at,
            bids=bids,
            asks=asks,
            trades=trades,
            deltas=deltas,
        )

    @staticmethod
    def save_to_json(session: KrakenMarketSession, output_file: str) -> None:
        """Saves session to JSON format."""
        data = {
            "pair": session.pair,
            "captured_at": session.captured_at,
            "initial_depth": {
                "bids": [[str(p), str(q)] for p, q in session.bids],
                "asks": [[str(p), str(q)] for p, q in session.asks],
            },
            "trades": [
                [
                    str(t.price),
                    str(t.quantity),
                    t.timestamp,
                    "b" if t.side == "BUY" else "s",
                    "m" if t.order_type == "MARKET" else "l",
                    "",
                    t.trade_id,
                ]
                for t in session.trades
            ],
        }
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def load_from_json(file_path: str) -> KrakenMarketSession:
        """Loads a recorded Kraken market dataset from JSON."""
        with open(file_path) as f:
            data = json.load(f)

        pair = data["pair"]
        captured_at = data.get("captured_at", 0.0)
        bids = [(float(p), float(q)) for p, q, *_ in data["initial_depth"]["bids"]]
        asks = [(float(p), float(q)) for p, q, *_ in data["initial_depth"]["asks"]]

        trades: list[KrakenTrade] = []
        for item in data.get("trades", []):
            price = float(item[0])
            qty = float(item[1])
            ts = float(item[2])
            side = "BUY" if item[3] == "b" else "SELL"
            ord_type = "MARKET" if item[4] == "m" else "LIMIT"
            tid = int(item[6]) if len(item) > 6 else 0
            trades.append(
                KrakenTrade(
                    price=price,
                    quantity=qty,
                    timestamp=ts,
                    side=side,
                    order_type=ord_type,
                    trade_id=tid,
                )
            )

        return KrakenMarketSession(
            pair=pair,
            captured_at=captured_at,
            bids=bids,
            asks=asks,
            trades=trades,
        )

    @classmethod
    def load_session(cls, path: str, pair: str = "ETHUSD") -> KrakenMarketSession:
        """Loads a session from either Parquet files or JSON with auto-detection."""
        p = Path(path)
        if p.is_dir():
            depth_pq = p / f"kraken_{pair.lower()}_depth.parquet"
            trades_pq = p / f"kraken_{pair.lower()}_trades.parquet"
            deltas_pq = p / f"kraken_{pair.lower()}_deltas.parquet"
            if depth_pq.exists() and trades_pq.exists():
                return cls.load_from_parquet(
                    str(depth_pq),
                    str(trades_pq),
                    pair=pair,
                    deltas_file=str(deltas_pq) if deltas_pq.exists() else None,
                )

            json_file = p / f"kraken_{pair.lower()}_sample.json"
            if json_file.exists():
                return cls.load_from_json(str(json_file))

        if str(path).endswith(".parquet"):
            base = str(path).replace("_depth.parquet", "").replace("_trades.parquet", "")
            deltas_pq = f"{base}_deltas.parquet"
            return cls.load_from_parquet(
                f"{base}_depth.parquet",
                f"{base}_trades.parquet",
                pair=pair,
                deltas_file=deltas_pq if Path(deltas_pq).exists() else None,
            )

        return cls.load_from_json(path)


def normalize_kraken_ws_pair(pair: str) -> str:
    """Normalizes symbol to Kraken WebSocket API v2 format with slash, e.g. ETH/USD."""
    p = pair.upper().strip()
    if "/" in p:
        return p
    if "-" in p:
        return p.replace("-", "/")
    if "_" in p:
        return p.replace("_", "/")
    for quote in ("USDT", "USDC", "USD", "EUR", "GBP", "JPY", "CAD", "CHF", "AUD", "BTC", "ETH"):
        if p.endswith(quote) and len(p) > len(quote):
            return f"{p[: -len(quote)]}/{quote}"
    return p


class KrakenWebSocketRecorder:
    """Streams and records authentic live Level 2 order book snapshots, deltas,
    and market trades from Kraken WebSocket API v2 directly to Apache Parquet.
    """

    WS_URL = "wss://ws.kraken.com/v2"

    def __init__(self, pair: str = "ETH/USD", depth: int = 100):
        self.pair = normalize_kraken_ws_pair(pair)
        self.depth = depth

    async def record_stream(
        self,
        duration_seconds: float = 60.0,
        max_updates: int | None = None,
        output_dir: str = "examples/data",
    ) -> KrakenMarketSession:
        """Connects to Kraken WebSocket v2, records L2 book deltas and trades,
        and saves the session to Parquet.
        """
        import asyncio

        import websockets

        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []
        deltas: list[KrakenBookDelta] = []
        trades: list[KrakenTrade] = []
        captured_at = time.time()
        update_count = 0
        last_progress_print = 0
        start_time = time.time()

        print(f"Connecting to Kraken WebSocket API v2 ({self.WS_URL})...")
        async with websockets.connect(self.WS_URL, ping_interval=20, ping_timeout=10) as ws:
            book_sub = {
                "method": "subscribe",
                "params": {
                    "channel": "book",
                    "symbol": [self.pair],
                    "depth": self.depth,
                },
            }
            await ws.send(json.dumps(book_sub))

            trade_sub = {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": [self.pair],
                },
            }
            await ws.send(json.dumps(trade_sub))
            print(f"Subscribed to book (depth={self.depth}) and trade channels for {self.pair}...")

            while True:
                now = time.time()
                if duration_seconds is not None and (now - start_time) >= duration_seconds:
                    break
                if max_updates is not None and update_count >= max_updates:
                    break

                try:
                    raw_msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                except TimeoutError:
                    continue

                msg = json.loads(raw_msg)
                if msg.get("method") == "subscribe":
                    if not msg.get("success", True):
                        err = msg.get("error", "Unknown error")
                        print(f"\n[Kraken WS Error] Subscription rejected: {err}")
                    continue

                channel = msg.get("channel")
                msg_type = msg.get("type")
                data_list = msg.get("data", [])

                if channel == "book":
                    if msg_type == "snapshot" and data_list:
                        snap = data_list[0]
                        bids = [(float(b["price"]), float(b["qty"])) for b in snap.get("bids", [])]
                        asks = [(float(a["price"]), float(a["qty"])) for a in snap.get("asks", [])]
                        captured_at = time.time()
                        print(
                            f"[{time.strftime('%H:%M:%S')}] Snapshot received: "
                            f"{len(bids)} bids, {len(asks)} asks."
                        )
                    elif msg_type == "update" and data_list:
                        upd = data_list[0]
                        ts = time.time()
                        for b in upd.get("bids", []):
                            deltas.append(
                                KrakenBookDelta(
                                    timestamp=ts,
                                    side="BUY",
                                    price=float(b["price"]),
                                    quantity=float(b["qty"]),
                                )
                            )
                            update_count += 1
                        for a in upd.get("asks", []):
                            deltas.append(
                                KrakenBookDelta(
                                    timestamp=ts,
                                    side="SELL",
                                    price=float(a["price"]),
                                    quantity=float(a["qty"]),
                                )
                            )
                            update_count += 1

                elif channel == "trade" and data_list:
                    for t in data_list:
                        ts = time.time()
                        side = "BUY" if t.get("side", "").lower() == "buy" else "SELL"
                        trades.append(
                            KrakenTrade(
                                price=float(t["price"]),
                                quantity=float(t["qty"]),
                                timestamp=ts,
                                side=side,
                                order_type="MARKET",
                                trade_id=int(t.get("trade_id", 0)),
                            )
                        )
                        update_count += 1

                if update_count - last_progress_print >= 15:
                    last_progress_print = update_count
                    elapsed = now - start_time
                    rem = max(0.0, (duration_seconds - elapsed)) if duration_seconds else 0.0
                    print(
                        f"\r[{time.strftime('%H:%M:%S')}] Streaming L2 flow: "
                        f"{len(deltas):,} deltas, {len(trades):,} trades "
                        f"({rem:.0f}s remaining)...",
                        end="",
                        flush=True,
                    )

        if not bids and not asks:
            raise RuntimeError(
                f"Failed to record order book data for {self.pair}. "
                "Check that the trading pair is supported on Kraken WebSocket v2."
            )

        print()  # Clear line from carriage return progress
        session = KrakenMarketSession(
            pair=self.pair,
            captured_at=captured_at,
            bids=bids,
            asks=asks,
            trades=trades,
            deltas=deltas,
        )

        os.makedirs(output_dir, exist_ok=True)
        pair_clean = self.pair.lower().replace("/", "").replace("-", "")
        depth_pq = os.path.join(output_dir, f"kraken_{pair_clean}_depth.parquet")
        trades_pq = os.path.join(output_dir, f"kraken_{pair_clean}_trades.parquet")
        deltas_pq = os.path.join(output_dir, f"kraken_{pair_clean}_deltas.parquet")

        KrakenClient.save_to_parquet(session, depth_pq, trades_pq, deltas_file=deltas_pq)
        print(
            f"Saved WebSocket session to Parquet ({len(bids)} bids, {len(asks)} asks, "
            f"{len(deltas)} deltas, {len(trades)} trades) in {output_dir}"
        )
        return session

    def record(
        self,
        duration_seconds: float = 60.0,
        max_updates: int | None = None,
        output_dir: str = "examples/data",
    ) -> KrakenMarketSession:
        """Synchronous wrapper for record_stream."""
        import asyncio

        return asyncio.run(self.record_stream(duration_seconds, max_updates, output_dir))


class KrakenOrderBookReplayer:
    """Replays real Kraken market order book depth and trade flow through the Engine."""

    def __init__(
        self,
        engine: Engine,
        session: KrakenMarketSession,
        symbol: str = "ETH-USDT",
        maker_account: str = "KRAKEN_MAKER",
        taker_account: str = "KRAKEN_TAKER",
    ):
        self.engine = engine
        self.session = session
        self.symbol = symbol
        self.maker_account = maker_account
        self.taker_account = taker_account
        self._current_trade_idx = 0
        self._current_delta_idx = 0

    def apply_deltas_until(self, timestamp: float) -> int:
        """Applies real-time order book additions and updates up to the given timestamp."""
        if not self.session.deltas:
            return 0

        applied = 0
        while self._current_delta_idx < len(self.session.deltas):
            delta = self.session.deltas[self._current_delta_idx]
            if delta.timestamp > timestamp:
                break

            self._current_delta_idx += 1
            applied += 1

            if delta.quantity > 0.0:
                try:
                    self.engine.submit_order(
                        symbol=self.symbol,
                        side=delta.side,
                        order_type="LIMIT",
                        price=delta.price,
                        quantity=delta.quantity,
                        time_in_force="GTC",
                        account_id=self.maker_account,
                    )
                except Exception:
                    pass

        return applied

    def seed_initial_book(self) -> None:
        """Seeds the trading engine's order book with Kraken's authentic bid/ask ladders."""
        # Insert bids from lowest to highest so higher bids rest properly in price-time queue
        for price, qty in reversed(self.session.bids):
            if qty > 0.0 and price > 0.0:
                try:
                    self.engine.submit_order(
                        symbol=self.symbol,
                        side="BUY",
                        order_type="LIMIT",
                        price=price,
                        quantity=qty,
                        time_in_force="GTC",
                        account_id=self.maker_account,
                    )
                except Exception:
                    pass

        # Insert asks from highest to lowest
        for price, qty in reversed(self.session.asks):
            if qty > 0.0 and price > 0.0:
                try:
                    self.engine.submit_order(
                        symbol=self.symbol,
                        side="SELL",
                        order_type="LIMIT",
                        price=price,
                        quantity=qty,
                        time_in_force="GTC",
                        account_id=self.maker_account,
                    )
                except Exception:
                    pass

    def has_next_trade(self) -> bool:
        return self._current_trade_idx < len(self.session.trades)

    def replay_next_trade(self) -> KrakenTrade | None:
        """Replays the next real market taker trade through the engine."""
        if not self.has_next_trade():
            return None

        trade = self.session.trades[self._current_trade_idx]
        self._current_trade_idx += 1

        # Real market taker flow crosses the book as a MARKET IOC order
        try:
            self.engine.submit_order(
                symbol=self.symbol,
                side=trade.side,
                order_type="MARKET",
                price=0.0,
                quantity=trade.quantity,
                time_in_force="IOC",
                account_id=self.taker_account,
            )
        except Exception:
            pass

        return trade

    @property
    def total_trades(self) -> int:
        return len(self.session.trades)

    @property
    def progress(self) -> float:
        return self._current_trade_idx / len(self.session.trades) if self.session.trades else 1.0
