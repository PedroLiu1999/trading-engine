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
    seq: int = 0


@dataclass
class KrakenBookDelta:
    timestamp: float
    side: str  # "BUY" or "SELL"
    price: float
    quantity: float  # 0.0 indicates level was removed/cancelled
    seq: int = 0


@dataclass
class KrakenMarketSession:
    pair: str
    captured_at: float
    bids: list[tuple[float, float]]  # (price, quantity)
    asks: list[tuple[float, float]]  # (price, quantity)
    trades: list[KrakenTrade]
    deltas: list[KrakenBookDelta] | None = None


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


def normalize_kraken_rest_pair(pair: str) -> str:
    """Normalizes symbol to Kraken REST API format without slash, e.g. ETHUSD."""
    return pair.upper().strip().replace("/", "").replace("-", "").replace("_", "")


def extract_kraken_pair_result(data: dict[str, Any], rest_pair: str) -> Any:
    """Extracts pair data from Kraken REST response without brittle key guessing."""
    if rest_pair in data:
        return data[rest_pair]
    # Check known Kraken prefixes: X/Z (e.g. XETHZUSD, XXBTZUSD)
    for key, val in data.items():
        if key in ("last", "count"):
            continue
        clean_key = key.replace("X", "").replace("Z", "")
        clean_target = rest_pair.replace("X", "").replace("Z", "")
        if clean_key == clean_target or key == rest_pair:
            return val
    # Fallback to the first non-metadata dictionary entry
    candidates = [k for k in data if k not in ("last", "count")]
    if candidates:
        return data[candidates[0]]
    raise KeyError(f"Pair '{rest_pair}' not found in Kraken response keys: {list(data.keys())}")


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
        rest_pair = normalize_kraken_rest_pair(pair)
        res = self._get("Depth", {"pair": rest_pair, "count": count})
        return extract_kraken_pair_result(res, rest_pair)

    def fetch_trades(self, pair: str = "ETHUSD", since: int | None = None) -> list[Any]:
        """Fetches recent public trades (single page up to 1,000 trades)."""
        rest_pair = normalize_kraken_rest_pair(pair)
        params: dict[str, Any] = {"pair": rest_pair}
        if since is not None:
            params["since"] = since
        res = self._get("Trades", params)
        return extract_kraken_pair_result(res, rest_pair)

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
        trade_seqs = [t.seq for t in session.trades]
        trade_sides = [t.side for t in session.trades]
        trade_types = [t.order_type for t in session.trades]
        trade_ids = [t.trade_id for t in session.trades]

        trades_table = pa.Table.from_arrays(
            [
                pa.array(trade_prices, type=pa.float64()),
                pa.array(trade_qtys, type=pa.float64()),
                pa.array(trade_ts, type=pa.float64()),
                pa.array(trade_seqs, type=pa.int64()),
                pa.array(trade_sides, type=pa.string()),
                pa.array(trade_types, type=pa.string()),
                pa.array(trade_ids, type=pa.int64()),
            ],
            names=["price", "quantity", "timestamp", "seq", "side", "order_type", "trade_id"],
        )
        pq.write_table(trades_table, trades_file, compression="snappy")

        # 3. Optional Deltas Table
        if session.deltas and deltas_file:
            d_ts = [d.timestamp for d in session.deltas]
            d_seqs = [d.seq for d in session.deltas]
            d_sides = [d.side for d in session.deltas]
            d_prices = [d.price for d in session.deltas]
            d_qtys = [d.quantity for d in session.deltas]
            deltas_table = pa.Table.from_arrays(
                [
                    pa.array(d_ts, type=pa.float64()),
                    pa.array(d_seqs, type=pa.int64()),
                    pa.array(d_sides, type=pa.string()),
                    pa.array(d_prices, type=pa.float64()),
                    pa.array(d_qtys, type=pa.float64()),
                ],
                names=["timestamp", "seq", "side", "price", "quantity"],
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
        t_seqs = (
            trades_table["seq"].to_pylist()
            if "seq" in trades_table.column_names
            else [0] * len(t_ts)
        )
        t_sides = trades_table["side"].to_pylist()
        t_types = trades_table["order_type"].to_pylist()
        t_ids = trades_table["trade_id"].to_pylist()

        trades: list[KrakenTrade] = []
        for p, q, ts, sq, s, ot, tid in zip(
            t_prices, t_qtys, t_ts, t_seqs, t_sides, t_types, t_ids
        ):
            trades.append(
                KrakenTrade(
                    price=p,
                    quantity=q,
                    timestamp=ts,
                    side=s,
                    order_type=ot,
                    trade_id=tid,
                    seq=sq,
                )
            )

        deltas: list[KrakenBookDelta] | None = None
        if deltas_file and Path(deltas_file).exists():
            deltas_table = pq.read_table(deltas_file)
            d_ts = deltas_table["timestamp"].to_pylist()
            d_seqs = (
                deltas_table["seq"].to_pylist()
                if "seq" in deltas_table.column_names
                else [0] * len(d_ts)
            )
            d_sides = deltas_table["side"].to_pylist()
            d_prices = deltas_table["price"].to_pylist()
            d_qtys = deltas_table["quantity"].to_pylist()
            deltas = [
                KrakenBookDelta(timestamp=ts, seq=sq, side=s, price=p, quantity=q)
                for ts, sq, s, p, q in zip(d_ts, d_seqs, d_sides, d_prices, d_qtys)
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
        flush_interval_records: int = 250,
    ) -> KrakenMarketSession:
        """Connects to Kraken WebSocket v2, streams L2 book deltas and trades directly
        to compressed Parquet on disk with incremental chunk flushes and Ctrl+C safety.
        """
        import asyncio

        import websockets

        if not HAS_PYARROW:
            raise ImportError("pyarrow is required for Parquet export. Run `uv add pyarrow`.")

        os.makedirs(output_dir, exist_ok=True)
        pair_clean = self.pair.lower().replace("/", "").replace("-", "")
        depth_pq = os.path.join(output_dir, f"kraken_{pair_clean}_depth.parquet")
        trades_pq = os.path.join(output_dir, f"kraken_{pair_clean}_trades.parquet")
        deltas_pq = os.path.join(output_dir, f"kraken_{pair_clean}_deltas.parquet")

        delta_schema = pa.schema(
            [
                ("timestamp", pa.float64()),
                ("seq", pa.int64()),
                ("side", pa.string()),
                ("price", pa.float64()),
                ("quantity", pa.float64()),
            ]
        )
        trade_schema = pa.schema(
            [
                ("price", pa.float64()),
                ("quantity", pa.float64()),
                ("timestamp", pa.float64()),
                ("seq", pa.int64()),
                ("side", pa.string()),
                ("order_type", pa.string()),
                ("trade_id", pa.int64()),
            ]
        )

        delta_writer = pq.ParquetWriter(deltas_pq, delta_schema, compression="snappy")
        trade_writer = pq.ParquetWriter(trades_pq, trade_schema, compression="snappy")

        bids: list[tuple[float, float]] = []
        asks: list[tuple[float, float]] = []
        deltas: list[KrakenBookDelta] = []
        trades: list[KrakenTrade] = []
        delta_buffer: list[KrakenBookDelta] = []
        trade_buffer: list[KrakenTrade] = []

        captured_at = time.time()
        monotonic_seq = 0
        update_count = 0
        last_progress_print = 0
        last_flush_time = time.time()
        start_time = time.time()

        def flush_buffers() -> None:
            nonlocal delta_buffer, trade_buffer, last_flush_time
            if delta_buffer:
                d_table = pa.Table.from_arrays(
                    [
                        pa.array([d.timestamp for d in delta_buffer], type=pa.float64()),
                        pa.array([d.seq for d in delta_buffer], type=pa.int64()),
                        pa.array([d.side for d in delta_buffer], type=pa.string()),
                        pa.array([d.price for d in delta_buffer], type=pa.float64()),
                        pa.array([d.quantity for d in delta_buffer], type=pa.float64()),
                    ],
                    schema=delta_schema,
                )
                delta_writer.write_table(d_table)
                delta_buffer.clear()

            if trade_buffer:
                t_table = pa.Table.from_arrays(
                    [
                        pa.array([t.price for t in trade_buffer], type=pa.float64()),
                        pa.array([t.quantity for t in trade_buffer], type=pa.float64()),
                        pa.array([t.timestamp for t in trade_buffer], type=pa.float64()),
                        pa.array([t.seq for t in trade_buffer], type=pa.int64()),
                        pa.array([t.side for t in trade_buffer], type=pa.string()),
                        pa.array([t.order_type for t in trade_buffer], type=pa.string()),
                        pa.array([t.trade_id for t in trade_buffer], type=pa.int64()),
                    ],
                    schema=trade_schema,
                )
                trade_writer.write_table(t_table)
                trade_buffer.clear()
            last_flush_time = time.time()

        print(f"Connecting to Kraken WebSocket API v2 ({self.WS_URL})...")
        try:
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
                print(
                    f"Subscribed to book (depth={self.depth}) and trade channels for {self.pair}..."
                )

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
                            bids = [
                                (float(b["price"]), float(b["qty"])) for b in snap.get("bids", [])
                            ]
                            asks = [
                                (float(a["price"]), float(a["qty"])) for a in snap.get("asks", [])
                            ]
                            captured_at = time.time()

                            # Immediately write initial depth table to Parquet
                            depth_sides = ["bid"] * len(bids) + ["ask"] * len(asks)
                            depth_prices = [p for p, _ in bids] + [p for p, _ in asks]
                            depth_qtys = [q for _, q in bids] + [q for _, q in asks]
                            depth_table = pa.Table.from_arrays(
                                [
                                    pa.array(depth_sides, type=pa.string()),
                                    pa.array(depth_prices, type=pa.float64()),
                                    pa.array(depth_qtys, type=pa.float64()),
                                ],
                                names=["side", "price", "quantity"],
                            )
                            pq.write_table(depth_table, depth_pq, compression="snappy")

                            print(
                                f"[{time.strftime('%H:%M:%S')}] Snapshot received and flushed to "
                                f"disk ({len(bids)} bids, {len(asks)} asks)."
                            )
                        elif msg_type == "update" and data_list:
                            upd = data_list[0]
                            ts = time.time()
                            for b in upd.get("bids", []):
                                monotonic_seq += 1
                                delta = KrakenBookDelta(
                                    timestamp=ts,
                                    seq=monotonic_seq,
                                    side="BUY",
                                    price=float(b["price"]),
                                    quantity=float(b["qty"]),
                                )
                                deltas.append(delta)
                                delta_buffer.append(delta)
                                update_count += 1
                            for a in upd.get("asks", []):
                                monotonic_seq += 1
                                delta = KrakenBookDelta(
                                    timestamp=ts,
                                    seq=monotonic_seq,
                                    side="SELL",
                                    price=float(a["price"]),
                                    quantity=float(a["qty"]),
                                )
                                deltas.append(delta)
                                delta_buffer.append(delta)
                                update_count += 1

                    elif channel == "trade" and data_list:
                        for t in data_list:
                            monotonic_seq += 1
                            ts = time.time()
                            side = "BUY" if t.get("side", "").lower() == "buy" else "SELL"
                            trade = KrakenTrade(
                                price=float(t["price"]),
                                quantity=float(t["qty"]),
                                timestamp=ts,
                                seq=monotonic_seq,
                                side=side,
                                order_type="MARKET",
                                trade_id=int(t.get("trade_id", 0)),
                            )
                            trades.append(trade)
                            trade_buffer.append(trade)
                            update_count += 1

                    # Incremental streaming flush to disk
                    if (
                        len(delta_buffer) >= flush_interval_records
                        or len(trade_buffer) >= 50
                        or (now - last_flush_time) >= 5.0
                    ):
                        flush_buffers()

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

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n[Kraken WS] Interrupted by user (Ctrl+C). Finalizing recorded session...")
        finally:
            flush_buffers()
            delta_writer.close()
            trade_writer.close()

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

        print(
            f"Flushed session to Parquet ({len(bids)} bids, {len(asks)} asks, "
            f"{len(deltas)} deltas, {len(trades)} trades) in {output_dir}"
        )
        return session

    def record(
        self,
        duration_seconds: float = 60.0,
        max_updates: int | None = None,
        output_dir: str = "examples/data",
    ) -> KrakenMarketSession:
        """Synchronous wrapper with graceful Ctrl+C interruption support."""
        import asyncio

        try:
            return asyncio.run(self.record_stream(duration_seconds, max_updates, output_dir))
        except KeyboardInterrupt:
            pair_clean = self.pair.lower().replace("/", "").replace("-", "")
            depth_pq = os.path.join(output_dir, f"kraken_{pair_clean}_depth.parquet")
            trades_pq = os.path.join(output_dir, f"kraken_{pair_clean}_trades.parquet")
            deltas_pq = os.path.join(output_dir, f"kraken_{pair_clean}_deltas.parquet")
            if os.path.exists(depth_pq) and os.path.exists(trades_pq):
                print(f"[Kraken WS] Loading interrupted session from disk ({self.pair})...")
                return KrakenClient.load_from_parquet(
                    depth_pq,
                    trades_pq,
                    pair=self.pair,
                    deltas_file=deltas_pq if os.path.exists(deltas_pq) else None,
                )
            raise


class KrakenOrderBookReplayer:
    """Replays real Kraken market order book depth and trade flow through the Engine.

    Correctness Invariants:
    1. Deltas are the SINGLE SOURCE OF TRUTH for market (KRAKEN_MAKER) liquidity.
    2. Market taker trades do NOT execute against KRAKEN_MAKER (avoids double-counting).
    3. Taker trades act solely as fill triggers for the strategy's resting orders
       via `try_fill_resting(trade, resting_order_id)`.
    4. Strict '<' ordering: Deltas stamped concurrently with or after a trade
       are never applied prior to that trade's decision cursor.
    """

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
        self.maker_orders: dict[tuple[str, float], int] = {}  # (side, price) -> order_id

    def seed_initial_book(self) -> None:
        """Seeds the trading engine's order book with Kraken's authentic bid/ask ladders."""
        self.maker_orders.clear()
        # Insert bids from lowest to highest so higher bids rest properly in price-time queue
        for price, qty in reversed(self.session.bids):
            if qty > 0.0 and price > 0.0:
                try:
                    order = self.engine.submit_order(
                        symbol=self.symbol,
                        side="BUY",
                        order_type="LIMIT",
                        price=price,
                        quantity=qty,
                        time_in_force="GTC",
                        account_id=self.maker_account,
                    )
                    self.maker_orders[("BUY", price)] = order.id
                except Exception:
                    pass

        # Insert asks from highest to lowest
        for price, qty in reversed(self.session.asks):
            if qty > 0.0 and price > 0.0:
                try:
                    order = self.engine.submit_order(
                        symbol=self.symbol,
                        side="SELL",
                        order_type="LIMIT",
                        price=price,
                        quantity=qty,
                        time_in_force="GTC",
                        account_id=self.maker_account,
                    )
                    self.maker_orders[("SELL", price)] = order.id
                except Exception:
                    pass

    def apply_deltas_until(self, timestamp: float, seq: int | None = None) -> int:
        """Applies order book additions and updates strictly up to timestamp/seq.

        Uses strict '<' ordering: deltas stamped at or after the trade's timestamp/seq
        are not applied, preventing lookahead bias into the trade's aftermath.
        """
        if not self.session.deltas:
            return 0

        applied = 0
        while self._current_delta_idx < len(self.session.deltas):
            delta = self.session.deltas[self._current_delta_idx]

            # Strict '<' lookahead check
            if seq is not None and delta.seq > 0 and seq > 0:
                if delta.timestamp > timestamp:
                    break
                if delta.timestamp == timestamp and delta.seq >= seq:
                    break
            else:
                if delta.timestamp >= timestamp:
                    break

            self._current_delta_idx += 1
            applied += 1

            # 1. Cancel prior maker resting order at this price level if present
            prior_id = self.maker_orders.pop((delta.side, delta.price), None)
            if prior_id is not None:
                try:
                    self.engine.cancel_order(self.symbol, prior_id)
                except Exception:
                    pass

            # 2. Place updated resting maker order if delta has positive quantity
            if delta.quantity > 0.0:
                try:
                    order = self.engine.submit_order(
                        symbol=self.symbol,
                        side=delta.side,
                        order_type="LIMIT",
                        price=delta.price,
                        quantity=delta.quantity,
                        time_in_force="GTC",
                        account_id=self.maker_account,
                    )
                    self.maker_orders[(delta.side, delta.price)] = order.id
                except Exception:
                    pass

        return applied

    def try_fill_resting(self, trade: KrakenTrade, resting_order_id: int | None) -> Any | None:
        """Checks whether a real market taker trade crossed the strategy's resting order.

        If crossed, executes a fill solely against the resting order via fill_resting_order.
        Does NOT touch KRAKEN_MAKER liquidity (deltas are the single source of truth).
        """
        if resting_order_id is None:
            return None

        order = self.engine.get_order(self.symbol, resting_order_id)
        if order is None or not order.is_active():
            return None

        order_side = str(order.side).upper()
        is_resting_buy = "BUY" in order_side
        is_resting_sell = "SELL" in order_side

        crossed = False
        # Market SELL trade crosses resting BUY order if trade.price <= order.price
        if is_resting_buy and trade.side == "SELL" and trade.price <= order.price:
            crossed = True
        # Market BUY trade crosses resting SELL order if trade.price >= order.price
        elif is_resting_sell and trade.side == "BUY" and trade.price >= order.price:
            crossed = True

        if not crossed:
            return None

        fill_qty = min(trade.quantity, order.remaining_quantity)
        if fill_qty <= 1e-9:
            return None

        try:
            return self.engine.fill_resting_order(
                symbol=self.symbol,
                order_id=resting_order_id,
                fill_price=order.price,
                fill_quantity=fill_qty,
                taker_account_id=self.taker_account,
            )
        except Exception:
            return None

    def has_next_trade(self) -> bool:
        return self._current_trade_idx < len(self.session.trades)

    def advance_trade(self) -> KrakenTrade | None:
        """Advances the trade cursor without executing against the book."""
        if not self.has_next_trade():
            return None
        trade = self.session.trades[self._current_trade_idx]
        self._current_trade_idx += 1
        return trade

    def replay_next_trade(self) -> KrakenTrade | None:
        """Deprecated legacy method: advances the trade cursor without double-counting."""
        return self.advance_trade()

    @property
    def total_trades(self) -> int:
        return len(self.session.trades)

    @property
    def progress(self) -> float:
        return self._current_trade_idx / len(self.session.trades) if self.session.trades else 1.0
