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
class KrakenMarketSession:
    pair: str
    captured_at: float
    bids: list[tuple[float, float]]  # (price, quantity)
    asks: list[tuple[float, float]]  # (price, quantity)
    trades: list[KrakenTrade]


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

    def fetch_historical_trades(
        self,
        pair: str = "ETHUSD",
        max_trades: int = 5000,
        delay_sec: float = 0.3,
        since: int | None = None,
        lookback_hours: float | None = None,
    ) -> list[Any]:
        """Fetches a long history of public trades by paginating the Kraken `since` cursor.

        If `max_trades > 1000` and `since` is not specified, probes recent trade velocity
        to estimate an earlier starting timestamp in nanoseconds, allowing multi-page
        pagination forward to collect thousands of historical trades.
        """
        if since is None:
            if lookback_hours is not None:
                since = int((time.time() - (lookback_hours * 3600.0)) * 1e9)
            elif max_trades > 1000:
                # Probe current trade rate to calculate appropriate lookback
                try:
                    probe = self._get("Trades", {"pair": pair})
                    p_key = next(k for k in probe.keys() if k != "last")
                    sample = probe[p_key]
                    if len(sample) >= 10:
                        t_start = float(sample[0][2])
                        t_end = float(sample[-1][2])
                        dt = max(t_end - t_start, 10.0)
                        rate = len(sample) / dt  # trades per second
                        # Buffer by 40% to guarantee capturing at least max_trades
                        est_seconds = (max_trades / max(rate, 0.05)) * 1.4
                        since = int((time.time() - est_seconds) * 1e9)
                except Exception:
                    pass

        all_trades: list[Any] = []
        seen_tids: set[str] = set()

        while len(all_trades) < max_trades:
            params: dict[str, Any] = {"pair": pair}
            if since is not None:
                params["since"] = since

            res = self._get("Trades", params)
            pair_key = next(k for k in res.keys() if k != "last")
            batch = res[pair_key]
            if not batch:
                break

            for item in batch:
                tid = str(item[6]) if len(item) > 6 else f"{item[2]}_{item[0]}_{item[1]}"
                if tid not in seen_tids:
                    seen_tids.add(tid)
                    all_trades.append(item)

            next_since = res.get("last")
            if next_since is None or next_since == since or len(batch) == 0:
                break
            since = next_since

            if len(all_trades) >= max_trades:
                break
            time.sleep(delay_sec)

        return all_trades[:max_trades]

    def record_session(
        self,
        pair: str = "ETHUSD",
        depth_count: int = 100,
        max_trades: int = 5000,
        output_format: str = "parquet",
        output_dir: str = "examples/data",
        since: int | None = None,
        lookback_hours: float | None = None,
    ) -> KrakenMarketSession:
        """Fetches a synchronized order book snapshot and paginated trade sequence."""
        depth = self.fetch_depth(pair=pair, count=depth_count)
        raw_trades = self.fetch_historical_trades(
            pair=pair,
            max_trades=max_trades,
            since=since,
            lookback_hours=lookback_hours,
        )

        bids = [(float(p), float(q)) for p, q, *_ in depth["bids"]]
        asks = [(float(p), float(q)) for p, q, *_ in depth["asks"]]

        trades: list[KrakenTrade] = []
        for item in raw_trades:
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

        session = KrakenMarketSession(
            pair=pair,
            captured_at=time.time(),
            bids=bids,
            asks=asks,
            trades=trades,
        )

        os.makedirs(output_dir, exist_ok=True)
        pair_clean = pair.lower().replace("/", "")

        if output_format.lower() == "parquet":
            depth_file = os.path.join(output_dir, f"kraken_{pair_clean}_depth.parquet")
            trades_file = os.path.join(output_dir, f"kraken_{pair_clean}_trades.parquet")
            self.save_to_parquet(session, depth_file, trades_file)
        else:
            json_file = os.path.join(output_dir, f"kraken_{pair_clean}_sample.json")
            self.save_to_json(session, json_file)

        return session

    @staticmethod
    def save_to_parquet(session: KrakenMarketSession, depth_file: str, trades_file: str) -> None:
        """Saves session L2 depth and trade history to compressed Apache Parquet format."""
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

    @staticmethod
    def load_from_parquet(
        depth_file: str, trades_file: str, pair: str = "ETHUSD"
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

        captured_at = trades[0].timestamp if trades else time.time()
        return KrakenMarketSession(
            pair=pair,
            captured_at=captured_at,
            bids=bids,
            asks=asks,
            trades=trades,
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
            if depth_pq.exists() and trades_pq.exists():
                return cls.load_from_parquet(str(depth_pq), str(trades_pq), pair=pair)

            json_file = p / f"kraken_{pair.lower()}_sample.json"
            if json_file.exists():
                return cls.load_from_json(str(json_file))

        if str(path).endswith(".parquet"):
            # Assume paired trades file
            base = str(path).replace("_depth.parquet", "").replace("_trades.parquet", "")
            return cls.load_from_parquet(f"{base}_depth.parquet", f"{base}_trades.parquet", pair)

        return cls.load_from_json(path)


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

    def seed_initial_book(self) -> None:
        """Seeds the trading engine's order book with Kraken's authentic bid/ask ladders."""
        shift = 0.0
        if self.session.trades and self.session.bids and self.session.asks:
            best_bid = self.session.bids[0][0]
            best_ask = self.session.asks[0][0]
            start_price = self.session.trades[0].price
            # Only shift if initial trade is significantly outside prevailing spread
            if start_price < best_bid or start_price > best_ask:
                snapshot_mid = (best_bid + best_ask) / 2.0
                if abs(start_price - snapshot_mid) > (best_ask - best_bid):
                    shift = round(start_price - snapshot_mid, 2)

        # Insert bids from lowest to highest so higher bids rest properly in price-time queue
        for price, qty in reversed(self.session.bids):
            p = round(price + shift, 2)
            if qty > 0.0 and p > 0.0:
                try:
                    self.engine.submit_order(
                        symbol=self.symbol,
                        side="BUY",
                        order_type="LIMIT",
                        price=p,
                        quantity=qty,
                        time_in_force="GTC",
                        account_id=self.maker_account,
                    )
                except Exception:
                    pass

        # Insert asks from highest to lowest
        for price, qty in reversed(self.session.asks):
            p = round(price + shift, 2)
            if qty > 0.0 and p > 0.0:
                try:
                    self.engine.submit_order(
                        symbol=self.symbol,
                        side="SELL",
                        order_type="LIMIT",
                        price=p,
                        quantity=qty,
                        time_in_force="GTC",
                        account_id=self.maker_account,
                    )
                except Exception:
                    pass

    def ensure_depth(self, reference_price: float) -> None:
        """Ensures that the order book has active two-sided liquidity around reference_price."""
        if reference_price <= 0.0:
            return
        try:
            depth = self.engine.get_depth(self.symbol, max_depth=5)
            if len(depth.bids) < 3:
                for step in range(1, 4):
                    p = round(reference_price - (step * 0.25), 2)
                    if p > 0:
                        self.engine.submit_order(
                            symbol=self.symbol,
                            side="BUY",
                            order_type="LIMIT",
                            price=p,
                            quantity=10.0 * step,
                            time_in_force="GTC",
                            account_id=self.maker_account,
                        )
            if len(depth.asks) < 3:
                for step in range(1, 4):
                    p = round(reference_price + (step * 0.25), 2)
                    if p > 0:
                        self.engine.submit_order(
                            symbol=self.symbol,
                            side="SELL",
                            order_type="LIMIT",
                            price=p,
                            quantity=10.0 * step,
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

        # Ensure active liquidity exists prior to taker trade execution
        self.ensure_depth(trade.price)

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

        # Replenish book depth after trade execution
        self.ensure_depth(trade.price)

        return trade

    @property
    def total_trades(self) -> int:
        return len(self.session.trades)

    @property
    def progress(self) -> float:
        return self._current_trade_idx / len(self.session.trades) if self.session.trades else 1.0
