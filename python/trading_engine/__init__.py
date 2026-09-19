"""High-performance trading engine built with Rust and exposed via PyO3."""

from ._trading_engine import (
    Account,
    AssetConfig,
    Engine,
    LevelQuote,
    MarketDepth,
    MultiAssetMarketSim,
    Order,
    OrderStatus,
    OrderType,
    Position,
    RiskConfig,
    Side,
    TimeInForce,
    Trade,
)
from .kraken import (
    KrakenBookDelta,
    KrakenClient,
    KrakenMarketSession,
    KrakenOrderBookReplayer,
    KrakenTrade,
    KrakenWebSocketRecorder,
)

__all__ = [
    "Account",
    "AssetConfig",
    "Engine",
    "KrakenBookDelta",
    "KrakenClient",
    "KrakenMarketSession",
    "KrakenOrderBookReplayer",
    "KrakenTrade",
    "KrakenWebSocketRecorder",
    "LevelQuote",
    "MarketDepth",
    "MultiAssetMarketSim",
    "Order",
    "OrderStatus",
    "OrderType",
    "Position",
    "RiskConfig",
    "Side",
    "TimeInForce",
    "Trade",
]
