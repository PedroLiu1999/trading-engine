"""Trading Engine Performance & Latency Benchmark.

Measures throughput (ops/sec) and latency distributions (p50, p90, p99, p99.9)
across core matching engine subsystems:
1. Limit Order Insertion (Passive Liquidity Book Building)
2. Trade Execution & Matching (Aggressive Crossing, Dual-Sided Fills, Ledger Updates)
3. Order Cancellation (Resting Order Lookup & Book Removal)
4. L2 Market Depth & Top-of-Book Queries
5. Multi-Account Concurrent Attribution (Multi-Tenant Ledger Isolation)
6. Multi-Asset Market Simulation (Correlated Cholesky GBM + MM Ladders + Noise Flow)
"""

import argparse
import platform
import sys
import time
from dataclasses import dataclass

from trading_engine import AssetConfig, Engine, MultiAssetMarketSim, RiskConfig


@dataclass
class BenchResult:
    name: str
    operations: int
    elapsed_sec: float
    throughput_ops_sec: float
    mean_us: float
    min_us: float
    p50_us: float
    p90_us: float
    p99_us: float
    p999_us: float
    max_us: float


def calculate_stats(name: str, latencies_ns: list[int], elapsed_sec: float) -> BenchResult:
    """Computes throughput and percentile latencies in microseconds (µs)."""
    n = len(latencies_ns)
    if n == 0:
        return BenchResult(name, 0, elapsed_sec, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    s = sorted(latencies_ns)
    throughput = n / elapsed_sec if elapsed_sec > 0 else 0.0

    return BenchResult(
        name=name,
        operations=n,
        elapsed_sec=elapsed_sec,
        throughput_ops_sec=throughput,
        mean_us=(sum(s) / n) / 1000.0,
        min_us=s[0] / 1000.0,
        p50_us=s[int(n * 0.50)] / 1000.0,
        p90_us=s[int(n * 0.90)] / 1000.0,
        p99_us=s[min(int(n * 0.99), n - 1)] / 1000.0,
        p999_us=s[min(int(n * 0.999), n - 1)] / 1000.0,
        max_us=s[-1] / 1000.0,
    )


def create_benchmark_engine() -> Engine:
    """Creates a fresh engine instance configured for maximum throughput benchmarking."""
    risk = RiskConfig(
        max_order_qty=1_000_000.0,
        max_order_notional=100_000_000.0,
        max_position_notional=1_000_000_000.0,
        price_collar_pct=0.99,
        max_drawdown_pct=0.99,
        max_orders_per_sec=0,  # Disable rate limiter for raw throughput measurement
        require_margin=False,
    )
    engine = Engine(initial_balance=100_000_000.0, leverage=10.0, risk_config=risk)
    engine.register_symbol("BTC-USDT", tick_size=0.10, lot_size=0.001)
    engine.register_symbol("ETH-USDT", tick_size=0.01, lot_size=0.01)
    return engine


def bench_limit_order_insertion(count: int) -> tuple[BenchResult, Engine, list[int]]:
    """Benchmark 1: Raw Limit Order Insertion into Price-Time FIFO queues."""
    engine = create_benchmark_engine()
    latencies: list[int] = []
    order_ids: list[int] = []

    # Alternate buy orders below mid and sell orders above mid
    # so they rest in the order book without crossing
    base_bid = 50_000.0
    base_ask = 70_000.0

    t0 = time.perf_counter()
    for i in range(count):
        price = base_bid - (i % 200) * 1.0 if i % 2 == 0 else base_ask + (i % 200) * 1.0
        side = "BUY" if i % 2 == 0 else "SELL"

        start = time.perf_counter_ns()
        ord_obj = engine.submit_order(
            "BTC-USDT", side, "LIMIT", price, 0.1, "GTC", account_id="MAKER"
        )
        dur = time.perf_counter_ns() - start

        latencies.append(dur)
        order_ids.append(ord_obj.id)
    elapsed = time.perf_counter() - t0

    result = calculate_stats("Limit Order Placement", latencies, elapsed)
    return result, engine, order_ids


def bench_order_cancellation(engine: Engine, order_ids: list[int]) -> BenchResult:
    """Benchmark 2: Order Cancellation & Book Removal."""
    latencies: list[int] = []

    t0 = time.perf_counter()
    for oid in order_ids:
        start = time.perf_counter_ns()
        engine.cancel_order("BTC-USDT", oid)
        dur = time.perf_counter_ns() - start
        latencies.append(dur)
    elapsed = time.perf_counter() - t0

    return calculate_stats("Order Cancellation", latencies, elapsed)


def bench_order_matching(count: int) -> BenchResult:
    """Benchmark 3: Trade Execution & Matching (Dual-Sided Fills, Ledger & Position Updates)."""
    engine = create_benchmark_engine()

    # Pre-seed resting maker quotes
    for i in range(count):
        engine.submit_order(
            "BTC-USDT",
            "SELL",
            "LIMIT",
            60_000.0 + (i % 50) * 0.50,
            0.1,
            "GTC",
            account_id="MAKER",
        )

    latencies: list[int] = []

    # Send aggressive IOC market/crossing orders that immediately match
    t0 = time.perf_counter()
    for _ in range(count):
        start = time.perf_counter_ns()
        engine.submit_order("BTC-USDT", "BUY", "MARKET", 0.0, 0.1, "IOC", account_id="TAKER")
        dur = time.perf_counter_ns() - start
        latencies.append(dur)
    elapsed = time.perf_counter() - t0

    return calculate_stats("Trade Matching / Execution", latencies, elapsed)


def bench_market_depth_query(count: int) -> BenchResult:
    """Benchmark 4: L2 Market Depth & Top-of-Book Queries."""
    engine = create_benchmark_engine()

    # Seed 100 bids and 100 asks
    for i in range(100):
        engine.submit_order(
            "BTC-USDT", "BUY", "LIMIT", 59_000.0 - i * 1.0, 1.0, "GTC", account_id="MAKER"
        )
        engine.submit_order(
            "BTC-USDT", "SELL", "LIMIT", 61_000.0 + i * 1.0, 1.0, "GTC", account_id="MAKER"
        )

    latencies: list[int] = []

    t0 = time.perf_counter()
    for _ in range(count):
        start = time.perf_counter_ns()
        depth = engine.get_depth("BTC-USDT", levels=10)
        _ = depth.mid_price()
        _ = depth.best_bid()
        _ = depth.best_ask()
        dur = time.perf_counter_ns() - start
        latencies.append(dur)
    elapsed = time.perf_counter() - t0

    return calculate_stats("L2 Depth & Top-of-Book Read", latencies, elapsed)


def bench_multi_account_isolation(count: int, num_accounts: int = 10) -> BenchResult:
    """Benchmark 5: Multi-Account Ledger Isolation & Position Updates."""
    engine = create_benchmark_engine()
    accounts = [f"ACCOUNT_{i:02d}" for i in range(num_accounts)]

    # Pre-seed resting sell orders from SIM_MM
    for i in range(count):
        engine.submit_order(
            "BTC-USDT", "SELL", "LIMIT", 60_000.0 + (i % 20) * 1.0, 0.1, "GTC", account_id="SIM_MM"
        )

    latencies: list[int] = []

    # Distribute matching trades across distinct accounts
    t0 = time.perf_counter()
    for i in range(count):
        acct = accounts[i % num_accounts]
        start = time.perf_counter_ns()
        engine.submit_order("BTC-USDT", "BUY", "MARKET", 0.0, 0.1, "IOC", account_id=acct)
        dur = time.perf_counter_ns() - start
        latencies.append(dur)
    elapsed = time.perf_counter() - t0

    return calculate_stats(f"Multi-Account Routing ({num_accounts} accts)", latencies, elapsed)


def bench_market_simulation(steps: int) -> BenchResult:
    """Benchmark 6: Multi-Asset Correlated Simulation (Cholesky GBM + MM Ladders + Noise Flow)."""
    engine = create_benchmark_engine()

    btc = AssetConfig(
        symbol="BTC-USDT",
        initial_price=60_000.0,
        drift=0.0,
        volatility=0.40,
        tick_size=0.50,
        lot_size=0.001,
        quote_levels=3,
        base_spread_bps=1.0,
        arrival_rate=20.0,
        avg_order_qty=0.5,
    )
    eth = AssetConfig(
        symbol="ETH-USDT",
        initial_price=3_000.0,
        drift=0.0,
        volatility=0.50,
        tick_size=0.10,
        lot_size=0.01,
        quote_levels=3,
        base_spread_bps=1.2,
        arrival_rate=25.0,
        avg_order_qty=5.0,
    )
    sol = AssetConfig(
        symbol="SOL-USDT",
        initial_price=150.0,
        drift=0.0,
        volatility=0.65,
        tick_size=0.05,
        lot_size=0.1,
        quote_levels=3,
        base_spread_bps=3.0,
        arrival_rate=25.0,
        avg_order_qty=20.0,
    )

    corr = [
        [1.00, 0.85, 0.70],
        [0.85, 1.00, 0.75],
        [0.70, 0.75, 1.00],
    ]

    sim = MultiAssetMarketSim(engine, [btc, eth, sol], corr, seed=42)

    latencies: list[int] = []

    t0 = time.perf_counter()
    for _ in range(steps):
        start = time.perf_counter_ns()
        sim.step(dt=1.0)
        dur = time.perf_counter_ns() - start
        latencies.append(dur)
    elapsed = time.perf_counter() - t0

    return calculate_stats("3-Asset Sim Step (GBM+MM+Noise)", latencies, elapsed)


def print_system_info():
    """Prints CPU, OS, and Python environment information."""
    print("=" * 96)
    print("TRADING ENGINE PERFORMANCE & LATENCY BENCHMARK SUITE")
    print("=" * 96)
    print(f"  Platform         : {platform.platform()}")
    print(f"  Architecture     : {platform.machine()} ({platform.processor() or 'x86_64'})")
    print(f"  Python Version   : {sys.version.split()[0]}")
    print("  Engine Core      : Rust Core (PyO3 Extension Module)")
    print("=" * 96)
    print()


def print_results_table(results: list[BenchResult]):
    """Renders results in an aligned tabular view."""
    header = (
        f"{'Benchmark Subsystem':<32} | {'Ops':>7} | {'Throughput':>13} | "
        f"{'Avg':>8} | {'p50':>8} | {'p90':>8} | {'p99':>8} | {'Max':>8}"
    )
    print(header)
    print("-" * 96)

    for r in results:
        # Determine throughput unit
        tp_str = f"{r.throughput_ops_sec:,.0f} op/s"
        line = (
            f"{r.name:<32} | {r.operations:>7,d} | {tp_str:>13} | "
            f"{r.mean_us:>7.2f}µs | {r.p50_us:>7.2f}µs | {r.p90_us:>7.2f}µs | "
            f"{r.p99_us:>7.2f}µs | {r.max_us:>7.2f}µs"
        )
        print(line)

    print("-" * 96)
    print("  * All latency metrics measured in microseconds (µs = 10⁻⁶ s) from Python API call")
    print("=" * 96)


def main():
    parser = argparse.ArgumentParser(description="Trading Engine Performance Benchmark")
    parser.add_argument(
        "--orders",
        type=int,
        default=25_000,
        help="Number of orders to bench (default: 25,000)",
    )
    parser.add_argument(
        "--depth-queries",
        type=int,
        default=50_000,
        help="Number of market depth queries (default: 50,000)",
    )
    parser.add_argument(
        "--sim-steps",
        type=int,
        default=500,
        help="Number of multi-asset simulation steps (default: 500)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick iteration benchmark with reduced sample counts",
    )
    args = parser.parse_args()

    order_count = 5_000 if args.quick else args.orders
    depth_count = 10_000 if args.quick else args.depth_queries
    sim_steps = 100 if args.quick else args.sim_steps

    print_system_info()
    print(
        f"Running benchmarks with:\n"
        f"  - Order count        : {order_count:,}\n"
        f"  - Depth queries      : {depth_count:,}\n"
        f"  - Sim steps          : {sim_steps:,}\n"
    )

    results: list[BenchResult] = []

    # 1. Limit Order Placement
    print("1/6 Benchmarking Limit Order Placement...")
    res_placement, engine, order_ids = bench_limit_order_insertion(order_count)
    results.append(res_placement)

    # 2. Order Cancellation
    print("2/6 Benchmarking Order Cancellation...")
    res_cancel = bench_order_cancellation(engine, order_ids)
    results.append(res_cancel)

    # 3. Trade Matching & Dual-Sided Execution
    print("3/6 Benchmarking Trade Execution & Matching...")
    res_matching = bench_order_matching(order_count)
    results.append(res_matching)

    # 4. L2 Market Depth Queries
    print("4/6 Benchmarking L2 Market Depth & Top-of-Book...")
    res_depth = bench_market_depth_query(depth_count)
    results.append(res_depth)

    # 5. Multi-Account Concurrent Attribution
    print("5/6 Benchmarking Multi-Account Execution...")
    res_multi_acct = bench_multi_account_isolation(order_count, num_accounts=10)
    results.append(res_multi_acct)

    # 6. Multi-Asset Market Simulation
    print("6/6 Benchmarking Correlated Multi-Asset Market Sim...")
    res_sim = bench_market_simulation(sim_steps)
    results.append(res_sim)

    print("\n")
    print_results_table(results)


if __name__ == "__main__":
    main()
