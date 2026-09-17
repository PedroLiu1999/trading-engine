use trading_engine::{
    ExecutionEngine, OrderType, RiskConfig, Side, TimeInForce,
};

fn main() {
    println!("=== High-Performance Rust Trading Engine Demo ===");

    // Configure risk manager
    let risk_config = RiskConfig {
        max_order_qty: 100.0,
        max_order_notional: 500_000.0,
        max_position_notional: 1_000_000.0,
        price_collar_pct: 0.10, // 10% collar
        max_drawdown_pct: 0.20,
        max_orders_per_sec: 10_000,
        require_margin: true,
    };

    let engine = ExecutionEngine::new(500_000.0, 1.0, risk_config);
    let event_rx = engine.event_bus().subscribe();

    let symbol = "BTC-USDT";
    engine.register_symbol(symbol, 0.50, 0.001);
    println!("Registered symbol '{}' (tick_size=0.50)", symbol);

    println!("\n--- Step 1: Seeding resting limit orders ---");
    // Seed Bids
    engine
        .submit_order(None, symbol, Side::Buy, OrderType::Limit, 50_000.0, 1.5, TimeInForce::GTC)
        .unwrap();
    engine
        .submit_order(None, symbol, Side::Buy, OrderType::Limit, 49_950.0, 2.0, TimeInForce::GTC)
        .unwrap();
    engine
        .submit_order(None, symbol, Side::Buy, OrderType::Limit, 49_900.0, 3.0, TimeInForce::GTC)
        .unwrap();

    // Seed Asks
    engine
        .submit_order(None, symbol, Side::Sell, OrderType::Limit, 50_050.0, 1.0, TimeInForce::GTC)
        .unwrap();
    engine
        .submit_order(None, symbol, Side::Sell, OrderType::Limit, 50_100.0, 2.5, TimeInForce::GTC)
        .unwrap();

    let depth = engine.get_depth(symbol, 5).unwrap();
    println!("Market Depth for {}:", symbol);
    println!("  Best Bid: {:?}, Best Ask: {:?}", depth.best_bid(), depth.best_ask());
    println!("  Mid Price: {:?}, Spread: {:?}", depth.mid_price(), depth.spread());
    for ask in depth.asks.iter().rev() {
        println!("  ASK: {:>8.2} | Qty: {:>6.3} ({} orders)", ask.price, ask.quantity, ask.order_count);
    }
    println!("  --------------------------------");
    for bid in &depth.bids {
        println!("  BID: {:>8.2} | Qty: {:>6.3} ({} orders)", bid.price, bid.quantity, bid.order_count);
    }

    println!("\n--- Step 2: Executing Market Buy (Crossing the spread) ---");
    let fill_order = engine
        .submit_order(
            Some("market_buy_1".to_string()),
            symbol,
            Side::Buy,
            OrderType::Market,
            0.0,
            1.5, // Will fill 1.0 @ 50050 and 0.5 @ 50100
            TimeInForce::IOC,
        )
        .unwrap();
    println!(
        "Market Order Status: {:?}, Filled: {} / {}",
        fill_order.status, fill_order.filled_quantity, fill_order.quantity
    );

    let pos = engine.get_position(symbol).unwrap();
    println!("Updated Position for {}:", symbol);
    println!("  Quantity: {}", pos.quantity);
    println!("  Avg Entry Price: {:.2}", pos.avg_entry_price);
    println!("  Realized PnL: {:.2}", pos.realized_pnl);
    println!("  Unrealized PnL: {:.2}", pos.unrealized_pnl);

    println!("\n--- Step 3: Testing Risk Manager Fat-Finger / Price Collar ---");
    let collar_err = engine.submit_order(
        None,
        symbol,
        Side::Buy,
        OrderType::Limit,
        60_000.0, // 20% above mid price of ~50,000 -> exceeds 10% collar
        0.1,
        TimeInForce::GTC,
    );
    match collar_err {
        Ok(_) => println!("Unexpected success!"),
        Err(e) => println!("Correctly rejected by Risk Manager: {}", e),
    }

    println!("\n--- Step 4: Event Bus Drain ---");
    let mut count = 0;
    while let Ok(event) = event_rx.try_recv() {
        count += 1;
        println!("  [Event #{}] {}: {:?}", count, event.event_type(), event);
    }

    println!("\nDemo completed successfully!");
}
