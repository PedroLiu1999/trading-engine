use trading_engine::{
    EngineEvent, ExecutionEngine, OrderBook, OrderStatus, OrderType, PositionManager, RiskConfig,
    RiskManager, RiskRejection, Side, TimeInForce,
};

#[test]
fn test_order_book_bids_and_asks_depth() {
    let mut book = OrderBook::new("AAPL", 0.01, 1.0);

    let o1 = trading_engine::Order::new(
        1, None, "AAPL", Side::Buy, OrderType::Limit, 150.0, 10.0, TimeInForce::GTC,
    );
    let o2 = trading_engine::Order::new(
        2, None, "AAPL", Side::Buy, OrderType::Limit, 149.5, 20.0, TimeInForce::GTC,
    );
    let o3 = trading_engine::Order::new(
        3, None, "AAPL", Side::Sell, OrderType::Limit, 151.0, 15.0, TimeInForce::GTC,
    );
    let o4 = trading_engine::Order::new(
        4, None, "AAPL", Side::Sell, OrderType::Limit, 151.5, 25.0, TimeInForce::GTC,
    );

    book.process_order(o1);
    book.process_order(o2);
    book.process_order(o3);
    book.process_order(o4);

    assert_eq!(book.best_bid(), Some((150.0, 10.0)));
    assert_eq!(book.best_ask(), Some((151.0, 15.0)));
    assert_eq!(book.mid_price(), Some(150.5));
    assert_eq!(book.spread(), Some(1.0));

    let depth = book.get_depth(5);
    assert_eq!(depth.bids.len(), 2);
    assert_eq!(depth.asks.len(), 2);
    assert_eq!(depth.bids[0].price, 150.0);
    assert_eq!(depth.bids[0].quantity, 10.0);
    assert_eq!(depth.asks[0].price, 151.0);
    assert_eq!(depth.asks[0].quantity, 15.0);
}

#[test]
fn test_order_matching_crossing_spread() {
    let mut book = OrderBook::new("ETH-USDT", 0.1, 0.01);

    // Resting asks: 1.0 @ 3000, 2.0 @ 3010
    book.process_order(trading_engine::Order::new(
        1, None, "ETH-USDT", Side::Sell, OrderType::Limit, 3000.0, 1.0, TimeInForce::GTC,
    ));
    book.process_order(trading_engine::Order::new(
        2, None, "ETH-USDT", Side::Sell, OrderType::Limit, 3010.0, 2.0, TimeInForce::GTC,
    ));

    // Incoming buy limit crossing spread: Buy 2.5 @ 3010.0
    let res = book.process_order(trading_engine::Order::new(
        3, None, "ETH-USDT", Side::Buy, OrderType::Limit, 3010.0, 2.5, TimeInForce::GTC,
    ));

    assert_eq!(res.trades.len(), 2);
    assert_eq!(res.trades[0].price, 3000.0);
    assert_eq!(res.trades[0].quantity, 1.0);
    assert_eq!(res.trades[1].price, 3010.0);
    assert_eq!(res.trades[1].quantity, 1.5);

    assert_eq!(res.order.filled_quantity, 2.5);
    assert_eq!(res.order.status, OrderStatus::Filled);

    // Remaining ask depth: 0.5 left @ 3010
    assert_eq!(book.best_ask(), Some((3010.0, 0.5)));
}

#[test]
fn test_market_order_walks_book() {
    let mut book = OrderBook::new("BTC-USD", 1.0, 0.01);

    book.process_order(trading_engine::Order::new(
        1, None, "BTC-USD", Side::Sell, OrderType::Limit, 60000.0, 0.5, TimeInForce::GTC,
    ));
    book.process_order(trading_engine::Order::new(
        2, None, "BTC-USD", Side::Sell, OrderType::Limit, 60100.0, 1.0, TimeInForce::GTC,
    ));

    // Market Buy 1.0
    let res = book.process_order(trading_engine::Order::new(
        3, None, "BTC-USD", Side::Buy, OrderType::Market, 0.0, 1.0, TimeInForce::IOC,
    ));

    assert_eq!(res.trades.len(), 2);
    assert_eq!(res.trades[0].quantity, 0.5);
    assert_eq!(res.trades[0].price, 60000.0);
    assert_eq!(res.trades[1].quantity, 0.5);
    assert_eq!(res.trades[1].price, 60100.0);
    assert_eq!(res.order.filled_quantity, 1.0);
}

#[test]
fn test_order_cancellation() {
    let mut book = OrderBook::new("AAPL", 0.01, 1.0);

    book.process_order(trading_engine::Order::new(
        1, None, "AAPL", Side::Buy, OrderType::Limit, 150.0, 10.0, TimeInForce::GTC,
    ));
    assert_eq!(book.total_orders(), 1);

    let cancelled = book.cancel_order(1);
    assert!(cancelled.is_some());
    assert_eq!(cancelled.unwrap().status, OrderStatus::Cancelled);
    assert_eq!(book.total_orders(), 0);
    assert_eq!(book.best_bid(), None);
}

#[test]
fn test_position_pnl_scale_in_and_close() {
    let mut pm = PositionManager::new(100_000.0, 1.0);

    // Buy 10 @ 100
    let t1 = trading_engine::Trade {
        execution_id: 1,
        maker_order_id: 1,
        taker_order_id: 2,
        symbol: "TEST".to_string(),
        side: Side::Buy,
        price: 100.0,
        quantity: 10.0,
        fee: 0.0,
        timestamp: 0,
    };
    pm.on_trade(&t1, Side::Buy);

    let pos = pm.get_position("TEST").unwrap();
    assert_eq!(pos.quantity, 10.0);
    assert_eq!(pos.avg_entry_price, 100.0);

    // Scale in: Buy 10 @ 120 -> Avg entry = (10*100 + 10*120)/20 = 110
    let t2 = trading_engine::Trade {
        execution_id: 2,
        maker_order_id: 3,
        taker_order_id: 4,
        symbol: "TEST".to_string(),
        side: Side::Buy,
        price: 120.0,
        quantity: 10.0,
        fee: 0.0,
        timestamp: 0,
    };
    pm.on_trade(&t2, Side::Buy);

    let pos = pm.get_position("TEST").unwrap();
    assert_eq!(pos.quantity, 20.0);
    assert_eq!(pos.avg_entry_price, 110.0);

    // Partial close: Sell 10 @ 130 -> Realized PnL = 10 * (130 - 110) = +200
    let t3 = trading_engine::Trade {
        execution_id: 3,
        maker_order_id: 5,
        taker_order_id: 6,
        symbol: "TEST".to_string(),
        side: Side::Sell,
        price: 130.0,
        quantity: 10.0,
        fee: 0.0,
        timestamp: 0,
    };
    pm.on_trade(&t3, Side::Sell);

    let pos = pm.get_position("TEST").unwrap();
    assert_eq!(pos.quantity, 10.0);
    assert_eq!(pos.avg_entry_price, 110.0);
    assert_eq!(pos.realized_pnl, 200.0);

    // Mark to market at 140 -> Unrealized PnL = 10 * (140 - 110) = +300
    pm.mark_to_market("TEST", 140.0);
    let pos = pm.get_position("TEST").unwrap();
    assert_eq!(pos.unrealized_pnl, 300.0);

    // Total account equity
    assert_eq!(pm.account().equity(pm.total_unrealized_pnl()), 100_000.0 + 200.0 + 300.0);
}

#[test]
fn test_risk_manager_validations() {
    let config = RiskConfig {
        max_order_qty: 10.0,
        max_order_notional: 100_000.0,
        max_position_notional: 200_000.0,
        price_collar_pct: 0.05, // 5%
        max_drawdown_pct: 0.20,
        max_orders_per_sec: 10,
        require_margin: true,
    };

    let mut rm = RiskManager::new(config, 50_000.0);
    let account = trading_engine::Account::new(50_000.0, 1.0);

    // 1. Order qty too large
    let o1 = trading_engine::Order::new(
        1, None, "BTC", Side::Buy, OrderType::Limit, 30000.0, 15.0, TimeInForce::GTC,
    );
    let res = rm.check_order(&o1, &account, None, Some(30000.0), 0.0);
    assert!(matches!(res, Err(RiskRejection::OrderQtyTooLarge { .. })));

    // 2. Price collar breach (>5% away from mid)
    let o2 = trading_engine::Order::new(
        2, None, "BTC", Side::Buy, OrderType::Limit, 35000.0, 1.0, TimeInForce::GTC,
    );
    let res = rm.check_order(&o2, &account, None, Some(30000.0), 0.0);
    assert!(matches!(res, Err(RiskRejection::PriceCollarBreached { .. })));

    // 3. Valid order
    let o3 = trading_engine::Order::new(
        3, None, "BTC", Side::Buy, OrderType::Limit, 30500.0, 1.0, TimeInForce::GTC,
    );
    let res = rm.check_order(&o3, &account, None, Some(30000.0), 0.0);
    assert!(res.is_ok());

    // 4. Kill switch active
    rm.trip_kill_switch("Manual test");
    let res = rm.check_order(&o3, &account, None, Some(30000.0), 0.0);
    assert!(matches!(res, Err(RiskRejection::KillSwitchActive(_))));
}

#[test]
fn test_execution_engine_integrated_flow() {
    let risk_config = RiskConfig {
        max_order_qty: 100.0,
        max_order_notional: 1_000_000.0,
        max_position_notional: 2_000_000.0,
        price_collar_pct: 0.10,
        max_drawdown_pct: 0.25,
        max_orders_per_sec: 1000,
        require_margin: true,
    };

    let engine = ExecutionEngine::new(100_000.0, 1.0, risk_config);
    let rx = engine.event_bus().subscribe();

    engine.register_symbol("BTC-USD", 1.0, 0.001);

    // Provide liquidity
    engine
        .submit_order(None, "BTC-USD", Side::Sell, OrderType::Limit, 50_000.0, 2.0, TimeInForce::GTC)
        .unwrap();

    // Take liquidity
    let buy = engine
        .submit_order(Some("taker_1".to_string()), "BTC-USD", Side::Buy, OrderType::Market, 0.0, 1.0, TimeInForce::IOC)
        .unwrap();

    assert_eq!(buy.status, OrderStatus::Filled);
    assert_eq!(buy.filled_quantity, 1.0);

    let pos = engine.get_position("BTC-USD").unwrap();
    assert_eq!(pos.quantity, 1.0);
    assert_eq!(pos.avg_entry_price, 50_000.0);

    // Verify events were broadcasted
    let mut events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }
    assert!(events.iter().any(|e| matches!(e, EngineEvent::OrderSubmitted(_))));
    assert!(events.iter().any(|e| matches!(e, EngineEvent::OrderAccepted(_))));
    assert!(events.iter().any(|e| matches!(e, EngineEvent::TradeExecuted(_))));
    assert!(events.iter().any(|e| matches!(e, EngineEvent::PositionUpdated { .. })));
}
