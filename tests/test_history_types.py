"""Small additive contracts for the hosted historical API."""

from synpath.types import (
    HistoryMetadata,
    HistoricalOrderBook,
    HistoricalTrade,
    OrderBook,
    Trade,
    OrderBookAtResponse,
    OrderBookRangeResponse,
    TradesRangeResponse,
)


def test_history_book_contract_round_trip():
    response = OrderBookAtResponse.model_validate({
        "metadata": {"dataset_version": "raw-preview-v1", "time_basis": "recorder_receive"},
        "market_id": "kalshi:TEST", "as_of_ms": 1025,
        "book": {
            "market_id": "kalshi:TEST", "side": "no", "venue": "kalshi",
            "bids": [{"price": 0.4, "size": 4}],
            "asks": [{"price": 0.5, "size": 8}],
            "timestamp": 1020, "datetime": "1970-01-01T00:00:01.020Z",
            "as_of_ms": 1025, "venue_timestamp_ms": 1018,
            "book_model": "shared_complement", "derived": True,
            "depth_scope": "full", "info": {},
        },
    })
    assert response.book is not None
    assert isinstance(response.book, HistoricalOrderBook)
    assert isinstance(response.book, OrderBook)
    assert response.book.best_bid.price == 0.4
    assert response.book.as_of_ms == 1025
    assert response.book.venue_timestamp_ms == 1018
    assert isinstance(response.metadata, HistoryMetadata)


def test_history_range_contracts():
    books = OrderBookRangeResponse.model_validate({
        "metadata": {"dataset_version": "raw-preview-v1"},
        "market_id": "kalshi:TEST", "start_ms": 1000, "end_ms": 2000,
        "segments": [{"kind": "absent", "start_ms": 1000, "end_ms": 2000,
                      "reason": "connection_gap"}],
    })
    assert books.segments[0].reason == "connection_gap"

    trades = TradesRangeResponse.model_validate({
        "metadata": {"dataset_version": "raw-preview-v1"},
        "market_id": "kalshi:TEST", "start_ms": 1000, "end_ms": 2000,
        "trades": [{
            "id": "T1", "market_id": "kalshi:TEST", "timestamp": 1029,
            "datetime": "1970-01-01T00:00:01.029Z", "price": 0.55,
            "amount": 3, "side": "buy", "info": {"trade_id": "T1"},
            "observed_at_ms": 1030, "timestamp_source": "venue",
        }],
        "coverage": [{"start_ms": 1000, "end_ms": 2000, "status": "available"}],
    })
    assert isinstance(trades.trades[0], HistoricalTrade)
    assert isinstance(trades.trades[0], Trade)
    assert trades.trades[0].timestamp != trades.trades[0].observed_at_ms
