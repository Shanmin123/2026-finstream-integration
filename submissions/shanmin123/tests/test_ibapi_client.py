from types import SimpleNamespace

from ibapi.contract import Contract
from ibapi.client import EClient

import pytest

from pipeline.ibapi_client import IBApiClient, IBRequestError


def qualified_contract():
    contract = Contract()
    contract.symbol = "AAPL"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    contract.conId = 265598
    return contract


def test_ibapi_client_converts_historical_bar_callbacks(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )

    def req_historical_data(req_id, *_args):
        bar = SimpleNamespace(
            date="20250102",
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=1000.0,
            barCount=10,
            average=100.5,
        )
        client.historicalData(req_id, bar)
        client.historicalDataEnd(req_id, "", "")

    monkeypatch.setattr(client, "reqHistoricalData", req_historical_data)

    rows = client.fetch_daily_bars("AAPL", "7 D")

    assert rows == [
        {
            "event_date": "2025-01-02",
            "symbol": "AAPL",
            "con_id": 265598,
            "exchange": "SMART",
            "currency": "USD",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 1000.0,
            "bar_count": 10,
            "wap": 100.5,
        }
    ]


def test_ibapi_client_converts_news_callbacks_and_cleans_tags(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )

    def req_historical_news(req_id, *_args):
        client.historicalNews(
            req_id,
            "20250102 12:00:00.0",
            "BRFG",
            "article-1",
            "{A:800015:L:en}Synthetic earnings headline",
        )
        client.historicalNewsEnd(req_id, False)

    monkeypatch.setattr(client, "reqHistoricalNews", req_historical_news)

    rows = client.fetch_news_headlines(
        "AAPL",
        ["BRFG"],
        "20250101 00:00:00",
        "20250103 00:00:00",
        10,
    )

    assert rows[0]["headline"] == "Synthetic earnings headline"
    assert rows[0]["published_at_utc"] == "2025-01-02T12:00:00+00:00"
    assert rows[0]["article_id"] == "article-1"
    assert "article_text" not in rows[0]


def test_ibapi_client_lists_provider_callbacks(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)

    def req_news_providers():
        client.newsProviders(
            [SimpleNamespace(providerCode="BRFG", providerName="Briefing.com")]
        )

    monkeypatch.setattr(client, "reqNewsProviders", req_news_providers)

    assert client.list_news_providers() == [{"code": "BRFG", "name": "Briefing.com"}]


def test_ibapi_client_raises_request_errors_instead_of_returning_empty_data(
    monkeypatch,
):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )

    def req_historical_data(req_id, *_args):
        client.error(req_id, 162, "Historical data pacing violation")

    monkeypatch.setattr(client, "reqHistoricalData", req_historical_data)

    with pytest.raises(IBRequestError, match="162.*pacing"):
        client.fetch_daily_bars("AAPL", "7 D")


def test_connect_does_not_start_network_thread_after_synchronous_socket_error(
    monkeypatch,
):
    client = IBApiClient(
        "127.0.0.1",
        4002,
        31,
        request_timeout_seconds=1,
        connect_timeout_seconds=1,
    )

    def fail_connect(_client, *_args, **_kwargs):
        client.error(-1, 502, "Cannot connect")

    monkeypatch.setattr(EClient, "connect", fail_connect)
    monkeypatch.setattr(EClient, "disconnect", lambda _client: None)

    with pytest.raises(IBRequestError, match="502"):
        client.connect()

    assert client._network_thread is None
