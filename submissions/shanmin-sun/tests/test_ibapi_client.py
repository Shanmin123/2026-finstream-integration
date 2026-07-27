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

    rows, exhausted = client.fetch_news_headlines(
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


def test_late_bars_after_a_timeout_do_not_recreate_the_freed_buffer(monkeypatch):
    """A historical response can land after its request timed out and was
    cleaned up. setdefault in the callback would silently recreate the buffer,
    which then leaks forever because no caller is left to pop it; the live
    request must also be cancelled so the retry does not stack on top of it."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=0.05)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )
    monkeypatch.setattr(client, "reqHistoricalData", lambda *_args: None)
    cancels = []
    monkeypatch.setattr(client, "cancelHistoricalData", lambda rid: cancels.append(rid))

    with pytest.raises(TimeoutError):
        client.fetch_daily_bars("AAPL", "7 D")
    assert cancels, "the timed-out request must be cancelled"

    late = SimpleNamespace(
        date="20250102", open=1.0, high=1.0, low=1.0, close=1.0,
        volume=1.0, barCount=1, average=1.0,
    )
    client.historicalData(cancels[0], late)
    assert client._bars == {}, "a late bar must not recreate the freed buffer"


def test_news_pagination_follows_has_more_and_stops_at_the_checkpoint(monkeypatch):
    """One page holds `limit` headlines; when IB flags hasMore the client must
    re-anchor at the oldest received time and keep walking backwards, or a
    burst larger than one page loses its older headlines permanently."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )
    anchors = []

    def req_historical_news(req_id, _con_id, _codes, start, end, _limit, _opts):
        anchors.append((start, end))
        if len(anchors) == 1:
            client.historicalNews(req_id, "2025-01-02 12:00:00.0", "BRFG", "a-3", "h3")
            # Article ids are provider-specific: a shared id from another
            # provider is a different article and must survive the dedup.
            client.historicalNews(req_id, "2025-01-02 12:00:00.0", "DJNL", "a-3", "h3-dj")
            client.historicalNews(req_id, "2025-01-02 11:00:00.0", "BRFG", "a-2", "h2")
            client.historicalNewsEnd(req_id, True)
        else:
            # The inclusive anchor re-returns the boundary article.
            client.historicalNews(req_id, "2025-01-02 11:00:00.0", "BRFG", "a-2", "h2")
            client.historicalNews(req_id, "2025-01-02 10:00:00.0", "BRFG", "a-1", "h1")
            client.historicalNewsEnd(req_id, False)

    monkeypatch.setattr(client, "reqHistoricalNews", req_historical_news)

    rows, exhausted = client.fetch_news_headlines(
        "AAPL", ["BRFG", "DJNL"], "2025-01-03 00:00:00.0", "2025-01-03 00:00:00.0", 3,
        since_utc="2025-01-01T00:00:00+00:00",
    )
    assert [(row["provider_code"], row["article_id"]) for row in rows] == [
        ("BRFG", "a-3"), ("DJNL", "a-3"), ("BRFG", "a-2"), ("BRFG", "a-1"),
    ]
    # Page 2 re-anchors both bounds at the oldest time of page 1.
    assert anchors[1] == ("2025-01-02 11:00:00.0", "2025-01-02 11:00:00.0")
    assert client._news == {}


def test_pagination_advances_past_a_page_of_boundary_duplicates(monkeypatch):
    """With a small page size the next page can consist ENTIRELY of the
    boundary article already seen. Stopping there would silently lose
    everything older; the walk must step one second past the boundary."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )
    anchors = []

    def req_historical_news(req_id, _con_id, _codes, start, _end, _limit, _opts):
        anchors.append(start)
        if len(anchors) <= 2:
            # Page 1 and the page anchored at its own time both return only
            # the boundary article (limit=1).
            client.historicalNews(req_id, "2025-01-02 12:00:00.0", "BRFG", "a-2", "h2")
            client.historicalNewsEnd(req_id, True)
        else:
            client.historicalNews(req_id, "2025-01-02 11:00:00.0", "BRFG", "a-1", "h1")
            client.historicalNewsEnd(req_id, False)

    monkeypatch.setattr(client, "reqHistoricalNews", req_historical_news)
    rows, exhausted = client.fetch_news_headlines(
        "AAPL", ["BRFG"], "2025-01-03 00:00:00.0", "2025-01-03 00:00:00.0", 1,
        since_utc="2025-01-01T00:00:00+00:00",
    )
    assert [row["article_id"] for row in rows] == ["a-2", "a-1"]
    assert anchors[2] == "2025-01-02 11:59:59.0"


def test_news_pagination_stops_once_the_checkpoint_is_reached(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )
    pages = []

    def req_historical_news(req_id, *_args):
        pages.append(req_id)
        client.historicalNews(req_id, "2025-01-02 12:00:00.0", "BRFG", "a-2", "h2")
        client.historicalNews(req_id, "2025-01-02 11:00:00.0", "BRFG", "a-1", "h1")
        client.historicalNewsEnd(req_id, True)   # IB says more exist...

    monkeypatch.setattr(client, "reqHistoricalNews", req_historical_news)
    rows, exhausted = client.fetch_news_headlines(
        "AAPL", ["BRFG"], "2025-01-03 00:00:00.0", "2025-01-03 00:00:00.0", 2,
        since_utc="2025-01-02T11:30:00+00:00",  # ...but the checkpoint is newer
    )
    assert len(pages) == 1, "older-than-checkpoint pages must not be requested"
    assert [row["article_id"] for row in rows] == ["a-2", "a-1"]


def test_ibapi_client_lists_provider_callbacks(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)

    def req_news_providers():
        client.newsProviders(
            [SimpleNamespace(providerCode="BRFG", providerName="Briefing.com")]
        )

    monkeypatch.setattr(client, "reqNewsProviders", req_news_providers)

    assert client.list_news_providers() == [{"code": "BRFG", "name": "Briefing.com"}]


def test_news_providers_accepts_ibapi_981_field_names(monkeypatch):
    # ibapi 9.81 (PyPI) names the fields code/name instead of providerCode/providerName.
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)

    def req_news_providers():
        client.newsProviders([SimpleNamespace(code="DJNL", name="Dow Jones")])

    monkeypatch.setattr(client, "reqNewsProviders", req_news_providers)
    assert client.list_news_providers() == [{"code": "DJNL", "name": "Dow Jones"}]


def test_parse_news_time_accepts_both_ibapi_formats():
    from pipeline.ibapi_client import IBApiClient

    # 10.x compact and 9.81 dashed, with and without fractions.
    assert IBApiClient._parse_news_time("20260501 14:27:06.0").startswith("2026-05-01T14:27:06")
    assert IBApiClient._parse_news_time("2026-05-01 14:27:06.0").startswith("2026-05-01T14:27:06")
    assert IBApiClient._parse_news_time("2026-05-01 14:27:06").startswith("2026-05-01T14:27:06")


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


def test_stream_tick_callbacks_record_rows_with_field_names():
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    client._stream_rows = []
    client._stream_symbols = {9001: "AAPL"}
    client.tickPrice(9001, 4, 250.25, None)      # 4 = last
    client.tickPrice(9001, 66, 250.10, None)     # 66 = delayed bid
    client.tickSize(9001, 8, 12345)              # 8 = volume
    client.tickPrice(9001, 4, -1, None)          # -1 sentinel ignored
    client.tickPrice(8888, 4, 1.0, None)         # unknown request ignored
    fields = [(r["symbol"], r["field"], r["value"]) for r in client._stream_rows]
    assert fields == [("AAPL", "last", 250.25), ("AAPL", "bid", 250.10), ("AAPL", "volume", 12345.0)]


def test_session_date_is_exchange_local_not_the_utc_receipt_date():
    from datetime import datetime, timezone

    from pipeline.ibapi_client import session_date_of

    # 00:30 UTC in January is 19:30 the previous day in New York.
    assert session_date_of(datetime(2026, 1, 6, 0, 30, tzinfo=timezone.utc)) == "2026-01-05"
    # Mid-session stays on the same date.
    assert session_date_of(datetime(2026, 1, 6, 15, 0, tzinfo=timezone.utc)) == "2026-01-06"


def test_unbounded_capture_returns_its_buffer_when_interrupted(monkeypatch):
    """duration 0 means 'until stopped', so a Ctrl-C must still return the ticks
    already collected -- draining after the finally silently discarded them."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _s: qualified_contract()
    )
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)
    cancels = []
    monkeypatch.setattr(client, "cancelMktData", lambda rid: cancels.append(rid))

    def interrupt(_seconds):
        client.tickPrice(client._stream_request_ids[0], 4, 250.0, None)
        raise KeyboardInterrupt

    monkeypatch.setattr("pipeline.ibapi_client.time.sleep", interrupt)
    rows = client.stream_quotes(["AAPL"], 0)
    # The tick buffered before the interrupt must come back, not be discarded.
    assert [(r["symbol"], r["field"], r["value"]) for r in rows] == [
        ("AAPL", "last", 250.0)
    ]
    assert cancels, "subscription should still be cancelled"


def test_rejected_subscription_raises_instead_of_reporting_zero_ticks(monkeypatch):
    """IB reports a market-data rejection on the request id, not as an
    exception, so a run would otherwise sit out its duration and 'succeed'."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)
    monkeypatch.setattr(client, "cancelMktData", lambda _r: None)

    def reject(_seconds):
        client.error(client._stream_request_ids[0], 354, "Requested market data is not subscribed")

    monkeypatch.setattr("pipeline.ibapi_client.time.sleep", reject)
    with pytest.raises(IBRequestError, match="354"):
        client.stream_quotes(["AAPL"], 5)


def test_connection_loss_stops_a_stream_instead_of_reporting_success(monkeypatch):
    """A dropped Gateway session is reported globally (1100), not against the
    subscription, so a stream would otherwise run on collecting nothing."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)
    monkeypatch.setattr(client, "cancelMktData", lambda _r: None)

    def drop(_seconds):
        client.error(-1, 1100, "Connectivity between IB and TWS has been lost")

    monkeypatch.setattr("pipeline.ibapi_client.time.sleep", drop)
    with pytest.raises(IBRequestError, match="1100"):
        client.stream_quotes(["AAPL"], 5)


def test_error_during_the_final_interval_is_not_lost_to_the_deadline(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)
    monkeypatch.setattr(client, "cancelMktData", lambda _r: None)

    import time as _time

    real_sleep = _time.sleep

    def reject_then_expire(seconds):
        # Arrive during the sleep that exhausts the run's duration.
        client.error(client._stream_request_ids[0], 354, "not subscribed")
        real_sleep(seconds)

    monkeypatch.setattr("pipeline.ibapi_client.time.sleep", reject_then_expire)
    with pytest.raises(IBRequestError, match="354"):
        client.stream_quotes(["AAPL"], 0.05)


def test_dropped_socket_stops_a_stream_even_without_an_error_code(monkeypatch):
    """Killing Gateway closes the socket through connectionClosed() without
    necessarily emitting 1100, which used to read as a successful empty run."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)
    monkeypatch.setattr(client, "cancelMktData", lambda _r: None)

    monkeypatch.setattr(
        "pipeline.ibapi_client.time.sleep", lambda _s: client.connectionClosed()
    )
    with pytest.raises(IBRequestError, match="1100"):
        client.stream_quotes(["AAPL"], 5)


def test_intentional_disconnect_is_not_reported_as_a_failure(monkeypatch):
    # A graceful shutdown also fires connectionClosed(); it must not be
    # recorded as a dropped connection.
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(EClient, "disconnect", lambda _c: client.connectionClosed())
    client.disconnect()
    assert client.stream_error() is None


def test_sentinel_ticks_are_rejected_not_recorded():
    """IB signals "no value yet" with sentinels rather than omitting the tick.
    Recording them produced impossible bars (volume -36356795, open 0.0) and
    through them nonsensical live alphas."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    client._stream_symbols = {1: "AAPL"}
    client._stream_rows = []
    client.tickPrice(1, 4, 250.0, None)       # good last
    client.tickPrice(1, 14, 0.0, None)        # open sentinel (0)
    client.tickPrice(1, 1, -3.0, None)        # negative price
    client.tickSize(1, 8, -36356795)          # negative volume
    client.tickSize(1, 8, 1_000_000)          # good volume
    assert [(r["field"], r["value"]) for r in client._stream_rows] == [
        ("last", 250.0),
        ("volume", 1000000.0),
    ]


def test_late_error_after_cleanup_neither_raises_nor_recreates_state():
    """error() runs on the socket reader thread; the caller's timeout cleanup
    can pop the request in between. A late error for a finished request must
    be dropped -- not KeyError inside the reader loop, not recreate the freed
    entries as orphans. The lock in error()/_clean_request closes the
    interleaved window by construction; this pins the end state."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    request_id = client._new_request()
    client._clean_request(request_id)
    client.error(request_id, 162, "late pacing violation")
    assert client._events == {} and client._errors == {}


def test_late_news_end_does_not_recreate_the_freed_has_more_entry():
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    client.historicalNewsEnd(4242, True)
    assert client._news == {}


def test_ticks_after_unsubscription_are_dropped_not_stranded(monkeypatch):
    """stop_quote_stream and _record_tick share one locked section: a tick
    decoded during shutdown either lands before the final drain or is
    dropped -- it can never append behind the drain and sit stranded for the
    next stream."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    client._stream_symbols = {9001: "AAPL"}
    client._stream_request_ids = [9001]
    monkeypatch.setattr(client, "cancelMktData", lambda _r: None)
    client.stop_quote_stream()
    client.tickPrice(9001, 4, 250.0, None)
    assert client.drain_ticks() == []


def test_rejection_during_shutdown_is_logged_and_the_ticks_are_kept(monkeypatch):
    """A rejection landing between the loop's last check and the shutdown used
    to be erased by stop_quote_stream's cleanup; raising it instead would
    discard the rows already captured. It is logged and the data returned."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)
    monkeypatch.setattr(client, "cancelMktData", lambda _r: None)

    def tick_error_interrupt(_seconds):
        client.tickPrice(client._stream_request_ids[0], 4, 250.0, None)
        client.error(client._stream_request_ids[0], 354, "not subscribed")
        raise KeyboardInterrupt

    monkeypatch.setattr("pipeline.ibapi_client.time.sleep", tick_error_interrupt)
    rows = client.stream_quotes(["AAPL"], 0)
    assert [(r["symbol"], r["value"]) for r in rows] == [("AAPL", 250.0)]


def test_connect_refuses_to_race_a_stale_reader_thread(monkeypatch):
    """A reader thread that outlived disconnect's bounded join could deliver
    its late nextValidId into the NEXT session and complete the handshake
    prematurely; connect() must wait it out or refuse."""
    import threading

    client = IBApiClient(
        "127.0.0.1", 4002, 31,
        request_timeout_seconds=1, connect_timeout_seconds=0.2,
    )
    release = threading.Event()
    stale = threading.Thread(target=release.wait, daemon=True)
    stale.start()
    client._stale_network_thread = stale
    monkeypatch.setattr(EClient, "connect", lambda *_a, **_k: None)
    monkeypatch.setattr(EClient, "disconnect", lambda _c: None)
    try:
        with pytest.raises(RuntimeError, match="reader thread"):
            client.connect()
    finally:
        release.set()


def test_connect_proceeds_once_the_previous_reader_finished(monkeypatch):
    import threading

    client = IBApiClient(
        "127.0.0.1", 4002, 31,
        request_timeout_seconds=1, connect_timeout_seconds=0.1,
    )
    finished = threading.Thread(target=lambda: None)
    finished.start()
    finished.join()
    client._stale_network_thread = finished
    monkeypatch.setattr(EClient, "connect", lambda *_a, **_k: None)
    monkeypatch.setattr(EClient, "disconnect", lambda _c: None)
    monkeypatch.setattr(client, "run", lambda: None)
    # Past the stale-reader gate, the handshake itself times out (no Gateway
    # here) -- which is the point: the gate did not block a clean reconnect.
    with pytest.raises(TimeoutError, match="Timed out connecting"):
        client.connect()
    assert client._stale_network_thread is None


def test_truncated_walk_reports_not_exhausted(monkeypatch):
    """When the page budget runs out with hasMore still flagged, the caller
    must know: advancing its checkpoint past the oldest row would skip the
    unread remainder forever."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )
    pages = {"n": 0}

    def req_historical_news(req_id, *_args):
        pages["n"] += 1
        hour = 12 - pages["n"]
        client.historicalNews(
            req_id, "2025-01-02 %02d:00:00.0" % hour, "BRFG",
            "a-%d" % pages["n"], "h",
        )
        client.historicalNewsEnd(req_id, True)   # always more

    monkeypatch.setattr(client, "reqHistoricalNews", req_historical_news)
    rows, exhausted = client.fetch_news_headlines(
        "AAPL", ["BRFG"], "2025-01-03 00:00:00.0", "2025-01-03 00:00:00.0", 1,
        since_utc="2025-01-01T00:00:00+00:00", max_pages=2,
    )
    assert len(rows) == 2 and exhausted is False


def test_connection_loss_wakes_pending_requests_immediately(monkeypatch):
    """A request already waiting when the socket dies must fail with the
    connection error at once, not sit out its full timeout for a response
    that can never arrive."""
    import time as _time

    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=5)
    monkeypatch.setattr(
        client, "resolve_stock_contract", lambda _symbol: qualified_contract()
    )

    def req_then_drop(_req_id, *_args):
        client.connectionClosed()

    monkeypatch.setattr(client, "reqHistoricalData", req_then_drop)
    monkeypatch.setattr(client, "cancelHistoricalData", lambda _r: None)
    started = _time.time()
    with pytest.raises(IBRequestError, match="1100"):
        client.fetch_daily_bars("AAPL", "7 D")
    assert _time.time() - started < 2, "must not wait out the 5s timeout"


def test_failed_subscription_startup_cancels_the_half_built_set(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    resolved = {"n": 0}

    def resolve(symbol):
        resolved["n"] += 1
        if resolved["n"] > 1:
            raise IBRequestError(-1, 200, "ambiguous contract for " + symbol)
        return qualified_contract()

    monkeypatch.setattr(client, "resolve_stock_contract", resolve)
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)
    cancels = []
    monkeypatch.setattr(client, "cancelMktData", lambda rid: cancels.append(rid))

    with pytest.raises(IBRequestError, match="200"):
        client.start_quote_stream(["AAPL", "BAD"])
    assert cancels, "the already-made subscription must be cancelled"
    assert client._stream_request_ids == [] and client._stream_symbols == {}


def test_rejection_during_the_cancel_itself_is_latched(monkeypatch):
    """stop_quote_stream erases per-request errors; the latch keeps a
    rejection that arrives during the cancels visible to the caller."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)

    def reject_during_cancel(request_id):
        client.error(request_id, 354, "not subscribed, reported during cancel")

    monkeypatch.setattr(client, "cancelMktData", reject_during_cancel)
    client.start_quote_stream(["AAPL"])
    client.stop_quote_stream()
    error = client.stream_error()
    assert error is not None and error.error_code == 354


def test_connection_loss_also_wakes_a_pending_providers_request(monkeypatch):
    import time as _time

    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=5)
    monkeypatch.setattr(
        client, "reqNewsProviders", lambda: client.connectionClosed()
    )
    started = _time.time()
    with pytest.raises(IBRequestError, match="1100"):
        client.list_news_providers()
    assert _time.time() - started < 2


def test_request_born_on_a_dead_connection_fails_immediately():
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=5)
    client._connection_error = IBRequestError(-1, 1100, "already dead")
    request_id = client._new_request()
    assert client._events[request_id].is_set()
    with pytest.raises(IBRequestError, match="1100"):
        client._wait_for_request(request_id, "anything")
    client._clean_request(request_id)


def test_cancel_failure_does_not_abandon_the_remaining_cleanup(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)
    client.start_quote_stream(["AAPL", "MSFT"])
    attempted = []

    def failing_cancel(request_id):
        attempted.append(request_id)
        if len(attempted) == 1:
            raise OSError("socket write failed")

    monkeypatch.setattr(client, "cancelMktData", failing_cancel)
    with pytest.raises(OSError, match="socket write failed"):
        client.stop_quote_stream()
    assert len(attempted) == 2, "the second cancel must still be attempted"
    assert client._stream_request_ids == [] and client._stream_symbols == {}


def test_rejection_landing_inside_the_cleanup_window_is_still_latched():
    """After _clean_request removed the event but before the subscription
    list is cleared, an arriving rejection must still latch: this is exactly
    stop_quote_stream's cleanup window."""
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    client._stream_request_ids = [9001]          # stop not finished yet
    client.error(9001, 354, "arrived mid-cleanup")   # no event registered
    error = client.stream_error()
    assert error is not None and error.error_code == 354


def test_cancel_failure_does_not_mask_the_capture_rejection(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
    monkeypatch.setattr(client, "reqMarketDataType", lambda _t: None)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqMktData", lambda *_a, **_k: None)

    def failing_cancel(_request_id):
        raise OSError("cancel socket write failed")

    monkeypatch.setattr(client, "cancelMktData", failing_cancel)

    def reject(_seconds):
        client.error(client._stream_request_ids[0], 354, "not subscribed")

    monkeypatch.setattr("pipeline.ibapi_client.time.sleep", reject)
    with pytest.raises(IBRequestError, match="354"):
        client.stream_quotes(["AAPL"], 5)


def test_timeout_survives_a_failing_cancel(monkeypatch):
    client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=0.05)
    monkeypatch.setattr(client, "resolve_stock_contract", lambda _s: qualified_contract())
    monkeypatch.setattr(client, "reqHistoricalData", lambda *_a: None)

    def failing_cancel(_request_id):
        raise OSError("dead socket")

    monkeypatch.setattr(client, "cancelHistoricalData", failing_cancel)
    with pytest.raises(TimeoutError):
        client.fetch_daily_bars("AAPL", "7 D")


def test_same_second_saturation_warns_but_sub_limit_pages_do_not(monkeypatch):
    """A FULL page of boundary duplicates can hide same-second articles the
    one-second step skips; a sub-limit duplicate page cannot, so it steps
    silently."""
    import logging

    def run(limit):
        client = IBApiClient("127.0.0.1", 4002, 31, request_timeout_seconds=1)
        monkeypatch.setattr(
            client, "resolve_stock_contract", lambda _s: qualified_contract()
        )
        pages = {"n": 0}

        def req(req_id, *_args):
            pages["n"] += 1
            if pages["n"] <= 2:
                client.historicalNews(req_id, "2025-01-02 12:00:00.0", "BRFG", "a-2", "h")
                client.historicalNewsEnd(req_id, True)
            else:
                client.historicalNews(req_id, "2025-01-02 11:00:00.0", "BRFG", "a-1", "h")
                client.historicalNewsEnd(req_id, False)

        monkeypatch.setattr(client, "reqHistoricalNews", req)
        records = []
        handler = logging.Handler()
        handler.emit = records.append
        logger = logging.getLogger("ibkr_pipeline")
        logger.addHandler(handler)
        try:
            client.fetch_news_headlines(
                "AAPL", ["BRFG"], "2025-01-03 00:00:00.0", "2025-01-03 00:00:00.0",
                limit, since_utc="2025-01-01T00:00:00+00:00",
            )
        finally:
            logger.removeHandler(handler)
        return [r.msg for r in records]

    assert any("same_second" in m for m in run(limit=1))
    assert not any("same_second" in m for m in run(limit=5))
