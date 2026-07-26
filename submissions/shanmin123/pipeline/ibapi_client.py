import logging
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper


US_EXCHANGE_TZ = "America/New_York"


def session_date_of(moment):
    """Exchange-local trading date for a UTC instant (US equities)."""
    import pandas as pd

    return str(pd.Timestamp(moment).tz_convert(US_EXCHANGE_TZ).date())


class IBRequestError(RuntimeError):
    def __init__(self, request_id, error_code, message):
        self.request_id = request_id
        self.error_code = error_code
        self.message = message
        super().__init__("IB API error {0}: {1}".format(error_code, message))


class IBApiClient(EWrapper, EClient):
    def __init__(
        self,
        host,
        port,
        client_id,
        request_timeout_seconds=30,
        connect_timeout_seconds=10,
    ):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.host = host
        self.port = port
        self.client_id = client_id
        self.request_timeout_seconds = request_timeout_seconds
        self.connect_timeout_seconds = connect_timeout_seconds

        self._connected_event = threading.Event()
        self._connection_error = None
        self._closing = False
        self._network_thread = None
        self._stale_network_thread = None
        self._id_lock = threading.Lock()
        self._next_id = 1000

        self._events = {}
        self._errors = {}
        self._contract_details = {}
        self._bars = {}
        self._news = {}
        self._news_has_more = {}

        self._providers_event = threading.Event()
        self._providers = []
        self._providers_error = None

        # Tick buffer: filled by the socket reader thread, drained by the caller.
        self._stream_lock = threading.Lock()
        self._stream_rows = []
        self._stream_symbols = {}
        self._stream_request_ids = []

    def connect(self):
        # A reader thread from a previous session that outlived disconnect's
        # bounded join must finish before a new one starts: its late
        # callbacks (nextValidId) would otherwise be attributed to the new
        # session and complete the handshake prematurely.
        stale = self._stale_network_thread
        if stale is not None and stale.is_alive():
            stale.join(self.connect_timeout_seconds)
            if stale.is_alive():
                raise RuntimeError(
                    "The previous IB reader thread is still running; refusing "
                    "to reconnect over it."
                )
        self._stale_network_thread = None
        self._connected_event.clear()
        self._connection_error = None
        self._closing = False
        EClient.connect(
            self,
            self.host,
            self.port,
            clientId=self.client_id,
        )
        if self._connection_error is not None:
            EClient.disconnect(self)
            raise self._connection_error
        self._network_thread = threading.Thread(
            target=self.run,
            name="ibapi-network-loop",
            daemon=True,
        )
        self._network_thread.start()
        if not self._connected_event.wait(self.connect_timeout_seconds):
            self.disconnect()
            raise TimeoutError(
                "Timed out connecting to IB Gateway at {0}:{1}".format(
                    self.host, self.port
                )
            )
        if self._connection_error is not None:
            self.disconnect()
            raise self._connection_error

    def disconnect(self):
        # Mark the shutdown as intentional so the connectionClosed callback it
        # triggers is not mistaken for a dropped socket.
        self._closing = True
        EClient.disconnect(self)
        network_thread = self._network_thread
        self._network_thread = None
        if (
            network_thread is not None
            and network_thread is not threading.current_thread()
        ):
            network_thread.join(timeout=2)
            if network_thread.is_alive():
                # Keep the handle: connect() must wait this thread out before
                # starting a successor.
                self._stale_network_thread = network_thread

    def nextValidId(self, orderId):
        with self._id_lock:
            self._next_id = max(self._next_id, orderId)
        self._connected_event.set()

    def connectionClosed(self):
        # The reader calls this for a graceful disconnect AND for a socket that
        # died (Gateway killed, cable pulled), which does not always come with a
        # 1100/502/504 error. Without recording it, a stream would keep waiting
        # or finish "successfully" having collected nothing.
        if not self._closing and self._connection_error is None:
            self._connection_error = IBRequestError(
                -1, 1100, "IB Gateway connection closed while the client was running"
            )
        self._connected_event.set()

    def error(self, reqId, errorCode, errorString, *args):
        if 2100 <= errorCode <= 2199:
            return

        error = IBRequestError(reqId, errorCode, errorString)
        # Runs on the socket reader thread while _clean_request may be popping
        # the same request from the caller thread. The snapshot and the error
        # write happen under one lock: a two-step membership check would
        # KeyError inside the reader loop when the pop lands in between, and
        # writing after the pop would recreate the freed entry as an orphan.
        with self._id_lock:
            event = self._events.get(reqId)
            if event is not None:
                self._errors[reqId] = error
        if event is not None:
            event.set()
            return

        if reqId == -1:
            self._providers_error = error
            self._providers_event.set()
            if errorCode in (502, 504, 1100):
                self._connection_error = error
                self._connected_event.set()

    def resolve_stock_contract(self, symbol):
        # SMART/USD alone is ambiguous for some tickers (IB error 200), in which
        # case the resolution is retried pinned to each major primary exchange.
        attempts = (None, "NYSE", "NASDAQ", "ARCA", "AMEX")
        last_error = None
        for primary in attempts:
            try:
                return self._resolve_stock_contract_once(symbol, primary)
            except (IBRequestError, LookupError) as exc:
                last_error = exc
                if isinstance(exc, IBRequestError) and exc.error_code != 200:
                    raise
        raise last_error

    def _resolve_stock_contract_once(self, symbol, primary_exchange):
        request_id = self._new_request()
        contract = Contract()
        # IB writes share classes with a space ("BRK B"); data vendors use
        # "BRK.B" or "BRK-B". Map both separators.
        contract.symbol = symbol.replace(".", " ").replace("-", " ")
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        if primary_exchange:
            contract.primaryExchange = primary_exchange

        self._contract_details[request_id] = []
        try:
            self.reqContractDetails(request_id, contract)
            self._wait_for_request(request_id, "contract details")
            details = self._contract_details[request_id]
            if not details:
                raise LookupError("No IBKR stock contract found for {0}".format(symbol))
            return details[0].contract
        finally:
            self._clean_request(request_id)
            self._contract_details.pop(request_id, None)

    # Response callbacks append only to a buffer that still exists. A response
    # landing AFTER its request timed out and was cleaned up must be ignored:
    # setdefault would silently recreate the buffer, which then leaks forever
    # because no caller is left to pop it.

    def contractDetails(self, reqId, contractDetails):
        bucket = self._contract_details.get(reqId)
        if bucket is not None:
            bucket.append(contractDetails)

    def contractDetailsEnd(self, reqId):
        self._finish_request(reqId)

    def fetch_daily_bars(self, symbol, duration, end_datetime="", use_rth=True):
        contract = self.resolve_stock_contract(symbol)
        request_id = self._new_request()
        self._bars[request_id] = []
        try:
            self.reqHistoricalData(
                request_id,
                contract,
                end_datetime,
                duration,
                "1 day",
                "TRADES",
                1 if use_rth else 0,
                1,
                False,
                [],
            )
            try:
                self._wait_for_request(request_id, "historical daily bars")
            except TimeoutError:
                # The request is still live on the Gateway: cancel it so the
                # retry does not stack a second identical request on top of it
                # (pacing pressure) and so its late bars go nowhere.
                self.cancelHistoricalData(request_id)
                raise
            return [
                self._normalise_bar(symbol, contract, bar)
                for bar in self._bars[request_id]
            ]
        finally:
            self._clean_request(request_id)
            self._bars.pop(request_id, None)

    def historicalData(self, reqId, bar):
        bucket = self._bars.get(reqId)
        if bucket is not None:
            bucket.append(bar)

    def historicalDataEnd(self, reqId, start, end):
        self._finish_request(reqId)

    def list_news_providers(self):
        # reqNewsProviders carries no request id, so a response from a
        # timed-out earlier call cannot be told apart from the current one.
        # The provider set is static per account (it changes only with
        # entitlements), so a late response carries the same content and is
        # accepted rather than fenced with machinery the wire cannot support.
        self._providers = []
        self._providers_error = None
        self._providers_event.clear()
        self.reqNewsProviders()
        if not self._providers_event.wait(self.request_timeout_seconds):
            raise TimeoutError("Timed out waiting for IBKR news providers")
        if self._providers_error is not None:
            raise self._providers_error
        return list(self._providers)

    def newsProviders(self, newsProviders):
        # ibapi 9.81 names the fields code/name; 10.x uses providerCode/providerName.
        # An exception here runs on the socket reader thread, so it is recorded and
        # re-raised on the caller side instead of killing the reader.
        try:
            self._providers = [
                {
                    "code": getattr(provider, "providerCode", None)
                    or getattr(provider, "code", ""),
                    "name": getattr(provider, "providerName", None)
                    or getattr(provider, "name", ""),
                }
                for provider in newsProviders
            ]
        except Exception as exc:
            self._providers_error = exc
        self._providers_event.set()

    def fetch_news_headlines(self, symbol, provider_codes, start, end, limit,
                             since_utc=None, max_pages=5):
        """Fetch headlines, following IB's backwards pagination.

        reqHistoricalNews returns up to ``limit`` headlines at or before the
        anchor time (newest first) and flags ``hasMore`` when the window holds
        older ones. Without following that flag, a burst larger than ``limit``
        between two runs would lose its older headlines permanently. Each next
        page re-anchors at the oldest time received; the walk stops when the
        feed is exhausted, ``since_utc`` (ISO timestamp, e.g. the caller's
        checkpoint) is reached, or ``max_pages`` is hit. The inclusive anchor
        re-returns the boundary article, so already-seen article ids are
        skipped.
        """
        contract = self.resolve_stock_contract(symbol)
        stop_at = None
        if since_utc:
            stop_at = datetime.fromisoformat(
                str(since_utc).replace("Z", "+00:00")
            )
            if stop_at.tzinfo is None:
                stop_at = stop_at.replace(tzinfo=timezone.utc)
        collected = []
        seen_ids = set()
        page_start, page_end = start, end
        for _page in range(max(1, int(max_pages))):
            request_id = self._new_request()
            self._news[request_id] = []
            self._news_has_more[request_id] = False
            try:
                self.reqHistoricalNews(
                    request_id,
                    contract.conId,
                    "+".join(provider_codes),
                    page_start,
                    page_end,
                    limit,
                    [],
                )
                self._wait_for_request(request_id, "historical news headlines")
                raw = list(self._news[request_id])
                has_more = self._news_has_more[request_id]
            finally:
                self._clean_request(request_id)
                self._news.pop(request_id, None)
                # Under the same lock historicalNewsEnd's check-and-set holds:
                # an unlocked pop could land between its check and its write,
                # which would recreate the entry as an orphan.
                with self._id_lock:
                    self._news_has_more.pop(request_id, None)
            fresh = []
            for row in raw:
                # Article ids are provider-specific: the persisted identity is
                # (symbol, provider_code, article_id), so the in-walk dedup
                # must match it or a shared id would drop another provider's
                # article.
                key = (row[1], row[2])
                if key in seen_ids:
                    continue
                seen_ids.add(key)
                fresh.append(
                    {
                        "published_at_utc": self._parse_news_time(row[0]),
                        "symbol": symbol,
                        "con_id": contract.conId,
                        "provider_code": row[1],
                        "article_id": row[2],
                        "headline": self._clean_headline(row[3]),
                    }
                )
            collected.extend(fresh)
            if not has_more:
                break
            if fresh:
                oldest = min(
                    datetime.fromisoformat(item["published_at_utc"])
                    for item in fresh
                )
            elif raw:
                # The whole page was boundary duplicates (possible when the
                # page size is small): step one second past them so the walk
                # still progresses instead of stalling and losing everything
                # older. Older articles inside that same second are the cost
                # of the anchor's second precision.
                oldest = min(
                    datetime.fromisoformat(self._parse_news_time(row[0]))
                    for row in raw
                ) - timedelta(seconds=1)
            else:
                break
            if stop_at is not None and oldest <= stop_at:
                break
            # Same both-bounds anchoring that the first page uses.
            page_start = page_end = oldest.strftime("%Y-%m-%d %H:%M:%S.0")
        return collected

    def historicalNews(self, requestId, time, providerCode, articleId, headline):
        bucket = self._news.get(requestId)
        if bucket is not None:
            bucket.append((time, providerCode, articleId, headline))

    def historicalNewsEnd(self, requestId, hasMore):
        # Check-and-set under the lock: writing after the caller's timeout
        # cleanup popped the entry would recreate it as an orphan.
        with self._id_lock:
            if requestId in self._news_has_more:
                self._news_has_more[requestId] = bool(hasMore)
        self._finish_request(requestId)

    def _new_request(self):
        with self._id_lock:
            request_id = self._next_id
            self._next_id += 1
        self._events[request_id] = threading.Event()
        return request_id

    def _finish_request(self, request_id):
        event = self._events.get(request_id)
        if event is not None:
            event.set()

    def _wait_for_request(self, request_id, description):
        if not self._events[request_id].wait(self.request_timeout_seconds):
            raise TimeoutError("Timed out waiting for {0}".format(description))
        error = self._errors.get(request_id)
        if error is not None:
            raise error

    def _clean_request(self, request_id):
        with self._id_lock:
            self._events.pop(request_id, None)
            self._errors.pop(request_id, None)

    @staticmethod
    def _normalise_bar(symbol, contract, bar):
        event_date = datetime.strptime(str(bar.date), "%Y%m%d").date().isoformat()
        return {
            "event_date": event_date,
            "symbol": symbol,
            "con_id": contract.conId,
            "exchange": contract.exchange,
            "currency": contract.currency,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "bar_count": bar.barCount,
            # ibapi 9.81 exposes the weighted average price as `average`; 10.x as `wap`.
            "wap": getattr(bar, "wap", getattr(bar, "average", None)),
        }

    def start_quote_stream(self, symbols, market_data_type=3):
        """Subscribe to quote ticks for ``symbols`` and return immediately.

        Uses reqMktData streaming. market_data_type 3 = delayed (the free paper
        tier, 15-20 minutes behind); with live market-data subscriptions the
        same subscription streams real time (type 1). Ticks accumulate in a
        buffer that ``drain_ticks`` empties.
        """
        self.reqMarketDataType(market_data_type)
        with self._stream_lock:
            self._stream_rows = []
        self._stream_symbols = {}
        self._stream_request_ids = []
        for symbol in symbols:
            contract = self.resolve_stock_contract(symbol)
            request_id = self._new_request()
            self._stream_symbols[request_id] = symbol
            self._stream_request_ids.append(request_id)
            self.reqMktData(request_id, contract, "", False, False, [])
        return list(self._stream_request_ids)

    def drain_ticks(self):
        """Return the ticks received since the last drain and clear the buffer."""
        with self._stream_lock:
            rows = self._stream_rows
            self._stream_rows = []
        return rows

    def stream_error(self):
        """The first error that invalidates the live stream, if any.

        Two kinds arrive without raising: market-data rejections (e.g. 354, not
        subscribed) are reported against the subscription's request id, and a
        dropped Gateway session (1100/502/504) is reported globally. Neither
        would otherwise stop a run, which would then report success having
        collected nothing.
        """
        for request_id in getattr(self, "_stream_request_ids", []):
            error = self._errors.get(request_id)
            if error is not None:
                return error
        return self._connection_error

    def stop_quote_stream(self):
        request_ids = list(getattr(self, "_stream_request_ids", []))
        for request_id in request_ids:
            self.cancelMktData(request_id)
        # One locked pass over the subscriptions (socket writes above stay
        # outside the lock): after this, a tick still being decoded finds no
        # symbol and is dropped inside _record_tick's own locked section, so
        # nothing can append behind the caller's final drain.
        with self._stream_lock:
            for request_id in request_ids:
                self._stream_symbols.pop(request_id, None)
        for request_id in request_ids:
            self._clean_request(request_id)
        self._stream_request_ids = []

    def stream_quotes(self, symbols, duration_seconds, market_data_type=3):
        """Collect ticks for a fixed window (batch wrapper over the stream primitives).

        ``duration_seconds`` of 0 means unbounded, matching the streaming
        pipeline, and the poll never sleeps past a bounded deadline.
        """
        self.start_quote_stream(symbols, market_data_type=market_data_type)
        collected = []
        try:
            deadline = time.time() + duration_seconds if duration_seconds else None
            while deadline is None or time.time() < deadline:
                error = self.stream_error()
                if error is not None:
                    raise error
                nap = 0.25 if deadline is None else min(0.25, deadline - time.time())
                if nap > 0:
                    time.sleep(nap)
            # The deadline is about to end the loop, so check once more: an
            # error raised during the final sleep would otherwise be discarded
            # by the shutdown that follows.
            error = self.stream_error()
            if error is not None:
                raise error
        except KeyboardInterrupt:
            # Stopping an unbounded capture is the documented way to end it, so
            # it returns what it collected rather than propagating and losing it.
            pass
        finally:
            # Drain BEFORE cancelling (stop_quote_stream drops the request
            # mapping, so later callbacks are discarded) and inside the finally,
            # or an interrupted unbounded capture would return nothing. The
            # post-cancel drain must run even if the cancel raises, or the
            # ticks decoded in between would be stranded in the buffer.
            collected.extend(self.drain_ticks())
            # A rejection can land between the loop's last check and here, and
            # stop_quote_stream erases per-request errors. The capture itself
            # already succeeded, so it is reported as a warning with the data
            # kept, not raised after the fact (which would discard the rows).
            pending_error = None
            if sys.exc_info()[1] is None:
                pending_error = self.stream_error()
            try:
                self.stop_quote_stream()
            finally:
                collected.extend(self.drain_ticks())
            if pending_error is not None:
                logging.getLogger("ibkr_pipeline").warning(
                    "stream_error_after_capture: %s", pending_error
                )
        return collected

    _TICK_PRICE_FIELDS = {
        1: "bid", 2: "ask", 4: "last", 6: "high", 7: "low", 9: "close",
        14: "open",
        66: "bid", 67: "ask", 68: "last", 72: "high", 73: "low", 75: "close",
        76: "open",
    }
    _TICK_SIZE_FIELDS = {
        0: "bid_size", 3: "ask_size", 5: "last_size", 8: "volume",
        69: "bid_size", 70: "ask_size", 71: "last_size", 74: "volume",
    }

    def tickPrice(self, reqId, tickType, price, attrib):
        self._record_tick(reqId, self._TICK_PRICE_FIELDS.get(tickType), price)

    def tickSize(self, reqId, tickType, size):
        self._record_tick(reqId, self._TICK_SIZE_FIELDS.get(tickType), size)

    #: Fields that carry a price: IB sends 0 or a negative as "no value yet".
    _PRICE_FIELDS = frozenset({"last", "bid", "ask", "open", "close", "high", "low"})

    def _record_tick(self, request_id, field, value):
        if field is None or value is None:
            return
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return
        # IB signals "unavailable" with sentinels rather than omitting the tick:
        # -1 on any field, 0 or negative on a price, and a negative size/volume
        # (observed: volume -36356795 outside market hours). Recording those
        # produced impossible bars and, through them, nonsensical live alphas.
        if numeric == -1:
            return
        if field in self._PRICE_FIELDS and numeric <= 0:
            return
        if field not in self._PRICE_FIELDS and numeric < 0:
            return
        received = datetime.now(timezone.utc)
        row = {
            "tick_time_utc": received.isoformat(),
            # The trading session is an exchange-local concept: a US
            # extended-hours tick received after UTC midnight still belongs to
            # the previous session, so the receipt date would misfile it.
            "session_date": session_date_of(received),
            "symbol": None,
            "field": field,
            "value": numeric,
        }
        # Symbol lookup and append share one locked section with
        # stop_quote_stream's unsubscription: looked up outside it, a tick
        # decoded during shutdown could append AFTER the final drain and sit
        # stranded in the buffer.
        with self._stream_lock:
            symbol = self._stream_symbols.get(request_id)
            if symbol is None:
                return
            row["symbol"] = symbol
            self._stream_rows.append(row)

    @staticmethod
    def _parse_news_time(value):
        # ibapi 10.x emits compact times ("20260501 14:27:06.0"); 9.81 emits
        # dash-separated ("2026-05-01 14:27:06.0"). Accept both, with or
        # without the fractional part.
        clean = value.strip()
        formats = (
            "%Y%m%d %H:%M:%S.%f",
            "%Y%m%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        )
        last_error = None
        for time_format in formats:
            try:
                parsed = datetime.strptime(clean, time_format)
            except ValueError as exc:
                last_error = exc
                continue
            return parsed.replace(tzinfo=timezone.utc).isoformat()
        raise last_error

    @staticmethod
    def _clean_headline(value):
        return re.sub(r"^(?:\{[^}]+\})+", "", value).strip()
