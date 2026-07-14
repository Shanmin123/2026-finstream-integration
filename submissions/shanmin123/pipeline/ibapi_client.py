import re
import threading
from datetime import datetime, timezone

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper


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
        self._network_thread = None
        self._id_lock = threading.Lock()
        self._next_id = 1000

        self._events = {}
        self._errors = {}
        self._contract_details = {}
        self._bars = {}
        self._news = {}

        self._providers_event = threading.Event()
        self._providers = []
        self._providers_error = None

    def connect(self):
        self._connected_event.clear()
        self._connection_error = None
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
        EClient.disconnect(self)
        network_thread = self._network_thread
        self._network_thread = None
        if (
            network_thread is not None
            and network_thread is not threading.current_thread()
        ):
            network_thread.join(timeout=2)

    def nextValidId(self, orderId):
        with self._id_lock:
            self._next_id = max(self._next_id, orderId)
        self._connected_event.set()

    def connectionClosed(self):
        self._connected_event.set()

    def error(self, reqId, errorCode, errorString, *args):
        if 2100 <= errorCode <= 2199:
            return

        error = IBRequestError(reqId, errorCode, errorString)
        if reqId in self._events:
            self._errors[reqId] = error
            self._events[reqId].set()
            return

        if reqId == -1:
            self._providers_error = error
            self._providers_event.set()
            if errorCode in (502, 504, 1100):
                self._connection_error = error
                self._connected_event.set()

    def resolve_stock_contract(self, symbol):
        request_id = self._new_request()
        contract = Contract()
        contract.symbol = symbol.replace(".", " ")
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

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

    def contractDetails(self, reqId, contractDetails):
        self._contract_details.setdefault(reqId, []).append(contractDetails)

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
            self._wait_for_request(request_id, "historical daily bars")
            return [
                self._normalise_bar(symbol, contract, bar)
                for bar in self._bars[request_id]
            ]
        finally:
            self._clean_request(request_id)
            self._bars.pop(request_id, None)

    def historicalData(self, reqId, bar):
        self._bars.setdefault(reqId, []).append(bar)

    def historicalDataEnd(self, reqId, start, end):
        self._finish_request(reqId)

    def list_news_providers(self):
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
        self._providers = [
            {"code": provider.providerCode, "name": provider.providerName}
            for provider in newsProviders
        ]
        self._providers_event.set()

    def fetch_news_headlines(self, symbol, provider_codes, start, end, limit):
        contract = self.resolve_stock_contract(symbol)
        request_id = self._new_request()
        self._news[request_id] = []
        try:
            self.reqHistoricalNews(
                request_id,
                contract.conId,
                "+".join(provider_codes),
                start,
                end,
                limit,
                [],
            )
            self._wait_for_request(request_id, "historical news headlines")
            return [
                {
                    "published_at_utc": self._parse_news_time(row[0]),
                    "symbol": symbol,
                    "con_id": contract.conId,
                    "provider_code": row[1],
                    "article_id": row[2],
                    "headline": self._clean_headline(row[3]),
                }
                for row in self._news[request_id]
            ]
        finally:
            self._clean_request(request_id)
            self._news.pop(request_id, None)

    def historicalNews(self, requestId, time, providerCode, articleId, headline):
        self._news.setdefault(requestId, []).append(
            (time, providerCode, articleId, headline)
        )

    def historicalNewsEnd(self, requestId, hasMore):
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
            "wap": bar.average,
        }

    @staticmethod
    def _parse_news_time(value):
        clean = value.strip()
        time_format = "%Y%m%d %H:%M:%S.%f" if "." in clean else "%Y%m%d %H:%M:%S"
        parsed = datetime.strptime(clean, time_format)
        return parsed.replace(tzinfo=timezone.utc).isoformat()

    @staticmethod
    def _clean_headline(value):
        return re.sub(r"^(?:\{[^}]+\})+", "", value).strip()
