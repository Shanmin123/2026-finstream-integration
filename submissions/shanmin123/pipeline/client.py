from typing import List, Protocol, Tuple


class IBDataClient(Protocol):
    def connect(self) -> None:
        ...

    def disconnect(self) -> None:
        ...

    def fetch_daily_bars(
        self,
        symbol: str,
        duration: str,
        end_datetime: str = "",
        use_rth: bool = True,
    ) -> List[dict]:
        ...

    def list_news_providers(self) -> List[dict]:
        ...

    def fetch_news_headlines(
        self,
        symbol: str,
        provider_codes: List[str],
        start: str,
        end: str,
        limit: int,
        since_utc: str = None,
        max_pages: int = 5,
    ) -> Tuple[List[dict], bool]:
        ...
