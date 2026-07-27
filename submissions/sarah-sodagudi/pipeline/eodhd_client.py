"""
EODHD Fundamental Data API client.
Author: Sarah Sodagudi

Data source   : EODHD (eodhd.com), commercial, paid all-in-one plan.
Endpoint      : GET {api.base_url}/{TICKER}.{EXCHANGE}
Auth method   : API token as the `api_token` query parameter
                (config["api"]["api_key"] -- never hardcoded, loaded
                from config.yaml which is gitignored).
Rate limits   : Confirmed empirically against the live account usage
                endpoint: 10 API requests per call (not 1) -- the whole
                fundamentals document (all annual + quarterly periods)
                comes back in a single request, so the true cost is a
                flat 10 requests/ticker regardless of how many fiscal
                periods are returned.
Output schema : Raw EODHD JSON, unmodified -- see fundamentals_mapper.py
                for how this gets flattened into per-period metrics.
Update freq   : Called once per ticker per pipeline run. EODHD updates
                statements as filings are published (irregular on their
                side); re-running naturally picks up new filings.

Retries on HTTP 429 with exponential backoff, honouring a Retry-After
header when present. Never raises on a per-ticker failure -- returns
None so the caller can log it and continue with the rest of the batch.
"""
import logging
import time
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)


def get_with_retry(url: str, params: Dict, max_retries: int, backoff_seconds: float, timeout_seconds: int) -> Optional[requests.Response]:
    """
    GET `url` with `params`, retrying on HTTP 429 (and 5xx) with
    exponential backoff. Returns the Response on success/non-retryable
    status, or None if every attempt failed.
    """
    backoff = backoff_seconds

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout_seconds)
        except requests.RequestException as exc:
            logger.warning("Request error (attempt %d/%d) for %s: %s", attempt, max_retries, url, exc)
            time.sleep(backoff)
            backoff *= 2
            continue

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after else backoff
            logger.warning("Rate-limited (429) on attempt %d/%d for %s; waiting %.1fs", attempt, max_retries, url, wait_s)
            time.sleep(wait_s)
            backoff *= 2
            continue

        if resp.status_code >= 500:
            logger.warning("Server error %d (attempt %d/%d) for %s", resp.status_code, attempt, max_retries, url)
            time.sleep(backoff)
            backoff *= 2
            continue

        return resp

    logger.error("Request permanently failed after %d attempts for %s", max_retries, url)
    return None


def fetch_fundamentals(ticker: str, exchange: str, config: Dict) -> Optional[Dict]:
    """Fetch the full EODHD fundamentals document for one ticker. Returns
    None (logged, not raised) if the ticker has no data or the request
    failed after retries -- callers should skip and continue."""
    api_cfg = config["api"]
    if not api_cfg.get("api_key") or api_cfg["api_key"] == "YOUR_EODHD_API_TOKEN_HERE":
        logger.error("api.api_key is not set in config.yaml -- cannot fetch fundamentals.")
        return None

    url = f"{api_cfg['base_url']}/{ticker}.{exchange}"
    params = {"api_token": api_cfg["api_key"], "fmt": "json"}

    resp = get_with_retry(
        url, params,
        max_retries=api_cfg.get("max_retries", 5),
        backoff_seconds=api_cfg.get("backoff_seconds", 2),
        timeout_seconds=api_cfg.get("http_timeout_seconds", 30),
    )
    if resp is None:
        logger.error("Fundamentals fetch failed for %s.%s (no response after retries).", ticker, exchange)
        return None

    if resp.status_code != 200:
        logger.error("Fundamentals fetch for %s.%s returned HTTP %d: %s", ticker, exchange, resp.status_code, resp.text[:200])
        return None

    try:
        data = resp.json()
    except ValueError as exc:
        logger.error("Fundamentals response for %s.%s was not valid JSON: %s", ticker, exchange, exc)
        return None

    if not isinstance(data, dict) or not data:
        logger.warning("Fundamentals response for %s.%s was empty/unexpected shape.", ticker, exchange)
        return None

    logger.info("Fetched fundamentals for %s.%s", ticker, exchange)
    return data
