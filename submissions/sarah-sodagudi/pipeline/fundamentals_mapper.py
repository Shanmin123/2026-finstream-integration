"""
Fundamental Data Mapper: EODHD JSON -> flattened per-period metrics.
Author: Sarah Sodagudi

Pure transform, no I/O. Takes the raw EODHD JSON document for one ticker
(see eodhd_client.py) and captures EVERY numeric field EODHD reports per
fiscal period across Income_Statement, Balance_Sheet, and Cash_Flow, plus
reported EPS from Earnings.History -- no fixed taxonomy, so nothing is
silently discarded. Each field is prefixed by which statement it came
from (income_/balance_/cashflow_) so identically-named fields across
statements can't collide.

Output schema: list[dict], one dict per fiscal period:
    {"year": int, "quarter": int|None, "publish_date": "YYYY-MM-DD",
     "report_type": "10-K"|"10-Q",
     "metrics": {"income_<field>": float, "balance_<field>": float,
                 "cashflow_<field>": float, "eps_actual": float, ...}}
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Bookkeeping fields EODHD includes on every period record that describe
# the record itself (not a financial metric) -- excluded from `metrics`.
_NON_METRIC_FIELDS = {"date", "filing_date", "currency_symbol"}


def _to_number(raw) -> Optional[float]:
    """EODHD returns numeric fields as strings, floats, ints, null, or the
    literal string "None" -- normalise all of that to float or None."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped or stripped.lower() == "none":
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _flatten_period(period_record: dict) -> Dict[str, float]:
    """Numeric-coerce every field on one EODHD statement-period record,
    dropping bookkeeping fields and anything non-numeric."""
    metrics = {}
    for field_name, raw_value in (period_record or {}).items():
        if field_name in _NON_METRIC_FIELDS:
            continue
        value = _to_number(raw_value)
        if value is not None:
            metrics[field_name] = value
    return metrics


def _build_eps_by_date(fundamentals: dict) -> Dict[str, float]:
    """EODHD reports EPS under Earnings.History, keyed by report date, not
    inside Financials -- build a date -> epsActual lookup we can join
    against the Income_Statement/Balance_Sheet/Cash_Flow periods."""
    eps_by_date = {}
    history = (fundamentals.get("Earnings") or {}).get("History") or {}
    for date_str, entry in history.items():
        eps = _to_number((entry or {}).get("epsActual"))
        if eps is not None:
            eps_by_date[date_str] = eps
    return eps_by_date


def extract_periods(fundamentals: dict, frequency: str) -> List[Dict]:
    """
    frequency: "yearly" or "quarterly"
    Returns one dict per period, with `metrics` holding every numeric
    field EODHD reported for that period across all three statements.
    """
    financials = fundamentals.get("Financials") or {}
    income_periods = ((financials.get("Income_Statement") or {}).get(frequency)) or {}
    balance_periods = ((financials.get("Balance_Sheet") or {}).get(frequency)) or {}
    cashflow_periods = ((financials.get("Cash_Flow") or {}).get(frequency)) or {}
    eps_by_date = _build_eps_by_date(fundamentals)

    report_type = "10-K" if frequency == "yearly" else "10-Q"
    all_dates = set(income_periods) | set(balance_periods) | set(cashflow_periods)

    periods = []
    for date_str in all_dates:
        try:
            period_date = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            logger.warning("Skipping period with unparsable date: %r", date_str)
            continue

        metrics = {}
        metrics.update({f"income_{k}": v for k, v in _flatten_period(income_periods.get(date_str)).items()})
        metrics.update({f"balance_{k}": v for k, v in _flatten_period(balance_periods.get(date_str)).items()})
        metrics.update({f"cashflow_{k}": v for k, v in _flatten_period(cashflow_periods.get(date_str)).items()})

        eps = eps_by_date.get(date_str)
        if eps is not None:
            metrics["eps_actual"] = eps

        periods.append({
            "year": period_date.year,
            "quarter": None if frequency == "yearly" else ((period_date.month - 1) // 3 + 1),
            "publish_date": date_str,
            "report_type": report_type,
            "metrics": metrics,
        })

    periods.sort(key=lambda p: p["publish_date"])
    return periods


def extract_all_periods(fundamentals: dict) -> Dict[str, List[Dict]]:
    """Returns both annual and quarterly periods from one fetched document."""
    return {
        "annual": extract_periods(fundamentals, "yearly"),
        "quarterly": extract_periods(fundamentals, "quarterly"),
    }
