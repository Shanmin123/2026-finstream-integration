import numpy as np
import pandas as pd
import pytest

from pipeline.features import (
    ALPHA_COLUMNS,
    INDICATOR_COLUMNS,
    compute_alphas,
    compute_indicators,
)
from pipeline.storage import LayeredStorage

COMPUTED_AT = "2026-07-25T10:00:00+00:00"


def _prices(symbol="TEST", n=80, start=100.0, step=1.0):
    dates = pd.date_range("2026-01-02", periods=n, freq="B").strftime("%Y-%m-%d")
    close = pd.Series(start + step * np.arange(n))
    return pd.DataFrame(
        {
            "symbol": symbol,
            "event_date": dates,
            "open": close - 0.5,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": 1000.0 + 10.0 * np.arange(n),
        }
    )


def test_indicator_columns_and_known_values_on_rising_series():
    frame = compute_indicators(_prices(), COMPUTED_AT)
    assert list(frame.columns) == list(INDICATOR_COLUMNS)
    last = frame.iloc[-1]
    # Monotonic gains -> Wilder RSI is exactly 100.
    assert last["rsi_14"] == pytest.approx(100.0)
    # SMA20 of a linear series = mean of the last 20 closes.
    closes = _prices()["close"]
    assert last["sma_20"] == pytest.approx(closes.iloc[-20:].mean())
    # MACD identity and Bollinger ordering.
    assert last["macd"] == pytest.approx(last["ema_12"] - last["ema_26"])
    assert last["bb_lower_20"] < last["bb_mid_20"] < last["bb_upper_20"]
    # Rising closes -> OBV accumulates every bar's volume after the first.
    vols = _prices()["volume"]
    assert last["obv"] == pytest.approx(vols.iloc[1:].sum())


def test_alpha_formulas_match_hand_computation():
    prices = _prices()
    frame = compute_alphas(prices, COMPUTED_AT)
    assert list(frame.columns) == list(ALPHA_COLUMNS)
    row = frame.iloc[-1]
    p = prices.iloc[-1]
    # alpha_101 = (close-open)/((high-low)+.001) with our synthetic geometry.
    assert row["alpha_101"] == pytest.approx(
        (p["close"] - p["open"]) / ((p["high"] - p["low"]) + 0.001)
    )
    # Steadily rising close and volume: delta(close,1)=step>0 for 5 straight days
    # -> alpha_9 keeps +delta; alpha_12 = sign(dV)*(-dC) = -step.
    assert row["alpha_9"] == pytest.approx(1.0)
    assert row["alpha_12"] == pytest.approx(-1.0)
    # Single symbol: the pct rank of a one-element cross-section is 1.0
    # (rank/count), so alpha_20 = (-1*1)*1*1 = -1.
    assert row["alpha_20"] == pytest.approx(-1.0)


def test_features_have_no_lookahead():
    prices = _prices(n=80)
    full_ind = compute_indicators(prices, COMPUTED_AT)
    full_alp = compute_alphas(prices, COMPUTED_AT)
    cut = prices.iloc[:50].reset_index(drop=True)
    cut_ind = compute_indicators(cut, COMPUTED_AT)
    cut_alp = compute_alphas(cut, COMPUTED_AT)
    for col in ("sma_20", "rsi_14", "atr_14", "obv"):
        pd.testing.assert_series_equal(
            full_ind[col].iloc[:50].reset_index(drop=True),
            cut_ind[col],
            check_names=False,
        )
    for col in ("alpha_6", "alpha_9", "alpha_23", "alpha_53", "alpha_101"):
        pd.testing.assert_series_equal(
            full_alp[col].iloc[:50].reset_index(drop=True),
            cut_alp[col],
            check_names=False,
        )


def test_alpha20_ranks_across_symbols():
    two = pd.concat(
        [_prices("AAA"), _prices("BBB", start=50.0, step=2.0)], ignore_index=True
    )
    frame = compute_alphas(two, COMPUTED_AT)
    day = frame[frame["event_date"] == frame["event_date"].max()]
    assert set(day["symbol"]) == {"AAA", "BBB"}
    assert not day["alpha_20"].isna().any()
    assert (day["alpha_20"] < 0).all()      # sign fixed by the -1*rank factor


def test_write_derived_partitions_and_dedups(tmp_path):
    storage = LayeredStorage(tmp_path / "raw", tmp_path / "mid", tmp_path / "final")
    frame = compute_indicators(_prices(n=30), COMPUTED_AT)
    first = storage.write_derived(
        "indicators", frame, ("symbol", "event_date"), "computed_at_utc"
    )
    assert first.final_rows == 30
    partitions = list((tmp_path / "final" / "indicators").glob("event_year=*/symbol=*/part-000.parquet"))
    assert partitions, "expected event_year/symbol partitions"
    # Re-writing the same rows must not duplicate (merge on symbol+event_date).
    again = storage.write_derived(
        "indicators", frame, ("symbol", "event_date"), "computed_at_utc"
    )
    assert again.final_rows == 30


def test_write_quotes_partitions_by_date_and_symbol(tmp_path):
    storage = LayeredStorage(tmp_path / "raw", tmp_path / "mid", tmp_path / "final")
    rows = [
        {"tick_time_utc": "2026-07-25T14:00:00+00:00", "symbol": "AAPL", "field": "last", "value": 250.1},
        {"tick_time_utc": "2026-07-25T14:00:01+00:00", "symbol": "AAPL", "field": "bid", "value": 250.0},
        {"tick_time_utc": "2026-07-25T14:00:01+00:00", "symbol": "MSFT", "field": "last", "value": 500.5},
    ]
    result = storage.write_quotes(rows, {"symbols": ["AAPL", "MSFT"]}, "2026-07-25T14:01:00+00:00", "quotes-test")
    assert result.final_rows == 3
    parts = list((tmp_path / "final" / "quotes").glob("event_date=*/symbol=*/part-000.parquet"))
    assert len(parts) == 2                      # AAPL + MSFT partitions
    again = storage.write_quotes(rows, {}, "2026-07-25T14:02:00+00:00", "quotes-test-2")
    assert again.final_rows == 3                # dedup on (symbol, tick_time_utc, field)
    assert result.raw_path.exists()


def test_live_features_converge_to_batch_when_last_equals_real_close():
    # If the streamed last equals the real next close, the provisional indicator
    # row must match the batch computation on the series that includes that bar.
    from pipeline.features import compute_live_features

    full = _prices(n=60)
    history = full.iloc[:59].reset_index(drop=True)     # up to yesterday
    real_next = full.iloc[59]
    live_i, live_a = compute_live_features(
        history,
        {"TEST": {"event_date": str(real_next["event_date"]),
                  "close": float(real_next["close"])}},
        COMPUTED_AT,
    )
    assert len(live_i) == 1 and len(live_a) == 1
    row = live_i.iloc[0]
    assert row["event_date"] == real_next["event_date"]
    assert bool(row["provisional"]) is True and row["as_of_utc"] == COMPUTED_AT
    # Close-derived indicators converge exactly (sma/ema/macd/rsi read closes only).
    batch = compute_indicators(
        pd.concat([history, pd.DataFrame([{**real_next.to_dict(),
            "open": real_next["close"], "high": real_next["close"],
            "low": real_next["close"], "volume": 0.0}])], ignore_index=True),
        COMPUTED_AT,
    ).iloc[-1]
    for col in ("sma_20", "ema_12", "macd", "rsi_14", "bb_mid_20"):
        assert row[col] == pytest.approx(batch[col])


def test_live_features_skip_symbols_without_ticks():
    from pipeline.features import compute_live_features

    live_i, live_a = compute_live_features(_prices(n=30), {}, COMPUTED_AT)
    assert live_i.empty and live_a.empty


def test_wilder_rsi_matches_the_textbook_seeding():
    # Wilder seeds the average gain/loss with the simple mean of the first 14
    # changes; ewm(adjust=False) seeds from the first observation and was ~17
    # RSI points off on this alternating series.
    n = 40
    close = pd.Series(100.0 + np.cumsum(np.where(np.arange(n) % 2 == 0, 1.0, -0.9)))
    frame = pd.DataFrame(
        {
            "symbol": "T",
            "event_date": pd.date_range("2026-01-02", periods=n, freq="B").strftime("%Y-%m-%d"),
            "open": close - 0.2, "high": close + 0.5, "low": close - 0.5,
            "close": close, "volume": 1000.0,
        }
    )
    first = compute_indicators(frame, COMPUTED_AT)["rsi_14"].dropna().iloc[0]
    change = close.diff()
    expected = 100.0 - 100.0 / (
        1.0
        + change.clip(lower=0).iloc[1:15].mean() / (-change).clip(lower=0).iloc[1:15].mean()
    )
    assert first == pytest.approx(expected)


def test_wilder_atr_matches_the_textbook_seeding():
    n = 30
    close = pd.Series(100.0 + np.arange(n) * 0.5)
    frame = pd.DataFrame(
        {
            "symbol": "T",
            "event_date": pd.date_range("2026-01-02", periods=n, freq="B").strftime("%Y-%m-%d"),
            "open": close, "high": close + 1.0, "low": close - 1.0,
            "close": close, "volume": 1000.0,
        }
    )
    first = compute_indicators(frame, COMPUTED_AT)["atr_14"].dropna().iloc[0]
    prev = close.shift(1)
    tr = pd.concat(
        [frame["high"] - frame["low"], (frame["high"] - prev).abs(),
         (frame["low"] - prev).abs()], axis=1
    ).max(axis=1)
    assert first == pytest.approx(tr.iloc[1:15].mean())


def test_live_features_do_not_fabricate_unobserved_fields():
    # With only a last price, range/volume formulas must be NaN, not a
    # fabricated constant (open=high=low=close made alpha_101 exactly 0 and
    # alpha_53/54 NaN via a zero denominator).
    from pipeline.features import compute_live_features

    history = _prices(n=40)
    _live_i, live_a = compute_live_features(
        history, {"TEST": {"event_date": "2026-03-02", "close": 150.0}}, COMPUTED_AT
    )
    row = live_a.iloc[0]
    assert pd.isna(row["alpha_101"])            # honest unknown, not 0.0
    assert pd.isna(row["alpha_53"])


def test_live_features_use_streamed_high_low_volume_when_present():
    from pipeline.features import compute_live_features

    history = _prices(n=40)
    _live_i, live_a = compute_live_features(
        history,
        {"TEST": {"event_date": "2026-03-02", "close": 150.0, "open": 148.0,
                  "high": 151.0, "low": 147.0, "volume": 5000.0}},
        COMPUTED_AT,
    )
    row = live_a.iloc[0]
    assert row["alpha_101"] == pytest.approx((150.0 - 148.0) / ((151.0 - 147.0) + 0.001))
    assert not pd.isna(row["alpha_53"])


def test_atr_seeds_on_the_first_real_true_range():
    # True range needs a previous close, so the first bar has none; a skipna
    # max() would use high-low there and seed ATR a row early off a bar that is
    # not a true range (9.0 instead of 2.0 on this fixture).
    n = 20
    close = pd.Series([100.0] * n)
    frame = pd.DataFrame(
        {
            "symbol": "T",
            "event_date": pd.date_range("2026-01-02", periods=n, freq="B").strftime("%Y-%m-%d"),
            "open": close,
            "high": pd.Series([200.0] + [101.0] * (n - 1)),   # extreme first bar
            "low": pd.Series([100.0] + [99.0] * (n - 1)),
            "close": close, "volume": 1000.0,
        }
    )
    atr = compute_indicators(frame, COMPUTED_AT)["atr_14"]
    assert atr.first_valid_index() == 14            # not 13
    assert atr.iloc[14] == pytest.approx(2.0)       # the first bar is excluded


def test_live_row_blanks_indicators_whose_inputs_were_not_streamed():
    # Wilder smoothing carries the previous average across a NaN and OBV treats
    # a missing volume as zero, so a close-only bar would report yesterday's
    # ATR and OBV as if they were today's.
    from pipeline.features import compute_live_features

    live_i, _live_a = compute_live_features(
        _prices(n=40), {"TEST": {"event_date": "2026-03-02", "close": 150.0}}, COMPUTED_AT
    )
    row = live_i.iloc[0]
    assert pd.isna(row["atr_14"])
    assert pd.isna(row["obv"])
    assert not pd.isna(row["sma_20"])               # close-only indicators still valid


def test_live_row_keeps_indicators_whose_inputs_were_streamed():
    from pipeline.features import compute_live_features

    live_i, _live_a = compute_live_features(
        _prices(n=40),
        {"TEST": {"event_date": "2026-03-02", "close": 150.0, "high": 151.0,
                  "low": 149.0, "volume": 5000.0}},
        COMPUTED_AT,
    )
    row = live_i.iloc[0]
    assert not pd.isna(row["atr_14"]) and not pd.isna(row["obv"])


def test_wilder_leaves_a_missing_observation_unknown():
    # A data gap must not report yesterday's smoothed value as today's; the
    # running average resumes correctly at the next real observation.
    from pipeline.features import _wilder_smooth

    values = pd.Series([1.0] * 14 + [np.nan, 1.0])
    out = _wilder_smooth(values, 14)
    assert out.iloc[13] == pytest.approx(1.0)     # seeded
    assert pd.isna(out.iloc[14])                  # the gap stays unknown
    assert out.iloc[15] == pytest.approx(1.0)     # and the series resumes


def test_alpha_9_is_undefined_until_its_five_delta_window_exists():
    # Before five deltas exist both rolling comparisons are False on NaN, which
    # would emit the -delta branch as if the condition had been evaluated.
    frame = _prices(n=10)
    alpha_9 = compute_alphas(frame, COMPUTED_AT)["alpha_9"]
    assert alpha_9.head(5).isna().all()
    assert not pd.isna(alpha_9.iloc[5])


def test_cumulative_and_recursive_indicators_need_full_history():
    # OBV is cumulative and EMA/Wilder are recursive, so truncating the warm
    # window silently changes them: this pins that the live path must not.
    frame = _prices(n=500)
    full = compute_indicators(frame, COMPUTED_AT).iloc[-1]
    truncated = compute_indicators(frame.tail(300).reset_index(drop=True), COMPUTED_AT).iloc[-1]
    assert full["obv"] != pytest.approx(truncated["obv"])


def test_wilder_resumes_from_the_pre_gap_average_not_a_fresh_seed():
    # An all-ones fixture would also pass if the smoother reset after a gap;
    # a level shift before the gap makes the two behaviours distinguishable.
    from pipeline.features import _wilder_smooth

    values = pd.Series([10.0] * 14 + [np.nan, 0.0])
    out = _wilder_smooth(values, 14)
    seeded = out.iloc[13]
    assert seeded == pytest.approx(10.0)
    assert pd.isna(out.iloc[14])
    # Resuming from the carried average gives (10*13 + 0)/14; a fresh seed would not.
    assert out.iloc[15] == pytest.approx(seeded * 13 / 14)


def test_degenerate_sessions_give_undefined_alphas_not_infinities():
    """close == low (closed on the low) and low == high (zero range) make the
    alpha_53/alpha_54 denominators zero. An infinity would propagate into the
    models and into PostgreSQL, which accepts Infinity in a double column."""
    frame = _prices(n=40)
    frame.loc[20, "low"] = frame.loc[20, "close"]       # closed on its low
    frame.loc[25, ["low", "high"]] = frame.loc[25, "close"]  # zero-range session
    alphas = compute_alphas(frame, COMPUTED_AT)
    values = alphas[[c for c in alphas.columns if c.startswith("alpha_")]]
    assert not np.isinf(values.to_numpy(dtype="float64")).any()


def test_db_mapping_drops_infinities_as_well_as_nans():
    from pipeline.db_sink import derived_frame_to_platform

    frame = pd.DataFrame(
        [{"symbol": "AAPL", "event_date": "2026-07-24", "alpha_53": np.inf,
          "alpha_54": -np.inf, "alpha_101": 0.5,
          "computed_at_utc": "2026-07-25T00:00:00+00:00"}]
    )
    mapped = derived_frame_to_platform(frame)
    assert [row[3] for row in mapped] == ["alpha_101"]


def test_flat_series_rsi_is_undefined_not_maximum_strength():
    """A 14-day window with zero gains AND zero losses is Wilder's 0/0 case.
    Halted and sub-dollar names really do flatline; reporting RSI 100 there
    read as strongest-possible momentum on a dead stock. Undefined -> NaN,
    the same policy as every other degenerate denominator. A falling series
    still reads 0 and a rising one 100."""
    import numpy as np
    import pandas as pd

    n = 60
    dates = pd.date_range("2025-01-02", periods=n, freq="B").strftime("%Y-%m-%d")

    def series(close):
        return pd.DataFrame(
            {
                "symbol": "FLAT", "event_date": dates, "open": close,
                "high": close, "low": close, "close": close, "volume": 0.0,
            }
        )

    flat = compute_indicators(series(pd.Series([50.0] * n)), COMPUTED_AT)
    assert flat["rsi_14"].isna().all()

    falling = compute_indicators(
        series(pd.Series(100.0 - np.arange(n))), COMPUTED_AT
    )
    assert falling["rsi_14"].dropna().iloc[-1] == pytest.approx(0.0)
