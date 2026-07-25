-- FinStream platform tables for the IBKR pipeline's datasets beyond price_data.
--
-- Naming follows the platform's existing price_data contract (ticker,
-- timestamp_ms, datetime_utc, interval, source). The derived datasets use the
-- long layout this project's collectors already use for indicators and alphas
-- (one row per name/value) so new indicators or alpha formulas need no
-- migration.
--
-- Conflict policy mirrors the semantics of each dataset:
--   * quote_ticks are immutable observations      -> DO NOTHING (as price_data)
--   * derived values are recomputable             -> DO UPDATE when newer
--     (this is what lets a provisional intraday row be replaced by the final
--      end-of-day value without a separate table)

CREATE TABLE IF NOT EXISTS quote_ticks (
    ticker           TEXT        NOT NULL,
    timestamp_ms     BIGINT      NOT NULL,
    datetime_utc     TIMESTAMPTZ NOT NULL,
    field            TEXT        NOT NULL,   -- last/bid/ask/close/high/low/sizes/volume
    value            DOUBLE PRECISION,
    market_data_type SMALLINT,               -- 1 = real time, 3 = delayed (free tier)
    source           TEXT        NOT NULL DEFAULT 'ibkr',
    PRIMARY KEY (ticker, timestamp_ms, field, source)
);

CREATE INDEX IF NOT EXISTS quote_ticks_ticker_time_idx
    ON quote_ticks (ticker, datetime_utc DESC);

CREATE TABLE IF NOT EXISTS technical_indicators (
    ticker          TEXT        NOT NULL,
    timestamp_ms    BIGINT      NOT NULL,
    datetime_utc    TIMESTAMPTZ NOT NULL,
    indicator_name  TEXT        NOT NULL,   -- sma_20, rsi_14, macd, atr_14, obv, ...
    value           DOUBLE PRECISION,
    interval        TEXT        NOT NULL DEFAULT '1d',
    is_provisional  BOOLEAN     NOT NULL DEFAULT FALSE,
    computed_at_utc TIMESTAMPTZ NOT NULL,
    source          TEXT        NOT NULL DEFAULT 'ibkr',
    PRIMARY KEY (ticker, timestamp_ms, indicator_name, interval, source)
);

CREATE INDEX IF NOT EXISTS technical_indicators_ticker_time_idx
    ON technical_indicators (ticker, datetime_utc DESC);

CREATE TABLE IF NOT EXISTS alpha_factors (
    ticker          TEXT        NOT NULL,
    timestamp_ms    BIGINT      NOT NULL,
    datetime_utc    TIMESTAMPTZ NOT NULL,
    alpha_id        TEXT        NOT NULL,   -- alpha_6, alpha_101, ...
    value           DOUBLE PRECISION,
    interval        TEXT        NOT NULL DEFAULT '1d',
    is_provisional  BOOLEAN     NOT NULL DEFAULT FALSE,
    computed_at_utc TIMESTAMPTZ NOT NULL,
    source          TEXT        NOT NULL DEFAULT 'ibkr',
    PRIMARY KEY (ticker, timestamp_ms, alpha_id, interval, source)
);

CREATE INDEX IF NOT EXISTS alpha_factors_ticker_time_idx
    ON alpha_factors (ticker, datetime_utc DESC);
