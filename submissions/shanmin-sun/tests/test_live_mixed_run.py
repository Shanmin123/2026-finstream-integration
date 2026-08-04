"""Mixed backfill and increment in one invocation, against a live IB Gateway.

Raised in review: `earliest_event_date` is one low-water mark for the whole run, so
what happens when one symbol backfills years while another is a few days behind? The rest
of the suite cannot answer it, because every stubbed symbol returns the same first
date; only a real fetch produces genuinely different spans in one call.

Skipped unless a Gateway is reachable, so `pytest tests/` stays green without one.
Run it with a paper Gateway up:

    python -m pytest tests/test_live_mixed_run.py -v

It reads market data only: no order is built, previewed or sent, and it writes to a
temporary directory rather than the configured data paths.
"""
import os
import socket
import time
from datetime import datetime, timedelta, timezone

import pytest

# load_config honours these, so the probe has to read them too or it would test a
# different endpoint from the one the run connects to.
GATEWAY_HOST = os.getenv("IB_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.getenv("IB_PORT", "4002"))
# High and unlikely to be in use. A collision with another IB client fails the
# connection, so it must not be a value anything else is likely to hold.
CLIENT_ID = int(os.getenv("IBKR_TEST_CLIENT_ID", "9271"))
BACKFILL_SYMBOL = "AAPL"
INCREMENT_SYMBOL = "MSFT"
# Long enough that the backfill starts years before the increment does.
BACKFILL_DURATION = "3 Y"
INCREMENT_LAG_DAYS = 4


def _gateway_answers(host, port, timeout=2.0):
    """True when a TWS API server answers on the port.

    An open socket is not enough: a Gateway with the API disabled accepts the
    connection and then sends nothing, which is a hang rather than a failure. Nor
    is any reply enough, since an unrelated service would also send bytes. The
    handshake reply starts with a 4-byte big-endian length followed by the server
    version as digits, so that shape is what is checked.
    """
    deadline = time.monotonic() + timeout

    def _read(sock, want):
        """Read `want` bytes, or give up once the overall deadline passes.

        The socket timeout is per recv, so a peer trickling one byte before each
        expiry could otherwise hold test collection for minutes. One deadline for
        the whole probe bounds it at `timeout`.
        """
        buf = b""
        while len(buf) < want:
            left = deadline - time.monotonic()
            if left <= 0:
                return None
            sock.settimeout(left)
            chunk = sock.recv(want - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.sendall(b"API\x00\x00\x00\x00\tv176..187")
            header = _read(sock, 4)
            if header is None:
                return False
            length = int.from_bytes(header, "big")
            # A TWS handshake reply is a short frame: the server version and the
            # connection time. The bound is a sanity cap, not a protocol rule.
            if not 0 < length <= 256:
                return False
            payload = _read(sock, length)
    except OSError:
        return False
    # The payload starts with the server version as decimal digits.
    return bool(payload) and payload[0:1].isdigit()


@pytest.fixture
def live_config(tmp_path, monkeypatch):
    """A throwaway config pointing at the Gateway, with its own client id."""
    pytest.importorskip("ibapi")
    from pipeline.config import load_config

    # Probed here rather than at import: a module-level skipif is evaluated once
    # at collection, which freezes the decision for the whole session and is made
    # independently by every xdist worker.
    if not _gateway_answers(GATEWAY_HOST, GATEWAY_PORT):
        pytest.skip(
            f"no IB Gateway answering the API on {GATEWAY_HOST}:{GATEWAY_PORT}")

    # load_config lets IB_CLIENT_ID override the file, which would undo the
    # isolation this test needs and could point it at a client id in use.
    monkeypatch.setenv("IB_CLIENT_ID", str(CLIENT_ID))

    (tmp_path / "symbols.csv").write_text(
        f"symbol\n{BACKFILL_SYMBOL}\n{INCREMENT_SYMBOL}\n", encoding="utf-8")
    (tmp_path / "config.yaml").write_text(f"""
ib:
  host: "{GATEWAY_HOST}"
  port: {GATEWAY_PORT}
  client_id: {CLIENT_ID}
  connect_timeout_seconds: 15
  request_timeout_seconds: 60
paths:
  raw_data_dir: "{(tmp_path / 'raw').as_posix()}"
  intermediate_data_dir: "{(tmp_path / 'proc').as_posix()}"
  output_dir: "{(tmp_path / 'out').as_posix()}"
  checkpoint_file: "{(tmp_path / 'checkpoint.json').as_posix()}"
  log_dir: "{(tmp_path / 'logs').as_posix()}"
prices:
  symbols_file: "./symbols.csv"
  duration: "{BACKFILL_DURATION}"
  use_rth: true
news:
  symbols: ["{BACKFILL_SYMBOL}"]
  providers: ["BRFG"]
  lookback_days: 1
  overlap_minutes: 5
  limit: 10
run:
  max_retries: 2
  retry_delay_seconds: 1
  continue_on_error: true
  symbol_delay_seconds: 0
""".strip(), encoding="utf-8")
    return load_config(tmp_path / "config.yaml")


def _run_mixed(config):
    """One invocation: BACKFILL_SYMBOL has no checkpoint, INCREMENT_SYMBOL is current.

    Built through main.build_runner so this exercises the same wiring the CLI uses.
    """
    from pipeline.checkpoint import CheckpointStore
    from pipeline.main import build_runner

    # UTC, matching how the runner computes the missing interval.
    recent = (datetime.now(timezone.utc).date()
              - timedelta(days=INCREMENT_LAG_DAYS)).isoformat()
    CheckpointStore(config.paths.checkpoint_file).update_price(INCREMENT_SYMBOL, recent)

    runner = build_runner(config)
    return runner, runner.run_prices(), recent


def test_the_watermark_is_the_earliest_date_any_symbol_fetched(live_config):
    runner, summary, checkpoint = _run_mixed(live_config)
    assert summary["errors"] == [], summary["errors"]

    panel = runner.storage.read_final_prices()
    first = {s: panel[panel["symbol"] == s]["event_date"].min()
             for s in (BACKFILL_SYMBOL, INCREMENT_SYMBOL)}

    # The spans really did differ in one call; without that the assertion below
    # would pass for the wrong reason.
    assert first[BACKFILL_SYMBOL] < first[INCREMENT_SYMBOL], (
        f"expected a deep backfill beside an increment, got {first}"
    )
    latest = panel[panel["symbol"] == INCREMENT_SYMBOL]["event_date"].max()
    assert latest > checkpoint, (
        f"{INCREMENT_SYMBOL} returned nothing past its checkpoint {checkpoint}; "
        "the resume overlap alone would satisfy the span assertions"
    )
    assert summary["earliest_event_date"] == min(first.values())


def test_no_freshly_collected_row_falls_outside_the_loader_filter(live_config):
    """The DAG pushes `event_date >= watermark` over one panel covering all symbols."""
    runner, summary, checkpoint = _run_mixed(live_config)
    panel = runner.storage.read_final_prices()

    # Establish the premise first. `pushed == panel` is trivially true when only
    # one symbol landed, so without this the test could pass on a run that never
    # mixed a backfill with an increment.
    assert summary["errors"] == [], summary["errors"]
    first = {s: panel[panel["symbol"] == s]["event_date"].min()
             for s in (BACKFILL_SYMBOL, INCREMENT_SYMBOL)}
    assert not any(v != v for v in first.values()), f"a symbol returned nothing: {first}"
    assert first[BACKFILL_SYMBOL] < first[INCREMENT_SYMBOL], (
        f"expected a deep backfill beside an increment, got {first}"
    )
    latest = panel[panel["symbol"] == INCREMENT_SYMBOL]["event_date"].max()
    assert latest > checkpoint, (
        f"{INCREMENT_SYMBOL} returned nothing past its checkpoint {checkpoint}; "
        "the resume overlap alone would satisfy the span assertions"
    )

    pushed = panel[panel["event_date"] >= summary["earliest_event_date"]]
    assert len(pushed) == len(panel), (
        "a row this run collected sits outside the watermark the loader filters on"
    )

    # The increment symbol's rows ride along on the backfill's watermark. This
    # panel is a fresh temporary directory, so nothing here is redundant yet;
    # against a populated price_data the same filter re-sends them, and whether
    # that is a no-op depends on whether the earlier load stored them. Printed so
    # the size is visible.
    increment = pushed[pushed["symbol"] == INCREMENT_SYMBOL]
    print(f"\nwatermark {summary['earliest_event_date']}: "
          f"{len(pushed)} rows pushed, {len(increment)} of them {INCREMENT_SYMBOL}")
