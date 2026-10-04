"""Collect bounded historical candles with endpoint-coverage reporting.

Upstream link:
- <https://tastyworks-api.readthedocs.io/en/latest/api/streamer.html#tastytrade.streamer.DXLinkStreamer.subscribe_candle>

Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3

PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\07_historical_candles.py

Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)
- TASTYTRADE_SYMBOL (optional, default SPY)
- TASTYTRADE_CANDLE_INTERVAL (optional, default 5m)
- TASTYTRADE_CANDLE_START (required ISO-8601 with timezone)
- TASTYTRADE_CANDLE_END (required ISO-8601 with timezone)
- TASTYTRADE_MARKET_CALENDAR (optional, default XNYS)
- TASTYTRADE_CANDLE_EVENT_LIMIT (optional, default 2000)
- TASTYTRADE_CANDLE_ATTEMPT_TIMEOUT_SECONDS (optional, default request timeout)
- TASTYTRADE_CANDLE_OVERALL_TIMEOUT_SECONDS (optional, default 120)
- TASTYTRADE_CANDLE_MAX_ATTEMPTS (optional, default 3)
- TASTYTRADE_CANDLE_EXTENDED_HOURS (optional, true/false, default false)
- TASTYTRADE_EXPORT_CSV (optional, true/false, default false)
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
from tastytrade.dxfeed import Candle
from tastytrade.session import Session
from tastytrade.streamer import DXLinkStreamer

from Tastyware_Demo_Scripts.demo_shared import (
    DemoConfigurationError,
    DemoHelperError,
    DemoTimeoutError,
    collect_bounded_stream,
    export_candles_to_csv,
    load_runtime_config,
    managed_async_resource,
    normalize_candles,
    parse_bool,
    parse_positive_int,
    parse_timeout_seconds,
)


@dataclass(frozen=True)
class EndpointPlan:
    calendar_name: str
    calendar_timezone: str
    start_session: date
    end_session: date
    effective_end_utc: datetime


@dataclass(frozen=True)
class CandleAttemptResult:
    rows: list[dict[str, object]]
    completion_state: str
    attempt_seconds: float
    error: DemoHelperError | None


@dataclass(frozen=True)
class CandleCollectionReport:
    rows: list[dict[str, object]]
    attempts: int
    stop_reason: str
    endpoint_coverage: bool
    start_session_observed: bool
    end_session_observed: bool
    endpoint_plan: EndpointPlan
    partial: bool
    output_csv: Path | None = None


def parse_aware_datetime(name: str, raw: str | None) -> datetime:
    if raw is None or not raw.strip():
        raise DemoConfigurationError(f"{name} is required and must be ISO-8601 with timezone.")
    try:
        parsed = datetime.fromisoformat(raw.strip())
    except ValueError as exc:
        raise DemoConfigurationError(f"{name} is not valid ISO-8601.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DemoConfigurationError(f"{name} must include timezone information.")
    return parsed


def resolve_endpoint_plan(
    *,
    calendar_name: str,
    start: datetime,
    end: datetime,
    now_utc: datetime | None = None,
) -> EndpointPlan:
    if end < start:
        raise DemoConfigurationError("TASTYTRADE_CANDLE_END must be >= TASTYTRADE_CANDLE_START.")
    now = datetime.now(UTC) if now_utc is None else now_utc.astimezone(UTC)
    effective_end = min(end.astimezone(UTC), now)
    if effective_end < start.astimezone(UTC):
        raise DemoConfigurationError("No eligible session: end is earlier than start after applying current-time bound.")

    try:
        calendar = mcal.get_calendar(calendar_name)
    except Exception as exc:
        raise DemoConfigurationError(
            f"Unknown market calendar '{calendar_name}'. Set TASTYTRADE_MARKET_CALENDAR explicitly."
        ) from exc

    timezone_name = str(getattr(calendar, "tz", "UTC"))
    calendar_tz = ZoneInfo(timezone_name)
    start_local = start.astimezone(calendar_tz)
    end_local = effective_end.astimezone(calendar_tz)
    schedule = calendar.schedule(
        start_date=start_local.date(),
        end_date=end_local.date(),
    )
    if schedule.empty:
        raise DemoConfigurationError(
            "No eligible market sessions exist in the requested range for the selected calendar."
        )

    start_label: date | None = None
    end_label: date | None = None

    for index_label, session in schedule.iterrows():
        market_open = session["market_open"].tz_convert(calendar_tz)
        market_close = session["market_close"].tz_convert(calendar_tz)
        if start_label is None and market_close >= start_local:
            start_label = index_label.date()
        if market_open <= end_local:
            end_label = index_label.date()

    if start_label is None or end_label is None:
        raise DemoConfigurationError("No eligible session could be selected for one or both range endpoints.")
    if end_label < start_label:
        raise DemoConfigurationError(
            "No eligible session range after applying pre-open/holiday/weekend endpoint adjustments."
        )

    return EndpointPlan(
        calendar_name=calendar_name,
        calendar_timezone=timezone_name,
        start_session=start_label,
        end_session=end_label,
        effective_end_utc=effective_end,
    )


def candle_timestamp(candle: Candle) -> datetime:
    raw = getattr(candle, "time", None)
    if raw is None:
        raise DemoConfigurationError("Candle event did not include a timestamp.")
    try:
        return datetime.fromtimestamp(float(raw) / 1000.0, tz=UTC)
    except Exception as exc:
        raise DemoConfigurationError("Candle timestamp could not be parsed.") from exc


def candle_to_row(candle: Candle, default_symbol: str) -> dict[str, object]:
    timestamp = candle_timestamp(candle)
    return {
        "symbol": str(getattr(candle, "event_symbol", None) or default_symbol),
        "timestamp": timestamp.isoformat(),
        "open": getattr(candle, "open", None),
        "high": getattr(candle, "high", None),
        "low": getattr(candle, "low", None),
        "close": getattr(candle, "close", None),
        "volume": getattr(candle, "volume", 0),
    }


def endpoint_observation(
    rows: list[dict[str, object]],
    endpoint_plan: EndpointPlan,
) -> tuple[bool, bool]:
    timezone_obj = ZoneInfo(endpoint_plan.calendar_timezone)
    seen_dates: set[date] = set()
    for row in rows:
        timestamp = datetime.fromisoformat(str(row["timestamp"])).astimezone(timezone_obj).date()
        seen_dates.add(timestamp)
    return endpoint_plan.start_session in seen_dates, endpoint_plan.end_session in seen_dates


def merge_rows(existing: list[dict[str, object]], new_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_key: dict[tuple[str, str], dict[str, object]] = {}
    for row in existing + new_rows:
        by_key[(str(row["symbol"]), str(row["timestamp"]))] = row
    merged = list(by_key.values())
    merged.sort(key=lambda row: (str(row["timestamp"]), str(row["symbol"])))
    return merged


async def collect_candle_attempt(
    *,
    streamer: DXLinkStreamer,
    symbol: str,
    interval: str,
    start_time: datetime,
    timeout_seconds: float,
    event_limit: int,
    extended_hours: bool,
) -> CandleAttemptResult:
    await asyncio.to_thread(
        streamer.subscribe_candle,
        [symbol],
        interval,
        start_time,
        extended_hours,
    )

    async def cleanup() -> None:
        await asyncio.to_thread(streamer.unsubscribe, Candle, [symbol])

    result = await collect_bounded_stream(
        streamer.listen(Candle),
        event_limit=event_limit,
        timeout_seconds=timeout_seconds,
        cleanup=cleanup,
    )
    rows = [candle_to_row(candle, symbol) for candle in result.events]
    return CandleAttemptResult(
        rows=rows,
        completion_state=result.completion_state,
        attempt_seconds=result.elapsed_seconds,
        error=result.error,
    )


async def collect_candle_block(
    *,
    session: Session,
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
    calendar_name: str,
    per_attempt_timeout_seconds: float,
    overall_timeout_seconds: float,
    max_attempts: int,
    event_limit: int,
    extended_hours: bool,
) -> CandleCollectionReport:
    plan = resolve_endpoint_plan(calendar_name=calendar_name, start=start, end=end)
    deadline = monotonic() + overall_timeout_seconds
    collected_rows: list[dict[str, object]] = []
    attempts = 0
    stop_reason = "max_attempts"
    no_progress_attempts = 0
    next_start = start

    while attempts < max_attempts:
        remaining = deadline - monotonic()
        if remaining <= 0:
            stop_reason = "overall_timeout"
            break

        attempts += 1
        timeout_seconds = min(per_attempt_timeout_seconds, remaining)
        streamer = DXLinkStreamer(session)
        async with managed_async_resource(streamer):
            attempt = await collect_candle_attempt(
                streamer=streamer,
                symbol=symbol,
                interval=interval,
                start_time=next_start,
                timeout_seconds=timeout_seconds,
                event_limit=event_limit,
                extended_hours=extended_hours,
            )

        before_count = len(collected_rows)
        filtered_rows = normalize_candles(
            attempt.rows,
            default_symbol=symbol,
            start=start,
            end=end,
        )
        collected_rows = merge_rows(collected_rows, filtered_rows)
        after_count = len(collected_rows)

        start_seen, end_seen = endpoint_observation(collected_rows, plan)
        if start_seen and end_seen:
            stop_reason = "endpoint_coverage"
            break

        if after_count == before_count:
            no_progress_attempts += 1
        else:
            no_progress_attempts = 0

        if no_progress_attempts >= 2:
            stop_reason = "no_progress"
            break

        if attempt.error is not None and not isinstance(attempt.error, DemoTimeoutError):
            stop_reason = "non_retryable_failure"
            raise attempt.error

        if collected_rows:
            latest = datetime.fromisoformat(str(collected_rows[-1]["timestamp"]))
            next_start = latest

        stop_reason = attempt.completion_state

    start_seen, end_seen = endpoint_observation(collected_rows, plan)
    endpoint_coverage = start_seen and end_seen
    return CandleCollectionReport(
        rows=collected_rows,
        attempts=attempts,
        stop_reason=stop_reason,
        endpoint_coverage=endpoint_coverage,
        start_session_observed=start_seen,
        end_session_observed=end_seen,
        endpoint_plan=plan,
        partial=True,
    )


async def run_historical_candles() -> int:
    config = load_runtime_config()
    symbol = (os.environ.get("TASTYTRADE_SYMBOL", "SPY").strip() or "SPY").upper()
    interval = os.environ.get("TASTYTRADE_CANDLE_INTERVAL", "5m").strip() or "5m"
    start = parse_aware_datetime("TASTYTRADE_CANDLE_START", os.environ.get("TASTYTRADE_CANDLE_START"))
    end = parse_aware_datetime("TASTYTRADE_CANDLE_END", os.environ.get("TASTYTRADE_CANDLE_END"))
    calendar_name = os.environ.get("TASTYTRADE_MARKET_CALENDAR", "XNYS").strip() or "XNYS"
    event_limit = parse_positive_int(
        os.environ.get("TASTYTRADE_CANDLE_EVENT_LIMIT"),
        name="TASTYTRADE_CANDLE_EVENT_LIMIT",
        default=2000,
    )
    per_attempt_timeout = parse_timeout_seconds(
        os.environ.get("TASTYTRADE_CANDLE_ATTEMPT_TIMEOUT_SECONDS"),
        default=config.timeout_seconds,
    )
    overall_timeout = parse_timeout_seconds(
        os.environ.get("TASTYTRADE_CANDLE_OVERALL_TIMEOUT_SECONDS"),
        default=120.0,
    )
    max_attempts = parse_positive_int(
        os.environ.get("TASTYTRADE_CANDLE_MAX_ATTEMPTS"),
        name="TASTYTRADE_CANDLE_MAX_ATTEMPTS",
        default=3,
    )
    extended_hours = parse_bool(os.environ.get("TASTYTRADE_CANDLE_EXTENDED_HOURS"), default=False)
    export_csv = parse_bool(os.environ.get("TASTYTRADE_EXPORT_CSV"), default=False)

    session = Session(provider_secret=config.client_secret, refresh_token=config.refresh_token)
    async with managed_async_resource(session):
        report = await collect_candle_block(
            session=session,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            calendar_name=calendar_name,
            per_attempt_timeout_seconds=per_attempt_timeout,
            overall_timeout_seconds=overall_timeout,
            max_attempts=max_attempts,
            event_limit=event_limit,
            extended_hours=extended_hours,
        )

    print(f"symbol={symbol}")
    print(f"interval={interval}")
    print(f"rows={len(report.rows)}")
    print(f"attempts={report.attempts}")
    print(f"stop_reason={report.stop_reason}")
    print(f"partial={report.partial}")
    print(f"endpoint_coverage={report.endpoint_coverage}")
    print(
        f"endpoint_sessions={report.endpoint_plan.start_session.isoformat()}..{report.endpoint_plan.end_session.isoformat()}"
    )
    print(f"calendar={report.endpoint_plan.calendar_name} tz={report.endpoint_plan.calendar_timezone}")
    print(f"effective_end_utc={report.endpoint_plan.effective_end_utc.isoformat()}")
    print(f"start_session_observed={report.start_session_observed}")
    print(f"end_session_observed={report.end_session_observed}")
    for row in report.rows:
        print(
            f"- symbol={row['symbol']} timestamp={row['timestamp']} open={row['open']} "
            f"high={row['high']} low={row['low']} close={row['close']} volume={row['volume']}"
        )

    if export_csv:
        output_path = config.export_dir / f"candles_{symbol}_{interval}.csv"
        export_candles_to_csv(report.rows, output_path)
        print(f"csv_export={output_path}")
    return 0


async def main() -> int:
    try:
        return await run_historical_candles()
    except (DemoConfigurationError, DemoHelperError) as exc:
        print(f"Historical candle collection failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
