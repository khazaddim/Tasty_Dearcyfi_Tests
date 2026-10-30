"""Calendar, selection, raw DXLink capture, and comparison-quality rules."""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from importlib.metadata import version
import json
import math
from pathlib import Path
import sys
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
from tastytrade.dxfeed import Candle, Greeks, Quote, Trade
from tastytrade.instruments import Option
from tastytrade.session import Session
from tastytrade.streamer import DXLinkStreamer

SCHEMA_VERSION = 1
SDK_VERSION = "13.2.3"
SOURCE = "tastytrade.dxlink"
EVENT_CLASSES = {"Quote": Quote, "Greeks": Greeks, "Trade": Trade, "Candle": Candle}


def utc_now() -> datetime:
    return datetime.now(UTC)


def json_text(value: object) -> str:
    def encode(item: object) -> str:
        if isinstance(item, (datetime, date)):
            return item.isoformat()
        if isinstance(item, (Decimal, Path)):
            return str(item)
        raise TypeError(f"Unsupported JSON value: {type(item).__name__}")

    return json.dumps(value, default=encode, sort_keys=True, separators=(",", ":"), allow_nan=False)


def identifier() -> str:
    return uuid4().hex


def number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    try:
        result = float(value)
    except (ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def source_time(value: object) -> datetime | None:
    milliseconds = number(value)
    if milliseconds is None or milliseconds <= 0:
        return None
    try:
        return datetime.fromtimestamp(milliseconds / 1000, UTC)
    except (ValueError, OverflowError, OSError):
        return None


@dataclass(frozen=True)
class CollectorConfig:
    database: Path = Path("Tastyware_Demo_Scripts/exports/spy_validation.duckdb")
    max_bytes: int = 10_000_000_000
    sample_seconds: int = 60
    quote_age: float = 5
    analytics_age: float = 15
    max_skew: float = 2
    max_lateness: float = 5
    candle_grace: float = 15
    max_contracts: int = 96
    puts_per_expiry: int = 9
    calls_per_expiry: int = 3
    expiry_days: tuple[int, ...] = (0, 7, 14, 17, 28, 56)
    put_moneyness: tuple[float, ...] = (0.95, 0.975, 1.0)
    refresh_seconds: int = 1800
    flush_seconds: float = 5
    overlap_minutes: int = 5
    candle_refresh_seconds: int = 300
    reconciliation_seconds: float = 30
    request_timeout: float = 30
    reconnect_attempts: int = 5
    transport_idle: float = 120
    max_pending: int = 50_000
    delay_status: str = "unknown"
    clock_offset_ms: float | None = None
    verbose: bool = False

    def validate(self) -> None:
        for name in (
            "max_bytes", "sample_seconds", "quote_age", "analytics_age", "max_skew",
            "max_lateness", "candle_grace", "max_contracts", "puts_per_expiry",
            "calls_per_expiry", "refresh_seconds", "flush_seconds", "overlap_minutes",
            "candle_refresh_seconds", "reconciliation_seconds", "request_timeout",
            "reconnect_attempts", "transport_idle", "max_pending",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and greater than zero.")
        if not self.expiry_days or any(day < 0 for day in self.expiry_days):
            raise ValueError("expiry_days must contain nonnegative calendar-day targets.")
        if not self.put_moneyness or any(
            not math.isfinite(ratio) or not 0 < ratio <= 1 for ratio in self.put_moneyness
        ):
            raise ValueError("put_moneyness must contain finite strike/spot ratios in (0, 1].")
        if self.delay_status not in {"unknown", "realtime", "delayed"}:
            raise ValueError("delay_status must be unknown, realtime, or delayed.")
        if self.clock_offset_ms is not None and not math.isfinite(self.clock_offset_ms):
            raise ValueError("clock_offset_ms must be finite.")
        if self.database.suffix != ".duckdb":
            raise ValueError("database must use the .duckdb extension.")


@dataclass(frozen=True)
class SessionWindow:
    day: date
    opens: datetime
    closes: datetime


def current_session(now: datetime) -> SessionWindow | None:
    """Resolve today's New York date only; never choose the next session."""
    today = now.astimezone(ZoneInfo("America/New_York")).date()
    schedule = mcal.get_calendar("XNYS").schedule(start_date=today, end_date=today)
    if schedule.empty:
        return None
    row = schedule.iloc[0]
    return SessionWindow(today, row["market_open"].to_pydatetime(), row["market_close"].to_pydatetime())


@dataclass(frozen=True)
class Instrument:
    symbol: str
    streamer_symbol: str
    option_type: str | None = None
    strike: float | None = None
    expiration: date | None = None
    expires_at: datetime | None = None
    stops_trading_at: datetime | None = None
    exercise_style: str | None = None
    settlement_type: str | None = None
    shares_per_contract: int | None = None
    reference: dict[str, Any] = field(default_factory=dict)


def select_universe(
    chain: Mapping[date, list[Option]], spot: float, now: datetime, config: CollectorConfig,
) -> tuple[list[Instrument], dict[str, object]]:
    if not math.isfinite(spot) or spot <= 0:
        raise ValueError("Universe selection requires a finite positive SPY spot.")
    today = now.astimezone(ZoneInfo("America/New_York")).date()
    eligible: dict[date, list[Option]] = {}
    rejected: Counter[str] = Counter()
    for expiry, options in chain.items():
        for option in options:
            if not option.active or option.stops_trading_at <= now or expiry < today:
                rejected["inactive_or_expired"] += 1
            elif option.root_symbol != "SPY" or option.shares_per_contract != 100:
                rejected["nonstandard_terms"] += 1
            elif not option.streamer_symbol or not option.strike_price.is_finite():
                rejected["missing_symbol_or_strike"] += 1
            else:
                eligible.setdefault(expiry, []).append(option)
    if not eligible:
        raise ValueError("No eligible standard-root SPY options were returned.")
    expiries = sorted(eligible)
    chosen_expiries = sorted({
        min(expiries, key=lambda exp: (abs((exp - today).days - target), exp))
        for target in config.expiry_days
    })
    selected: list[Instrument] = []
    for expiry in chosen_expiries:
        put_candidates = sorted(
            (o for o in eligible[expiry] if o.option_type.value == "P" and float(o.strike_price) <= spot),
            key=lambda o: o.strike_price, reverse=True,
        )
        ladders = [
            [option for option in put_candidates if float(option.strike_price) <= spot * ratio]
            for ratio in config.put_moneyness
        ]
        put_by_symbol: dict[str, Option] = {}
        for depth in range(len(put_candidates)):
            for ladder in ladders:
                if depth < len(ladder):
                    put_by_symbol.setdefault(ladder[depth].symbol, ladder[depth])
                if len(put_by_symbol) >= config.puts_per_expiry:
                    break
            if len(put_by_symbol) >= config.puts_per_expiry:
                break
        puts = sorted(put_by_symbol.values(), key=lambda option: option.strike_price, reverse=True)
        calls = sorted(
            (o for o in eligible[expiry] if o.option_type.value == "C"),
            key=lambda o: (abs(float(o.strike_price) - spot), o.strike_price),
        )[:config.calls_per_expiry]
        for option in puts + calls:
            selected.append(Instrument(
                option.symbol, option.streamer_symbol, option.option_type.value,
                float(option.strike_price), option.expiration_date, option.expires_at,
                option.stops_trading_at, option.exercise_style, option.settlement_type,
                option.shares_per_contract, option.model_dump(mode="json"),
            ))
    if len(selected) > config.max_contracts:
        raise ValueError(
            f"Pilot selects {len(selected)} contracts, above max_contracts={config.max_contracts}; "
            "reduce strikes/expiry buckets or explicitly raise the cap."
        )
    return selected, {
        "spot": spot, "expiry_targets_calendar_days": config.expiry_days,
        "actual_expirations": [exp.isoformat() for exp in chosen_expiries],
        "rejected_counts": dict(rejected), "selected_count": len(selected),
        "put_moneyness_anchors": config.put_moneyness,
        "rule": "nearest listed expiries; round-robin adjacent puts below strike/spot anchors; calls nearest spot",
        "deliverable_status": "SPY root and 100 shares checked; full deliverable not exposed by Option",
    }


@dataclass(frozen=True)
class Observation:
    kind: str
    symbol: str
    received_at: datetime
    fields: dict[str, Any]
    payload: dict[str, Any]
    record_id: str = field(default_factory=identifier)

    @property
    def raw_id(self) -> str:
        return sha256((self.kind + json_text(self.payload)).encode()).hexdigest()

    @property
    def timestamp(self) -> datetime | None:
        if self.kind == "Quote":
            times = [source_time(self.fields.get(name)) for name in ("bid_time", "ask_time")]
            known = [timestamp for timestamp in times if timestamp is not None]
            return max(known) if len(known) == 2 else None
        return source_time(self.fields.get("time"))


def decode_events(kind: str, message: list[Any], received: datetime) -> list[Observation]:
    """Decode requested COMPACT fields without the SDK's lossy validators."""
    model = EVENT_CLASSES[kind]
    names = list(model.model_fields)
    aliases = [model.model_fields[name].alias or name for name in names]
    if not message:
        raise ValueError("Empty DXLink market-data frame.")
    if isinstance(message[0], dict):
        payloads = message
    else:
        if len(message) != 2 or message[0] != kind or not isinstance(message[1], list):
            raise ValueError("Unexpected DXLink COMPACT frame.")
        values = message[1]
        if len(values) % len(names):
            raise ValueError("DXLink field count differs from the pinned SDK schema.")
        payloads = [
            dict(zip(aliases, values[index:index + len(names)], strict=True))
            for index in range(0, len(values), len(names))
        ]
    events = []
    for payload in payloads:
        fields = {name: payload.get(alias, payload.get(name)) for name, alias in zip(names, aliases)}
        symbol = fields["event_symbol"]
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("DXLink market-data event has no symbol.")
        events.append(Observation(kind, symbol, received, fields, dict(payload)))
    return events


class RawMarketStreamer(DXLinkStreamer):
    """SDK-13.2.3-only hook; never intercepts authentication or account frames."""

    def __init__(self, session: Session, receive: Callable[[Observation], None]):
        if version("tastytrade") != SDK_VERSION:
            raise RuntimeError(f"Raw collector adapter requires tastytrade=={SDK_VERSION}.")
        super().__init__(session)
        # The raw adapter does not use the SDK's unbounded validated-event queues.
        for stream in self._send.values():
            stream.close()
        for stream in self._recv.values():
            stream.close()
        self.receive = receive

    def _map_message(self, message: list[Any], channel: int) -> None:
        kind = self._channels_reversed[channel]
        if kind not in EVENT_CLASSES:
            raise ValueError(f"Unexpected market-data channel: {kind}")
        for observation in decode_events(kind, message, utc_now()):
            self.receive(observation)

    def channels_active(self, include_greeks: bool) -> bool:
        names = ("Quote", "Trade", "Candle", "Greeks") if include_greeks else ("Quote", "Trade", "Candle")
        return all(
            name in self._subscription_state and self._subscription_state[name].is_set()
            for name in names
        )


class CaptureState:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.latest: dict[tuple[str, str], Observation] = {}
        self.spot_history: deque[Observation] = deque(maxlen=50_000)
        self.candles: list[Observation] = []
        self.diagnostics: list[tuple[str, str, dict[str, object]]] = []
        self.counts: Counter[str] = Counter()
        self.last_received: datetime | None = None

    def diagnostic(self, code: str, symbol: str, details: dict[str, object]) -> None:
        if len(self.diagnostics) >= self.config.max_pending:
            raise BufferError("Diagnostic buffer exhausted; collection must stop.")
        self.diagnostics.append((code, symbol, details))

    def receive(self, observation: Observation) -> None:
        self.last_received = observation.received_at
        self.counts[observation.kind] += 1
        if observation.kind == "Candle":
            if len(self.candles) >= self.config.max_pending:
                raise BufferError("Candle buffer exhausted; collection must stop.")
            self.candles.append(observation)
            return
        key = (observation.kind, observation.symbol)
        previous = self.latest.get(key)
        if previous is not None and previous.payload == observation.payload:
            self.counts[f"duplicate_{observation.kind}"] += 1
        if previous is not None and previous.timestamp and observation.timestamp:
            if observation.timestamp < previous.timestamp:
                self.diagnostic("out_of_order", observation.symbol, {
                    "kind": observation.kind, "payload": observation.payload,
                    "received_at": observation.received_at,
                })
                return
        self.latest[key] = observation
        if observation.kind == "Quote" and observation.symbol == "SPY":
            self.spot_history.append(observation)
            cutoff = observation.received_at - timedelta(seconds=max(120, self.config.quote_age * 2))
            while self.spot_history and self.spot_history[0].received_at < cutoff:
                self.spot_history.popleft()

    def reset_connection(self) -> None:
        self.latest.clear()
        self.spot_history.clear()
        self.last_received = None

    def pair_spot(self, option: Observation | None, cutoff: datetime) -> Observation | None:
        if option is None or option.timestamp is None:
            return None
        candidates = (
            quote for quote in self.spot_history
            if quote.received_at <= cutoff and quote.timestamp is not None
            and quote.timestamp <= option.timestamp
        )
        return max(candidates, key=lambda quote: (quote.timestamp, quote.received_at), default=None)


def quote_quality(
    quote: Observation | None, now: datetime, max_age: float,
) -> tuple[float | None, float | None, list[str]]:
    if quote is None:
        return None, None, ["quote.missing"]
    fields = quote.fields
    bid, ask = number(fields.get("bid_price")), number(fields.get("ask_price"))
    flags: list[str] = []
    if bid is None or ask is None:
        flags.append("quote.one_sided_or_nonfinite")
    elif bid < 0 or ask < 0:
        flags.append("quote.negative")
    elif bid > ask:
        flags.append("quote.crossed")
    elif bid == ask:
        flags.append("quote.locked")
    if bid == 0:
        flags.append("bid.zero")
    for name in ("bid_size", "ask_size"):
        size = number(fields.get(name))
        if size is None or size < 0:
            flags.append(f"{name}.invalid")
    ages: list[float] = []
    for name in ("bid_time", "ask_time"):
        timestamp = source_time(fields.get(name))
        if timestamp is None:
            flags.append(f"{name}.unknown")
        else:
            age = (now - timestamp).total_seconds()
            ages.append(age)
            if age < 0:
                flags.append(f"{name}.future")
            elif age > max_age:
                flags.append(f"{name}.stale")
    midpoint = (bid + ask) / 2 if bid is not None and ask is not None and 0 <= bid <= ask else None
    return midpoint, max(ages) if len(ages) == 2 else None, flags


def analytics_quality(
    analytics: Observation | None, now: datetime, max_age: float,
) -> tuple[float | None, list[str]]:
    if analytics is None:
        return None, ["analytics.missing"]
    flags = []
    timestamp = analytics.timestamp
    age = (now - timestamp).total_seconds() if timestamp else None
    if age is None:
        flags.append("analytics.time_unknown")
    elif age < 0 or age > max_age:
        flags.append("analytics.future_or_stale")
    for name in ("price", "volatility", "delta", "gamma", "theta", "vega", "rho"):
        value = number(analytics.fields.get(name))
        if value is None or (name in {"price", "volatility"} and value <= 0):
            flags.append(f"analytics.{name}_invalid")
    event_flags = number(analytics.fields.get("event_flags"))
    if event_flags is None or int(event_flags) & 3:
        flags.append("analytics.pending_or_removed")
    return age, flags


def quality_objects(flags: list[str]) -> list[dict[str, str]]:
    return [
        {"field": code.split(".")[0], "reason": code, "severity": "warning"}
        for code in sorted(set(flags))
    ]


def capability_manifest(config: CollectorConfig) -> dict[str, object]:
    directory = Path(__file__).parent
    return {
        "sdk": f"tastytrade=={version('tastytrade')}", "schema_version": SCHEMA_VERSION,
        "duckdb": version("duckdb"), "calendar": "XNYS", "source": SOURCE,
        "python": sys.version, "pandas_market_calendars": version("pandas-market-calendars"),
        "collector_file_sha256": {
            name: sha256((directory / name).read_bytes()).hexdigest()
            for name in (
                "collector_core.py", "collector_store.py", "13_collect_spy_validation.py",
                "requirements-collector.lock",
            )
        },
        "environment": "production", "delay_status": config.delay_status,
        "clock_offset_ms_user_reported": config.clock_offset_ms,
        "interfaces": ["DXLink Quote", "DXLink Trade", "DXLink Greeks", "DXLink Candle", "REST option chain"],
        "raw": "Requested market-data fields before SDK validation; not authentication frames or all ticks.",
        "timestamp_units": "Unix milliseconds: Quote bidTime/askTime, Trade/Greeks/Candle time. Zero is unknown.",
        "precision": "Original timeNanoPart and integer source fields retained in raw JSON; normalized timestamps use milliseconds.",
        "quote_time_reference": "https://docs.dxfeed.com/dxfeed/api/com/dxfeed/event/market/Quote.html",
        "candle_interval": "1m; DXLink interval-start milliseconds; regular-hours request",
        "candle_price_basis": "provider default; independent live verification required",
        "candle_finality": "unknown; elapsed-with-grace is not provider finality",
        "adjustment_basis": "unknown",
        "analytics_conventions": "Raw provider values; Greek units, pricing model, rates/dividends and mark definition unverified.",
        "missing_fields": {
            "broker_mark": "Not exposed by Quote/Trade; Greeks.price is retained separately, not renamed mark.",
            "analytics_underlying_input": "Not exposed by Greeks",
            "analytics_rates_dividends": "Not exposed by Greeks",
            "quote_conditions_trading_status": "Not exposed by Quote",
            "permissions_rate_symbol_limits": "Provider/account dependent; not certified by this collector",
        },
        "retention_permissions": "User must acknowledge authorization before collection.",
        "known_sdk_limitations": "Pinned private market-data mapping hook; schema mismatch is fatal.",
        "comparisons": {
            "quote_iv": "Input candidate only, subject to quality flags and downstream rate/dividend/term policy.",
            "broker_greeks": "Not certified: broker conventions must be verified before strict numerical comparison.",
            "historical_replay_approval": False,
        },
        "config": asdict(config),
    }
