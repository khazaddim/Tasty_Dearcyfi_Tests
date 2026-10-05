"""Shared safety and formatting helpers for Tastyware demo scripts.

Template for demo-module docstrings:

{module_docstring_template}
"""

from __future__ import annotations

import asyncio
import csv
import inspect
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from time import monotonic
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, Literal, Mapping, TypeVar


MODULE_DOCSTRING_TEMPLATE = """\
\"\"\"<Script purpose in one sentence>\n
Upstream link:
- <https://github.com/tastyware/tastytrade#readme>\n
Tested versions:
- CPython 3.14.8 free-threaded (3.14t)
- tastytrade 13.2.3\n
PowerShell command (run from repository root):
- .\\.venv\\Scripts\\python.exe .\\Tastyware_Demo_Scripts\\<script_name>.py\n
Inputs:
- Tasty_SECRET (required, env var)
- Tasty_Refresh (required, env var)
- TASTYTRADE_ENV=production (required)\n
Safety mode:
- Production-only read path, no secret or token output.
- Finite timeouts for all network or streaming operations.\n
Expected redacted output:
- \"Connected to production with account ****1234\"
- \"Completed with stop_reason=event_limit\"\n
Common failures:
- Missing env var -> actionable setup guidance
- Authentication/permission/network/timeout -> distinct error messages
\"\"\"
"""


DEFAULT_TIMEOUT_SECONDS = 30.0
EXPORT_COLUMNS = ("symbol", "timestamp", "open", "high", "low", "close", "volume")
SENSITIVE_FIELDS = ("secret", "token", "refresh", "password", "credential", "authorization")


class DemoHelperError(Exception):
    """Base error for shared demo helpers."""


class DemoConfigurationError(DemoHelperError):
    """Raised when required local configuration is missing or invalid."""


class DemoAuthenticationError(DemoHelperError):
    """Raised when authentication fails."""


class DemoPermissionError(DemoHelperError):
    """Raised when account permissions are insufficient."""


class DemoNetworkError(DemoHelperError):
    """Raised when network transport fails."""


class DemoTimeoutError(DemoHelperError):
    """Raised when an operation reaches its timeout budget."""


@dataclass(frozen=True)
class DemoRuntimeConfig:
    """Validated runtime configuration for authenticated demo scripts."""

    client_secret: str
    refresh_token: str
    environment: str
    timeout_seconds: float
    account_number: str | None
    export_dir: Path


CompletionState = Literal["event_limit", "timeout", "stream_complete", "failed"]


@dataclass(frozen=True)
class StreamCollectionResult:
    """Structured result for bounded streaming collection."""

    events: list[Any]
    completion_state: CompletionState
    elapsed_seconds: float
    error: DemoHelperError | None = None


def require_env_var(name: str, value: str | None) -> str:
    """Return a stripped env value or raise an actionable configuration error."""
    if value is None or not value.strip():
        raise DemoConfigurationError(
            f"Missing required environment variable '{name}'. "
            f"Set {name} in your local shell before running this script."
        )
    return value.strip()


def require_production_environment(value: str | None) -> str:
    """Validate explicit production-only execution."""
    environment = require_env_var("TASTYTRADE_ENV", value).lower()
    if environment != "production":
        raise DemoConfigurationError(
            "Only production is supported for this knowledge base. "
            "Set TASTYTRADE_ENV=production explicitly."
        )
    return environment


def parse_timeout_seconds(value: str | None, default: float = DEFAULT_TIMEOUT_SECONDS) -> float:
    """Parse timeout configuration with a strict positive numeric requirement."""
    if value is None or not value.strip():
        return default
    try:
        parsed = float(value.strip())
    except ValueError as exc:
        raise DemoConfigurationError(
            "TASTYTRADE_TIMEOUT_SECONDS must be a positive number of seconds."
        ) from exc
    if parsed <= 0:
        raise DemoConfigurationError(
            "TASTYTRADE_TIMEOUT_SECONDS must be greater than zero."
        )
    return parsed


def parse_positive_int(value: str | None, *, name: str, default: int) -> int:
    """Parse a positive integer from configuration."""
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value.strip())
    except ValueError as exc:
        raise DemoConfigurationError(f"{name} must be a positive integer.") from exc
    if parsed <= 0:
        raise DemoConfigurationError(f"{name} must be greater than zero.")
    return parsed


def parse_bool(value: str | None, *, default: bool = False) -> bool:
    """Parse a boolean from common true/false string values."""
    if value is None or not value.strip():
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    raise DemoConfigurationError(
        f"Invalid boolean value '{value}'. Use true/false, yes/no, 1/0."
    )


def parse_csv_symbols(value: str | None, *, default: str = "SPY") -> list[str]:
    """Parse comma-separated symbols into a normalized, de-duplicated list."""
    source = value if value is not None and value.strip() else default
    symbols = [part.strip().upper() for part in source.split(",") if part.strip()]
    if not symbols:
        raise DemoConfigurationError("At least one symbol must be provided.")
    deduped: list[str] = []
    for symbol in symbols:
        if symbol not in deduped:
            deduped.append(symbol)
    return deduped


def parse_decimal(value: str | None, *, name: str) -> Decimal:
    """Parse a Decimal configuration value with actionable failures."""
    raw = require_env_var(name, value)
    try:
        return Decimal(raw)
    except Exception as exc:
        raise DemoConfigurationError(f"{name} must be a valid decimal value.") from exc


def get_first_env_value(values: Mapping[str, str], *names: str) -> str | None:
    """Return the first present env var value across candidate names."""
    for name in names:
        if name in values:
            return values[name]
    return None


def load_runtime_config(environ: Mapping[str, str] | None = None) -> DemoRuntimeConfig:
    """Load and validate shared environment-driven demo configuration."""
    values = dict(os.environ if environ is None else environ)
    client_secret = require_env_var(
        "Tasty_SECRET",
        get_first_env_value(values, "Tasty_SECRET", "TASTY_SECRET"),
    )
    refresh_token = require_env_var(
        "Tasty_Refresh",
        get_first_env_value(values, "Tasty_Refresh", "TASTY_REFRESH"),
    )
    environment = require_production_environment(values.get("TASTYTRADE_ENV"))
    timeout_seconds = parse_timeout_seconds(values.get("TASTYTRADE_TIMEOUT_SECONDS"))
    account_number = values.get("TASTYTRADE_ACCOUNT_NUMBER")
    account_number = account_number.strip() if account_number and account_number.strip() else None
    export_dir = Path(values.get("TASTYTRADE_EXPORT_DIR", "Tastyware_Demo_Scripts/exports"))
    return DemoRuntimeConfig(
        client_secret=client_secret,
        refresh_token=refresh_token,
        environment=environment,
        timeout_seconds=timeout_seconds,
        account_number=account_number,
        export_dir=export_dir,
    )


def mask_account_number(account_number: str, visible_digits: int = 4) -> str:
    """Return a masked account number with only trailing digits visible."""
    cleaned = "".join(char for char in account_number if char.isalnum())
    if not cleaned:
        return "unknown-account"
    visible_digits = max(0, min(visible_digits, len(cleaned)))
    hidden = "*" * (len(cleaned) - visible_digits)
    return f"{hidden}{cleaned[-visible_digits:]}" if visible_digits else hidden


def account_number_from_object(account: Any) -> str:
    """Extract and normalize an account number from SDK objects."""
    number = getattr(account, "account_number", None)
    text = str(number).strip() if number is not None else ""
    if not text:
        raise DemoConfigurationError("Received an account object with no account_number value.")
    return text


def normalize_account_collection(accounts: Any) -> list[Any]:
    """Normalize SDK account responses that can be one object or a list."""
    if accounts is None:
        return []
    if isinstance(accounts, list):
        return accounts
    return [accounts]


def require_selected_account(accounts: Iterable[Any], selected_account: str | None) -> Any:
    """Select an account or raise when explicit selection is required."""
    account_list = list(accounts)
    if not account_list:
        raise DemoConfigurationError("No accessible accounts were returned for this session.")

    by_number = {account_number_from_object(account): account for account in account_list}
    if selected_account:
        selected = by_number.get(selected_account)
        if selected is None:
            available = ", ".join(mask_account_number(number) for number in sorted(by_number))
            raise DemoConfigurationError(
                f"TASTYTRADE_ACCOUNT_NUMBER={selected_account!r} is not accessible. "
                f"Accessible accounts: {available}"
            )
        return selected

    if len(account_list) == 1:
        return account_list[0]

    masked = ", ".join(mask_account_number(number) for number in sorted(by_number))
    raise DemoConfigurationError(
        "Multiple accounts are accessible. Set TASTYTRADE_ACCOUNT_NUMBER to one of: "
        f"{masked}"
    )


def redact_sensitive_value(value: str | None) -> str:
    """Redact a sensitive value without exposing source content."""
    if value is None or not value:
        return "<redacted-empty>"
    return "<redacted>"


def redact_sensitive_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    """Redact mapping fields whose key names indicate sensitive values."""
    redacted: dict[str, Any] = {}
    for key, value in values.items():
        lowered = key.lower()
        if any(marker in lowered for marker in SENSITIVE_FIELDS):
            redacted[key] = redact_sensitive_value(str(value) if value is not None else None)
        else:
            redacted[key] = value
    return redacted


def classify_failure(exc: Exception, operation: str) -> DemoHelperError:
    """Classify operation failures into actionable, non-sensitive categories."""
    if isinstance(exc, asyncio.TimeoutError):
        return DemoTimeoutError(f"{operation} timed out before completion.")

    message = str(exc).lower()
    if "401" in message or "unauthorized" in message or "authentication" in message:
        return DemoAuthenticationError(f"{operation} failed due to authentication.")
    if "403" in message or "forbidden" in message or "permission" in message:
        return DemoPermissionError(f"{operation} failed due to insufficient permissions.")
    if isinstance(exc, OSError) or "network" in message or "connection" in message:
        return DemoNetworkError(f"{operation} failed due to a network transport error.")
    return DemoHelperError(f"{operation} failed: {exc.__class__.__name__}.")


async def run_with_timeout(
    operation_name: str,
    awaitable: Awaitable[Any],
    timeout_seconds: float,
) -> Any:
    """Execute an awaitable within a timeout and raise classified failures."""
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_seconds)
    except Exception as exc:
        raise classify_failure(exc, operation_name) from exc


async def run_callable_with_timeout(
    operation_name: str,
    operation: Callable[..., Any],
    timeout_seconds: float,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run a callable that may be sync or async under one timeout/error policy."""
    if inspect.iscoroutinefunction(operation):
        return await run_with_timeout(operation_name, operation(*args, **kwargs), timeout_seconds)

    result = await run_with_timeout(
        operation_name,
        asyncio.to_thread(operation, *args, **kwargs),
        timeout_seconds,
    )
    if inspect.isawaitable(result):
        return await run_with_timeout(operation_name, result, timeout_seconds)
    return result


async def close_async_resource(resource: Any) -> None:
    """Close a resource via aclose/close when available."""
    for method_name in ("aclose", "close"):
        method = getattr(resource, method_name, None)
        if method is None:
            continue
        result = method()
        if asyncio.iscoroutine(result):
            await result
        return


ResourceType = TypeVar("ResourceType")


@asynccontextmanager
async def managed_async_resource(resource: ResourceType) -> AsyncIterator[ResourceType]:
    """Yield a resource and guarantee closure in finally."""
    enter = getattr(resource, "__aenter__", None)
    exit_method = getattr(resource, "__aexit__", None)
    if callable(enter) and callable(exit_method):
        async with resource as entered:
            yield entered
        return
    try:
        yield resource
    finally:
        await close_async_resource(resource)


async def collect_bounded_stream(
    events: AsyncIterator[Any],
    event_limit: int,
    timeout_seconds: float,
    cleanup: Callable[[], Awaitable[None] | None] | None = None,
) -> StreamCollectionResult:
    """Collect from an async iterator with limit/timeout and guaranteed cleanup."""
    if event_limit <= 0:
        raise DemoConfigurationError("event_limit must be greater than zero.")
    if timeout_seconds <= 0:
        raise DemoConfigurationError("timeout_seconds must be greater than zero.")

    received: list[Any] = []
    start = monotonic()
    completion_state: CompletionState = "stream_complete"
    error: DemoHelperError | None = None
    try:
        async with asyncio.timeout(timeout_seconds):
            async for event in events:
                received.append(event)
                if len(received) >= event_limit:
                    completion_state = "event_limit"
                    break
    except TimeoutError as exc:
        completion_state = "timeout"
        error = classify_failure(exc, "stream collection")
    except Exception as exc:
        completion_state = "failed"
        error = classify_failure(exc, "stream collection")
    finally:
        if cleanup is not None:
            cleanup_result = cleanup()
            if asyncio.iscoroutine(cleanup_result):
                await cleanup_result

    return StreamCollectionResult(
        events=received,
        completion_state=completion_state,
        elapsed_seconds=monotonic() - start,
        error=error,
    )


def _parse_aware_datetime(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        parsed = raw
    elif isinstance(raw, str):
        parsed = datetime.fromisoformat(raw)
    else:
        raise DemoConfigurationError("Candle timestamp must be a datetime or ISO-8601 string.")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DemoConfigurationError("Candle timestamp must include timezone information.")
    return parsed


def _to_decimal(value: Any, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise DemoConfigurationError(f"Invalid candle field '{field}'.") from exc


def normalize_candles(
    candles: Iterable[Mapping[str, Any]],
    *,
    default_symbol: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[dict[str, Any]]:
    """Normalize candles to deterministic, timezone-preserving OHLCV rows."""
    if start is not None and (start.tzinfo is None or start.utcoffset() is None):
        raise DemoConfigurationError("start must include timezone information.")
    if end is not None and (end.tzinfo is None or end.utcoffset() is None):
        raise DemoConfigurationError("end must include timezone information.")
    if start and end and start > end:
        raise DemoConfigurationError("start must be less than or equal to end.")

    by_key: dict[tuple[str, datetime], dict[str, Any]] = {}
    for candle in candles:
        symbol_value = str(candle.get("symbol") or default_symbol or "").strip()
        if not symbol_value:
            raise DemoConfigurationError("Each candle requires a symbol.")
        timestamp = _parse_aware_datetime(candle.get("timestamp"))
        row = {
            "symbol": symbol_value,
            "timestamp": timestamp,
            "open": _to_decimal(candle.get("open"), "open"),
            "high": _to_decimal(candle.get("high"), "high"),
            "low": _to_decimal(candle.get("low"), "low"),
            "close": _to_decimal(candle.get("close"), "close"),
            "volume": int(candle.get("volume")),
        }
        by_key[(symbol_value, timestamp)] = row

    normalized = sorted(by_key.values(), key=lambda item: (item["timestamp"], item["symbol"]))
    if start is not None:
        normalized = [row for row in normalized if row["timestamp"] >= start]
    if end is not None:
        normalized = [row for row in normalized if row["timestamp"] <= end]
    return normalized


def export_candles_to_csv(candles: Iterable[Mapping[str, Any]], output_path: Path) -> Path:
    """Write normalized candle rows to CSV using the documented export schema."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()
        for candle in candles:
            writer.writerow(
                {
                    "symbol": candle["symbol"],
                    "timestamp": _parse_aware_datetime(candle["timestamp"]).isoformat(),
                    "open": str(_to_decimal(candle["open"], "open")),
                    "high": str(_to_decimal(candle["high"], "high")),
                    "low": str(_to_decimal(candle["low"], "low")),
                    "close": str(_to_decimal(candle["close"], "close")),
                    "volume": int(candle["volume"]),
                }
            )
    return output_path
