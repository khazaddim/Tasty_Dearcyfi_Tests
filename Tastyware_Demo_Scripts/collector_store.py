"""Single-writer DuckDB storage and offline portable export for SPY captures."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

import duckdb

try:
    from Tastyware_Demo_Scripts.collector_core import (
        SCHEMA_VERSION, SOURCE, CaptureState, CollectorConfig, Instrument, Observation,
        SessionWindow, analytics_quality, capability_manifest, identifier, json_text,
        number, quality_objects, quote_quality, source_time, utc_now,
    )
except ModuleNotFoundError:
    from collector_core import (
        SCHEMA_VERSION, SOURCE, CaptureState, CollectorConfig, Instrument, Observation,
        SessionWindow, analytics_quality, capability_manifest, identifier, json_text,
        number, quality_objects, quote_quality, source_time, utc_now,
    )


class StorageLimitReached(Exception):
    """Collection stopped intentionally at the configured storage threshold."""


COMMON = """
    schema_version INTEGER, run_id VARCHAR, record_id VARCHAR PRIMARY KEY,
    source VARCHAR, received_at_utc TIMESTAMPTZ, raw_record_id VARCHAR
"""
TABLES = {
    "raw_records": "kind VARCHAR, payload JSON",
    "instruments": """
        symbol VARCHAR, streamer_symbol VARCHAR, option_type VARCHAR, strike DOUBLE,
        expiration DATE, expires_at TIMESTAMPTZ, stops_trading_at TIMESTAMPTZ,
        exercise_style VARCHAR, settlement_type VARCHAR, shares_per_contract INTEGER,
        effective_at TIMESTAMPTZ, terms_status VARCHAR
    """,
    "universe_revisions": "effective_at TIMESTAMPTZ, selection JSON, symbols JSON",
    "quotes": """
        symbol VARCHAR, source_time TIMESTAMPTZ, bid_time TIMESTAMPTZ,
        ask_time TIMESTAMPTZ, bid DOUBLE, ask DOUBLE, bid_size DOUBLE, ask_size DOUBLE,
        bid_exchange VARCHAR, ask_exchange VARCHAR
    """,
    "trades": """
        symbol VARCHAR, source_time TIMESTAMPTZ, price DOUBLE, size DOUBLE,
        day_volume DOUBLE, exchange VARCHAR, extended_hours BOOLEAN
    """,
    "analytics": """
        symbol VARCHAR, source_time TIMESTAMPTZ, price DOUBLE, volatility DOUBLE,
        delta DOUBLE, gamma DOUBLE, theta DOUBLE, vega DOUBLE, rho DOUBLE, event_flags BIGINT
    """,
    "cycles": """
        scheduled_at TIMESTAMPTZ, sampled_at TIMESTAMPTZ, lateness_seconds DOUBLE,
        expected_options INTEGER, status VARCHAR
    """,
    "samples": """
        cycle_id VARCHAR, symbol VARCHAR, scheduled_at TIMESTAMPTZ, sampled_at TIMESTAMPTZ,
        quote_id VARCHAR, underlying_quote_id VARCHAR, analytics_id VARCHAR, trade_id VARCHAR,
        underlying_trade_id VARCHAR, option_mid DOUBLE, underlying_mid DOUBLE,
        quote_age_seconds DOUBLE, underlying_age_seconds DOUBLE, analytics_age_seconds DOUBLE,
        source_skew_seconds DOUBLE, receipt_skew_seconds DOUBLE, quote_eligible BOOLEAN,
        analytics_fresh BOOLEAN, broker_comparison_ready BOOLEAN, quality JSON
    """,
    "candles": """
        symbol VARCHAR, interval_start TIMESTAMPTZ, interval_end TIMESTAMPTZ,
        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
        count DOUBLE, vwap DOUBLE, event_flags BIGINT, revision_id VARCHAR,
        elapsed_with_grace BOOLEAN, quality JSON
    """,
    "diagnostics": "code VARCHAR, symbol VARCHAR, details JSON",
    "requests": """
        operation VARCHAR, started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ,
        status VARCHAR, symbols JSON, details JSON
    """,
}


def database_bytes(path: Path) -> int:
    return sum(file.stat().st_size for file in (path, Path(str(path) + ".wal")) if file.exists())


class CollectorStore:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.run_id = identifier()
        self.window: SessionWindow | None = None
        self.in_transaction = False
        self.pending: dict[tuple[str, tuple[str, ...]], dict[str, dict[str, object]]] = {}
        config.database.parent.mkdir(parents=True, exist_ok=True)
        if database_bytes(config.database) >= config.max_bytes:
            raise StorageLimitReached("Existing database/WAL is already at the storage threshold.")
        self.connection = duckdb.connect(str(config.database))
        try:
            self.connection.execute("SET TimeZone='UTC'")
            self.connection.execute("SET memory_limit='512MB'")
            self.connection.execute("SET threads=2")
            tables = {row[0] for row in self.connection.execute("SHOW TABLES").fetchall()}
            if tables and "collector_schema" not in tables:
                raise ValueError("Refusing to modify a database without a collector schema.")
            if "collector_schema" in tables:
                versions = self.connection.execute("SELECT version FROM collector_schema").fetchall()
                if versions != [(SCHEMA_VERSION,)]:
                    raise ValueError("Unsupported collector database schema version.")
            self.connection.execute("CREATE TABLE IF NOT EXISTS collector_schema (version INTEGER)")
            if "collector_schema" not in tables:
                self.connection.execute("INSERT INTO collector_schema VALUES (?)", [SCHEMA_VERSION])
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id VARCHAR PRIMARY KEY, schema_version INTEGER, session_date DATE,
                    session_open TIMESTAMPTZ, session_close TIMESTAMPTZ,
                    started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ, status VARCHAR,
                    stop_reason VARCHAR, config JSON, capabilities JSON, code_revision VARCHAR
                )
            """)
            for table, columns in TABLES.items():
                self.connection.execute(f"CREATE TABLE IF NOT EXISTS {table} ({COMMON}, {columns})")
            self.connection.execute("""
                CREATE OR REPLACE VIEW latest_candles AS
                SELECT * EXCLUDE (revision_rank) FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY run_id, symbol, interval_start
                        ORDER BY received_at_utc DESC, record_id DESC
                    ) AS revision_rank FROM candles
                ) WHERE revision_rank=1
            """)
            self.connection.execute("""
                CREATE OR REPLACE VIEW run_coverage AS
                SELECT r.run_id, r.status, r.stop_reason,
                    (SELECT count(*) FROM cycles c WHERE c.run_id=r.run_id) AS recorded_cycles,
                    (SELECT count(*) FROM cycles c WHERE c.run_id=r.run_id AND c.status='missed') AS missed_cycles,
                    (SELECT count(*) FROM samples s WHERE s.run_id=r.run_id) AS option_samples,
                    (SELECT count(*) FROM samples s WHERE s.run_id=r.run_id AND s.quote_eligible) AS eligible_quote_samples,
                    (SELECT count(*) FROM samples s WHERE s.run_id=r.run_id AND s.analytics_fresh) AS fresh_analytics_samples,
                    (SELECT count(*) FROM diagnostics d WHERE d.run_id=r.run_id) AS diagnostic_count
                FROM runs r
            """)
        except BaseException:
            self.connection.close()
            raise

    def check_size(self) -> None:
        if database_bytes(self.config.database) >= self.config.max_bytes:
            raise StorageLimitReached("Database plus WAL reached the configured storage threshold.")

    @contextmanager
    def transaction(self) -> Generator[None]:
        self.check_size()
        self.connection.execute("BEGIN")
        self.in_transaction = True
        try:
            yield
            for (table, columns), records in self.pending.items():
                self.write_rows(table, columns, list(records.values()))
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        finally:
            self.in_transaction = False
            self.pending.clear()
        self.check_size()

    def checkpoint(self) -> None:
        self.connection.execute("CHECKPOINT")
        self.check_size()

    def insert(self, table: str, values: dict[str, object]) -> None:
        columns = tuple(values)
        if self.in_transaction:
            record_id = values["record_id"]
            if not isinstance(record_id, str):
                raise TypeError("Persisted records require a string record ID.")
            self.pending.setdefault((table, columns), {}).setdefault(record_id, values)
        else:
            self.write_rows(table, columns, [values])

    def write_rows(self, table: str, columns: tuple[str, ...], rows: list[dict[str, object]]) -> None:
        names = ", ".join(columns)
        fields = ", ".join(f"r.{name}" for name in columns)
        for offset in range(0, len(rows), 1000):
            self.connection.execute(
                f"INSERT INTO {table} ({names}) SELECT {fields} FROM unnest(?) AS batch(r) "
                "ON CONFLICT DO NOTHING",
                [rows[offset:offset + 1000]],
            )

    def common(self, record_id: str, received: datetime, raw_id: str | None) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION, "run_id": self.run_id, "record_id": record_id,
            "source": SOURCE, "received_at_utc": received, "raw_record_id": raw_id,
        }

    def raw(self, kind: str, payload: object, received: datetime) -> str:
        text = json_text(payload)
        raw_id = sha256((kind + text).encode()).hexdigest()
        self.insert("raw_records", {
            **self.common(raw_id, received, raw_id), "kind": kind, "payload": text,
        })
        return raw_id

    def diagnostic(self, code: str, details: dict[str, object], symbol: str = "") -> None:
        self.insert("diagnostics", {
            **self.common(identifier(), utc_now(), None),
            "code": code, "symbol": symbol, "details": json_text(details),
        })

    def start(self, window: SessionWindow, code_revision: str) -> None:
        self.window = window
        now = utc_now()
        with self.transaction():
            interrupted = self.connection.execute(
                "SELECT run_id FROM runs WHERE status IN ('waiting','running')"
            ).fetchall()
            self.connection.execute("""
                UPDATE runs SET status='interrupted', stop_reason='unclean_exit_end_time_unknown'
                WHERE status IN ('waiting','running')
            """)
            self.connection.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
                self.run_id, SCHEMA_VERSION, window.day, window.opens, window.closes,
                now, None, "waiting" if now < window.opens else "running", None,
                json_text(asdict(self.config)), json_text(capability_manifest(self.config)), code_revision,
            ])
            for (previous_run,) in interrupted:
                self.diagnostic("recovered_interrupted_run", {"previous_run_id": previous_run})
            self.diagnostic("capability_limitations", {
                "delay_status": self.config.delay_status,
                "clock_offset_ms_user_reported": self.config.clock_offset_ms,
                "broker_conventions": "unknown; broker numerical comparison not certified",
            })

    def finish(self, status: str, reason: str) -> None:
        # A tiny final status write is allowed after the size trigger.
        self.connection.execute(
            "UPDATE runs SET status=?, stop_reason=?, ended_at=? WHERE run_id=?",
            [status, reason, utc_now(), self.run_id],
        )
        self.connection.execute("CHECKPOINT")

    def close(self) -> None:
        self.connection.close()

    def universe(
        self, instruments: list[Instrument], selection: dict[str, object], effective: datetime,
    ) -> None:
        with self.transaction():
            raw_id = self.raw("selection", selection, effective)
            self.insert("universe_revisions", {
                **self.common(identifier(), effective, raw_id),
                "effective_at": effective, "selection": json_text(selection),
                "symbols": json_text([instrument.streamer_symbol for instrument in instruments]),
            })
            for instrument in instruments:
                values = asdict(instrument)
                reference = values.pop("reference")
                raw_id = self.raw("instrument_reference", reference, effective)
                self.insert("instruments", {
                    **self.common(identifier(), effective, raw_id), **values,
                    "effective_at": effective,
                    "terms_status": "provider terms; currency/full deliverable unknown" if instrument.option_type else "underlying",
                })

    def observation(self, observation: Observation | None) -> str | None:
        if observation is None:
            return None
        raw_id = self.raw(observation.kind, observation.payload, observation.received_at)
        values = {
            **self.common(observation.record_id, observation.received_at, raw_id),
            "symbol": observation.symbol, "source_time": observation.timestamp,
        }
        fields = observation.fields
        if observation.kind == "Quote":
            values.update({
                "bid_time": source_time(fields.get("bid_time")),
                "ask_time": source_time(fields.get("ask_time")),
                "bid": number(fields.get("bid_price")), "ask": number(fields.get("ask_price")),
                "bid_size": number(fields.get("bid_size")), "ask_size": number(fields.get("ask_size")),
                "bid_exchange": fields.get("bid_exchange_code"), "ask_exchange": fields.get("ask_exchange_code"),
            })
            table = "quotes"
        elif observation.kind == "Trade":
            values.update({name: number(fields.get(name)) for name in ("price", "size", "day_volume")})
            values.update({"exchange": fields.get("exchange_code"), "extended_hours": fields.get("extended_trading_hours")})
            table = "trades"
        elif observation.kind == "Greeks":
            values.update({
                name: number(fields.get(name))
                for name in ("price", "volatility", "delta", "gamma", "theta", "vega", "rho", "event_flags")
            })
            table = "analytics"
        else:
            raise ValueError("Use candle() for Candle observations.")
        self.insert(table, values)
        return observation.record_id

    def sample(
        self, state: CaptureState, instruments: list[Instrument],
        scheduled: datetime, sampled: datetime, missed: bool = False,
    ) -> None:
        cycle_id = identifier()
        lateness = max(0.0, (sampled - scheduled).total_seconds())
        with self.transaction():
            self.insert("cycles", {
                **self.common(cycle_id, sampled, None), "scheduled_at": scheduled,
                "sampled_at": sampled, "lateness_seconds": lateness,
                "expected_options": len(instruments), "status": "missed" if missed else "sampled",
            })
            if missed:
                self.diagnostic("missed_cycle", {"scheduled_at": scheduled})
                return
            spot_now = state.latest.get(("Quote", "SPY"))
            self.observation(spot_now)
            underlying_trade = state.latest.get(("Trade", "SPY"))
            underlying_trade_id = self.observation(underlying_trade)
            for instrument in instruments:
                symbol = instrument.streamer_symbol
                quote = state.latest.get(("Quote", symbol))
                analytics = state.latest.get(("Greeks", symbol))
                trade = state.latest.get(("Trade", symbol))
                spot = state.pair_spot(quote, sampled)
                mid, age, flags = quote_quality(quote, sampled, self.config.quote_age)
                spot_mid, spot_age, spot_flags = quote_quality(spot, sampled, self.config.quote_age)
                flags += ["underlying_" + flag for flag in spot_flags]
                skew = (
                    (quote.timestamp - spot.timestamp).total_seconds()
                    if quote and quote.timestamp and spot and spot.timestamp else None
                )
                receipt_skew = (quote.received_at - spot.received_at).total_seconds() if quote and spot else None
                if skew is None or skew > self.config.max_skew:
                    flags.append("pair.missing_or_skewed")
                if lateness > self.config.max_lateness:
                    flags.append("sample.late")
                if instrument.stops_trading_at and sampled >= instrument.stops_trading_at:
                    flags.append("contract.expired")
                if self.config.delay_status != "realtime":
                    flags.append("feed.delay_unverified_or_delayed")
                if self.config.clock_offset_ms is None:
                    flags.append("clock.offset_unknown")
                elif abs(self.config.clock_offset_ms) / 1000 > self.config.max_skew:
                    flags.append("clock.offset_excessive")
                quote_eligible = not flags and mid is not None and mid > 0
                analytics_age, analytics_flags = analytics_quality(analytics, sampled, self.config.analytics_age)
                self.insert("samples", {
                    **self.common(identifier(), sampled, quote.raw_id if quote else None),
                    "cycle_id": cycle_id, "symbol": symbol,
                    "scheduled_at": scheduled, "sampled_at": sampled,
                    "quote_id": self.observation(quote), "underlying_quote_id": self.observation(spot),
                    "analytics_id": self.observation(analytics), "trade_id": self.observation(trade),
                    "underlying_trade_id": underlying_trade_id,
                    "option_mid": mid, "underlying_mid": spot_mid,
                    "quote_age_seconds": age, "underlying_age_seconds": spot_age,
                    "analytics_age_seconds": analytics_age, "source_skew_seconds": skew,
                    "receipt_skew_seconds": receipt_skew, "quote_eligible": quote_eligible,
                    "analytics_fresh": not analytics_flags, "broker_comparison_ready": False,
                    "quality": json_text(quality_objects(flags + analytics_flags)),
                })

    def candle(self, observation: Observation) -> None:
        if self.window is None:
            raise RuntimeError("Run must be started before candle persistence.")
        fields = observation.fields
        start = observation.timestamp
        end = start + timedelta(minutes=1) if start else None
        flags: list[str] = []
        if start is None:
            flags.append("candle.time_unknown")
        elif start < self.window.opens or start >= self.window.closes:
            flags.append("candle.outside_session")
        elif start.second != 0 or start.microsecond != 0:
            flags.append("candle.interval_not_minute_aligned")
        values = {name: number(fields.get(name)) for name in ("open", "high", "low", "close", "volume", "count", "vwap")}
        prices = [values[name] for name in ("open", "high", "low", "close")]
        if any(price is None or price <= 0 for price in prices):
            flags.append("candle.invalid_ohlc")
        elif not (values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]):
            flags.append("candle.inconsistent_ohlc")
        for name in ("volume", "count"):
            if values[name] is None or values[name] < 0:
                flags.append(f"candle.{name}_invalid")
        event_flags = number(fields.get("event_flags"))
        if event_flags is None or int(event_flags) & 3:
            flags.append("candle.pending_or_removed")
        raw_id = self.raw("Candle", observation.payload, observation.received_at)
        self.insert("candles", {
            **self.common(observation.record_id, observation.received_at, raw_id),
            "symbol": observation.symbol.split("{", 1)[0], "interval_start": start,
            "interval_end": end, **values, "event_flags": event_flags,
            "revision_id": raw_id,
            "elapsed_with_grace": end is not None and observation.received_at >= end + timedelta(seconds=self.config.candle_grace),
            "quality": json_text(quality_objects(flags)),
        })

    def flush(self, state: CaptureState) -> None:
        if not state.candles and not state.diagnostics:
            self.check_size()
            return
        # The event loop is not yielded while this transaction drains its buffers.
        with self.transaction():
            for observation in state.candles:
                self.candle(observation)
            for code, symbol, details in state.diagnostics:
                self.diagnostic(code, details, symbol)
        state.candles.clear()
        state.diagnostics.clear()

    def request(
        self, operation: str, start: datetime, symbols: list[str], status: str,
        details: dict[str, object] | None = None,
    ) -> None:
        with self.transaction():
            self.insert("requests", {
                **self.common(identifier(), utc_now(), None), "operation": operation,
                "started_at": start, "ended_at": utc_now(), "status": status,
                "symbols": json_text(symbols), "details": json_text(details or {}),
            })

    def summary(self) -> dict[str, object]:
        return report(self.connection, self.run_id)


def report(connection: duckdb.DuckDBPyConnection, run_id: str) -> dict[str, object]:
    result = connection.execute("SELECT * FROM run_coverage WHERE run_id=?", [run_id])
    row = result.fetchone()
    if row is None:
        raise ValueError("Unknown collector run.")
    summary = dict(zip([column[0] for column in result.description], row))
    run = connection.execute(
        "SELECT session_open, session_close, config FROM runs WHERE run_id=?", [run_id],
    ).fetchone()
    if run is None:
        raise ValueError("Missing run metadata.")
    config = json.loads(run[2])
    expected_cycles = int((run[1] - run[0]).total_seconds() // config["sample_seconds"])
    contracts = connection.execute(
        "SELECT count(DISTINCT streamer_symbol), count(DISTINCT expiration) FROM instruments WHERE run_id=? AND option_type IS NOT NULL",
        [run_id],
    ).fetchone()
    summary.update({
        "expected_session_cycles": expected_cycles,
        "unrecorded_cycles": max(0, expected_cycles - summary["recorded_cycles"]),
        "distinct_contracts": contracts[0], "distinct_expirations": contracts[1],
        "storage_bytes_database_plus_wal": database_bytes(Path(config["database"])),
        "broker_comparison_ready": False,
        "limitations": "Broker conventions, adjustment basis and full deliverables require verification.",
    })
    candle_counts = connection.execute("""
        SELECT symbol, count(DISTINCT interval_start),
               count(*) FILTER (WHERE elapsed_with_grace AND quality='[]')
        FROM latest_candles
        WHERE run_id=? AND interval_start>=? AND interval_end<=?
        GROUP BY symbol ORDER BY symbol
    """, [run_id, run[0], run[1]]).fetchall()
    observed = {row[0]: (row[1], row[2]) for row in candle_counts}
    symbols = [row[0] for row in connection.execute(
        "SELECT DISTINCT streamer_symbol FROM instruments WHERE run_id=? ORDER BY streamer_symbol", [run_id],
    ).fetchall()]
    minutes = int((run[1] - run[0]).total_seconds() // 60)
    summary["candle_coverage"] = [
        {"symbol": symbol, "expected_intervals": minutes,
         "observed_intervals": observed.get(symbol, (0, 0))[0],
         "usable_elapsed_intervals": observed.get(symbol, (0, 0))[1],
         "absent_intervals": minutes - observed.get(symbol, (0, 0))[0],
         "absence_reason": "unknown; not proof of no trades"}
        for symbol in symbols
    ]
    summary["timing"] = connection.execute("""
        SELECT max(quote_age_seconds), max(analytics_age_seconds), max(source_skew_seconds),
               quantile_cont(source_skew_seconds, 0.95),
               count(*) FILTER (WHERE quote_id IS NULL),
               count(*) FILTER (WHERE analytics_id IS NULL)
        FROM samples WHERE run_id=?
    """, [run_id]).fetchone()
    summary["timing_columns"] = [
        "max_quote_age_seconds", "max_analytics_age_seconds", "max_source_skew_seconds",
        "p95_source_skew_seconds", "missing_quotes", "missing_analytics",
    ]
    summary["missing_quote_samples"] = summary["timing"][4]
    summary["missing_analytics_samples"] = summary["timing"][5]
    summary["unobserved_candle_intervals"] = sum(
        row["absent_intervals"] for row in summary["candle_coverage"]
    )
    summary["quality_reason_counts"] = [
        {"reason": row[0], "samples": row[1]}
        for row in connection.execute("""
            SELECT json_extract_string(flag.value, '$.reason'), count(*)
            FROM samples, json_each(samples.quality) AS flag WHERE run_id=?
            GROUP BY 1 ORDER BY 1
        """, [run_id]).fetchall()
    ]
    return summary


def export_database(database: Path, destination: Path) -> dict[str, object]:
    """Export after the writer exits; refuse to overwrite any existing export."""
    destination.mkdir(parents=True, exist_ok=False)
    connection = duckdb.connect(str(database), read_only=True)
    try:
        if connection.execute("SELECT version FROM collector_schema").fetchall() != [(SCHEMA_VERSION,)]:
            raise ValueError("Unsupported collector schema.")
        manifest: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION, "exported_at_utc": utc_now(),
            "files": [], "runs": [],
            "null_encoding": "Parquet null; consult quality flags/capability manifest for reasons",
            "units": "UTC timestamps; numeric feed values unscaled; raw payloads retain original representation",
        }
        tables = ["collector_schema", "runs", *TABLES, "latest_candles", "run_coverage"]
        for table in tables:
            path = destination / f"{table}.parquet"
            escaped = str(path).replace("'", "''")
            connection.execute(f"COPY (SELECT * FROM {table}) TO '{escaped}' (FORMAT PARQUET)")
            with path.open("rb") as handle:
                digest = sha256()
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            schema = connection.execute(f"DESCRIBE {table}").fetchall()
            manifest["files"].append({
                "file": path.name, "sha256": digest.hexdigest(), "bytes": path.stat().st_size,
                "rows": connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                "columns": [{"name": row[0], "type": row[1]} for row in schema],
            })
        for (run_id,) in connection.execute("SELECT run_id FROM runs ORDER BY started_at").fetchall():
            manifest["runs"].append(report(connection, run_id))
        (destination / "manifest.json").write_text(json_text(manifest), encoding="utf-8")
        return manifest
    finally:
        connection.close()
