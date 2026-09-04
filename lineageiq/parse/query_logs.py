"""DuckDB-backed, schema-validated Parquet query-log loader."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from lineageiq.models import EvidencePointer, EvidenceSource, StrictModel

ProgressCallback = Callable[[str], None]
EXPECTED_COLUMNS = ("dashboard_id", "tile_id", "user", "timestamp")


class QueryLogParseError(ValueError):
    """Raised when a query-log file violates its schema contract."""


class QueryLogEntry(StrictModel):
    """One normalized query-log observation."""

    dashboard_id: str
    tile_id: str
    user: str
    timestamp: datetime
    evidence: EvidencePointer


class UsageSummary(StrictModel):
    """Per-dashboard/tile usage within a deterministic observation window."""

    dashboard_id: str
    tile_id: str
    last_queried_at: datetime
    query_count_90d: int


class QueryLogBatch(StrictModel):
    """A deterministically ordered set of query-log observations."""

    source_path: str
    as_of: datetime
    window_days: int
    entries: tuple[QueryLogEntry, ...]
    usage: tuple[UsageSummary, ...]

    @property
    def min_timestamp(self) -> datetime | None:
        return self.entries[0].timestamp if self.entries else None

    @property
    def max_timestamp(self) -> datetime | None:
        return self.entries[-1].timestamp if self.entries else None

    def for_dashboard(self, dashboard_id: str) -> tuple[QueryLogEntry, ...]:
        return tuple(entry for entry in self.entries if entry.dashboard_id == dashboard_id)

    def usage_for(self, dashboard_id: str, tile_id: str) -> UsageSummary:
        matches = [
            summary
            for summary in self.usage
            if summary.dashboard_id == dashboard_id and summary.tile_id == tile_id
        ]
        if len(matches) != 1:
            raise KeyError(
                f"expected one usage summary for {dashboard_id!r}/{tile_id!r}, "
                f"found {len(matches)}"
            )
        return matches[0]


def _row_hash(
    dashboard_id: str, tile_id: str, user: str, timestamp: datetime
) -> str:
    value = {
        "dashboard_id": dashboard_id,
        "tile_id": tile_id,
        "timestamp": timestamp.isoformat(),
        "user": user,
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def load_query_logs(
    path: Path,
    *,
    as_of: datetime | None = None,
    window_days: int = 90,
    batch_size: int = 10_000,
    progress: ProgressCallback | None = None,
) -> QueryLogBatch:
    """Load and normalize Parquet rows in a stable total order."""

    path = Path(path)
    if not path.is_file():
        raise QueryLogParseError(f"query-log parquet does not exist: {path}")
    if batch_size < 1:
        raise QueryLogParseError("batch_size must be at least 1")
    if window_days < 1:
        raise QueryLogParseError("window_days must be at least 1")

    connection = duckdb.connect()
    try:
        try:
            description = connection.execute(
                "describe select * from read_parquet(?)", [str(path)]
            ).fetchall()
        except duckdb.Error as exc:
            raise QueryLogParseError(f"cannot read query-log parquet {path}: {exc}") from exc
        columns = tuple(row[0] for row in description)
        if columns != EXPECTED_COLUMNS:
            raise QueryLogParseError(
                f"query-log columns must be {EXPECTED_COLUMNS}, found {columns}"
            )
        types = {row[0]: row[1].upper() for row in description}
        for name in EXPECTED_COLUMNS[:3]:
            if types[name] != "VARCHAR":
                raise QueryLogParseError(f"query-log column {name} must be VARCHAR")
        if not types["timestamp"].startswith("TIMESTAMP"):
            raise QueryLogParseError("query-log column timestamp must be TIMESTAMP")

        total = connection.execute(
            "select count(*) from read_parquet(?)", [str(path)]
        ).fetchone()[0]
        if total == 0 and as_of is None:
            raise QueryLogParseError("as_of is required when query logs are empty")
        if as_of is None:
            raw_as_of = connection.execute(
                'select max("timestamp") from read_parquet(?)', [str(path)]
            ).fetchone()[0]
            normalized_as_of = _utc(raw_as_of)
        else:
            normalized_as_of = _utc(as_of)
        cursor = connection.execute(
            'select dashboard_id, tile_id, "user", "timestamp" '
            "from read_parquet(?) "
            'order by "timestamp", dashboard_id, tile_id, "user"',
            [str(path)],
        )
        entries: list[QueryLogEntry] = []
        while rows := cursor.fetchmany(batch_size):
            for dashboard_id, tile_id, user, raw_timestamp in rows:
                timestamp = _utc(raw_timestamp)
                locator = (
                    f"dashboard_id={dashboard_id}/tile_id={tile_id}/"
                    f"timestamp={timestamp.isoformat().replace('+00:00', 'Z')}/user={user}"
                )
                entries.append(
                    QueryLogEntry(
                        dashboard_id=dashboard_id,
                        tile_id=tile_id,
                        user=user,
                        timestamp=timestamp,
                        evidence=EvidencePointer(
                            source=EvidenceSource.QUERY_LOG,
                            uri=path.as_posix(),
                            locator=locator,
                            content_hash=_row_hash(
                                dashboard_id, tile_id, user, timestamp
                            ),
                        ),
                    )
                )
            if progress:
                progress(f"query_logs={len(entries)}/{total}")
    finally:
        connection.close()

    cutoff = normalized_as_of - timedelta(days=window_days)
    grouped: dict[tuple[str, str], list[QueryLogEntry]] = {}
    for entry in entries:
        grouped.setdefault((entry.dashboard_id, entry.tile_id), []).append(entry)
    usage = tuple(
        UsageSummary(
            dashboard_id=dashboard_id,
            tile_id=tile_id,
            last_queried_at=max(entry.timestamp for entry in group),
            query_count_90d=sum(
                cutoff <= entry.timestamp <= normalized_as_of for entry in group
            ),
        )
        for (dashboard_id, tile_id), group in sorted(grouped.items())
    )
    return QueryLogBatch(
        source_path=path.as_posix(),
        as_of=normalized_as_of,
        window_days=window_days,
        entries=tuple(entries),
        usage=usage,
    )
