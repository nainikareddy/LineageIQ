"""Strict loader for Looker-style dashboard metadata."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from lineageiq.models import EvidencePointer, EvidenceSource, StrictModel, Tile

ProgressCallback = Callable[[str], None]


class DashboardParseError(ValueError):
    """Raised when dashboard metadata violates its input contract."""


class _InputModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _TileInput(_InputModel):
    id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    metric_label: str = Field(min_length=1)
    sql: str = Field(min_length=1)


class _DashboardInput(_InputModel):
    id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str
    tiles: tuple[_TileInput, ...]


class _CatalogInput(_InputModel):
    schema_version: int
    seed: int
    generated_as_of: str
    dashboards: tuple[_DashboardInput, ...]


class Dashboard(StrictModel):
    """A validated dashboard containing core ``Tile`` objects."""

    id: str
    title: str
    description: str
    tiles: tuple[Tile, ...]
    evidence: tuple[EvidencePointer, ...]


class DashboardCatalog(StrictModel):
    """Validated dashboard export."""

    schema_version: int
    seed: int
    generated_as_of: str
    source_path: str
    dashboards: tuple[Dashboard, ...]

    @property
    def tiles(self) -> tuple[Tile, ...]:
        return tuple(tile for dashboard in self.dashboards for tile in dashboard.tiles)

    def dashboard(self, dashboard_id: str) -> Dashboard:
        matches = [item for item in self.dashboards if item.id == dashboard_id]
        if len(matches) != 1:
            raise KeyError(f"expected one dashboard {dashboard_id!r}, found {len(matches)}")
        return matches[0]

    def tile(self, dashboard_id: str, tile_id: str) -> Tile:
        matches = [
            tile for tile in self.dashboard(dashboard_id).tiles if tile.id == tile_id
        ]
        if len(matches) != 1:
            raise KeyError(
                f"expected one tile {dashboard_id!r}/{tile_id!r}, found {len(matches)}"
            )
        return matches[0]


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _queried_tables(sql: str, *, locator: str) -> tuple[str, ...]:
    try:
        expression = parse_one(sql)
    except ParseError as exc:
        raise DashboardParseError(f"invalid tile SQL at {locator}: {exc}") from exc
    tables: list[str] = []
    for table in expression.find_all(exp.Table):
        relation = ".".join(
            part for part in (table.catalog, table.db, table.name) if part
        )
        if relation and relation not in tables:
            tables.append(relation)
    if not tables:
        raise DashboardParseError(f"tile SQL queries no table at {locator}")
    return tuple(tables)


def load_dashboards(
    path: Path,
    *,
    progress: ProgressCallback | None = None,
) -> DashboardCatalog:
    """Load dashboard JSON, rejecting unknown fields and duplicate IDs."""

    path = Path(path)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        catalog = _CatalogInput.model_validate(raw)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise DashboardParseError(f"invalid dashboard metadata {path}: {exc}") from exc

    dashboard_ids = [dashboard.id for dashboard in catalog.dashboards]
    if len(dashboard_ids) != len(set(dashboard_ids)):
        raise DashboardParseError("dashboard IDs must be unique")
    all_tile_ids = [tile.id for dashboard in catalog.dashboards for tile in dashboard.tiles]
    if len(all_tile_ids) != len(set(all_tile_ids)):
        raise DashboardParseError("tile IDs must be globally unique")

    dashboards: list[Dashboard] = []
    for dashboard_index, dashboard in enumerate(catalog.dashboards, start=1):
        dashboard_locator = f"dashboard={dashboard.id}"
        dashboard_evidence = EvidencePointer(
            source=EvidenceSource.DASHBOARD_METADATA,
            uri=path.as_posix(),
            locator=dashboard_locator,
            content_hash=_canonical_hash(dashboard.model_dump(mode="json")),
        )
        tiles: list[Tile] = []
        for tile in dashboard.tiles:
            tile_locator = f"{dashboard_locator}/tile={tile.id}"
            tile_evidence = EvidencePointer(
                source=EvidenceSource.DASHBOARD_METADATA,
                uri=path.as_posix(),
                locator=tile_locator,
                content_hash=_canonical_hash(tile.model_dump(mode="json")),
                excerpt=tile.sql,
            )
            tiles.append(
                Tile(
                    id=tile.id,
                    dashboard_id=dashboard.id,
                    title=tile.title,
                    query_sql=tile.sql,
                    metric_labels=(tile.metric_label,),
                    queried_tables=_queried_tables(tile.sql, locator=tile_locator),
                    evidence=(tile_evidence,),
                )
            )
        dashboards.append(
            Dashboard(
                id=dashboard.id,
                title=dashboard.title,
                description=dashboard.description,
                tiles=tuple(tiles),
                evidence=(dashboard_evidence,),
            )
        )
        if progress and (
            dashboard_index == len(catalog.dashboards) or dashboard_index % 5 == 0
        ):
            progress(f"dashboards={dashboard_index}/{len(catalog.dashboards)}")

    return DashboardCatalog(
        schema_version=catalog.schema_version,
        seed=catalog.seed,
        generated_as_of=catalog.generated_as_of,
        source_path=path.as_posix(),
        dashboards=tuple(dashboards),
    )
