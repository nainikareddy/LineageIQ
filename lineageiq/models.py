"""Strict, immutable domain contracts shared across LineageIQ."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

StableId = Annotated[str, Field(min_length=1)]


class StrictModel(BaseModel):
    """Base contract that rejects coercion, mutation, and unknown fields."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, str_strip_whitespace=True)


class EvidenceSource(StrEnum):
    DBT_SQL = "dbt_sql"
    DBT_METADATA = "dbt_metadata"
    DASHBOARD_METADATA = "dashboard_metadata"
    QUERY_LOG = "query_log"
    GRAPH = "graph"


class EvidencePointer(StrictModel):
    """Stable pointer to an observation in an input artifact or derived graph."""

    source: EvidenceSource
    uri: Annotated[str, Field(min_length=1)]
    locator: Annotated[str, Field(min_length=1)]
    content_hash: str | None = None
    excerpt: str | None = None


class Column(StrictModel):
    """A column owned by a model/source relation."""

    id: StableId
    name: Annotated[str, Field(min_length=1)]
    parent_id: StableId
    data_type: str | None = None
    expression_sql: str | None = None
    evidence: tuple[EvidencePointer, ...] = ()


class Model(StrictModel):
    """A dbt model or declared source relation."""

    id: StableId
    name: Annotated[str, Field(min_length=1)]
    path: Annotated[str, Field(min_length=1)]
    database: str | None = None
    schema_name: str | None = None
    materialization: str | None = None
    column_ids: tuple[StableId, ...] = ()
    evidence: tuple[EvidencePointer, ...] = ()


class Tile(StrictModel):
    """A BI tile and the metric/lineage metadata extracted from it."""

    id: StableId
    dashboard_id: StableId
    title: Annotated[str, Field(min_length=1)]
    query_sql: Annotated[str, Field(min_length=1)]
    metric_labels: tuple[str, ...] = ()
    queried_tables: tuple[str, ...] = ()
    column_ids: tuple[StableId, ...] = ()
    evidence: tuple[EvidencePointer, ...] = ()


class EdgeKind(StrEnum):
    COLUMN_LINEAGE = "column_lineage"
    MODEL_DEPENDENCY = "model_dependency"
    TILE_DEPENDENCY = "tile_dependency"
    QUERY_OBSERVED = "query_observed"
    UNRESOLVED = "unresolved"


class Edge(StrictModel):
    """A directed dependency backed by at least one evidence pointer."""

    source_id: StableId
    target_id: StableId
    kind: EdgeKind
    evidence: Annotated[tuple[EvidencePointer, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def reject_self_loop(self) -> Edge:
        if self.source_id == self.target_id:
            raise ValueError("lineage edges cannot be self-loops")
        return self


class DefectType(StrEnum):
    DUPLICATE_DASHBOARD = "duplicate_dashboard"
    STALE_ASSET = "stale_asset"
    ORPHANED_MODEL = "orphaned_model"
    METRIC_DEFINITION_CONFLICT = "metric_definition_conflict"
    BROKEN_LINEAGE = "broken_lineage"
    UNUSED_COLUMN_PROPAGATION = "unused_column_propagation"


class DetectorKind(StrEnum):
    DETERMINISTIC = "deterministic"
    SEMANTIC_LLM = "semantic_llm"


class Finding(StrictModel):
    """An evidenced defect emitted by deterministic code or a validated judge."""

    id: StableId
    defect_type: DefectType
    summary: Annotated[str, Field(min_length=1)]
    asset_ids: Annotated[tuple[StableId, ...], Field(min_length=1)]
    evidence: Annotated[tuple[EvidencePointer, ...], Field(min_length=1)]
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]
    detector: DetectorKind

    @model_validator(mode="after")
    def enforce_detector_scope(self) -> Finding:
        if (
            self.detector is DetectorKind.SEMANTIC_LLM
            and self.defect_type is not DefectType.METRIC_DEFINITION_CONFLICT
        ):
            raise ValueError("the semantic LLM may only report metric_definition_conflict")
        return self
