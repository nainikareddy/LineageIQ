"""Schema-aware, column-level lineage extraction using SQLGlot."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Iterable
from enum import StrEnum

import networkx as nx
from pydantic import model_validator
from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.lineage import Node, lineage

from lineageiq.models import (
    Column,
    DefectType,
    DetectorKind,
    Edge,
    EdgeKind,
    EvidencePointer,
    Finding,
    StrictModel,
)
from lineageiq.parse.dashboards import DashboardCatalog
from lineageiq.parse.dbt import ParsedDbtModel, ParsedDbtProject

ProgressCallback = Callable[[str], None]
DEFAULT_UNUSED_PROPAGATION_MIN_COLUMNS = 10


class SqlLineageError(ValueError):
    """Raised when the project cannot be analyzed deterministically."""


class UnresolvedReason(StrEnum):
    MISSING_UPSTREAM_COLUMN = "missing_upstream_column"
    UNKNOWN_UPSTREAM_RELATION = "unknown_upstream_relation"
    AMBIGUOUS_COLUMN = "ambiguous_column"
    ROW_LEVEL_AGGREGATION = "row_level_aggregation"
    UNSUPPORTED_CONSTRUCT = "unsupported_construct"
    PARSE_ERROR = "parse_error"


class UnresolvedLineageEdge(StrictModel):
    """An explicit failed dependency with a machine-readable cause."""

    target_id: str
    upstream_relation: str | None = None
    column_name: str | None = None
    reason_code: UnresolvedReason
    reason: str
    expression_sql: str
    evidence: tuple[EvidencePointer, ...]


class LineageColumn(StrictModel):
    """A column plus extraction metadata used by structural checks."""

    column: Column
    relation_name: str
    asset_kind: str
    star_expanded: bool = False


class ColumnLineageResult(StrictModel):
    """Complete deterministic column-lineage extraction result."""

    columns: tuple[LineageColumn, ...]
    edges: tuple[Edge, ...]
    unresolved_edges: tuple[UnresolvedLineageEdge, ...]
    source_column_ids: tuple[str, ...]
    tile_column_ids: tuple[str, ...]
    fully_traced_tile_column_ids: tuple[str, ...]
    coverage_pct: float

    @model_validator(mode="after")
    def validate_coverage(self) -> ColumnLineageResult:
        expected = (
            100.0
            if not self.tile_column_ids
            else 100.0
            * len(self.fully_traced_tile_column_ids)
            / len(self.tile_column_ids)
        )
        if abs(self.coverage_pct - expected) > 1e-9:
            raise ValueError("coverage_pct does not match fully traced tile columns")
        return self

    def column(self, column_id: str) -> LineageColumn:
        matches = [item for item in self.columns if item.column.id == column_id]
        if len(matches) != 1:
            raise KeyError(f"expected one column {column_id!r}, found {len(matches)}")
        return matches[0]

    def source_columns_for(self, column_id: str) -> tuple[str, ...]:
        """Return every declared source column reachable upstream."""

        incoming: dict[str, set[str]] = defaultdict(set)
        for edge in self.edges:
            incoming[edge.target_id].add(edge.source_id)
        source_ids = set(self.source_column_ids)
        found: set[str] = set()
        pending = [column_id]
        visited: set[str] = set()
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            if current in source_ids:
                found.add(current)
            else:
                pending.extend(incoming.get(current, ()))
        return tuple(sorted(found))

    def unresolved_for(self, column_id: str) -> tuple[UnresolvedLineageEdge, ...]:
        """Return unresolved dependencies reachable upstream from a column."""

        incoming: dict[str, set[str]] = defaultdict(set)
        for edge in self.edges:
            incoming[edge.target_id].add(edge.source_id)
        targets: set[str] = set()
        pending = [column_id]
        while pending:
            current = pending.pop()
            if current in targets:
                continue
            targets.add(current)
            pending.extend(incoming.get(current, ()))
        return tuple(
            edge for edge in self.unresolved_edges if edge.target_id in targets
        )


def source_column_id(source_id: str, column_name: str) -> str:
    return f"column.{source_id}.{column_name}"


def model_column_id(model_name: str, column_name: str) -> str:
    return f"column.model.{model_name}.{column_name}"


def tile_column_id(dashboard_id: str, tile_id: str, column_name: str) -> str:
    return f"column.tile.{dashboard_id}.{tile_id}.{column_name}"


def _tile_asset_id(dashboard_id: str, tile_id: str) -> str:
    return f"tile.{dashboard_id}.{tile_id}"


def _relation_parts(table: exp.Table) -> tuple[str, ...]:
    return tuple(part for part in (table.catalog, table.db, table.name) if part)


def _relation_name(table: exp.Table) -> str:
    return ".".join(_relation_parts(table))


def _schema_context(
    relation_columns: dict[str, tuple[str, ...]],
) -> tuple[dict[str, object], str]:
    catalogs = {
        parts[0]
        for relation in relation_columns
        if len(parts := relation.split(".")) >= 3
    }
    default_catalog = next(iter(catalogs)) if len(catalogs) == 1 else "lineageiq"
    schema: dict[str, object] = {}
    for relation, columns in sorted(relation_columns.items()):
        parts = relation.split(".")
        if len(parts) == 2:
            parts = [default_catalog, *parts]
        elif len(parts) == 1:
            parts = [default_catalog, "default", *parts]
        cursor: dict[str, object] = schema
        for part in parts[:-1]:
            child = cursor.setdefault(part, {})
            if not isinstance(child, dict):
                raise SqlLineageError(f"schema collision at relation {relation}")
            cursor = child
        cursor[parts[-1]] = {column: "UNKNOWN" for column in columns}
    return schema, default_catalog


def _topological_models(dbt: ParsedDbtProject) -> tuple[ParsedDbtModel, ...]:
    graph = nx.DiGraph()
    for model in dbt.models:
        graph.add_node(model.name)
    for model in dbt.models:
        for upstream in model.refs:
            graph.add_edge(upstream, model.name)
    try:
        generations = tuple(nx.topological_generations(graph))
    except nx.NetworkXUnfeasible as exc:
        raise SqlLineageError("dbt model dependencies contain a cycle") from exc
    by_name = {model.name: model for model in dbt.models}
    return tuple(
        by_name[name]
        for generation in generations
        for name in sorted(generation)
    )


def _projection_star_outputs(sql: str, output_names: Iterable[str]) -> set[str]:
    try:
        expression = parse_one(sql)
    except SqlglotError:
        return set()
    if not isinstance(expression, exp.Query):
        return set()
    explicit: set[str] = set()
    has_star = False
    for selection in expression.selects:
        is_star = isinstance(selection, exp.Star) or (
            isinstance(selection, exp.Column) and isinstance(selection.this, exp.Star)
        )
        if is_star:
            has_star = True
        elif selection.alias_or_name:
            explicit.add(selection.alias_or_name)
    return set(output_names) - explicit if has_star else set()


def _terminal_nodes(root: Node) -> tuple[Node, ...]:
    return tuple(node for node in root.walk() if not node.downstream)


def _known_relations(
    expression: exp.Expr,
    relation_columns: dict[str, tuple[str, ...]],
    default_catalog: str,
) -> tuple[str, ...]:
    known = {
        relation.casefold(): relation for relation in relation_columns
    }
    matches: list[str] = []
    for table in expression.find_all(exp.Table):
        parts = _relation_parts(table)
        candidates = [".".join(parts)]
        if len(parts) == 3 and parts[0].casefold() == default_catalog.casefold():
            candidates.append(".".join(parts[1:]))
        canonical = next(
            (
                known[candidate.casefold()]
                for candidate in candidates
                if candidate.casefold() in known
            ),
            None,
        )
        if canonical and canonical not in matches:
            matches.append(canonical)
    return tuple(matches)


def _evidence_for_relations(
    relations: Iterable[str],
    relation_evidence: dict[str, tuple[EvidencePointer, ...]],
    target_evidence: tuple[EvidencePointer, ...],
) -> tuple[EvidencePointer, ...]:
    combined = list(target_evidence)
    for relation in relations:
        for evidence in relation_evidence.get(relation, ()):
            if evidence not in combined:
                combined.append(evidence)
    return tuple(combined)


def _classify_unresolved(
    *,
    terminal: Node,
    target_id: str,
    target_evidence: tuple[EvidencePointer, ...],
    relation_columns: dict[str, tuple[str, ...]],
    relation_evidence: dict[str, tuple[EvidencePointer, ...]],
    default_catalog: str,
    context_source: exp.Expr,
) -> UnresolvedLineageEdge:
    expression = terminal.expression
    expression_sql = expression.sql()
    known_relations = _known_relations(
        terminal.source, relation_columns, default_catalog
    )
    if not known_relations:
        known_relations = _known_relations(
            context_source, relation_columns, default_catalog
        )
    column_name = terminal.name.rsplit(".", 1)[-1] if terminal.name else None

    if expression.find(exp.Count) and expression.find(exp.Star):
        return UnresolvedLineageEdge(
            target_id=target_id,
            upstream_relation=known_relations[0] if len(known_relations) == 1 else None,
            reason_code=UnresolvedReason.ROW_LEVEL_AGGREGATION,
            reason="COUNT(*) is row-level and has no specific upstream source column.",
            expression_sql=expression_sql,
            evidence=_evidence_for_relations(
                known_relations, relation_evidence, target_evidence
            ),
        )

    if isinstance(expression, exp.Placeholder):
        if len(known_relations) == 1 and column_name:
            relation = known_relations[0]
            if column_name not in relation_columns[relation]:
                return UnresolvedLineageEdge(
                    target_id=target_id,
                    upstream_relation=relation,
                    column_name=column_name,
                    reason_code=UnresolvedReason.MISSING_UPSTREAM_COLUMN,
                    reason=(
                        f"Column {column_name!r} is missing from upstream relation "
                        f"{relation!r}."
                    ),
                    expression_sql=expression_sql,
                    evidence=_evidence_for_relations(
                        known_relations, relation_evidence, target_evidence
                    ),
                )
        if len(known_relations) > 1:
            return UnresolvedLineageEdge(
                target_id=target_id,
                column_name=column_name,
                reason_code=UnresolvedReason.AMBIGUOUS_COLUMN,
                reason=(
                    f"Column {column_name!r} could not be assigned to one of "
                    f"{known_relations!r}."
                ),
                expression_sql=expression_sql,
                evidence=_evidence_for_relations(
                    known_relations, relation_evidence, target_evidence
                ),
            )

    return UnresolvedLineageEdge(
        target_id=target_id,
        upstream_relation=known_relations[0] if len(known_relations) == 1 else None,
        column_name=column_name,
        reason_code=UnresolvedReason.UNSUPPORTED_CONSTRUCT,
        reason=f"SQLGlot could not trace construct {expression_sql!r} to a source column.",
        expression_sql=expression_sql,
        evidence=_evidence_for_relations(
            known_relations, relation_evidence, target_evidence
        ),
    )


def _extract_query(
    *,
    sql: str,
    parent_id: str,
    relation_name: str,
    asset_kind: str,
    evidence: tuple[EvidencePointer, ...],
    relation_columns: dict[str, tuple[str, ...]],
    relation_evidence: dict[str, tuple[EvidencePointer, ...]],
    id_factory: Callable[[str], str],
) -> tuple[
    tuple[LineageColumn, ...],
    tuple[Edge, ...],
    tuple[UnresolvedLineageEdge, ...],
]:
    schema, default_catalog = _schema_context(relation_columns)
    try:
        roots = lineage(
            None,
            sql,
            schema=schema,
            catalog=default_catalog,
            identify=False,
            validate_qualify_columns=False,
        )
    except SqlglotError as exc:
        try:
            expression = parse_one(sql)
            output_names = tuple(
                selection.alias_or_name or f"expression_{index}"
                for index, selection in enumerate(expression.selects, start=1)
            )
        except SqlglotError:
            output_names = ("unparsed_output",)
        columns = tuple(
            LineageColumn(
                column=Column(
                    id=id_factory(name),
                    name=name,
                    parent_id=parent_id,
                    expression_sql=sql,
                    evidence=evidence,
                ),
                relation_name=relation_name,
                asset_kind=asset_kind,
            )
            for name in output_names
        )
        unresolved = tuple(
            UnresolvedLineageEdge(
                target_id=column.column.id,
                reason_code=UnresolvedReason.PARSE_ERROR,
                reason=f"SQLGlot lineage failed: {exc}",
                expression_sql=sql,
                evidence=evidence,
            )
            for column in columns
        )
        return columns, (), unresolved

    output_names = tuple(roots)
    star_outputs = _projection_star_outputs(sql, output_names)
    columns: list[LineageColumn] = []
    edges: list[Edge] = []
    unresolved: list[UnresolvedLineageEdge] = []
    known_relation_keys = {
        relation.casefold(): relation for relation in relation_columns
    }

    for output_name, root in roots.items():
        target_id = id_factory(output_name)
        columns.append(
            LineageColumn(
                column=Column(
                    id=target_id,
                    name=output_name,
                    parent_id=parent_id,
                    expression_sql=root.expression.sql(),
                    evidence=evidence,
                ),
                relation_name=relation_name,
                asset_kind=asset_kind,
                star_expanded=output_name in star_outputs,
            )
        )
        terminals = _terminal_nodes(root)
        if not terminals:
            terminals = (root,)
        for terminal in terminals:
            if isinstance(terminal.expression, exp.Table):
                parts = _relation_parts(terminal.expression)
                raw_relation = ".".join(parts)
                relation_candidates = [raw_relation]
                if (
                    len(parts) == 3
                    and parts[0].casefold() == default_catalog.casefold()
                ):
                    relation_candidates.append(".".join(parts[1:]))
                upstream_relation = next(
                    (
                        known_relation_keys[candidate.casefold()]
                        for candidate in relation_candidates
                        if candidate.casefold() in known_relation_keys
                    ),
                    None,
                )
                column_name = terminal.name.rsplit(".", 1)[-1]
                if upstream_relation is None:
                    unresolved.append(
                        UnresolvedLineageEdge(
                            target_id=target_id,
                            upstream_relation=raw_relation or None,
                            column_name=column_name,
                            reason_code=UnresolvedReason.UNKNOWN_UPSTREAM_RELATION,
                            reason=f"Upstream relation {raw_relation!r} is not declared.",
                            expression_sql=terminal.expression.sql(),
                            evidence=evidence,
                        )
                    )
                    continue
                if column_name not in relation_columns[upstream_relation]:
                    unresolved.append(
                        UnresolvedLineageEdge(
                            target_id=target_id,
                            upstream_relation=upstream_relation,
                            column_name=column_name,
                            reason_code=UnresolvedReason.MISSING_UPSTREAM_COLUMN,
                            reason=(
                                f"Column {column_name!r} is missing from upstream "
                                f"relation {upstream_relation!r}."
                            ),
                            expression_sql=terminal.expression.sql(),
                            evidence=_evidence_for_relations(
                                (upstream_relation,), relation_evidence, evidence
                            ),
                        )
                    )
                    continue
                upstream_id = _column_id_for_relation(
                    upstream_relation, column_name, relation_columns
                )
                edges.append(
                    Edge(
                        source_id=upstream_id,
                        target_id=target_id,
                        kind=EdgeKind.COLUMN_LINEAGE,
                        evidence=evidence,
                    )
                )
            else:
                unresolved.append(
                    _classify_unresolved(
                        terminal=terminal,
                        target_id=target_id,
                        target_evidence=evidence,
                        relation_columns=relation_columns,
                        relation_evidence=relation_evidence,
                        default_catalog=default_catalog,
                        context_source=root.source,
                    )
                )

    unique_edges = {
        (edge.source_id, edge.target_id, edge.kind): edge for edge in edges
    }
    unique_unresolved = {
        (
            edge.target_id,
            edge.upstream_relation,
            edge.column_name,
            edge.reason_code,
            edge.reason,
        ): edge
        for edge in unresolved
    }
    return (
        tuple(columns),
        tuple(unique_edges[key] for key in sorted(unique_edges, key=str)),
        tuple(unique_unresolved[key] for key in sorted(unique_unresolved, key=str)),
    )


def _column_id_for_relation(
    relation: str,
    column_name: str,
    relation_columns: dict[str, tuple[str, ...]],
) -> str:
    if column_name not in relation_columns[relation]:
        raise KeyError(f"unknown column {relation}.{column_name}")
    parts = relation.split(".")
    if len(parts) >= 3 and parts[0] == "warehouse" and parts[1] == "raw":
        return source_column_id(f"source.raw.{parts[-1]}", column_name)
    return model_column_id(parts[-1], column_name)


def _fully_traced_columns(
    *,
    tile_column_ids: tuple[str, ...],
    source_column_ids: tuple[str, ...],
    edges: tuple[Edge, ...],
    unresolved: tuple[UnresolvedLineageEdge, ...],
) -> tuple[str, ...]:
    incoming: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        incoming[edge.target_id].add(edge.source_id)
    unresolved_targets = {edge.target_id for edge in unresolved}
    source_ids = set(source_column_ids)
    memo: dict[str, bool] = {}

    def traced(column_id: str, visiting: set[str]) -> bool:
        if column_id in memo:
            return memo[column_id]
        if column_id in unresolved_targets or column_id in visiting:
            memo[column_id] = False
            return False
        if column_id in source_ids:
            memo[column_id] = True
            return True
        parents = incoming.get(column_id, set())
        if not parents:
            memo[column_id] = False
            return False
        result = all(traced(parent, visiting | {column_id}) for parent in parents)
        memo[column_id] = result
        return result

    return tuple(column_id for column_id in tile_column_ids if traced(column_id, set()))


def build_column_lineage(
    dbt: ParsedDbtProject,
    dashboards: DashboardCatalog,
    *,
    progress: ProgressCallback | None = None,
) -> ColumnLineageResult:
    """Trace every model and tile output to declared source columns."""

    relation_columns: dict[str, tuple[str, ...]] = {}
    relation_evidence: dict[str, tuple[EvidencePointer, ...]] = {}
    columns: list[LineageColumn] = []
    edges: list[Edge] = []
    unresolved: list[UnresolvedLineageEdge] = []
    source_ids: list[str] = []

    for source in sorted(dbt.sources, key=lambda item: item.id):
        relation_columns[source.relation_name] = source.columns
        relation_evidence[source.relation_name] = source.evidence
        for column_name in source.columns:
            column_id = source_column_id(source.id, column_name)
            source_ids.append(column_id)
            columns.append(
                LineageColumn(
                    column=Column(
                        id=column_id,
                        name=column_name,
                        parent_id=source.id,
                        evidence=source.evidence,
                    ),
                    relation_name=source.relation_name,
                    asset_kind="source",
                )
            )

    ordered_models = _topological_models(dbt)
    for index, model in enumerate(ordered_models, start=1):
        model_columns, model_edges, model_unresolved = _extract_query(
            sql=model.resolved_sql,
            parent_id=model.asset.id,
            relation_name=model.relation_name,
            asset_kind="model",
            evidence=model.asset.evidence,
            relation_columns=relation_columns,
            relation_evidence=relation_evidence,
            id_factory=lambda name, model_name=model.name: model_column_id(
                model_name, name
            ),
        )
        output_names = tuple(item.column.name for item in model_columns)
        if len(output_names) != len(set(output_names)):
            raise SqlLineageError(f"model {model.name} has duplicate output columns")
        relation_columns[model.relation_name] = output_names
        relation_evidence[model.relation_name] = model.asset.evidence
        columns.extend(model_columns)
        edges.extend(model_edges)
        unresolved.extend(model_unresolved)
        if progress and (index == len(ordered_models) or index % 10 == 0):
            progress(f"column_lineage models={index}/{len(ordered_models)}")

    tile_ids: list[str] = []
    ordered_tiles = sorted(
        dashboards.tiles, key=lambda tile: (tile.dashboard_id, tile.id)
    )
    for index, tile in enumerate(ordered_tiles, start=1):
        parent_id = _tile_asset_id(tile.dashboard_id, tile.id)
        tile_columns, tile_edges, tile_unresolved = _extract_query(
            sql=tile.query_sql,
            parent_id=parent_id,
            relation_name=f"tile.{tile.dashboard_id}.{tile.id}",
            asset_kind="tile",
            evidence=tile.evidence,
            relation_columns=relation_columns,
            relation_evidence=relation_evidence,
            id_factory=lambda name, dashboard_id=tile.dashboard_id, tile_id=tile.id: (
                tile_column_id(dashboard_id, tile_id, name)
            ),
        )
        tile_ids.extend(item.column.id for item in tile_columns)
        columns.extend(tile_columns)
        edges.extend(tile_edges)
        unresolved.extend(tile_unresolved)
        if progress and (index == len(ordered_tiles) or index % 10 == 0):
            progress(f"column_lineage tiles={index}/{len(ordered_tiles)}")

    sorted_edges = tuple(
        sorted(edges, key=lambda edge: (edge.source_id, edge.target_id, edge.kind.value))
    )
    sorted_unresolved = tuple(
        sorted(
            unresolved,
            key=lambda edge: (
                edge.target_id,
                edge.reason_code.value,
                edge.upstream_relation or "",
                edge.column_name or "",
            ),
        )
    )
    tile_column_ids = tuple(tile_ids)
    source_column_ids = tuple(sorted(source_ids))
    fully_traced = _fully_traced_columns(
        tile_column_ids=tile_column_ids,
        source_column_ids=source_column_ids,
        edges=sorted_edges,
        unresolved=sorted_unresolved,
    )
    coverage = (
        100.0
        if not tile_column_ids
        else 100.0 * len(fully_traced) / len(tile_column_ids)
    )
    return ColumnLineageResult(
        columns=tuple(columns),
        edges=sorted_edges,
        unresolved_edges=sorted_unresolved,
        source_column_ids=source_column_ids,
        tile_column_ids=tile_column_ids,
        fully_traced_tile_column_ids=fully_traced,
        coverage_pct=coverage,
    )


def _column_descendants(result: ColumnLineageResult) -> dict[str, set[str]]:
    graph = nx.DiGraph(
        (edge.source_id, edge.target_id) for edge in result.edges
    )
    tile_ids = set(result.tile_column_ids)
    return {
        item.column.id: nx.descendants(graph, item.column.id) & tile_ids
        if item.column.id in graph
        else set()
        for item in result.columns
    }


def find_broken_column_refs(
    result: ColumnLineageResult,
) -> tuple[Finding, ...]:
    """Emit deterministic Findings for missing upstream columns."""

    column_index = {item.column.id: item for item in result.columns}
    descendants = _column_descendants(result)
    findings: list[Finding] = []
    for unresolved in result.unresolved_edges:
        if unresolved.reason_code is not UnresolvedReason.MISSING_UPSTREAM_COLUMN:
            continue
        target = column_index[unresolved.target_id]
        affected_assets = [target.column.parent_id]
        evidence = list(unresolved.evidence)
        for tile_column_id in sorted(descendants.get(unresolved.target_id, set())):
            tile_column = column_index[tile_column_id]
            if tile_column.column.parent_id not in affected_assets:
                affected_assets.append(tile_column.column.parent_id)
            for pointer in tile_column.column.evidence:
                if pointer not in evidence:
                    evidence.append(pointer)
        findings.append(
            Finding(
                id=f"broken_lineage:{target.column.parent_id}.{target.column.name}",
                defect_type=DefectType.BROKEN_LINEAGE,
                summary=unresolved.reason,
                asset_ids=tuple(affected_assets),
                evidence=tuple(evidence),
                confidence=1.0,
                detector=DetectorKind.DETERMINISTIC,
            )
        )
    return tuple(findings)


def find_unused_column_propagation(
    result: ColumnLineageResult,
    *,
    minimum_unused_columns: int = DEFAULT_UNUSED_PROPAGATION_MIN_COLUMNS,
) -> tuple[Finding, ...]:
    """Flag materially wide ``SELECT *`` propagation unused by every tile."""

    if minimum_unused_columns < 1:
        raise ValueError("minimum_unused_columns must be at least 1")
    outgoing: dict[str, set[str]] = defaultdict(set)
    for edge in result.edges:
        outgoing[edge.source_id].add(edge.target_id)
    grouped: dict[str, list[LineageColumn]] = defaultdict(list)
    for item in result.columns:
        if (
            item.asset_kind == "model"
            and item.star_expanded
            and not outgoing[item.column.id]
        ):
            grouped[item.column.parent_id].append(item)

    findings: list[Finding] = []
    for model_id, unused in sorted(grouped.items()):
        if len(unused) < minimum_unused_columns:
            continue
        names = tuple(sorted(item.column.name for item in unused))
        evidence = unused[0].column.evidence
        findings.append(
            Finding(
                id=f"unused_column_propagation:{model_id.removeprefix('model.')}",
                defect_type=DefectType.UNUSED_COLUMN_PROPAGATION,
                summary=(
                    f"Model {model_id.removeprefix('model.')} propagates "
                    f"{len(names)} SELECT * columns unused by every dashboard tile: "
                    f"{', '.join(names)}."
                ),
                asset_ids=(model_id,),
                evidence=evidence,
                confidence=1.0,
                detector=DetectorKind.DETERMINISTIC,
            )
        )
    return tuple(findings)


def lineage_fingerprint(result: ColumnLineageResult) -> str:
    """Hash deterministic topology, unresolved causes, and coverage."""

    payload = {
        "columns": sorted(
            (
                item.column.id,
                item.relation_name,
                item.asset_kind,
                item.star_expanded,
            )
            for item in result.columns
        ),
        "edges": sorted((edge.source_id, edge.target_id) for edge in result.edges),
        "unresolved": [
            (
                edge.target_id,
                edge.upstream_relation,
                edge.column_name,
                edge.reason_code.value,
                edge.reason,
            )
            for edge in result.unresolved_edges
        ],
        "coverage_pct": result.coverage_pct,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
