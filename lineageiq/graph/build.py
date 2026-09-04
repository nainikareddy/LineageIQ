"""Deterministic model-grain NetworkX DAG construction."""

from __future__ import annotations

import hashlib
import json

import networkx as nx

from lineageiq.models import Edge, EdgeKind
from lineageiq.parse.dashboards import DashboardCatalog
from lineageiq.parse.dbt import ParsedDbtProject

LAYER_ORDER = {
    "source": 0,
    "staging": 1,
    "intermediate": 2,
    "marts": 3,
    "tile": 4,
}


class GraphBuildError(ValueError):
    """Raised when parser outputs cannot form a valid model-level DAG."""


def tile_node_id(dashboard_id: str, tile_id: str) -> str:
    return f"tile.{dashboard_id}.{tile_id}"


def _relation_key(relation: str) -> str:
    return relation.casefold()


def build_model_dag(
    dbt: ParsedDbtProject,
    dashboards: DashboardCatalog,
) -> nx.DiGraph:
    """Build and validate source → model → tile dependencies.

    Nodes and edges are inserted in stable order. This function creates a new
    graph and never mutates parser outputs.
    """

    graph = nx.DiGraph()
    relation_index: dict[str, str] = {}

    for source in sorted(dbt.sources, key=lambda item: item.id):
        graph.add_node(
            source.id,
            kind="source",
            layer="source",
            asset=source,
            evidence=source.evidence,
        )
        key = _relation_key(source.relation_name)
        if key in relation_index:
            raise GraphBuildError(f"duplicate relation {source.relation_name}")
        relation_index[key] = source.id

    for model in sorted(
        dbt.models,
        key=lambda item: (LAYER_ORDER.get(item.layer, 99), item.asset.id),
    ):
        graph.add_node(
            model.asset.id,
            kind="model",
            layer=model.layer,
            asset=model.asset,
            parsed=model,
            evidence=model.asset.evidence,
        )
        key = _relation_key(model.relation_name)
        if key in relation_index:
            raise GraphBuildError(f"duplicate relation {model.relation_name}")
        relation_index[key] = model.asset.id

    for dashboard in sorted(dashboards.dashboards, key=lambda item: item.id):
        for tile in sorted(dashboard.tiles, key=lambda item: item.id):
            node_id = tile_node_id(dashboard.id, tile.id)
            graph.add_node(
                node_id,
                kind="tile",
                layer="tile",
                asset=tile,
                dashboard=dashboard,
                evidence=tile.evidence,
            )

    edges: list[Edge] = []
    for model in sorted(dbt.models, key=lambda item: item.asset.id):
        for source_id in model.sources:
            if source_id not in graph:
                raise GraphBuildError(
                    f"model {model.name} references unknown source node {source_id}"
                )
            edges.append(
                Edge(
                    source_id=source_id,
                    target_id=model.asset.id,
                    kind=EdgeKind.MODEL_DEPENDENCY,
                    evidence=model.asset.evidence,
                )
            )
        for ref_name in model.refs:
            upstream_id = f"model.{ref_name}"
            if upstream_id not in graph:
                raise GraphBuildError(
                    f"model {model.name} references unknown model node {upstream_id}"
                )
            edges.append(
                Edge(
                    source_id=upstream_id,
                    target_id=model.asset.id,
                    kind=EdgeKind.MODEL_DEPENDENCY,
                    evidence=model.asset.evidence,
                )
            )

    for dashboard in sorted(dashboards.dashboards, key=lambda item: item.id):
        for tile in sorted(dashboard.tiles, key=lambda item: item.id):
            target_id = tile_node_id(dashboard.id, tile.id)
            for relation in tile.queried_tables:
                source_id = relation_index.get(_relation_key(relation))
                if source_id is None:
                    raise GraphBuildError(
                        f"tile {dashboard.id}/{tile.id} queries unknown relation {relation}"
                    )
                edges.append(
                    Edge(
                        source_id=source_id,
                        target_id=target_id,
                        kind=EdgeKind.TILE_DEPENDENCY,
                        evidence=tile.evidence,
                    )
                )

    for edge in sorted(
        edges,
        key=lambda item: (item.source_id, item.target_id, item.kind.value),
    ):
        if graph.has_edge(edge.source_id, edge.target_id):
            raise GraphBuildError(
                f"duplicate dependency {edge.source_id} -> {edge.target_id}"
            )
        graph.add_edge(
            edge.source_id,
            edge.target_id,
            kind=edge.kind.value,
            edge=edge,
            evidence=edge.evidence,
        )

    from lineageiq.graph.queries import assert_acyclic

    assert_acyclic(graph)
    return graph


def graph_fingerprint(graph: nx.DiGraph) -> str:
    """Hash the model-grain topology and stable node/edge classifications."""

    payload = {
        "nodes": sorted(
            (
                node_id,
                attributes["kind"],
                attributes["layer"],
            )
            for node_id, attributes in graph.nodes(data=True)
        ),
        "edges": sorted(
            (
                source_id,
                target_id,
                attributes["kind"],
            )
            for source_id, target_id, attributes in graph.edges(data=True)
        ),
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()
