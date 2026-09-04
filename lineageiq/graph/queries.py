"""Deterministic model-level graph checks and summaries."""

from __future__ import annotations

from collections import Counter

import networkx as nx

from lineageiq.models import DefectType, DetectorKind, Finding, Model


class LineageCycleError(ValueError):
    """Raised when the lineage graph is not acyclic."""


def assert_acyclic(graph: nx.DiGraph) -> None:
    """Raise with a stable cycle description when a cycle exists."""

    try:
        cycle = nx.find_cycle(graph, orientation="original")
    except nx.NetworkXNoCycle:
        return
    cycle_edges = sorted((source, target) for source, target, _ in cycle)
    rendered = ", ".join(f"{source} -> {target}" for source, target in cycle_edges)
    raise LineageCycleError(f"lineage graph contains a cycle: {rendered}")


def find_orphan_models(graph: nx.DiGraph) -> tuple[Finding, ...]:
    """Return model nodes with no downstream model or tile consumer."""

    findings: list[Finding] = []
    for node_id, attributes in sorted(graph.nodes(data=True)):
        if attributes.get("kind") != "model" or graph.out_degree(node_id) != 0:
            continue
        model = attributes.get("asset")
        if not isinstance(model, Model):
            raise TypeError(f"model node {node_id} is missing its Model contract")
        findings.append(
            Finding(
                id=f"orphaned_model:{model.name}",
                defect_type=DefectType.ORPHANED_MODEL,
                summary=f"Model {model.name} has no downstream model or dashboard tile.",
                asset_ids=(model.id,),
                evidence=model.evidence,
                confidence=1.0,
                detector=DetectorKind.DETERMINISTIC,
            )
        )
    return tuple(findings)


def node_counts_by_layer(graph: nx.DiGraph) -> tuple[tuple[str, int], ...]:
    counts = Counter(attributes["layer"] for _, attributes in graph.nodes(data=True))
    return tuple(sorted(counts.items()))


def edge_counts_by_layer(graph: nx.DiGraph) -> tuple[tuple[str, int], ...]:
    counts = Counter(
        f"{graph.nodes[source]['layer']}->{graph.nodes[target]['layer']}"
        for source, target in graph.edges
    )
    return tuple(sorted(counts.items()))
