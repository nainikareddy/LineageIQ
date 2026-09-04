from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lineageiq.graph import (
    LineageCycleError,
    assert_acyclic,
    build_model_dag,
    edge_counts_by_layer,
    find_orphan_models,
    graph_fingerprint,
    node_counts_by_layer,
)
from lineageiq.models import DefectType, DetectorKind
from lineageiq.parse import load_dashboards, load_dbt_project

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC = REPO_ROOT / "synthetic"


def _real_graph():
    dbt = load_dbt_project(SYNTHETIC / "dbt_project")
    dashboards = load_dashboards(SYNTHETIC / "dashboards.json")
    return build_model_dag(dbt, dashboards)


def test_real_model_dag_counts_orphans_and_repeatability() -> None:
    first = _real_graph()
    second = _real_graph()

    assert first.number_of_nodes() == second.number_of_nodes() == 73
    assert first.number_of_edges() == second.number_of_edges() == 73
    assert node_counts_by_layer(first) == (
        ("intermediate", 10),
        ("marts", 10),
        ("source", 10),
        ("staging", 10),
        ("tile", 33),
    )
    assert edge_counts_by_layer(first) == (
        ("intermediate->marts", 11),
        ("marts->tile", 33),
        ("source->staging", 10),
        ("staging->intermediate", 17),
        ("staging->marts", 2),
    )
    assert graph_fingerprint(first) == graph_fingerprint(second)

    findings = find_orphan_models(first)
    assert len(findings) == 1
    finding = findings[0]
    assert finding.id == "orphaned_model:stg_legacy_orders"
    assert finding.defect_type is DefectType.ORPHANED_MODEL
    assert finding.detector is DetectorKind.DETERMINISTIC
    assert finding.confidence == 1.0
    assert finding.asset_ids == ("model.stg_legacy_orders",)
    assert finding.evidence[0].uri.endswith(
        "synthetic/dbt_project/models/staging/stg_legacy_orders.sql"
    )

    manifest = yaml.safe_load((SYNTHETIC / "manifest.yaml").read_text(encoding="utf-8"))
    d2 = next(item for item in manifest["defects"] if item["id"] == "D2")
    assert finding.evidence[0].uri.endswith(d2["locations"][0]["path"])


def test_cycle_check_raises_on_a_cycle_added_to_real_dag() -> None:
    graph = _real_graph()
    graph.add_edge(
        "model.fct_orders",
        "model.stg_orders",
        kind="model_dependency",
    )

    with pytest.raises(LineageCycleError, match="lineage graph contains a cycle"):
        assert_acyclic(graph)
