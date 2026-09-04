"""Export the lineage DAG plus audit findings as a single JSON payload.

Read-only. Rebuilds the graph exactly as ``lineageiq build`` does, then joins in
the findings written by ``lineageiq audit``. Nothing here mutates the pipeline or
its outputs; it exists so the static lineage viewer has a data file to render.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lineageiq.graph import build_model_dag, graph_fingerprint
from lineageiq.parse import build_column_lineage, load_dashboards, load_dbt_project


def _label(node_id: str) -> str:
    return node_id.split(".", 1)[1] if "." in node_id else node_id


def export(repo_root: Path) -> dict:
    synthetic = repo_root / "synthetic"
    dbt = load_dbt_project(synthetic / "dbt_project")
    dashboards = load_dashboards(synthetic / "dashboards.json")
    graph = build_model_dag(dbt, dashboards)
    column_lineage = build_column_lineage(dbt, dashboards)

    dashboard_of: dict[str, str] = {}
    for node_id, attrs in graph.nodes(data=True):
        if attrs.get("kind") == "tile":
            dashboard_of[node_id] = attrs["dashboard"].id

    findings = json.loads((repo_root / ".lineageiq" / "audit.json").read_text())["findings"]

    # A finding may cite a dashboard, which is not itself a graph node. Fan those
    # out to every tile on that dashboard so the highlight lands somewhere visible.
    node_findings: dict[str, list[str]] = {}
    for finding in findings:
        targets: set[str] = set()
        for asset_id in finding["asset_ids"]:
            if asset_id in graph:
                targets.add(asset_id)
            elif asset_id.startswith("dashboard."):
                dash = asset_id.split(".", 1)[1]
                targets.update(n for n, d in dashboard_of.items() if d == dash)
        for target in targets:
            node_findings.setdefault(target, []).append(finding["id"])

    nodes = [
        {
            "id": node_id,
            "kind": attrs["kind"],
            "layer": attrs["layer"],
            "label": _label(node_id),
            "dashboard": dashboard_of.get(node_id),
            "findings": sorted(node_findings.get(node_id, [])),
        }
        for node_id, attrs in sorted(graph.nodes(data=True))
    ]
    edges = [
        {"source": s, "target": t, "kind": attrs["kind"]}
        for s, t, attrs in sorted(graph.edges(data=True))
    ]

    return {
        "nodes": nodes,
        "edges": edges,
        "findings": findings,
        "stats": {
            "node_count": graph.number_of_nodes(),
            "edge_count": graph.number_of_edges(),
            "finding_count": len(findings),
            "graph_fingerprint": graph_fingerprint(graph),
            "coverage_pct": column_lineage.coverage_pct,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, default=Path("tools/graph_export.json"))
    args = parser.parse_args()
    payload = export(args.repo_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(f"[export_graph] wrote {args.output} nodes={payload['stats']['node_count']} "
          f"edges={payload['stats']['edge_count']} findings={payload['stats']['finding_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
