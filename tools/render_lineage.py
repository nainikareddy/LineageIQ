"""Render tools/graph_export.json into a standalone, interactive lineage HTML page.

Reads the pre-built graph + audit-findings export (see ``tools/export_graph.py``)
and writes a single self-contained HTML file: a layered left-to-right DAG
(source -> staging -> intermediate -> marts -> tile) with the 7 audit findings
highlighted on their nodes. Layout (column assignment, barycenter node
ordering, and pixel coordinates) is computed here in pure Python and embedded
as static data; the page's inline JS only draws SVG from that data and wires
up hover/click interactions. No network access, no third-party libraries,
standard library only. Deterministic: identical input bytes always produce
identical output bytes.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

LAYER_ORDER = ["source", "staging", "intermediate", "marts", "tile"]

LAYER_TITLES = {
    "source": "Sources",
    "staging": "Staging",
    "intermediate": "Intermediate",
    "marts": "Marts",
    "tile": "Dashboard tiles",
}

NODE_WIDTH = {
    "source": 168,
    "staging": 168,
    "intermediate": 190,
    "marts": 190,
    "tile": 214,
}

NODE_HEIGHT = {
    "source": 42,
    "staging": 42,
    "intermediate": 42,
    "marts": 42,
    "tile": 50,
}

COL_GAP = 260
GAP_Y = 14
MARGIN_TOP = 84
MARGIN_LEFT = 70
MARGIN_RIGHT = 70
MARGIN_BOTTOM = 60
BARYCENTER_PASSES = 4

# Fixed display order for defect types -> categorical palette slot. Order is
# hardcoded (not derived from data) so the color assignment never shifts
# between runs even if new defect types appear in a future audit.
DEFECT_TYPE_META: list[tuple[str, str, str]] = [
    # (defect_type key, short display name, 2-letter badge code)
    ("broken_lineage", "Broken lineage", "BL"),
    ("duplicate_dashboard", "Duplicate dashboard", "DD"),
    ("metric_definition_conflict", "Metric definition conflict", "MC"),
    ("orphaned_model", "Orphaned model", "OM"),
    ("stale_asset", "Stale asset", "SA"),
    ("unused_column_propagation", "Unused column propagation", "UC"),
]
DEFECT_TYPE_ORDER = {key: i for i, (key, _, _) in enumerate(DEFECT_TYPE_META)}
FALLBACK_DEFECT_INDEX = len(DEFECT_TYPE_META)


def _layout_graph(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, Any]:
    """Assign each node an (x, y, w, h) box via layered barycenter ordering."""
    adjacency: dict[str, set[str]] = {n["id"]: set() for n in nodes}
    for e in edges:
        adjacency[e["source"]].add(e["target"])
        adjacency[e["target"]].add(e["source"])

    layer_nodes: dict[str, list[str]] = {layer: [] for layer in LAYER_ORDER}
    for n in sorted(nodes, key=lambda n: n["id"]):
        layer_nodes[n["layer"]].append(n["id"])

    pos: dict[str, float] = {}

    def reindex(layer: str) -> None:
        for i, nid in enumerate(layer_nodes[layer]):
            pos[nid] = float(i)

    for layer in LAYER_ORDER:
        reindex(layer)

    for pass_num in range(BARYCENTER_PASSES):
        sweep = LAYER_ORDER if pass_num % 2 == 0 else list(reversed(LAYER_ORDER))
        for layer in sweep:
            neighbors_of = adjacency

            def barycenter(nid: str, neighbors_of: dict[str, set[str]] = neighbors_of) -> float:
                neighbors = neighbors_of[nid]
                if not neighbors:
                    return pos[nid]
                return sum(pos[nb] for nb in neighbors) / len(neighbors)

            layer_nodes[layer].sort(key=lambda nid: (barycenter(nid), nid))
            reindex(layer)

    col_centers: dict[str, float] = {}
    cx = float(MARGIN_LEFT) + NODE_WIDTH[LAYER_ORDER[0]] / 2
    for layer in LAYER_ORDER:
        col_centers[layer] = cx
        cx += COL_GAP

    col_height: dict[str, float] = {}
    for layer in LAYER_ORDER:
        n = len(layer_nodes[layer])
        h = NODE_HEIGHT[layer]
        col_height[layer] = (n * h + (n - 1) * GAP_Y) if n > 0 else 0.0

    max_col_height = max(col_height.values()) if col_height else 0.0

    box_by_id: dict[str, dict[str, float]] = {}
    for layer in LAYER_ORDER:
        w = NODE_WIDTH[layer]
        h = NODE_HEIGHT[layer]
        cxi = col_centers[layer]
        y = MARGIN_TOP + (max_col_height - col_height[layer]) / 2
        for nid in layer_nodes[layer]:
            box_by_id[nid] = {
                "x": round(cxi - w / 2, 2),
                "y": round(y, 2),
                "w": w,
                "h": h,
                "cx": round(cxi, 2),
                "cy": round(y + h / 2, 2),
            }
            y += h + GAP_Y

    total_width = col_centers[LAYER_ORDER[-1]] + NODE_WIDTH[LAYER_ORDER[-1]] / 2 + MARGIN_RIGHT
    total_height = MARGIN_TOP + max_col_height + MARGIN_BOTTOM

    return {
        "boxes": box_by_id,
        "width": round(total_width, 2),
        "height": round(total_height, 2),
        "col_centers": col_centers,
    }


def _build_payload(export: dict[str, Any]) -> dict[str, Any]:
    nodes = export["nodes"]
    edges = export["edges"]
    findings = export["findings"]
    stats = export["stats"]

    findings_by_id = {f["id"]: f for f in findings}
    layout = _layout_graph(nodes, edges)
    boxes = layout["boxes"]

    defect_counts: dict[str, int] = {}
    for f in findings:
        defect_counts[f["defect_type"]] = defect_counts.get(f["defect_type"], 0) + 1

    out_nodes = []
    for n in sorted(nodes, key=lambda n: n["id"]):
        box = boxes[n["id"]]
        if n["kind"] == "tile" and "." in n["label"]:
            dash_label, short_label = n["label"].split(".", 1)
        else:
            dash_label, short_label = None, n["label"]

        finding_ids = n["findings"]
        defect_types = sorted(
            {findings_by_id[fid]["defect_type"] for fid in finding_ids},
            key=lambda t: DEFECT_TYPE_ORDER.get(t, FALLBACK_DEFECT_INDEX),
        )
        out_nodes.append(
            {
                "id": n["id"],
                "kind": n["kind"],
                "layer": n["layer"],
                "label": short_label,
                "captionLabel": dash_label,
                "fullLabel": n["label"],
                "dashboard": n["dashboard"],
                "findingIds": finding_ids,
                "defectTypes": defect_types,
                "x": box["x"],
                "y": box["y"],
                "w": box["w"],
                "h": box["h"],
                "cx": box["cx"],
                "cy": box["cy"],
            }
        )

    out_edges = [
        {"source": e["source"], "target": e["target"], "kind": e["kind"]}
        for e in sorted(edges, key=lambda e: (e["source"], e["target"]))
    ]

    out_findings = []
    for f in sorted(findings, key=lambda f: f["id"]):
        out_findings.append(
            {
                "id": f["id"],
                "defectType": f["defect_type"],
                "summary": f["summary"],
                "confidence": f["confidence"],
                "detector": f["detector"],
                "assetIds": f["asset_ids"],
                "evidence": [
                    {
                        "source": ev["source"],
                        "uri": ev["uri"],
                        "locator": ev["locator"],
                        "excerpt": ev.get("excerpt"),
                    }
                    for ev in f["evidence"]
                ],
            }
        )

    defect_legend = [
        {
            "key": key,
            "name": name,
            "code": code,
            "count": defect_counts.get(key, 0),
        }
        for key, name, code in DEFECT_TYPE_META
    ]

    return {
        "stats": {
            "nodeCount": stats["node_count"],
            "edgeCount": stats["edge_count"],
            "findingCount": stats["finding_count"],
            "coveragePct": stats["coverage_pct"],
            "fingerprint": stats["graph_fingerprint"],
        },
        "layerOrder": LAYER_ORDER,
        "layerTitles": LAYER_TITLES,
        "colCenters": {k: round(v, 2) for k, v in layout["col_centers"].items()},
        "svg": {"width": layout["width"], "height": layout["height"]},
        "nodes": out_nodes,
        "edges": out_edges,
        "findings": out_findings,
        "defectLegend": defect_legend,
    }


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LineageIQ &mdash; Lineage &amp; Audit Explorer</title>
<style>
:root {
  color-scheme: light;
  --surface-0: #f9f9f7;
  --surface-1: #fcfcfb;
  --surface-2: #ffffff;
  --surface-sunken: #f1f0ec;
  --text-primary: #0b0b0b;
  --text-secondary: #52514e;
  --text-muted: #898781;
  --border: rgba(11,11,11,0.10);
  --border-strong: rgba(11,11,11,0.22);
  --gridline: #e1e0d9;
  --edge: #c3c2b7;
  --edge-active: #52514e;
  --node-fill: #ffffff;
  --node-stroke: #c3c2b7;
  --shadow: 0 1px 2px rgba(11,11,11,0.06), 0 4px 14px rgba(11,11,11,0.05);

  --c-broken_lineage: #2a78d6;
  --c-duplicate_dashboard: #eb6834;
  --c-metric_definition_conflict: #1baf7a;
  --c-orphaned_model: #eda100;
  --c-stale_asset: #e87ba4;
  --c-unused_column_propagation: #008300;
  --c-fallback: #8a8880;

  --detector-llm: #4a3aa7;
  --detector-llm-bg: rgba(74,58,167,0.10);
  --detector-det: #52514e;
  --detector-det-bg: rgba(82,81,78,0.08);
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --surface-0: #0d0d0d;
    --surface-1: #1a1a19;
    --surface-2: #201f1e;
    --surface-sunken: #161615;
    --text-primary: #ffffff;
    --text-secondary: #c3c2b7;
    --text-muted: #8f8d87;
    --border: rgba(255,255,255,0.12);
    --border-strong: rgba(255,255,255,0.26);
    --gridline: #2c2c2a;
    --edge: #46453f;
    --edge-active: #d8d6cd;
    --node-fill: #201f1e;
    --node-stroke: #46453f;
    --shadow: 0 1px 2px rgba(0,0,0,0.4), 0 6px 18px rgba(0,0,0,0.35);

    --c-broken_lineage: #3987e5;
    --c-duplicate_dashboard: #d95926;
    --c-metric_definition_conflict: #199e70;
    --c-orphaned_model: #c98500;
    --c-stale_asset: #d55181;
    --c-unused_column_propagation: #1f9a1f;
    --c-fallback: #9a9890;

    --detector-llm: #9085e9;
    --detector-llm-bg: rgba(144,133,233,0.16);
    --detector-det: #c3c2b7;
    --detector-det-bg: rgba(195,194,183,0.10);
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --surface-0: #0d0d0d;
  --surface-1: #1a1a19;
  --surface-2: #201f1e;
  --surface-sunken: #161615;
  --text-primary: #ffffff;
  --text-secondary: #c3c2b7;
  --text-muted: #8f8d87;
  --border: rgba(255,255,255,0.12);
  --border-strong: rgba(255,255,255,0.26);
  --gridline: #2c2c2a;
  --edge: #46453f;
  --edge-active: #d8d6cd;
  --node-fill: #201f1e;
  --node-stroke: #46453f;
  --shadow: 0 1px 2px rgba(0,0,0,0.4), 0 6px 18px rgba(0,0,0,0.35);

  --c-broken_lineage: #3987e5;
  --c-duplicate_dashboard: #d95926;
  --c-metric_definition_conflict: #199e70;
  --c-orphaned_model: #c98500;
  --c-stale_asset: #d55181;
  --c-unused_column_propagation: #1f9a1f;
  --c-fallback: #9a9890;

  --detector-llm: #9085e9;
  --detector-llm-bg: rgba(144,133,233,0.16);
  --detector-det: #c3c2b7;
  --detector-det-bg: rgba(195,194,183,0.10);
}

* { box-sizing: border-box; }
html, body {
  margin: 0;
  padding: 0;
  background: var(--surface-0);
  color: var(--text-primary);
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
}
body { display: flex; flex-direction: column; min-height: 100vh; }

/* ---------- header ---------- */
header.app-header {
  padding: 18px 28px;
  background: var(--surface-1);
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 24px;
  flex-wrap: wrap;
}
.app-title { display: flex; flex-direction: column; gap: 2px; }
.app-title h1 { font-size: 16px; margin: 0; font-weight: 650; letter-spacing: -0.01em; }
.app-title p { margin: 0; font-size: 12px; color: var(--text-secondary); }
.stat-row { display: flex; gap: 10px; flex-wrap: wrap; align-items: stretch; }
.stat {
  background: var(--surface-2);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 6px 12px;
  min-width: 74px;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  justify-content: center;
}
.stat .v { font-size: 15px; font-weight: 650; font-variant-numeric: tabular-nums; }
.stat .k {
  font-size: 10px; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.04em;
}
.stat.fingerprint { min-width: 0; }
.stat.fingerprint .v {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 11px;
  font-weight: 500;
  max-width: 132px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
}

/* ---------- legend ---------- */
.legend-bar {
  display: flex;
  gap: 22px;
  flex-wrap: wrap;
  align-items: center;
  padding: 10px 28px;
  background: var(--surface-0);
  border-bottom: 1px solid var(--border);
  font-size: 11.5px;
}
.legend-group { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
.legend-group-label {
  color: var(--text-muted); font-size: 10px;
  text-transform: uppercase; letter-spacing: 0.05em; margin-right: 2px;
}
.legend-chip {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 3px 8px 3px 6px; border-radius: 999px;
  background: var(--surface-2); border: 1px solid var(--border);
  color: var(--text-secondary);
  cursor: default;
}
.legend-swatch {
  width: 16px; height: 16px; border-radius: 5px; flex: none;
  display: inline-flex; align-items: center; justify-content: center;
  font-size: 8.5px; font-weight: 750; color: #fff;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
.legend-count { color: var(--text-muted); font-variant-numeric: tabular-nums; }
.detector-chip { display: inline-flex; align-items: center; gap: 5px; padding: 3px 9px; border-radius: 999px; font-weight: 600; }
.detector-chip.llm { color: var(--detector-llm); background: var(--detector-llm-bg); }
.detector-chip.det { color: var(--detector-det); background: var(--detector-det-bg); }
.detector-chip .dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; flex: none; }

/* ---------- layout ---------- */
.layout {
  display: flex;
  flex: 1;
  min-height: 0;
}
.sidebar {
  width: 300px;
  flex: none;
  border-right: 1px solid var(--border);
  background: var(--surface-1);
  display: flex;
  flex-direction: column;
  max-height: calc(100vh - 118px);
  position: sticky;
  top: 0;
}
.sidebar h2 {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin: 0;
  padding: 14px 16px 8px;
}
.finding-list { list-style: none; margin: 0; padding: 0 8px 12px; overflow-y: auto; flex: 1; }
.finding-item {
  border: 1px solid var(--border);
  background: var(--surface-2);
  border-radius: 10px;
  padding: 10px 11px;
  margin-bottom: 8px;
  cursor: pointer;
  transition: border-color 0.12s ease, background 0.12s ease;
}
.finding-item:hover { border-color: var(--border-strong); }
.finding-item.active { border-color: var(--fi-color, var(--text-primary)); background: var(--surface-sunken); }
.finding-item .fi-head { display: flex; align-items: center; gap: 6px; margin-bottom: 5px; }
.finding-item .fi-summary { font-size: 12px; line-height: 1.4; color: var(--text-primary); margin: 0 0 6px; }
.finding-item .fi-meta { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
.badge-type {
  display: inline-flex; align-items: center; gap: 4px;
  font-size: 10px; font-weight: 700; padding: 2px 7px 2px 5px; border-radius: 999px;
  color: #fff; white-space: nowrap;
}
.badge-type .code {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 9px;
  background: rgba(255,255,255,0.28);
  border-radius: 4px;
  padding: 0 3px;
}
.badge-detector {
  font-size: 9.5px; font-weight: 650; padding: 2px 7px; border-radius: 999px;
  text-transform: uppercase; letter-spacing: 0.02em;
}
.badge-detector.semantic_llm { color: var(--detector-llm); background: var(--detector-llm-bg); }
.badge-detector.deterministic { color: var(--detector-det); background: var(--detector-det-bg); }
.badge-conf { font-size: 10px; color: var(--text-muted); font-variant-numeric: tabular-nums; }
.sidebar-footer { padding: 10px 16px; border-top: 1px solid var(--border); font-size: 11px; color: var(--text-muted); }
.sidebar-footer button {
  font: inherit; font-size: 11px; color: var(--text-secondary);
  background: var(--surface-2); border: 1px solid var(--border); border-radius: 6px;
  padding: 5px 9px; cursor: pointer;
}
.sidebar-footer button:hover { border-color: var(--border-strong); }

/* ---------- graph area ---------- */
.graph-scroll {
  flex: 1;
  overflow: auto;
  background: var(--surface-0);
  background-image:
    linear-gradient(var(--gridline) 1px, transparent 1px),
    linear-gradient(90deg, var(--gridline) 1px, transparent 1px);
  background-size: 32px 32px;
  background-position: -1px -1px;
  position: relative;
}
svg#graph { display: block; }
.col-label {
  fill: var(--text-muted);
  font-size: 11px;
  font-weight: 650;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.edge {
  fill: none;
  stroke: var(--edge);
  stroke-width: 1.4;
  transition: stroke 0.12s ease, stroke-width 0.12s ease, opacity 0.12s ease;
}
.edge.hover-active { stroke: var(--edge-active); stroke-width: 2.4; }
.edge.dim { opacity: 0.14; }

.node { cursor: default; }
.node .box {
  fill: var(--node-fill);
  stroke: var(--node-stroke);
  stroke-width: 1.2;
  transition: stroke 0.12s ease, filter 0.12s ease, opacity 0.12s ease;
}
.node.flagged .box { stroke-width: 2; stroke: var(--flag-color, var(--text-primary)); }
.node.flagged.glow .box { filter: drop-shadow(0 0 5px var(--flag-color, transparent)); }
.node.clickable { cursor: pointer; }
.node.dim { opacity: 0.22; }
.node.dim .box { filter: none; }
.node.selected .box { stroke-width: 2.6; }
.node.hover-neighbor .box { stroke: var(--edge-active); }

.node-label {
  font-size: 11px;
  fill: var(--text-primary);
  font-weight: 550;
}
.node-caption {
  font-size: 9px;
  fill: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}
.node-html {
  width: 100%; height: 100%;
  display: flex; flex-direction: column; justify-content: center;
  padding: 0 10px;
  overflow: hidden;
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
  pointer-events: none;
}
.node-html .cap {
  font-size: 9px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.03em;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.node-html .lbl {
  font-size: 11px; color: var(--text-primary); font-weight: 560;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.badge-dot {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 8px; font-weight: 750; fill: #fff;
}

/* ---------- detail panel ---------- */
.detail-panel {
  width: 340px;
  flex: none;
  border-left: 1px solid var(--border);
  background: var(--surface-1);
  overflow-y: auto;
  max-height: calc(100vh - 118px);
  position: sticky;
  top: 0;
}
.detail-empty {
  padding: 32px 20px;
  color: var(--text-muted);
  font-size: 12.5px;
  line-height: 1.6;
}
.detail-body { padding: 16px; }
.detail-body h3 { font-size: 13px; margin: 0 0 4px; line-height: 1.4; }
.detail-node-id {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 10.5px;
  color: var(--text-muted);
  margin: 0 0 14px;
  word-break: break-all;
}
.detail-finding {
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 12px;
  margin-bottom: 12px;
  background: var(--surface-2);
}
.detail-finding .dhead { display: flex; gap: 6px; align-items: center; margin-bottom: 8px; flex-wrap: wrap; }
.detail-finding p.summary { font-size: 12.5px; line-height: 1.5; margin: 0 0 10px; }
.kv-row { display: flex; justify-content: space-between; font-size: 11px; padding: 3px 0; border-top: 1px dashed var(--border); }
.kv-row span.k { color: var(--text-muted); }
.kv-row span.v { font-weight: 600; font-variant-numeric: tabular-nums; }
.evidence-title { font-size: 10px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted); margin: 12px 0 6px; }
.evidence-item { margin-bottom: 8px; }
.evidence-item .uri {
  font-size: 10.5px; color: var(--text-secondary);
  word-break: break-all; margin-bottom: 2px;
}
.evidence-item .locator {
  font-size: 10px; color: var(--text-muted);
  margin-bottom: 4px;
}
.evidence-item pre {
  margin: 0;
  background: var(--surface-sunken);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 9px;
  font-size: 10.5px;
  line-height: 1.5;
  overflow-x: auto;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  color: var(--text-primary);
  white-space: pre-wrap;
  word-break: break-word;
}

footer.app-footer {
  padding: 8px 28px;
  font-size: 10.5px;
  color: var(--text-muted);
  border-top: 1px solid var(--border);
  background: var(--surface-1);
}

@media (max-width: 900px) {
  .sidebar { width: 240px; }
  .detail-panel { width: 280px; }
}
</style>
</head>
<body>

<header class="app-header">
  <div class="app-title">
    <h1>LineageIQ &mdash; Lineage &amp; Audit Explorer</h1>
    <p>Column-level lineage DAG with audit findings overlaid. Static export, no live pipeline access.</p>
  </div>
  <div class="stat-row" id="stat-row"></div>
</header>

<div class="legend-bar" id="legend-bar"></div>

<div class="layout">
  <aside class="sidebar">
    <h2>Findings (<span id="finding-count-label"></span>)</h2>
    <ul class="finding-list" id="finding-list"></ul>
    <div class="sidebar-footer">
      <button id="clear-selection" type="button">Clear selection</button>
    </div>
  </aside>

  <div class="graph-scroll" id="graph-scroll">
    <svg id="graph" xmlns="http://www.w3.org/2000/svg"></svg>
  </div>

  <aside class="detail-panel" id="detail-panel">
    <div class="detail-empty">Click a flagged node (colored outline) or a finding in the sidebar to see details here. Hover any node to trace its immediate upstream and downstream edges.</div>
  </aside>
</div>

<footer class="app-footer">
  Layout: layered left&rarr;right (source &rarr; staging &rarr; intermediate &rarr; marts &rarr; tile), node order reduced via barycenter heuristic. Rendered entirely client-side from embedded data &mdash; no network requests.
</footer>

<script id="lineage-data" type="application/json">__LINEAGE_DATA_JSON__</script>
<script>
(function () {
  "use strict";
  var DATA = JSON.parse(document.getElementById("lineage-data").textContent);

  var SVGNS = "http://www.w3.org/2000/svg";
  var XHTMLNS = "http://www.w3.org/1999/xhtml";

  function el(tag, attrs, ns) {
    var e = document.createElementNS(ns || SVGNS, tag);
    if (attrs) {
      for (var k in attrs) {
        if (Object.prototype.hasOwnProperty.call(attrs, k)) e.setAttribute(k, attrs[k]);
      }
    }
    return e;
  }

  // ---------- header stats ----------
  var statRow = document.getElementById("stat-row");
  var s = DATA.stats;
  var stats = [
    ["Nodes", s.nodeCount],
    ["Edges", s.edgeCount],
    ["Findings", s.findingCount],
    ["Coverage", s.coveragePct.toFixed(1) + "%"]
  ];
  stats.forEach(function (pair) {
    var div = document.createElement("div");
    div.className = "stat";
    div.innerHTML = '<span class="v">' + pair[1] + '</span><span class="k">' + pair[0] + '</span>';
    statRow.appendChild(div);
  });
  var fpDiv = document.createElement("div");
  fpDiv.className = "stat fingerprint";
  fpDiv.title = s.fingerprint;
  fpDiv.innerHTML = '<span class="v">' + s.fingerprint + '</span><span class="k">Fingerprint</span>';
  statRow.appendChild(fpDiv);

  // ---------- legend ----------
  var legendBar = document.getElementById("legend-bar");
  var typeGroup = document.createElement("div");
  typeGroup.className = "legend-group";
  var typeLabel = document.createElement("span");
  typeLabel.className = "legend-group-label";
  typeLabel.textContent = "Defect types";
  typeGroup.appendChild(typeLabel);
  DATA.defectLegend.forEach(function (d) {
    var chip = document.createElement("span");
    chip.className = "legend-chip";
    chip.innerHTML =
      '<span class="legend-swatch" style="background:var(--c-' + d.key + ')">' + d.code + '</span>' +
      '<span>' + d.name + '</span><span class="legend-count">' + d.count + '</span>';
    typeGroup.appendChild(chip);
  });
  legendBar.appendChild(typeGroup);

  var detGroup = document.createElement("div");
  detGroup.className = "legend-group";
  detGroup.innerHTML =
    '<span class="legend-group-label">Detector</span>' +
    '<span class="detector-chip det"><span class="dot"></span>deterministic &mdash; rule-based, writes findings</span>' +
    '<span class="detector-chip llm"><span class="dot"></span>semantic_llm &mdash; LLM judgment only, never writes to the graph</span>';
  legendBar.appendChild(detGroup);

  // ---------- lookups ----------
  var nodeById = {};
  DATA.nodes.forEach(function (n) { nodeById[n.id] = n; });
  var findingById = {};
  DATA.findings.forEach(function (f) { findingById[f.id] = f; });
  var findingNodeIds = {};
  DATA.findings.forEach(function (f) { findingNodeIds[f.id] = []; });
  DATA.nodes.forEach(function (n) {
    n.findingIds.forEach(function (fid) {
      if (findingNodeIds[fid]) findingNodeIds[fid].push(n.id);
    });
  });

  function defectColorVar(type) {
    return "var(--c-" + type + ", var(--c-fallback))";
  }

  // ---------- svg scaffold ----------
  var svg = document.getElementById("graph");
  svg.setAttribute("viewBox", "0 0 " + DATA.svg.width + " " + DATA.svg.height);
  svg.setAttribute("width", DATA.svg.width);
  svg.setAttribute("height", DATA.svg.height);

  var colLabelsG = el("g", { class: "col-labels" });
  DATA.layerOrder.forEach(function (layer) {
    var t = el("text", {
      x: DATA.colCenters[layer],
      y: 40,
      "text-anchor": "middle",
      class: "col-label"
    });
    t.textContent = DATA.layerTitles[layer];
    colLabelsG.appendChild(t);
  });
  svg.appendChild(colLabelsG);

  var edgesG = el("g", { class: "edges" });
  var nodesG = el("g", { class: "nodes" });
  svg.appendChild(edgesG);
  svg.appendChild(nodesG);

  // ---------- edges ----------
  var edgeEls = [];
  var nodeEdgeMap = {}; // nodeId -> [edge dom els]
  DATA.nodes.forEach(function (n) { nodeEdgeMap[n.id] = []; });

  DATA.edges.forEach(function (e) {
    var a = nodeById[e.source], b = nodeById[e.target];
    if (!a || !b) return;
    var x1 = a.x + a.w, y1 = a.cy;
    var x2 = b.x, y2 = b.cy;
    var dx = Math.max(24, (x2 - x1) * 0.5);
    var d = "M " + x1 + " " + y1 +
      " C " + (x1 + dx) + " " + y1 + ", " + (x2 - dx) + " " + y2 + ", " + x2 + " " + y2;
    var path = el("path", { d: d, class: "edge" });
    path.dataset.source = e.source;
    path.dataset.target = e.target;
    edgesG.appendChild(path);
    edgeEls.push(path);
    nodeEdgeMap[e.source].push(path);
    nodeEdgeMap[e.target].push(path);
  });

  // ---------- nodes ----------
  var nodeEls = {};

  DATA.nodes.forEach(function (n) {
    var flagged = n.findingIds.length > 0;
    var g = el("g", { class: "node" + (flagged ? " flagged glow" : "") + (flagged ? " clickable" : "") });
    g.dataset.id = n.id;
    if (flagged) {
      g.style.setProperty("--flag-color", defectColorVar(n.defectTypes[0]));
    }

    var rect = el("rect", {
      class: "box",
      x: n.x, y: n.y, width: n.w, height: n.h, rx: 8, ry: 8
    });
    g.appendChild(rect);

    var titleEl = el("title");
    titleEl.textContent = n.fullLabel + (flagged ? " — " + n.findingIds.length + " finding(s)" : "");
    g.appendChild(titleEl);

    var fo = el("foreignObject", { x: n.x, y: n.y, width: n.w, height: n.h });
    var div = document.createElement("div");
    div.setAttribute("xmlns", XHTMLNS);
    div.className = "node-html";
    var inner = "";
    if (n.captionLabel) inner += '<div class="cap">' + escapeHtml(n.captionLabel) + '</div>';
    inner += '<div class="lbl">' + escapeHtml(n.label) + '</div>';
    div.innerHTML = inner;
    fo.appendChild(div);
    g.appendChild(fo);

    if (flagged) {
      var badgeX = n.x + n.w - 9;
      var badgeY = n.y - 2;
      n.defectTypes.slice(0, 3).forEach(function (dt, i) {
        var cx = badgeX - i * 13;
        var meta = DATA.defectLegend.filter(function (d) { return d.key === dt; })[0];
        var circ = el("circle", { cx: cx, cy: badgeY, r: 7.5, fill: defectColorVar(dt), stroke: "var(--surface-1)", "stroke-width": 1.5 });
        var code = el("text", { x: cx, y: badgeY + 2.6, "text-anchor": "middle", class: "badge-dot" });
        code.textContent = meta ? meta.code : "?";
        g.appendChild(circ);
        g.appendChild(code);
      });
    }

    nodesG.appendChild(g);
    nodeEls[n.id] = g;

    // hover: highlight immediate edges + neighbor nodes
    g.addEventListener("mouseenter", function () { setHover(n.id); });
    g.addEventListener("mouseleave", function () { setHover(null); });

    if (flagged) {
      g.addEventListener("click", function (evt) {
        evt.stopPropagation();
        showNodeDetail(n.id);
      });
    }
  });

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  // ---------- hover: immediate edges ----------
  function setHover(nodeId) {
    edgeEls.forEach(function (p) { p.classList.remove("hover-active", "dim"); });
    Object.keys(nodeEls).forEach(function (id) { nodeEls[id].classList.remove("hover-neighbor"); });
    if (!nodeId) return;
    var related = nodeEdgeMap[nodeId] || [];
    var relatedSet = new Set(related);
    edgeEls.forEach(function (p) {
      if (relatedSet.has(p)) {
        p.classList.add("hover-active");
      } else {
        p.classList.add("dim");
      }
    });
    related.forEach(function (p) {
      var otherId = p.dataset.source === nodeId ? p.dataset.target : p.dataset.source;
      if (nodeEls[otherId]) nodeEls[otherId].classList.add("hover-neighbor");
    });
  }

  // ---------- selection (dim all but selected node set) ----------
  var currentSelection = null; // Set of node ids, or null

  function applySelection(nodeIdSet) {
    currentSelection = nodeIdSet;
    Object.keys(nodeEls).forEach(function (id) {
      var g = nodeEls[id];
      g.classList.remove("dim", "selected");
      if (nodeIdSet) {
        if (nodeIdSet.has(id)) {
          g.classList.add("selected");
        } else {
          g.classList.add("dim");
        }
      }
    });
  }

  function clearSelection() {
    applySelection(null);
    setActiveFindingItem(null);
    var panel = document.getElementById("detail-panel");
    panel.innerHTML = '<div class="detail-empty">Click a flagged node (colored outline) or a finding in the sidebar to see details here. Hover any node to trace its immediate upstream and downstream edges.</div>';
  }
  document.getElementById("clear-selection").addEventListener("click", clearSelection);
  svg.addEventListener("click", function (evt) {
    if (evt.target === svg) clearSelection();
  });

  // ---------- detail panel rendering ----------
  function detectorBadge(detector) {
    var label = detector === "semantic_llm" ? "semantic_llm" : "deterministic";
    return '<span class="badge-detector ' + detector + '">' + label + '</span>';
  }

  function typeBadge(defectType) {
    var meta = DATA.defectLegend.filter(function (d) { return d.key === defectType; })[0];
    var name = meta ? meta.name : defectType;
    var code = meta ? meta.code : "?";
    return '<span class="badge-type" style="background:' + defectColorVar(defectType) + '">' +
      '<span class="code">' + code + '</span>' + escapeHtml(name) + '</span>';
  }

  function renderFindingBlock(f) {
    var html = '<div class="detail-finding">';
    html += '<div class="dhead">' + typeBadge(f.defectType) + detectorBadge(f.detector) + '</div>';
    html += '<p class="summary">' + escapeHtml(f.summary) + '</p>';
    html += '<div class="kv-row"><span class="k">Confidence</span><span class="v">' + f.confidence.toFixed(2) + '</span></div>';
    html += '<div class="kv-row"><span class="k">Finding ID</span><span class="v" style="font-weight:500;font-size:10px;word-break:break-all;text-align:right;">' + escapeHtml(f.id) + '</span></div>';
    html += '<div class="evidence-title">Evidence (' + f.evidence.length + ')</div>';
    f.evidence.forEach(function (ev) {
      html += '<div class="evidence-item">';
      html += '<div class="uri">' + escapeHtml(ev.uri) + '</div>';
      html += '<div class="locator">' + escapeHtml(ev.source) + ' &middot; ' + escapeHtml(ev.locator) + '</div>';
      if (ev.excerpt) {
        html += '<pre>' + escapeHtml(ev.excerpt) + '</pre>';
      }
      html += '</div>';
    });
    html += '</div>';
    return html;
  }

  function showNodeDetail(nodeId) {
    var n = nodeById[nodeId];
    if (!n) return;
    applySelection(new Set([nodeId]));
    setActiveFindingItem(null);
    var panel = document.getElementById("detail-panel");
    var html = '<div class="detail-body">';
    html += '<h3>' + escapeHtml(n.fullLabel) + '</h3>';
    html += '<p class="detail-node-id">' + escapeHtml(n.id) + '</p>';
    n.findingIds.forEach(function (fid) {
      var f = findingById[fid];
      if (f) html += renderFindingBlock(f);
    });
    html += '</div>';
    panel.innerHTML = html;
  }

  function showFindingDetail(findingId) {
    var f = findingById[findingId];
    if (!f) return;
    var ids = findingNodeIds[findingId] || [];
    applySelection(new Set(ids));
    setActiveFindingItem(findingId);
    var panel = document.getElementById("detail-panel");
    var html = '<div class="detail-body">';
    html += '<h3>' + escapeHtml(f.defectType.replace(/_/g, " ")) + '</h3>';
    html += '<p class="detail-node-id">' + ids.length + ' node(s) affected</p>';
    html += renderFindingBlock(f);
    html += '</div>';
    panel.innerHTML = html;
    if (ids.length && nodeEls[ids[0]]) {
      nodeEls[ids[0]].scrollIntoView({ block: "center", inline: "center", behavior: "smooth" });
    }
  }

  // ---------- sidebar findings list ----------
  var findingListEl = document.getElementById("finding-list");
  document.getElementById("finding-count-label").textContent = DATA.findings.length;
  var findingItemEls = {};

  DATA.findings.forEach(function (f) {
    var li = document.createElement("li");
    li.className = "finding-item";
    li.dataset.id = f.id;
    li.style.setProperty("--fi-color", defectColorVar(f.defectType));
    var meta = DATA.defectLegend.filter(function (d) { return d.key === f.defectType; })[0];
    var head = '<div class="fi-head">' + typeBadge(f.defectType) + '</div>';
    var summary = '<p class="fi-summary">' + escapeHtml(f.summary) + '</p>';
    var metaRow = '<div class="fi-meta">' + detectorBadge(f.detector) +
      '<span class="badge-conf">conf ' + f.confidence.toFixed(2) + '</span></div>';
    li.innerHTML = head + summary + metaRow;
    li.addEventListener("click", function () { showFindingDetail(f.id); });
    findingListEl.appendChild(li);
    findingItemEls[f.id] = li;
  });

  function setActiveFindingItem(findingId) {
    Object.keys(findingItemEls).forEach(function (id) {
      findingItemEls[id].classList.toggle("active", id === findingId);
    });
  }
})();
</script>
</body>
</html>
"""


def render(export: dict[str, Any]) -> str:
    payload = _build_payload(export)
    data_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    # Neutralize any "</" sequence (e.g. inside a SQL excerpt or URI) so it can
    # never prematurely close the surrounding <script> element.
    data_json = data_json.replace("</", "<\\/")
    return HTML_TEMPLATE.replace("__LINEAGE_DATA_JSON__", data_json)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=Path("tools/graph_export.json"),
        help="Path to the graph_export.json produced by tools/export_graph.py",
    )
    parser.add_argument(
        "--output", type=Path, default=Path("lineage.html"),
        help="Path to write the standalone lineage HTML page to",
    )
    args = parser.parse_args()

    export = json.loads(args.input.read_text())
    html = render(export)
    args.output.write_text(html)
    print(
        f"[render_lineage] wrote {args.output} "
        f"nodes={export['stats']['node_count']} "
        f"edges={export['stats']['edge_count']} "
        f"findings={export['stats']['finding_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
