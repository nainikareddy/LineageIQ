"""Command-line surface for the LineageIQ scaffold."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path


def _not_implemented(command: str, *, can_use_llm: bool = False) -> int:
    print(
        f"[lineageiq] command={command} stage=scaffold progress=0/0 "
        "status=not_implemented",
        file=sys.stderr,
    )
    if can_use_llm:
        print(
            "[lineageiq] cost llm_requests=0 input_tokens=0 output_tokens=0 "
            "estimated_cost_usd=0.000000",
            file=sys.stderr,
        )
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lineageiq",
        description="Audit BI sprawl against evidenced column-level lineage.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate_parser = subparsers.add_parser(
        "generate", help="Generate synthetic inputs and planted defects."
    )
    generate_parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "synthetic",
        help="Synthetic artifact directory.",
    )
    build_parser = subparsers.add_parser(
        "build", help="Build and validate the model-level lineage graph."
    )
    build_parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="LineageIQ repository root.",
    )
    audit_parser = subparsers.add_parser(
        "audit", help="Run structural and semantic audits."
    )
    audit_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    audit_parser.add_argument(
        "--semantic-backend",
        choices=("auto", "openai", "reference"),
        default="auto",
    )
    audit_parser.add_argument("--semantic-model", default="gpt-4.1-mini")
    audit_parser.add_argument("--output", type=Path, default=None)

    ask_parser = subparsers.add_parser("ask", help="Ask a read-only lineage question.")
    ask_parser.add_argument("question", help="Question to answer from lineage evidence.")

    eval_parser = subparsers.add_parser(
        "eval", help="Score findings against the defect manifest."
    )
    eval_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    eval_parser.add_argument("--audit-file", type=Path, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "generate":
        from synthetic.generate import generate

        generate(args.output_dir)
        return 0
    if args.command == "build":
        from lineageiq.graph import (
            build_model_dag,
            edge_counts_by_layer,
            find_orphan_models,
            graph_fingerprint,
            node_counts_by_layer,
        )
        from lineageiq.parse import (
            build_column_lineage,
            find_broken_column_refs,
            find_unused_column_propagation,
            lineage_fingerprint,
            load_dashboards,
            load_dbt_project,
            load_query_logs,
        )

        repo_root = args.repo_root
        synthetic = repo_root / "synthetic"

        def progress(message: str) -> None:
            print(f"[lineageiq] build {message}")

        progress("stage=1/5 parser=dbt status=running")
        dbt = load_dbt_project(synthetic / "dbt_project", progress=progress)
        progress("stage=2/5 parser=dashboards status=running")
        dashboards = load_dashboards(synthetic / "dashboards.json", progress=progress)
        progress("stage=3/5 parser=query_logs status=running")
        as_of = datetime.fromisoformat(
            dashboards.generated_as_of.replace("Z", "+00:00")
        )
        logs = load_query_logs(
            synthetic / "query_logs" / "query_logs.parquet",
            as_of=as_of,
            progress=progress,
        )
        progress("stage=4/5 graph=model_dag status=running")
        graph = build_model_dag(dbt, dashboards)
        progress("stage=5/5 graph=column_lineage status=running")
        column_lineage = build_column_lineage(dbt, dashboards, progress=progress)
        findings = (
            *find_orphan_models(graph),
            *find_broken_column_refs(column_lineage),
            *find_unused_column_propagation(column_lineage),
        )
        result = {
            "nodes": graph.number_of_nodes(),
            "edges": graph.number_of_edges(),
            "node_counts_by_layer": dict(node_counts_by_layer(graph)),
            "edge_counts_by_layer": dict(edge_counts_by_layer(graph)),
            "usage_summaries": len(logs.usage),
            "graph_fingerprint": graph_fingerprint(graph),
            "column_lineage": {
                "columns": len(column_lineage.columns),
                "edges": len(column_lineage.edges),
                "tile_columns": len(column_lineage.tile_column_ids),
                "fully_traced_tile_columns": len(
                    column_lineage.fully_traced_tile_column_ids
                ),
                "coverage_pct": column_lineage.coverage_pct,
                "fingerprint": lineage_fingerprint(column_lineage),
                "unresolved_edges": [
                    edge.model_dump(mode="json")
                    for edge in column_lineage.unresolved_edges
                ],
            },
            "findings": [
                finding.model_dump(mode="json") for finding in findings
            ],
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        print(
            "[lineageiq] cost llm_requests=0 input_tokens=0 output_tokens=0 "
            "estimated_cost_usd=0.000000"
        )
        return 0
    if args.command == "audit":
        from lineageiq.agents.auditor import run_audit, write_findings

        def progress(message: str) -> None:
            print(f"[lineageiq] audit {message}", file=sys.stderr, flush=True)

        output = args.output or args.repo_root / ".lineageiq" / "audit.json"
        result = run_audit(
            args.repo_root,
            semantic_backend=args.semantic_backend,
            semantic_model=args.semantic_model,
            progress=progress,
        )
        write_findings(output, result)
        print(json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True))
        llm_requests = (
            result.judge.usage.requests if result.judge.backend == "openai" else 0
        )
        print(
            "[lineageiq] cost "
            f"llm_requests={llm_requests} "
            f"judge_requests={result.judge.usage.requests} "
            f"cache_hits={result.judge.usage.cache_hits} "
            f"input_tokens={result.judge.usage.input_tokens} "
            f"output_tokens={result.judge.usage.output_tokens} "
            f"estimated_cost_usd={result.judge.usage.estimated_cost_usd:.6f}",
            file=sys.stderr,
        )
        print(f"[lineageiq] audit output={output}", file=sys.stderr)
        return 0
    if args.command == "eval":
        from lineageiq.evals.scoring import load_audit, score_findings

        audit_file = args.audit_file or args.repo_root / ".lineageiq" / "audit.json"
        print("[lineageiq] eval stage=1/2 load=audit,manifest", file=sys.stderr)
        findings, backend = load_audit(audit_file)
        print(
            f"[lineageiq] eval stage=2/2 findings={len(findings)} status=scoring",
            file=sys.stderr,
        )
        scorecard = score_findings(
            findings,
            args.repo_root / "synthetic" / "manifest.yaml",
            semantic_backend=backend,
        )
        print(json.dumps(scorecard.model_dump(mode="json"), indent=2, sort_keys=True))
        print(
            "[lineageiq] cost llm_requests=0 input_tokens=0 output_tokens=0 "
            "estimated_cost_usd=0.000000",
            file=sys.stderr,
        )
        return 0
    return _not_implemented(
        args.command,
        can_use_llm=args.command in {"audit", "ask"},
    )


if __name__ == "__main__":
    raise SystemExit(main())
