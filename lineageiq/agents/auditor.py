"""End-to-end deterministic and schema-validated semantic audit orchestration."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from lineageiq.agents.semantic_judge import (
    JudgeRun,
    OpenAICompatibleProvider,
    ReferenceSemanticProvider,
    SemanticVerdict,
    generate_candidates,
    judge_candidates,
)
from lineageiq.graph import build_model_dag, find_orphan_models
from lineageiq.models import (
    DefectType,
    DetectorKind,
    EvidencePointer,
    EvidenceSource,
    Finding,
    StrictModel,
)
from lineageiq.parse import (
    build_column_lineage,
    find_broken_column_refs,
    find_unused_column_propagation,
    load_dashboards,
    load_dbt_project,
    load_query_logs,
)


class AuditResult(StrictModel):
    findings: tuple[Finding, ...]
    semantic_candidates: int
    judge: JudgeRun


def _canonical_sql(sql: str) -> str:
    return " ".join(sql.casefold().split())


def find_stale_dashboards(dashboards, logs, *, stale_days: int = 120) -> tuple[Finding, ...]:
    findings = []
    for dashboard in dashboards.dashboards:
        entries = logs.for_dashboard(dashboard.id)
        if not entries:
            continue
        last = max(entry.timestamp for entry in entries)
        if last > logs.as_of - timedelta(days=stale_days):
            continue
        locator = (
            f"dashboard_id={dashboard.id}/max(timestamp)="
            f"{last.isoformat().replace('+00:00', 'Z')}"
        )
        evidence = EvidencePointer(
            source=EvidenceSource.QUERY_LOG,
            uri=logs.source_path,
            locator=locator,
            excerpt=f"last_queried_at={last.isoformat()}",
        )
        findings.append(
            Finding(
                id=f"stale_asset:{dashboard.id}",
                defect_type=DefectType.STALE_ASSET,
                summary=(
                    f"Dashboard {dashboard.id} has been inactive for at least "
                    f"{stale_days} days."
                ),
                asset_ids=(f"dashboard.{dashboard.id}",),
                evidence=(evidence, *dashboard.evidence),
                confidence=1.0,
                detector=DetectorKind.DETERMINISTIC,
            )
        )
    return tuple(findings)


def _tile_signature(tile) -> tuple[str, str]:
    return (tile.metric_labels[0].casefold(), _canonical_sql(tile.query_sql))


def find_duplicate_dashboards(
    dashboards, *, minimum_shared_ratio: float = 0.8
) -> tuple[Finding, ...]:
    findings = []
    ordered = sorted(dashboards.dashboards, key=lambda dashboard: dashboard.id)
    for index, left in enumerate(ordered):
        left_tiles = {_tile_signature(tile) for tile in left.tiles}
        for right in ordered[index + 1 :]:
            right_tiles = {_tile_signature(tile) for tile in right.tiles}
            denominator = max(len(left_tiles), len(right_tiles))
            ratio = len(left_tiles & right_tiles) / denominator if denominator else 0.0
            if ratio < minimum_shared_ratio:
                continue
            findings.append(
                Finding(
                    id=f"duplicate_dashboard:{left.id}:{right.id}",
                    defect_type=DefectType.DUPLICATE_DASHBOARD,
                    summary=(
                        f"Dashboards {left.id} and {right.id} share "
                        f"{ratio:.0%} of tile definitions."
                    ),
                    asset_ids=(f"dashboard.{left.id}", f"dashboard.{right.id}"),
                    evidence=(*left.evidence, *right.evidence),
                    confidence=ratio,
                    detector=DetectorKind.DETERMINISTIC,
                )
            )
    return tuple(findings)


def _semantic_findings(candidates, run: JudgeRun) -> tuple[Finding, ...]:
    by_metric: dict[str, list] = defaultdict(list)
    by_id = {candidate.id: candidate for candidate in candidates}
    for response in run.responses:
        if response.verdict is SemanticVerdict.DRIFTED:
            by_metric[by_id[response.candidate_id].metric_name].append(response)
    findings = []
    for metric, responses in sorted(by_metric.items()):
        candidates_for_metric = [by_id[response.candidate_id] for response in responses]
        asset_ids = tuple(
            sorted(
                {
                    asset_id
                    for candidate in candidates_for_metric
                    for asset_id in (candidate.left_asset_id, candidate.right_asset_id)
                }
            )
        )
        evidence = []
        for candidate, response in zip(candidates_for_metric, responses, strict=True):
            evidence.extend(candidate.evidence)
            evidence.extend(
                (
                    EvidencePointer(
                        source=EvidenceSource.DBT_SQL
                        if candidate.match_kind == "same_column_name"
                        else EvidenceSource.DASHBOARD_METADATA,
                        uri=f"semantic://{candidate.id}/left",
                        locator=f"candidate={candidate.id}/side=left",
                        excerpt=response.left_sql_quote,
                    ),
                    EvidencePointer(
                        source=EvidenceSource.DBT_SQL
                        if candidate.match_kind == "same_column_name"
                        else EvidenceSource.DASHBOARD_METADATA,
                        uri=f"semantic://{candidate.id}/right",
                        locator=f"candidate={candidate.id}/side=right",
                        excerpt=response.right_sql_quote,
                    ),
                )
            )
        unique = {
            (item.source, item.uri, item.locator, item.excerpt): item for item in evidence
        }
        digest = hashlib.sha256(
            "|".join(response.candidate_id for response in responses).encode()
        ).hexdigest()[:12]
        findings.append(
            Finding(
                id=f"metric_definition_conflict:{metric}:{digest}",
                defect_type=DefectType.METRIC_DEFINITION_CONFLICT,
                summary=f"Metric {metric!r} has incompatible SQL definitions.",
                asset_ids=asset_ids,
                evidence=tuple(unique.values()),
                confidence=min(response.confidence for response in responses),
                detector=DetectorKind.SEMANTIC_LLM,
            )
        )
    return tuple(findings)


def run_audit(
    repo_root: Path,
    *,
    semantic_backend: str = "auto",
    semantic_model: str = "gpt-4.1-mini",
    cache_path: Path | None = None,
    progress=None,
) -> AuditResult:
    repo_root = Path(repo_root)
    synthetic = repo_root / "synthetic"
    emit = progress or (lambda message: None)
    emit("stage=1/6 load=dbt,dashboards,logs status=running")
    dbt = load_dbt_project(synthetic / "dbt_project", progress=emit)
    dashboards = load_dashboards(synthetic / "dashboards.json", progress=emit)
    from datetime import datetime

    as_of = datetime.fromisoformat(dashboards.generated_as_of.replace("Z", "+00:00"))
    logs = load_query_logs(
        synthetic / "query_logs" / "query_logs.parquet", as_of=as_of, progress=emit
    )
    emit("stage=2/6 graph=model status=running")
    graph = build_model_dag(dbt, dashboards)
    emit("stage=3/6 graph=columns status=running")
    column_lineage = build_column_lineage(dbt, dashboards, progress=emit)
    emit("stage=4/6 audit=structural status=running")
    structural = (
        *find_orphan_models(graph),
        *find_stale_dashboards(dashboards, logs),
        *find_duplicate_dashboards(dashboards),
        *find_broken_column_refs(column_lineage),
        *find_unused_column_propagation(column_lineage),
    )
    emit("stage=5/6 audit=semantic_candidates status=running")
    candidates = generate_candidates(dashboards, dbt)
    if semantic_backend == "auto":
        semantic_backend = (
            "openai"
            if __import__("os").environ.get("OPENAI_API_KEY")
            else "reference"
        )
    provider = (
        OpenAICompatibleProvider(model=semantic_model)
        if semantic_backend == "openai"
        else ReferenceSemanticProvider()
    )
    emit(
        f"stage=6/6 audit=semantic backend={provider.name} "
        f"candidates={len(candidates)} status=running"
    )
    judge = judge_candidates(
        candidates,
        provider=provider,
        cache_path=cache_path or repo_root / ".lineageiq" / "semantic_cache.json",
        progress=emit,
    )
    findings = tuple(
        sorted((*structural, *_semantic_findings(candidates, judge)), key=lambda item: item.id)
    )
    return AuditResult(
        findings=findings,
        semantic_candidates=len(candidates),
        judge=judge,
    )


def write_findings(path: Path, result: AuditResult) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
    )
    temporary.replace(path)
