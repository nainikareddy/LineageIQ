from __future__ import annotations

import json
from pathlib import Path

import pytest

from lineageiq.agents.semantic_judge import (
    ReferenceSemanticProvider,
    SemanticJudgeError,
    SemanticVerdict,
    generate_candidates,
    judge_candidates,
    validate_response,
)
from lineageiq.parse import load_dashboards, load_dbt_project

ROOT = Path(__file__).resolve().parents[1]


def test_real_stack_catches_d1_and_d6_and_validates_cache(tmp_path: Path) -> None:
    dbt = load_dbt_project(ROOT / "synthetic" / "dbt_project")
    dashboards = load_dashboards(ROOT / "synthetic" / "dashboards.json")
    candidates = generate_candidates(dashboards, dbt)
    by_id = {candidate.id: candidate for candidate in candidates}

    assert "label:total_revenue:rev_exec_total:rev_fin_total" in by_id
    assert "column:active_users:mart_active_users_30d:mart_active_users_7d" in by_id
    assert not any("exec_order_count:fin_order_count" in item for item in by_id)

    cache = tmp_path / "semantic.json"
    first = judge_candidates(
        candidates, provider=ReferenceSemanticProvider(), cache_path=cache
    )
    second = judge_candidates(
        candidates, provider=ReferenceSemanticProvider(), cache_path=cache
    )
    drifted = {
        response.candidate_id
        for response in first.responses
        if response.verdict is SemanticVerdict.DRIFTED
    }
    assert "label:total_revenue:rev_exec_total:rev_fin_total" in drifted
    assert "label:active_users:health_active_users:product_active_users" in drifted
    assert second.usage.requests == 0
    assert second.usage.cache_hits == len(candidates)


def test_rejects_non_json_and_non_verbatim_evidence() -> None:
    dbt = load_dbt_project(ROOT / "synthetic" / "dbt_project")
    dashboards = load_dashboards(ROOT / "synthetic" / "dashboards.json")
    candidate = next(
        item
        for item in generate_candidates(dashboards, dbt)
        if "total_revenue" in item.id
    )
    with pytest.raises(SemanticJudgeError):
        validate_response("not json", candidate)
    invalid = {
        "candidate_id": candidate.id,
        "verdict": "drifted",
        "rationale": "different",
        "left_sql_quote": "invented SQL",
        "right_sql_quote": candidate.right_sql,
        "confidence": 1.0,
    }
    with pytest.raises(SemanticJudgeError):
        validate_response(json.dumps(invalid), candidate)
