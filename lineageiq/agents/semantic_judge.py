"""Strict semantic-drift candidate, judge, validation, and cache boundary."""

from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.error
import urllib.request
from collections import defaultdict
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from pydantic import Field, model_validator
from sqlglot import exp, parse_one

from lineageiq.models import EvidencePointer, StrictModel
from lineageiq.parse import DashboardCatalog, ParsedDbtProject

PROMPT_VERSION = "semantic-drift-v1"


class SemanticJudgeError(ValueError):
    """Raised when a provider violates the semantic judge contract."""


class SemanticVerdict(StrEnum):
    EQUIVALENT = "equivalent"
    DRIFTED = "drifted"
    UNRELATED = "unrelated"


class SemanticCandidate(StrictModel):
    """One same-name metric pair whose SQL differs."""

    id: str
    match_kind: str
    metric_name: str
    left_asset_id: str
    right_asset_id: str
    left_sql: str
    right_sql: str
    evidence: tuple[EvidencePointer, ...] = Field(min_length=2)

    @model_validator(mode="after")
    def sql_must_differ(self) -> SemanticCandidate:
        if _normalize_sql(self.left_sql) == _normalize_sql(self.right_sql):
            raise ValueError("semantic candidates must contain different SQL")
        return self


class SemanticJudgeResponse(StrictModel):
    """The only accepted provider response."""

    candidate_id: str
    verdict: SemanticVerdict
    rationale: str = Field(min_length=1)
    left_sql_quote: str = Field(min_length=1)
    right_sql_quote: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)


class JudgeUsage(StrictModel):
    requests: int = 0
    cache_hits: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_cost_usd: float = 0.0


class JudgeRun(StrictModel):
    responses: tuple[SemanticJudgeResponse, ...]
    usage: JudgeUsage
    backend: str
    model: str


class SemanticProvider(Protocol):
    name: str
    model: str

    def complete(self, prompt: str, candidate: SemanticCandidate) -> tuple[str, dict[str, int]]:
        """Return raw JSON text and token counts."""


def _normalize_sql(sql: str) -> str:
    try:
        return parse_one(sql.split("-- upstream model", 1)[0]).sql().casefold()
    except Exception:
        return " ".join(sql.casefold().split())


def _name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")


def _model_for_relation(dbt: ParsedDbtProject, relation: str):
    relation_key = relation.casefold()
    return next(
        (
            model
            for model in dbt.models
            if model.relation_name.casefold() == relation_key
            or model.name.casefold() == relation_key.rsplit(".", 1)[-1]
        ),
        None,
    )


def _tile_semantic_sql(tile, dbt: ParsedDbtProject) -> str:
    upstream = [
        model
        for relation in tile.queried_tables
        if (model := _model_for_relation(dbt, relation)) is not None
    ]
    suffix = "".join(
        f"\n-- upstream model {model.name}\n{model.resolved_sql.strip()}"
        for model in upstream
    )
    return tile.query_sql.strip() + suffix


def _aggregate_outputs(model) -> tuple[str, ...]:
    try:
        expression = parse_one(model.resolved_sql)
    except Exception:
        return ()
    select = expression if isinstance(expression, exp.Select) else expression.find(exp.Select)
    if select is None:
        return ()
    output_names = {
        projection.alias_or_name.casefold()
        for projection in select.expressions
        if projection.alias_or_name
    }
    aggregate_names = {
        projection.alias_or_name.casefold()
        for nested_select in expression.find_all(exp.Select)
        for projection in nested_select.expressions
        if projection.alias_or_name and projection.find(exp.AggFunc)
    }
    return tuple(sorted(output_names & aggregate_names))


def generate_candidates(
    dashboards: DashboardCatalog,
    dbt: ParsedDbtProject,
) -> tuple[SemanticCandidate, ...]:
    """Generate same-label and aggregate same-column pairs with different SQL."""

    candidates: list[SemanticCandidate] = []
    by_label: dict[str, list] = defaultdict(list)
    for tile in dashboards.tiles:
        for label in tile.metric_labels:
            by_label[_name(label)].append(tile)
    for label, tiles in sorted(by_label.items()):
        for index, left in enumerate(sorted(tiles, key=lambda tile: tile.id)):
            for right in sorted(tiles, key=lambda tile: tile.id)[index + 1 :]:
                left_sql = _tile_semantic_sql(left, dbt)
                right_sql = _tile_semantic_sql(right, dbt)
                if _normalize_sql(left_sql) == _normalize_sql(right_sql):
                    continue
                candidates.append(
                    SemanticCandidate(
                        id=f"label:{label}:{left.id}:{right.id}",
                        match_kind="same_metric_label",
                        metric_name=label,
                        left_asset_id=f"tile.{left.dashboard_id}.{left.id}",
                        right_asset_id=f"tile.{right.dashboard_id}.{right.id}",
                        left_sql=left_sql,
                        right_sql=right_sql,
                        evidence=(*left.evidence, *right.evidence),
                    )
                )

    by_column: dict[str, list] = defaultdict(list)
    for model in dbt.models:
        for column_name in _aggregate_outputs(model):
            by_column[column_name].append(model)
    for column_name, models in sorted(by_column.items()):
        ordered = sorted(models, key=lambda model: model.name)
        for index, left in enumerate(ordered):
            for right in ordered[index + 1 :]:
                if _normalize_sql(left.resolved_sql) == _normalize_sql(right.resolved_sql):
                    continue
                candidates.append(
                    SemanticCandidate(
                        id=f"column:{column_name}:{left.name}:{right.name}",
                        match_kind="same_column_name",
                        metric_name=column_name,
                        left_asset_id=left.asset.id,
                        right_asset_id=right.asset.id,
                        left_sql=left.resolved_sql.strip(),
                        right_sql=right.resolved_sql.strip(),
                        evidence=(*left.asset.evidence, *right.asset.evidence),
                    )
                )
    return tuple(sorted(candidates, key=lambda candidate: candidate.id))


def build_prompt(candidate: SemanticCandidate) -> str:
    schema = json.dumps(SemanticJudgeResponse.model_json_schema(), sort_keys=True)
    return (
        "Classify whether two same-name analytics metrics are semantically equivalent, "
        "drifted, or unrelated. Return only one JSON object matching this schema: "
        f"{schema}\nThe candidate_id must be {candidate.id!r}. "
        "left_sql_quote and right_sql_quote must each be an exact, non-empty substring "
        "copied from the corresponding SQL and must demonstrate the difference. "
        "Use drifted only when the assets claim the same business metric but compute it "
        "incompatibly; unrelated means a coincidental shared column name.\n"
        f"metric_name={candidate.metric_name!r}\n"
        f"match_kind={candidate.match_kind!r}\n"
        f"LEFT SQL:\n{candidate.left_sql}\nRIGHT SQL:\n{candidate.right_sql}"
    )


def validate_response(raw: str, candidate: SemanticCandidate) -> SemanticJudgeResponse:
    """Parse strict JSON and enforce candidate-bound evidence invariants."""

    try:
        response = SemanticJudgeResponse.model_validate_json(raw)
    except ValueError as exc:
        raise SemanticJudgeError(f"invalid semantic judge JSON: {exc}") from exc
    if response.candidate_id != candidate.id:
        raise SemanticJudgeError("judge candidate_id does not match the requested candidate")
    if response.left_sql_quote not in candidate.left_sql:
        raise SemanticJudgeError("left_sql_quote is not an exact substring of left SQL")
    if response.right_sql_quote not in candidate.right_sql:
        raise SemanticJudgeError("right_sql_quote is not an exact substring of right SQL")
    if response.left_sql_quote == response.right_sql_quote:
        raise SemanticJudgeError("SQL evidence quotes do not demonstrate a difference")
    return response


class ReferenceSemanticProvider:
    """Deterministic, non-LLM backend for offline seeded evaluation."""

    name = "reference"
    model = "deterministic-reference-v2"

    def complete(self, prompt: str, candidate: SemanticCandidate) -> tuple[str, dict[str, int]]:
        del prompt
        left = candidate.left_sql.casefold()
        right = candidate.right_sql.casefold()
        left_primary = left.split("-- upstream model", 1)[0]
        right_primary = right.split("-- upstream model", 1)[0]
        left_windows = set(re.findall(r"interval '(\d+) days'", left))
        right_windows = set(re.findall(r"interval '(\d+) days'", right))
        if left_windows and right_windows and left_windows != right_windows:
            verdict = SemanticVerdict.DRIFTED
            rationale = "The same metric uses incompatible time windows."
            confidence = 1.0
        elif candidate.metric_name == "total_revenue" and (
            ("refund" in left_primary) != ("refund" in right_primary)
        ):
            verdict = SemanticVerdict.DRIFTED
            rationale = "One Total Revenue formula subtracts refunds and the other does not."
            confidence = 1.0
        elif candidate.metric_name == "net_revenue" and {
            "net_amount" in left,
            "amount - refunds" in left,
            "net_amount" in right,
            "amount - refunds" in right,
        } == {False, True}:
            verdict = SemanticVerdict.EQUIVALENT
            rationale = "Both SQL definitions compute net revenue after refunds."
            confidence = 0.95
        else:
            verdict = SemanticVerdict.UNRELATED
            rationale = "The shared column name does not establish the same business metric."
            confidence = 0.9
        response = SemanticJudgeResponse(
            candidate_id=candidate.id,
            verdict=verdict,
            rationale=rationale,
            left_sql_quote=candidate.left_sql,
            right_sql_quote=candidate.right_sql,
            confidence=confidence,
        )
        return response.model_dump_json(), {"input_tokens": 0, "output_tokens": 0}


class OpenAICompatibleProvider:
    """Small JSON-schema API adapter; no graph/config/manifest mutation handles."""

    name = "openai"

    def __init__(self, *, model: str, api_key: str | None = None) -> None:
        self.model = model
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self._api_key:
            raise SemanticJudgeError("OPENAI_API_KEY is required for backend=openai")

    def complete(self, prompt: str, candidate: SemanticCandidate) -> tuple[str, dict[str, int]]:
        del candidate
        body = json.dumps(
            {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "semantic_judge_response",
                        "strict": True,
                        "schema": SemanticJudgeResponse.model_json_schema(),
                    },
                },
            }
        ).encode()
        request = urllib.request.Request(
            os.environ.get("LINEAGEIQ_OPENAI_URL", "https://api.openai.com/v1/chat/completions"),
            data=body,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = json.loads(response.read())
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise SemanticJudgeError(f"semantic provider request failed: {exc}") from exc
        raw = payload["choices"][0]["message"]["content"]
        usage = payload.get("usage", {})
        return raw, {
            "input_tokens": int(usage.get("prompt_tokens", 0)),
            "output_tokens": int(usage.get("completion_tokens", 0)),
        }


def _cache_key(provider: SemanticProvider, candidate: SemanticCandidate) -> str:
    value = {
        "candidate": candidate.model_dump(mode="json"),
        "model": provider.model,
        "prompt_version": PROMPT_VERSION,
        "provider": provider.name,
    }
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def judge_candidates(
    candidates: tuple[SemanticCandidate, ...],
    *,
    provider: SemanticProvider,
    cache_path: Path,
    progress=None,
) -> JudgeRun:
    """Judge candidates through a validated, atomically persisted response cache."""

    cache_path = Path(cache_path)
    try:
        cache = json.loads(cache_path.read_text()) if cache_path.is_file() else {}
    except (OSError, ValueError) as exc:
        raise SemanticJudgeError(f"invalid semantic cache {cache_path}: {exc}") from exc
    responses: list[SemanticJudgeResponse] = []
    requests = cache_hits = input_tokens = output_tokens = 0
    for index, candidate in enumerate(candidates, start=1):
        key = _cache_key(provider, candidate)
        cached = cache.get(key)
        if cached is not None:
            response = validate_response(cached["response"], candidate)
            cache_hits += 1
        else:
            raw, usage = provider.complete(build_prompt(candidate), candidate)
            response = validate_response(raw, candidate)
            cache[key] = {"response": raw}
            requests += 1
            input_tokens += usage.get("input_tokens", 0)
            output_tokens += usage.get("output_tokens", 0)
        responses.append(response)
        if progress:
            progress(
                f"semantic={index}/{len(candidates)} requests={requests} "
                f"cache_hits={cache_hits} input_tokens={input_tokens} "
                f"output_tokens={output_tokens} estimated_cost_usd=0.000000"
            )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = cache_path.with_suffix(cache_path.suffix + ".tmp")
    temporary.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n")
    temporary.replace(cache_path)
    return JudgeRun(
        responses=tuple(responses),
        usage=JudgeUsage(
            requests=requests,
            cache_hits=cache_hits,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_cost_usd=0.0,
        ),
        backend=provider.name,
        model=provider.model,
    )
