"""Manifest-backed evaluation of evidenced findings."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from lineageiq.models import Finding, StrictModel


class DefectScore(StrictModel):
    id: str
    defect_type: str
    hit: bool
    finding_id: str | None = None
    diagnosis: str


class EvalScorecard(StrictModel):
    per_defect: tuple[DefectScore, ...]
    hits: int
    total_positives: int
    recall: float
    false_positive_count_against_negatives: int
    flagged_negative_ids: tuple[str, ...]
    unmatched_finding_ids: tuple[str, ...]
    finding_count: int
    semantic_backend: str


def _matched_location_count(finding: Finding, item: dict) -> int:
    evidence_locators = [evidence.locator.casefold() for evidence in finding.evidence]
    evidence_uris = [evidence.uri.casefold() for evidence in finding.evidence]
    asset_text = " ".join(finding.asset_ids).casefold().replace(".", "=")
    matched = 0
    for location in item.get("locations", []):
        locator = str(location.get("locator", "")).casefold()
        path = str(location.get("path", "")).casefold()
        locator_head = locator.split("/", 1)[0]
        locator_asset = locator_head.replace("dashboard=", "dashboard.").replace(
            "model=", "model."
        )
        is_match = False
        if any(locator in actual or actual in locator for actual in evidence_locators):
            is_match = True
        elif locator_asset and locator_asset in " ".join(finding.asset_ids).casefold():
            is_match = True
        elif locator_head and locator_head in asset_text:
            is_match = True
        elif path.endswith(".sql") and any(uri.endswith(path) for uri in evidence_uris):
            is_match = True
        matched += is_match
    return matched


def _location_score(finding: Finding, item: dict) -> int:
    return _matched_location_count(finding, item)


def score_findings(
    findings: tuple[Finding, ...],
    manifest_path: Path,
    *,
    semantic_backend: str,
) -> EvalScorecard:
    manifest = yaml.safe_load(Path(manifest_path).read_text())
    positives = manifest["defects"]
    negatives = manifest["negatives"]
    available = set(range(len(findings)))
    scores = []
    for expected in positives:
        choices = [
            (_location_score(finding, expected), index, finding)
            for index, finding in enumerate(findings)
            if index in available and finding.defect_type.value == expected["type"]
        ]
        best = max(choices, default=(0, -1, None), key=lambda value: (value[0], -value[1]))
        hit = best[0] > 0
        if hit:
            available.remove(best[1])
        scores.append(
            DefectScore(
                id=expected["id"],
                defect_type=expected["type"],
                hit=hit,
                finding_id=best[2].id if hit else None,
                diagnosis=(
                    "Matched by defect type and manifest evidence location."
                    if hit
                    else "No emitted finding matched this defect's type and evidence locations."
                ),
            )
        )
    flagged_negatives = tuple(
        negative["id"]
        for negative in negatives
        if any(
            finding.defect_type.value == negative["type"]
            and _matched_location_count(finding, negative)
            == len(negative.get("locations", []))
            for finding in findings
        )
    )
    hits = sum(score.hit for score in scores)
    return EvalScorecard(
        per_defect=tuple(scores),
        hits=hits,
        total_positives=len(positives),
        recall=hits / len(positives) if positives else 1.0,
        false_positive_count_against_negatives=len(flagged_negatives),
        flagged_negative_ids=flagged_negatives,
        unmatched_finding_ids=tuple(findings[index].id for index in sorted(available)),
        finding_count=len(findings),
        semantic_backend=semantic_backend,
    )


def load_audit(path: Path) -> tuple[tuple[Finding, ...], str]:
    payload = json.loads(Path(path).read_text())
    findings = tuple(
        Finding.model_validate_json(json.dumps(item)) for item in payload["findings"]
    )
    return findings, payload["judge"]["backend"]
