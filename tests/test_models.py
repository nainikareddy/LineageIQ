from __future__ import annotations

import pytest
from pydantic import ValidationError

from lineageiq.models import (
    DefectType,
    DetectorKind,
    Edge,
    EdgeKind,
    EvidencePointer,
    EvidenceSource,
    Finding,
)


@pytest.fixture
def evidence() -> EvidencePointer:
    return EvidencePointer(
        source=EvidenceSource.DBT_SQL,
        uri="dbt://models/orders.sql",
        locator="select[0].revenue",
    )


def test_edge_requires_evidence_and_rejects_self_loops(
    evidence: EvidencePointer,
) -> None:
    with pytest.raises(ValidationError):
        Edge(
            source_id="column.orders.amount",
            target_id="column.orders.amount",
            kind=EdgeKind.COLUMN_LINEAGE,
            evidence=(evidence,),
        )

    with pytest.raises(ValidationError):
        Edge(
            source_id="column.orders.amount",
            target_id="column.marts.revenue",
            kind=EdgeKind.COLUMN_LINEAGE,
            evidence=(),
        )


def test_finding_enforces_evidence_confidence_and_llm_scope(
    evidence: EvidencePointer,
) -> None:
    with pytest.raises(ValidationError):
        Finding(
            id="finding-1",
            defect_type=DefectType.STALE_ASSET,
            summary="This semantic detector is outside its allowed scope.",
            asset_ids=("tile.old",),
            evidence=(evidence,),
            confidence=0.8,
            detector=DetectorKind.SEMANTIC_LLM,
        )

    with pytest.raises(ValidationError):
        Finding(
            id="finding-2",
            defect_type=DefectType.ORPHANED_MODEL,
            summary="Confidence is invalid.",
            asset_ids=("model.orphan",),
            evidence=(evidence,),
            confidence=1.1,
            detector=DetectorKind.DETERMINISTIC,
        )


def test_unknown_fields_are_rejected(evidence: EvidencePointer) -> None:
    with pytest.raises(ValidationError):
        Finding.model_validate(
            {
                "id": "finding-3",
                "defect_type": "metric_definition_conflict",
                "summary": "Revenue differs.",
                "asset_ids": ("tile.a", "tile.b"),
                "evidence": (evidence,),
                "confidence": 0.9,
                "detector": "semantic_llm",
                "graph_write": True,
            }
        )

