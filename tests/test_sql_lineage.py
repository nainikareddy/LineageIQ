from __future__ import annotations

from pathlib import Path

import yaml

from lineageiq.models import DefectType
from lineageiq.parse import load_dashboards, load_dbt_project
from lineageiq.parse.sql_lineage import (
    UnresolvedReason,
    build_column_lineage,
    find_broken_column_refs,
    find_unused_column_propagation,
    lineage_fingerprint,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC = REPO_ROOT / "synthetic"


def _real_lineage():
    return build_column_lineage(
        load_dbt_project(SYNTHETIC / "dbt_project"),
        load_dashboards(SYNTHETIC / "dashboards.json"),
    )


def test_real_lineage_expands_stars_and_traces_ctes_joins_and_aggregations() -> None:
    result = _real_lineage()

    order_wide = [
        item
        for item in result.columns
        if item.column.parent_id == "model.int_order_wide"
    ]
    assert len(order_wide) == 13
    assert all(item.star_expanded for item in order_wide)
    assert {item.column.name for item in order_wide} == {
        "order_id",
        "user_id",
        "amount",
        "refunds",
        "ordered_at",
        "status",
        "channel",
        "region",
        "currency",
        "coupon_code",
        "shipping_amount",
        "tax_amount",
        "payment_method",
    }

    gross_revenue = (
        "column.tile.revenue_executive.rev_exec_total.total_revenue"
    )
    assert result.source_columns_for(gross_revenue) == (
        "column.source.raw.orders.amount",
    )

    net_revenue = "column.tile.revenue_finance.rev_fin_total.total_revenue"
    assert result.source_columns_for(net_revenue) == (
        "column.source.raw.orders.amount",
        "column.source.raw.orders.refunds",
        "column.source.raw.refunds.refund_amount",
    )

    campaign_roas = "column.tile.marketing_overview.mkt_roas.roas"
    assert result.source_columns_for(campaign_roas) == (
        "column.source.raw.campaigns.spend",
        "column.source.raw.orders.amount",
    )


def test_real_lineage_records_every_unresolved_edge_and_coverage() -> None:
    result = _real_lineage()

    assert len(result.columns) == 258
    assert len(result.edges) == 202
    assert len(result.tile_column_ids) == 42
    assert len(result.fully_traced_tile_column_ids) == 37
    assert result.coverage_pct == 100.0 * 37 / 42
    assert len(result.unresolved_edges) == 7
    assert all(edge.reason and edge.evidence for edge in result.unresolved_edges)

    reasons = [edge.reason_code for edge in result.unresolved_edges]
    assert reasons.count(UnresolvedReason.MISSING_UPSTREAM_COLUMN) == 1
    assert reasons.count(UnresolvedReason.ROW_LEVEL_AGGREGATION) == 6
    broken = next(
        edge
        for edge in result.unresolved_edges
        if edge.reason_code is UnresolvedReason.MISSING_UPSTREAM_COLUMN
    )
    assert broken.target_id == "column.model.stg_users.email_hash"
    assert broken.upstream_relation == "warehouse.raw.users"
    assert broken.column_name == "email_hash"


def test_d5_and_d7_findings_match_real_manifest() -> None:
    result = _real_lineage()
    manifest = yaml.safe_load(
        (SYNTHETIC / "manifest.yaml").read_text(encoding="utf-8")
    )

    broken = find_broken_column_refs(result)
    assert len(broken) == 1
    assert broken[0].defect_type is DefectType.BROKEN_LINEAGE
    assert broken[0].asset_ids == (
        "model.stg_users",
        "tile.user_privacy_audit.privacy_email_hash",
    )
    d5 = next(item for item in manifest["defects"] if item["id"] == "D5")
    assert {
        "dashboard=user_privacy_audit/tile=privacy_email_hash",
        "model=stg_users",
        "source=raw.users",
    }.issubset({evidence.locator for evidence in broken[0].evidence})
    assert all((REPO_ROOT / location["path"]).exists() for location in d5["locations"])

    unused = find_unused_column_propagation(result)
    assert len(unused) == 1
    assert unused[0].defect_type is DefectType.UNUSED_COLUMN_PROPAGATION
    assert unused[0].asset_ids == ("model.int_order_wide",)
    d7 = next(item for item in manifest["defects"] if item["id"] == "D7")
    assert all(column in unused[0].summary for column in d7["unused_columns"])
    assert "12 SELECT * columns" in unused[0].summary


def test_real_column_lineage_is_deterministic() -> None:
    first = _real_lineage()
    second = _real_lineage()

    assert lineage_fingerprint(first) == lineage_fingerprint(second)
    assert first.coverage_pct == second.coverage_pct
    assert first.edges == second.edges
    assert first.unresolved_edges == second.unresolved_edges
