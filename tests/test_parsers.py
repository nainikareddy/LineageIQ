from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from lineageiq.parse.dashboards import DashboardParseError, load_dashboards
from lineageiq.parse.dbt import DbtParseError, load_dbt_project
from lineageiq.parse.query_logs import QueryLogParseError, load_query_logs
from lineageiq.parse.verify import verify_parsers_against_manifest
from synthetic.generate import generated_hashes

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTHETIC = REPO_ROOT / "synthetic"


def test_dbt_parser_resolves_generated_refs_and_sources() -> None:
    project = load_dbt_project(SYNTHETIC / "dbt_project")

    assert len(project.models) == 30
    assert len(project.sources) == 10
    assert project.model("fct_orders").refs == ("int_order_financials",)
    assert "from analytics.int_order_financials" in project.model(
        "fct_orders"
    ).resolved_sql
    assert project.model("stg_orders").sources == ("source.raw.orders",)
    assert "from warehouse.raw.orders" in project.model("stg_orders").resolved_sql
    assert "email_hash" not in project.source("raw", "users").columns
    assert all("{{" not in model.resolved_sql for model in project.models)


def test_dbt_parser_fails_closed_on_unknown_jinja(tmp_path: Path) -> None:
    project = tmp_path / "dbt"
    shutil.copytree(SYNTHETIC / "dbt_project", project)
    real_model = project / "models" / "staging" / "stg_orders.sql"
    real_model.write_text(
        real_model.read_text(encoding="utf-8")
        + "\nwhere region = {{ var('untrusted_region') }}\n",
        encoding="utf-8",
    )

    with pytest.raises(DbtParseError, match="unsupported Jinja"):
        load_dbt_project(project)


def test_dashboard_loader_builds_evidenced_tiles_and_rejects_extra_fields(
    tmp_path: Path,
) -> None:
    catalog = load_dashboards(SYNTHETIC / "dashboards.json")

    assert len(catalog.dashboards) == 12
    assert len(catalog.tiles) == 33
    tile = catalog.tile("revenue_executive", "rev_exec_total")
    assert tile.metric_labels == ("Total Revenue",)
    assert tile.queried_tables == ("analytics.fct_orders",)
    assert tile.evidence[0].locator == (
        "dashboard=revenue_executive/tile=rev_exec_total"
    )

    raw = json.loads((SYNTHETIC / "dashboards.json").read_text(encoding="utf-8"))
    raw["dashboards"][0]["tiles"][0]["graph_write"] = True
    invalid = tmp_path / "dashboards.json"
    invalid.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DashboardParseError, match="extra_forbidden"):
        load_dashboards(invalid)


def test_query_log_loader_validates_schema_and_orders_utc_rows(tmp_path: Path) -> None:
    logs = load_query_logs(
        SYNTHETIC / "query_logs" / "query_logs.parquet",
        as_of=datetime(2026, 7, 25, 12, 0, tzinfo=UTC),
        batch_size=257,
    )

    assert len(logs.entries) == 1_409
    assert logs.entries == tuple(
        sorted(
            logs.entries,
            key=lambda entry: (
                entry.timestamp,
                entry.dashboard_id,
                entry.tile_id,
                entry.user,
            ),
        )
    )
    assert all(entry.timestamp.tzinfo is UTC for entry in logs.entries)
    assert max(entry.timestamp for entry in logs.for_dashboard("weekly_ops")) == datetime(
        2026, 3, 21, 12, 0, tzinfo=UTC
    )
    assert logs.usage_for("weekly_ops", "ops_orders").query_count_90d == 0
    assert (
        logs.usage_for("weekly_ops", "ops_orders").last_queried_at
        == datetime(2026, 3, 21, 12, 0, tzinfo=UTC)
    )
    assert logs.usage_for("revenue_executive", "rev_exec_total").query_count_90d == 90

    wrong_schema = tmp_path / "logs.parquet"
    source_path = str(
        SYNTHETIC / "query_logs" / "query_logs.parquet"
    ).replace("'", "''")
    target_path = str(wrong_schema).replace("'", "''")
    duckdb.sql(
        "copy (select dashboard_id, tile_id, timestamp "
        f"from read_parquet('{source_path}') limit 10) "
        f"to '{target_path}' (format parquet)"
    )
    with pytest.raises(QueryLogParseError, match="columns must be"):
        load_query_logs(wrong_schema)


def test_parsers_are_verified_against_every_manifest_case() -> None:
    before = generated_hashes(SYNTHETIC)
    result = verify_parsers_against_manifest(REPO_ROOT)
    after = generated_hashes(SYNTHETIC)

    assert before == after
    assert result.model_counts == (
        ("intermediate", 10),
        ("marts", 10),
        ("staging", 10),
    )
    assert result.dashboard_count == 12
    assert result.tile_count == 33
    assert result.query_log_count == 1_409
    assert {check.id for check in result.checks} == {
        "D1",
        "D2",
        "D3",
        "D4",
        "D5",
        "D6",
        "D7",
        "N1",
        "N2",
        "N3",
        "N4",
        "N5",
    }
    assert all(check.passed for check in result.checks)
