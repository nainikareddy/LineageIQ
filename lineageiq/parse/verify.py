"""Verify parser outputs against the generated ground-truth manifest."""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import yaml

from lineageiq.models import StrictModel
from lineageiq.parse.dashboards import DashboardCatalog, load_dashboards
from lineageiq.parse.dbt import load_dbt_project
from lineageiq.parse.query_logs import QueryLogBatch, load_query_logs

ProgressCallback = Callable[[str], None]


class ParserGroundTruthCheck(StrictModel):
    """One manifest case confirmed to be represented in parser output."""

    id: str
    expected: str
    passed: bool
    evidence: tuple[str, ...]


class ParserVerification(StrictModel):
    """Manifest-backed parser verification result."""

    model_counts: tuple[tuple[str, int], ...]
    dashboard_count: int
    tile_count: int
    query_log_count: int
    checks: tuple[ParserGroundTruthCheck, ...]


def _tile_signatures(catalog: DashboardCatalog, dashboard_id: str) -> set[tuple[str, str]]:
    dashboard = catalog.dashboard(dashboard_id)
    return {(tile.title, tile.query_sql) for tile in dashboard.tiles}


def _max_timestamp(logs: QueryLogBatch, dashboard_id: str) -> datetime:
    entries = logs.for_dashboard(dashboard_id)
    if not entries:
        raise AssertionError(f"no query logs for dashboard {dashboard_id}")
    return max(entry.timestamp for entry in entries)


def _as_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _manifest_item(manifest: dict[str, object], check_id: str) -> dict[str, object]:
    items = [*manifest["defects"], *manifest["negatives"]]
    return next(item for item in items if item["id"] == check_id)


def _path_checks(repo_root: Path, manifest: dict[str, object]) -> dict[str, bool]:
    results: dict[str, bool] = {}
    for item in [*manifest["defects"], *manifest["negatives"]]:
        results[item["id"]] = all(
            (repo_root / location["path"]).is_file() for location in item["locations"]
        )
    return results


def verify_parsers_against_manifest(
    repo_root: Path,
    *,
    progress: ProgressCallback | None = None,
) -> ParserVerification:
    """Confirm all planted cases remain observable through parser outputs.

    This validates parser coverage and evidence preservation. It is not an audit
    implementation and does not emit Findings.
    """

    repo_root = Path(repo_root)
    synthetic_root = repo_root / "synthetic"
    manifest_path = synthetic_root / "manifest.yaml"
    try:
        manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise AssertionError(f"cannot load ground-truth manifest {manifest_path}: {exc}") from exc
    if progress:
        progress("stage=1/4 parser=dbt status=running")
    dbt = load_dbt_project(synthetic_root / "dbt_project", progress=progress)
    if progress:
        progress("stage=2/4 parser=dashboards status=running")
    dashboards = load_dashboards(synthetic_root / "dashboards.json", progress=progress)
    if progress:
        progress("stage=3/4 parser=query_logs status=running")
    logs = load_query_logs(
        synthetic_root / "query_logs" / "query_logs.parquet",
        as_of=_as_utc(dashboards.generated_as_of),
        progress=progress,
    )
    if progress:
        progress("stage=4/4 parser=manifest_evidence status=running")

    path_checks = _path_checks(repo_root, manifest)
    checks: dict[str, tuple[bool, tuple[str, ...]]] = {}

    revenue_exec = dashboards.tile("revenue_executive", "rev_exec_total")
    revenue_finance = dashboards.tile("revenue_finance", "rev_fin_total")
    checks["D1"] = (
        path_checks["D1"]
        and revenue_exec.metric_labels == revenue_finance.metric_labels == ("Total Revenue",)
        and "sum(amount)" in revenue_exec.query_sql.lower()
        and "sum(amount - refunds)" in revenue_finance.query_sql.lower(),
        (
            revenue_exec.evidence[0].locator,
            revenue_finance.evidence[0].locator,
        ),
    )

    downstream_refs = {
        ref_name for model in dbt.models for ref_name in model.refs
    }
    dashboard_sql = "\n".join(tile.query_sql for tile in dashboards.tiles)
    checks["D2"] = (
        path_checks["D2"]
        and "stg_legacy_orders" not in downstream_refs
        and "stg_legacy_orders" not in dashboard_sql,
        (dbt.model("stg_legacy_orders").asset.path,),
    )

    weekly_last = _max_timestamp(logs, "weekly_ops")
    checks["D3"] = (
        path_checks["D3"]
        and weekly_last == datetime(2026, 3, 21, 12, 0, tzinfo=UTC),
        (f"dashboard_id=weekly_ops/max(timestamp)={weekly_last.isoformat()}",),
    )

    sales = _tile_signatures(dashboards, "sales_kpis")
    sales_v2 = _tile_signatures(dashboards, "sales_kpis_v2")
    sales_ratio = len(sales & sales_v2) / max(len(sales), len(sales_v2))
    checks["D4"] = (
        path_checks["D4"] and sales_ratio == 0.8,
        (f"dashboard=sales_kpis/sales_kpis_v2/shared_ratio={sales_ratio}",),
    )

    raw_users = dbt.source("raw", "users")
    stg_users = dbt.model("stg_users")
    privacy_tile = dashboards.tile("user_privacy_audit", "privacy_email_hash")
    checks["D5"] = (
        path_checks["D5"]
        and "email_hash" not in raw_users.columns
        and "email_hash" in stg_users.raw_sql
        and stg_users.sources == ("source.raw.users",)
        and "users.email_hash" in privacy_tile.query_sql,
        (
            privacy_tile.evidence[0].locator,
            stg_users.asset.path,
            raw_users.evidence[0].locator,
        ),
    )

    active_7d = dbt.model("mart_active_users_7d")
    active_30d = dbt.model("mart_active_users_30d")
    product_active = dashboards.tile("product_engagement", "product_active_users")
    health_active = dashboards.tile("customer_health", "health_active_users")
    checks["D6"] = (
        path_checks["D6"]
        and "as active_users" in active_7d.raw_sql.lower()
        and "interval '7 days'" in active_7d.raw_sql.lower()
        and "as active_users" in active_30d.raw_sql.lower()
        and "interval '30 days'" in active_30d.raw_sql.lower()
        and product_active.metric_labels == health_active.metric_labels == ("Active Users",),
        (
            active_7d.asset.path,
            active_30d.asset.path,
            product_active.evidence[0].locator,
            health_active.evidence[0].locator,
        ),
    )

    order_wide = dbt.model("int_order_wide")
    daily_sales = dbt.model("mart_daily_sales")
    d7_manifest = _manifest_item(manifest, "D7")
    source_order_columns = set(dbt.source("raw", "orders").columns)
    unused_columns = set(d7_manifest["unused_columns"])
    checks["D7"] = (
        path_checks["D7"]
        and "select *" in order_wide.raw_sql.lower()
        and "int_order_wide" in daily_sales.refs
        and unused_columns == source_order_columns - {"order_id"}
        and len(unused_columns) == 12,
        (order_wide.asset.path, daily_sales.asset.path),
    )

    marketing = _tile_signatures(dashboards, "marketing_overview")
    marketing_ratio = len(marketing & sales) / max(len(marketing), len(sales))
    checks["N1"] = (
        path_checks["N1"] and marketing_ratio == 0.4,
        (f"dashboard=marketing_overview/sales_kpis/shared_ratio={marketing_ratio}",),
    )

    monthly_last = _max_timestamp(logs, "monthly_board")
    checks["N2"] = (
        path_checks["N2"]
        and monthly_last == datetime(2026, 7, 5, 12, 0, tzinfo=UTC),
        (f"dashboard_id=monthly_board/max(timestamp)={monthly_last.isoformat()}",),
    )

    checks["N3"] = (
        path_checks["N3"]
        and "analytics.mart_subscription_health" in dashboard_sql,
        (
            dbt.model("mart_subscription_health").asset.path,
            dashboards.tile("monthly_board", "monthly_subscriptions").evidence[0].locator,
        ),
    )

    executive_order = dashboards.tile("executive_orders", "exec_order_count")
    finance_order = dashboards.tile("finance_orders", "fin_order_count")
    checks["N4"] = (
        path_checks["N4"]
        and executive_order.metric_labels == finance_order.metric_labels == ("Order Count",)
        and executive_order.query_sql == finance_order.query_sql,
        (
            executive_order.evidence[0].locator,
            finance_order.evidence[0].locator,
        ),
    )

    gross = dashboards.tile("executive_orders", "exec_gross_revenue")
    net = dashboards.tile("finance_orders", "fin_net_revenue")
    checks["N5"] = (
        path_checks["N5"]
        and gross.metric_labels == ("Gross Revenue",)
        and net.metric_labels == ("Net Revenue",)
        and gross.query_sql != net.query_sql,
        (gross.evidence[0].locator, net.evidence[0].locator),
    )

    manifest_ids = {
        item["id"] for item in [*manifest["defects"], *manifest["negatives"]]
    }
    if manifest_ids != set(checks):
        raise AssertionError(
            f"parser checks do not match manifest IDs: checks={set(checks)}, "
            f"manifest={manifest_ids}"
        )
    results = tuple(
        ParserGroundTruthCheck(
            id=check_id,
            expected=_manifest_item(manifest, check_id)["expected"],
            passed=passed,
            evidence=evidence,
        )
        for check_id, (passed, evidence) in checks.items()
    )
    failures = [result.id for result in results if not result.passed]
    if failures:
        raise AssertionError(f"parser ground-truth verification failed: {failures}")

    counts = Counter(model.layer for model in dbt.models)
    return ParserVerification(
        model_counts=tuple(sorted(counts.items())),
        dashboard_count=len(dashboards.dashboards),
        tile_count=len(dashboards.tiles),
        query_log_count=len(logs.entries),
        checks=results,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)

    def report(message: str) -> None:
        print(f"[lineageiq] parse_verify {message}")

    result = verify_parsers_against_manifest(args.repo_root, progress=report)
    passed = sum(check.passed for check in result.checks)
    print(
        f"[lineageiq] parse_verify verification={passed}/{len(result.checks)} "
        f"models={sum(count for _, count in result.model_counts)} "
        f"dashboards={result.dashboard_count} tiles={result.tile_count} "
        f"query_logs={result.query_log_count} status=passed"
    )
    print(
        "[lineageiq] cost llm_requests=0 input_tokens=0 output_tokens=0 "
        "estimated_cost_usd=0.000000"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
