"""Build the deterministic synthetic analytics stack used by LineageIQ evaluations."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import yaml

SEED = 424242
AS_OF = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
LAYERS = ("staging", "intermediate", "marts")
USERS = ("analyst@acme.test", "finance@acme.test", "ops@acme.test", "growth@acme.test")


STAGING_MODELS = {
    "stg_orders": """select
    order_id,
    user_id,
    amount,
    refunds,
    ordered_at,
    status,
    channel,
    region,
    currency,
    coupon_code,
    shipping_amount,
    tax_amount,
    payment_method
from {{ source('raw', 'orders') }}
""",
    "stg_users": """select
    user_id,
    email_hash,
    created_at,
    country,
    acquisition_channel
from {{ source('raw', 'users') }}
""",
    "stg_payments": """select payment_id, order_id, amount, payment_method, paid_at
from {{ source('raw', 'payments') }}
""",
    "stg_refunds": """select refund_id, order_id, refund_amount, refunded_at
from {{ source('raw', 'refunds') }}
""",
    "stg_events": """select event_id, user_id, event_name, event_at, session_id
from {{ source('raw', 'events') }}
""",
    "stg_products": """select product_id, product_name, category, unit_cost
from {{ source('raw', 'products') }}
""",
    "stg_subscriptions": """select subscription_id, user_id, plan_name, status, started_at, ended_at
from {{ source('raw', 'subscriptions') }}
""",
    "stg_support_tickets": """select ticket_id, user_id, priority, status, opened_at, resolved_at
from {{ source('raw', 'support_tickets') }}
""",
    "stg_campaigns": """select campaign_id, campaign_name, channel, spend, started_at
from {{ source('raw', 'campaigns') }}
""",
    "stg_legacy_orders": """select legacy_order_id, legacy_customer_id, legacy_total, imported_at
from {{ source('raw', 'legacy_orders') }}
""",
}

INTERMEDIATE_MODELS = {
    "int_order_financials": """with refund_totals as (
    select order_id, sum(refund_amount) as refund_amount
    from {{ ref('stg_refunds') }}
    group by 1
),
payment_totals as (
    select order_id, sum(amount) as captured_payment_amount
    from {{ ref('stg_payments') }}
    group by 1
)
select
    o.order_id,
    o.user_id,
    o.amount,
    coalesce(r.refund_amount, o.refunds) as refunds,
    o.amount - coalesce(r.refund_amount, o.refunds) as net_amount,
    p.captured_payment_amount,
    o.ordered_at,
    o.status,
    o.channel,
    o.region
from {{ ref('stg_orders') }} as o
left join refund_totals as r using (order_id)
left join payment_totals as p using (order_id)
""",
    "int_user_orders": """select
    u.user_id,
    u.country,
    count(o.order_id) as lifetime_orders,
    sum(o.amount) as lifetime_revenue
from {{ ref('stg_users') }} as u
left join {{ ref('stg_orders') }} as o using (user_id)
group by 1, 2
""",
    "int_daily_sales": """select
    cast(ordered_at as date) as order_date,
    sum(amount) as gross_revenue,
    sum(amount - refunds) as net_revenue,
    count(distinct order_id) as order_count
from {{ ref('stg_orders') }}
group by 1
""",
    "int_product_sales": """select
    p.product_id,
    p.product_name,
    p.category,
    count(o.order_id) as order_count
from {{ ref('stg_products') }} as p
left join {{ ref('stg_orders') }} as o on p.product_id = o.order_id
group by 1, 2, 3
""",
    "int_subscription_status": """select
    s.subscription_id,
    s.user_id,
    s.plan_name,
    s.status,
    u.country
from {{ ref('stg_subscriptions') }} as s
join {{ ref('stg_users') }} as u using (user_id)
""",
    "int_event_sessions": """select
    session_id,
    user_id,
    min(event_at) as session_started_at,
    max(event_at) as session_ended_at,
    count(*) as event_count
from {{ ref('stg_events') }}
group by 1, 2
""",
    "int_support_metrics": """select
    cast(t.opened_at as date) as opened_date,
    t.priority,
    count(*) as opened_tickets,
    count(t.resolved_at) as resolved_tickets
from {{ ref('stg_support_tickets') }} as t
left join {{ ref('stg_users') }} as u using (user_id)
group by 1, 2
""",
    "int_campaign_attribution": """select
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.spend,
    count(o.order_id) as attributed_orders,
    sum(o.amount) as attributed_revenue
from {{ ref('stg_campaigns') }} as c
left join {{ ref('stg_orders') }} as o on c.channel = o.channel
group by 1, 2, 3, 4
""",
    "int_order_wide": """-- Deliberate D7: one key plus 12 propagated columns.
select *
from {{ ref('stg_orders') }}
""",
    "int_user_activity": """select
    user_id,
    cast(event_at as date) as activity_date,
    count(*) as event_count
from {{ ref('stg_events') }}
group by 1, 2
""",
}

MART_MODELS = {
    "fct_orders": """select
    order_id,
    user_id,
    amount,
    refunds,
    net_amount,
    ordered_at,
    status,
    channel,
    region
from {{ ref('int_order_financials') }}
""",
    "dim_users": """-- Deliberate D5: email_hash was removed from the raw users source.
select user_id, email_hash, created_at, country, acquisition_channel
from {{ ref('stg_users') }}
""",
    "dim_products": """select product_id, product_name, category, unit_cost
from {{ ref('stg_products') }}
""",
    "mart_daily_sales": """with daily as (
    select * from {{ ref('int_daily_sales') }}
),
order_volume as (
    -- Only order_id is consumed; the other 12 int_order_wide columns are unused.
    select count(distinct order_id) as all_time_order_count
    from {{ ref('int_order_wide') }}
),
user_rollup as (
    select count(distinct user_id) as known_users
    from {{ ref('int_user_orders') }}
)
select daily.*, order_volume.all_time_order_count, user_rollup.known_users
from daily
cross join order_volume
cross join user_rollup
""",
    "mart_product_performance": """select *
from {{ ref('int_product_sales') }}
""",
    "mart_subscription_health": """select
    plan_name,
    status,
    count(distinct subscription_id) as subscriptions
from {{ ref('int_subscription_status') }}
group by 1, 2
""",
    "mart_support_overview": """select *
from {{ ref('int_support_metrics') }}
""",
    "mart_campaign_roi": """select
    *,
    attributed_revenue / nullif(spend, 0) as return_on_ad_spend
from {{ ref('int_campaign_attribution') }}
""",
    "mart_active_users_7d": """-- Deliberate D6: 7-day definition.
with activity as (
    select count(distinct user_id) as active_users
    from {{ ref('int_user_activity') }}
    where activity_date >= current_date - interval '7 days'
),
sessions as (
    select count(*) as sessions_7d
    from {{ ref('int_event_sessions') }}
    where session_started_at >= current_date - interval '7 days'
)
select activity.active_users, sessions.sessions_7d
from activity cross join sessions
""",
    "mart_active_users_30d": """-- Deliberate D6: 30-day definition.
select count(distinct user_id) as active_users
from {{ ref('int_user_activity') }}
where activity_date >= current_date - interval '30 days'
""",
}

MODEL_LAYERS = {
    "staging": STAGING_MODELS,
    "intermediate": INTERMEDIATE_MODELS,
    "marts": MART_MODELS,
}


def _tile(tile_id: str, title: str, sql: str, metric_label: str | None = None) -> dict[str, Any]:
    return {
        "id": tile_id,
        "title": title,
        "metric_label": metric_label or title,
        "sql": sql,
    }


COMMON_SALES_TILES = (
    (
        "total_revenue",
        "Net Revenue",
        "select sum(net_amount) as net_revenue from analytics.fct_orders",
    ),
    (
        "order_count",
        "Order Count",
        "select count(distinct order_id) as order_count from analytics.fct_orders",
    ),
    (
        "customer_count",
        "Customers",
        "select count(distinct user_id) as customers from analytics.fct_orders",
    ),
    (
        "average_order",
        "Average Order Value",
        "select avg(amount) as average_order_value from analytics.fct_orders",
    ),
)


def dashboard_fixture() -> dict[str, Any]:
    sales_tiles = [
        _tile(f"sales_{tile_id}", title, sql) for tile_id, title, sql in COMMON_SALES_TILES
    ]
    sales_tiles.append(
        _tile(
            "sales_channel_mix",
            "Revenue by Channel",
            "select channel, sum(net_amount) as revenue from analytics.fct_orders group by 1",
        )
    )
    sales_v2_tiles = [
        _tile(f"sales_v2_{tile_id}", title, sql) for tile_id, title, sql in COMMON_SALES_TILES
    ]
    sales_v2_tiles.append(
        _tile(
            "sales_v2_region",
            "Revenue by Region",
            "select region, sum(net_amount) as revenue from analytics.fct_orders group by 1",
        )
    )

    dashboards = [
        {
            "id": "revenue_executive",
            "title": "Executive Revenue",
            "description": "Gross booked revenue for executive reporting.",
            "tiles": [
                _tile(
                    "rev_exec_total",
                    "Total Revenue",
                    "select sum(amount) as total_revenue from analytics.fct_orders",
                ),
                _tile(
                    "rev_exec_trend",
                    "Gross Revenue Trend",
                    "select order_date, gross_revenue from analytics.mart_daily_sales order by 1",
                ),
            ],
        },
        {
            "id": "revenue_finance",
            "title": "Finance Revenue",
            "description": "Net recognized revenue after refunds.",
            "tiles": [
                _tile(
                    "rev_fin_total",
                    "Total Revenue",
                    "select sum(amount - refunds) as total_revenue from analytics.fct_orders",
                ),
                _tile(
                    "rev_fin_trend",
                    "Net Revenue Trend",
                    "select order_date, net_revenue from analytics.mart_daily_sales order by 1",
                ),
            ],
        },
        {
            "id": "weekly_ops",
            "title": "Weekly Operations",
            "description": "Legacy operations review dashboard.",
            "tiles": [
                _tile(
                    "ops_orders",
                    "Weekly Orders",
                    "select date_trunc('week', ordered_at) as week, count(*) as orders "
                    "from analytics.fct_orders group by 1",
                ),
                _tile(
                    "ops_tickets",
                    "Weekly Tickets",
                    "select date_trunc('week', opened_date) as week, "
                    "sum(opened_tickets) as tickets "
                    "from analytics.mart_support_overview group by 1",
                ),
            ],
        },
        {
            "id": "sales_kpis",
            "title": "Sales KPIs",
            "description": "Canonical sales performance.",
            "tiles": sales_tiles,
        },
        {
            "id": "sales_kpis_v2",
            "title": "Sales KPIs v2",
            "description": "Unapproved copy with a regional tile.",
            "tiles": sales_v2_tiles,
        },
        {
            "id": "marketing_overview",
            "title": "Marketing Overview",
            "description": "Campaign efficiency and channel mix.",
            "tiles": [
                _tile(
                    "mkt_channel_mix",
                    "Revenue by Channel",
                    "select channel, sum(net_amount) as revenue "
                    "from analytics.fct_orders group by 1",
                ),
                _tile(
                    "mkt_order_count",
                    "Order Count",
                    "select count(distinct order_id) as order_count from analytics.fct_orders",
                ),
                _tile(
                    "mkt_spend",
                    "Campaign Spend",
                    "select sum(spend) as campaign_spend from analytics.mart_campaign_roi",
                ),
                _tile(
                    "mkt_roas",
                    "Return on Ad Spend",
                    "select avg(return_on_ad_spend) as roas from analytics.mart_campaign_roi",
                ),
                _tile(
                    "mkt_campaigns",
                    "Campaign Count",
                    "select count(distinct campaign_id) as campaigns "
                    "from analytics.mart_campaign_roi",
                ),
            ],
        },
        {
            "id": "product_engagement",
            "title": "Product Engagement",
            "description": "Short-window engagement used by product.",
            "tiles": [
                _tile(
                    "product_active_users",
                    "Active Users",
                    "select active_users from analytics.mart_active_users_7d",
                ),
                _tile(
                    "product_sessions",
                    "Sessions",
                    "select sessions_7d as sessions from analytics.mart_active_users_7d",
                ),
                _tile(
                    "product_performance",
                    "Product Performance",
                    "select product_name, order_count "
                    "from analytics.mart_product_performance",
                ),
                _tile(
                    "product_catalog",
                    "Product Catalog Size",
                    "select count(*) as products from analytics.dim_products",
                ),
            ],
        },
        {
            "id": "customer_health",
            "title": "Customer Health",
            "description": "Long-window engagement used by success.",
            "tiles": [
                _tile(
                    "health_active_users",
                    "Active Users",
                    "select active_users from analytics.mart_active_users_30d",
                ),
                _tile(
                    "health_subscriptions",
                    "Subscriptions",
                    "select sum(subscriptions) as subscriptions "
                    "from analytics.mart_subscription_health",
                ),
            ],
        },
        {
            "id": "user_privacy_audit",
            "title": "User Privacy Audit",
            "description": "Checks hashed user identifiers.",
            "tiles": [
                _tile(
                    "privacy_email_hash",
                    "Hashed Emails",
                    "select users.email_hash from analytics.dim_users as users",
                )
            ],
        },
        {
            "id": "executive_orders",
            "title": "Executive Orders",
            "description": "Executive order volume and gross revenue.",
            "tiles": [
                _tile(
                    "exec_order_count",
                    "Order Count",
                    "select count(distinct order_id) as order_count from analytics.fct_orders",
                ),
                _tile(
                    "exec_gross_revenue",
                    "Gross Revenue",
                    "select sum(amount) as gross_revenue from analytics.fct_orders",
                ),
            ],
        },
        {
            "id": "finance_orders",
            "title": "Finance Orders",
            "description": "Finance order volume and net revenue.",
            "tiles": [
                _tile(
                    "fin_order_count",
                    "Order Count",
                    "select count(distinct order_id) as order_count from analytics.fct_orders",
                ),
                _tile(
                    "fin_net_revenue",
                    "Net Revenue",
                    "select sum(amount - refunds) as net_revenue from analytics.fct_orders",
                ),
            ],
        },
        {
            "id": "monthly_board",
            "title": "Monthly Subscription Board",
            "description": "Low-frequency but current board reporting.",
            "tiles": [
                _tile(
                    "monthly_subscriptions",
                    "Subscriptions by Plan",
                    "select plan_name, sum(subscriptions) as subscriptions "
                    "from analytics.mart_subscription_health group by 1",
                )
            ],
        },
    ]
    return {
        "schema_version": 1,
        "seed": SEED,
        "generated_as_of": AS_OF.isoformat().replace("+00:00", "Z"),
        "dashboards": dashboards,
    }


def source_fixture() -> dict[str, Any]:
    columns = {
        "orders": [
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
        ],
        # email_hash was deliberately removed; stg_users still references it (D5).
        "users": ["user_id", "created_at", "country", "acquisition_channel"],
        "payments": ["payment_id", "order_id", "amount", "payment_method", "paid_at"],
        "refunds": ["refund_id", "order_id", "refund_amount", "refunded_at"],
        "events": ["event_id", "user_id", "event_name", "event_at", "session_id"],
        "products": ["product_id", "product_name", "category", "unit_cost"],
        "subscriptions": [
            "subscription_id",
            "user_id",
            "plan_name",
            "status",
            "started_at",
            "ended_at",
        ],
        "support_tickets": [
            "ticket_id",
            "user_id",
            "priority",
            "status",
            "opened_at",
            "resolved_at",
        ],
        "campaigns": ["campaign_id", "campaign_name", "channel", "spend", "started_at"],
        "legacy_orders": [
            "legacy_order_id",
            "legacy_customer_id",
            "legacy_total",
            "imported_at",
        ],
    }
    return {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "database": "warehouse",
                "schema": "raw",
                "tables": [
                    {"name": table, "columns": [{"name": column} for column in table_columns]}
                    for table, table_columns in columns.items()
                ],
            }
        ],
    }


def manifest_fixture() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": "lineageiq_synthetic_v1",
        "seed": SEED,
        "as_of": AS_OF.isoformat().replace("+00:00", "Z"),
        "defects": [
            {
                "id": "D1",
                "expected": "positive",
                "type": "metric_definition_conflict",
                "description": 'Tiles labeled "Total Revenue" use gross and net formulas.',
                "locations": [
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=revenue_executive/tile=rev_exec_total",
                        "expression": "sum(amount)",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=revenue_finance/tile=rev_fin_total",
                        "expression": "sum(amount - refunds)",
                    },
                ],
            },
            {
                "id": "D2",
                "expected": "positive",
                "type": "orphaned_model",
                "description": "A staging model has no downstream model or dashboard reference.",
                "locations": [
                    {
                        "path": "synthetic/dbt_project/models/staging/stg_legacy_orders.sql",
                        "locator": "model=stg_legacy_orders",
                    }
                ],
            },
            {
                "id": "D3",
                "expected": "positive",
                "type": "stale_asset",
                "description": "weekly_ops has no query-log activity in the last 120 days.",
                "locations": [
                    {
                        "path": "synthetic/query_logs/query_logs.parquet",
                        "locator": "dashboard_id=weekly_ops/max(timestamp)=2026-03-21T12:00:00Z",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=weekly_ops",
                    },
                ],
            },
            {
                "id": "D4",
                "expected": "positive",
                "type": "duplicate_dashboard",
                "description": "sales_kpis and sales_kpis_v2 share four of five tile definitions.",
                "locations": [
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=sales_kpis",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=sales_kpis_v2",
                    },
                ],
                "shared_tile_ratio": 0.8,
            },
            {
                "id": "D5",
                "expected": "positive",
                "type": "broken_lineage",
                "description": (
                    "A tile traces to users.email_hash, absent from raw source metadata."
                ),
                "locations": [
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=user_privacy_audit/tile=privacy_email_hash",
                        "column": "users.email_hash",
                    },
                    {
                        "path": "synthetic/dbt_project/models/marts/dim_users.sql",
                        "locator": "column=email_hash",
                    },
                    {
                        "path": "synthetic/dbt_project/models/staging/stg_users.sql",
                        "locator": "source=raw.users/column=email_hash",
                    },
                    {
                        "path": "synthetic/dbt_project/models/staging/sources.yml",
                        "locator": "source=raw.users/columns (email_hash absent)",
                    },
                ],
            },
            {
                "id": "D6",
                "expected": "positive",
                "type": "metric_definition_conflict",
                "description": "active_users has incompatible 7-day and 30-day windows.",
                "locations": [
                    {
                        "path": "synthetic/dbt_project/models/marts/mart_active_users_7d.sql",
                        "locator": "column=active_users/window=7 days",
                    },
                    {
                        "path": "synthetic/dbt_project/models/marts/mart_active_users_30d.sql",
                        "locator": "column=active_users/window=30 days",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=product_engagement/tile=product_active_users",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=customer_health/tile=health_active_users",
                    },
                ],
            },
            {
                "id": "D7",
                "expected": "positive",
                "type": "unused_column_propagation",
                "description": "select * propagates 12 columns that its only consumer never uses.",
                "locations": [
                    {
                        "path": "synthetic/dbt_project/models/intermediate/int_order_wide.sql",
                        "locator": "select=*",
                    },
                    {
                        "path": "synthetic/dbt_project/models/marts/mart_daily_sales.sql",
                        "locator": "ref=int_order_wide/used_column=order_id",
                    },
                ],
                "unused_columns": [
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
                ],
            },
        ],
        "negatives": [
            {
                "id": "N1",
                "expected": "negative",
                "type": "duplicate_dashboard",
                "description": "Marketing and sales share two tiles but serve distinct purposes.",
                "locations": [
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=marketing_overview",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=sales_kpis",
                    },
                ],
                "shared_tile_ratio": 0.4,
            },
            {
                "id": "N2",
                "expected": "negative",
                "type": "stale_asset",
                "description": "monthly_board is low-frequency but was queried 20 days ago.",
                "locations": [
                    {
                        "path": "synthetic/query_logs/query_logs.parquet",
                        "locator": "dashboard_id=monthly_board/max(timestamp)=2026-07-05T12:00:00Z",
                    }
                ],
            },
            {
                "id": "N3",
                "expected": "negative",
                "type": "orphaned_model",
                "description": "mart_subscription_health is low-use but feeds monthly_board.",
                "locations": [
                    {
                        "path": "synthetic/dbt_project/models/marts/mart_subscription_health.sql",
                        "locator": "model=mart_subscription_health",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=monthly_board/tile=monthly_subscriptions",
                    },
                ],
            },
            {
                "id": "N4",
                "expected": "negative",
                "type": "metric_definition_conflict",
                "description": 'Two "Order Count" tiles use byte-identical SQL.',
                "locations": [
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=executive_orders/tile=exec_order_count",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=finance_orders/tile=fin_order_count",
                    },
                ],
            },
            {
                "id": "N5",
                "expected": "negative",
                "type": "metric_definition_conflict",
                "description": "Gross Revenue and Net Revenue are intentionally distinct labels.",
                "locations": [
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=executive_orders/tile=exec_gross_revenue",
                    },
                    {
                        "path": "synthetic/dashboards.json",
                        "locator": "dashboard=finance_orders/tile=fin_net_revenue",
                    },
                ],
            },
        ],
    }


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8", newline="\n")


def _write_json(path: Path, value: Any) -> None:
    _write_text(path, json.dumps(value, indent=2, ensure_ascii=False))


def _write_yaml(path: Path, value: Any) -> None:
    _write_text(path, yaml.safe_dump(value, sort_keys=False, allow_unicode=True))


def _write_dbt_project(root: Path) -> None:
    dbt_root = root / "dbt_project"
    _write_yaml(
        dbt_root / "dbt_project.yml",
        {
            "name": "lineageiq_synthetic",
            "version": "1.0.0",
            "config-version": 2,
            "profile": "lineageiq_synthetic",
            "model-paths": ["models"],
            "models": {
                "lineageiq_synthetic": {
                    "staging": {"materialized": "view"},
                    "intermediate": {"materialized": "ephemeral"},
                    "marts": {"materialized": "table"},
                }
            },
        },
    )
    _write_yaml(dbt_root / "models" / "staging" / "sources.yml", source_fixture())
    for layer in LAYERS:
        for name, sql in sorted(MODEL_LAYERS[layer].items()):
            _write_text(dbt_root / "models" / layer / f"{name}.sql", sql)


def _query_log_rows(dashboards: dict[str, Any]) -> list[tuple[str, str, str, datetime]]:
    rows: list[tuple[str, str, str, datetime]] = []
    for dashboard_index, dashboard in enumerate(dashboards["dashboards"]):
        dashboard_id = dashboard["id"]
        if dashboard_id == "weekly_ops":
            for tile_index, tile in enumerate(dashboard["tiles"]):
                rows.append(
                    (
                        dashboard_id,
                        tile["id"],
                        USERS[tile_index % len(USERS)],
                        AS_OF - timedelta(days=126, minutes=tile_index),
                    )
                )
            continue
        if dashboard_id == "monthly_board":
            for day_offset in (80, 50, 20):
                rows.append(
                    (
                        dashboard_id,
                        dashboard["tiles"][0]["id"],
                        "finance@acme.test",
                        AS_OF - timedelta(days=day_offset),
                    )
                )
            continue

        cadence_days = 1 + (dashboard_index % 5)
        for day_offset in range(89, -1, -1):
            if day_offset % cadence_days:
                continue
            for tile_index, tile in enumerate(dashboard["tiles"]):
                hour_shift = (dashboard_index * 3 + tile_index * 2 + SEED) % 10
                minute_shift = (dashboard_index * 11 + tile_index * 7 + SEED) % 60
                rows.append(
                    (
                        dashboard_id,
                        tile["id"],
                        USERS[(dashboard_index + tile_index + day_offset) % len(USERS)],
                        AS_OF
                        - timedelta(days=day_offset)
                        - timedelta(hours=hour_shift, minutes=minute_shift),
                    )
                )
    ordered = sorted(rows, key=lambda row: (row[3], row[0], row[1], row[2]))
    # Store wall-clock UTC in DuckDB TIMESTAMP so host timezone cannot alter bytes.
    return [
        (dashboard_id, tile_id, user, timestamp.replace(tzinfo=None))
        for dashboard_id, tile_id, user, timestamp in ordered
    ]


def _write_query_logs(root: Path, dashboards: dict[str, Any]) -> None:
    output = root / "query_logs" / "query_logs.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".parquet.tmp")
    temporary.unlink(missing_ok=True)
    rows = _query_log_rows(dashboards)
    connection = duckdb.connect()
    try:
        connection.execute(
            'create table query_logs (dashboard_id varchar, tile_id varchar, "user" varchar, '
            '"timestamp" timestamp)'
        )
        connection.executemany("insert into query_logs values (?, ?, ?, ?)", rows)
        escaped_path = str(temporary).replace("'", "''")
        connection.execute(
            "copy (select dashboard_id, tile_id, \"user\", \"timestamp\" "
            "from query_logs order by \"timestamp\", dashboard_id, tile_id, \"user\") "
            f"to '{escaped_path}' "
            "(format parquet, compression zstd, row_group_size 10000)"
        )
    finally:
        connection.close()
    temporary.replace(output)


def _dashboards_by_id(dashboards: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {dashboard["id"]: dashboard for dashboard in dashboards["dashboards"]}


def _tiles_by_id(dashboard: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {tile["id"]: tile for tile in dashboard["tiles"]}


def _tile_signatures(dashboard: dict[str, Any]) -> set[tuple[str, str]]:
    return {(tile["title"], tile["sql"]) for tile in dashboard["tiles"]}


def _max_log_timestamp(parquet_path: Path, dashboard_id: str) -> datetime:
    row = duckdb.sql(
        "select max(timestamp) from read_parquet(?) where dashboard_id = ?",
        params=[str(parquet_path), dashboard_id],
    ).fetchone()
    if row is None or row[0] is None:
        raise AssertionError(f"no logs for {dashboard_id}")
    return row[0].replace(tzinfo=UTC)


def verify_generated_stack(root: Path) -> dict[str, Any]:
    """Verify every planted positive and negative against generated artifacts."""

    dashboards = json.loads((root / "dashboards.json").read_text(encoding="utf-8"))
    dashboard_map = _dashboards_by_id(dashboards)
    source_data = yaml.safe_load(
        (root / "dbt_project" / "models" / "staging" / "sources.yml").read_text(
            encoding="utf-8"
        )
    )
    manifest = yaml.safe_load((root / "manifest.yaml").read_text(encoding="utf-8"))
    parquet = root / "query_logs" / "query_logs.parquet"
    model_root = root / "dbt_project" / "models"

    model_counts = {
        layer: len(list((model_root / layer).glob("*.sql"))) for layer in LAYERS
    }
    assert model_counts == {"staging": 10, "intermediate": 10, "marts": 10}
    all_models = set().union(*(set(models) for models in MODEL_LAYERS.values()))
    for sql_path in model_root.rglob("*.sql"):
        sql_text = sql_path.read_text(encoding="utf-8")
        refs = re.findall(r"\{\{\s*ref\(['\"]([^'\"]+)['\"]\)\s*\}\}", sql_text)
        for ref_name in refs:
            assert ref_name in all_models, f"unresolved ref {ref_name} in {sql_path}"

    checks: dict[str, bool] = {}

    exec_tile = _tiles_by_id(dashboard_map["revenue_executive"])["rev_exec_total"]
    finance_tile = _tiles_by_id(dashboard_map["revenue_finance"])["rev_fin_total"]
    checks["D1"] = (
        exec_tile["metric_label"] == finance_tile["metric_label"] == "Total Revenue"
        and "sum(amount)" in exec_tile["sql"]
        and "sum(amount - refunds)" in finance_tile["sql"]
    )

    downstream_sql = "\n".join(
        path.read_text(encoding="utf-8")
        for path in model_root.rglob("*.sql")
        if path.name != "stg_legacy_orders.sql"
    )
    dashboard_sql = "\n".join(
        tile["sql"] for dashboard in dashboards["dashboards"] for tile in dashboard["tiles"]
    )
    checks["D2"] = (
        "ref('stg_legacy_orders')" not in downstream_sql
        and "stg_legacy_orders" not in dashboard_sql
    )

    weekly_ops_last = _max_log_timestamp(parquet, "weekly_ops")
    checks["D3"] = AS_OF - weekly_ops_last >= timedelta(days=120)

    sales = _tile_signatures(dashboard_map["sales_kpis"])
    sales_v2 = _tile_signatures(dashboard_map["sales_kpis_v2"])
    checks["D4"] = len(sales & sales_v2) / max(len(sales), len(sales_v2)) == 0.8

    raw_users = next(
        table
        for source in source_data["sources"]
        for table in source["tables"]
        if source["name"] == "raw" and table["name"] == "users"
    )
    raw_user_columns = {column["name"] for column in raw_users["columns"]}
    privacy_tile = _tiles_by_id(dashboard_map["user_privacy_audit"])["privacy_email_hash"]
    checks["D5"] = (
        "users.email_hash" in privacy_tile["sql"]
        and "email_hash" in STAGING_MODELS["stg_users"]
        and "email_hash" in MART_MODELS["dim_users"]
        and "email_hash" not in raw_user_columns
    )

    checks["D6"] = (
        "as active_users" in MART_MODELS["mart_active_users_7d"]
        and "interval '7 days'" in MART_MODELS["mart_active_users_7d"]
        and "as active_users" in MART_MODELS["mart_active_users_30d"]
        and "interval '30 days'" in MART_MODELS["mart_active_users_30d"]
    )

    unused_columns = next(defect for defect in manifest["defects"] if defect["id"] == "D7")[
        "unused_columns"
    ]
    order_columns = source_fixture()["sources"][0]["tables"][0]["columns"]
    checks["D7"] = (
        "select *" in INTERMEDIATE_MODELS["int_order_wide"].lower()
        and len(order_columns) == 13
        and len(unused_columns) == 12
        and set(unused_columns) == {column["name"] for column in order_columns} - {"order_id"}
        and "ref('int_order_wide')" in MART_MODELS["mart_daily_sales"]
    )

    marketing = _tile_signatures(dashboard_map["marketing_overview"])
    checks["N1"] = len(marketing & sales) / max(len(marketing), len(sales)) == 0.4
    checks["N2"] = AS_OF - _max_log_timestamp(parquet, "monthly_board") == timedelta(days=20)
    checks["N3"] = "analytics.mart_subscription_health" in dashboard_sql
    executive_order = _tiles_by_id(dashboard_map["executive_orders"])["exec_order_count"]
    finance_order = _tiles_by_id(dashboard_map["finance_orders"])["fin_order_count"]
    checks["N4"] = (
        executive_order["metric_label"] == finance_order["metric_label"] == "Order Count"
        and executive_order["sql"] == finance_order["sql"]
    )
    gross = _tiles_by_id(dashboard_map["executive_orders"])["exec_gross_revenue"]
    net = _tiles_by_id(dashboard_map["finance_orders"])["fin_net_revenue"]
    checks["N5"] = (
        gross["metric_label"] == "Gross Revenue"
        and net["metric_label"] == "Net Revenue"
        and gross["metric_label"] != net["metric_label"]
    )

    expected_ids = {item["id"] for item in manifest["defects"] + manifest["negatives"]}
    assert expected_ids == set(checks)
    failed = [check_id for check_id, passed in checks.items() if not passed]
    if failed:
        raise AssertionError(f"manifest verification failed: {', '.join(failed)}")

    query_log_count = duckdb.sql(
        "select count(*) from read_parquet(?)", params=[str(parquet)]
    ).fetchone()[0]
    recent_bounds = duckdb.sql(
        "select min(timestamp), max(timestamp) from read_parquet(?) "
        "where dashboard_id <> 'weekly_ops'",
        params=[str(parquet)],
    ).fetchone()
    recent_min = recent_bounds[0].replace(tzinfo=UTC)
    recent_max = recent_bounds[1].replace(tzinfo=UTC)
    assert recent_min <= AS_OF - timedelta(days=89)
    assert recent_max <= AS_OF
    return {
        "model_counts": model_counts,
        "query_log_rows": query_log_count,
        "checks": checks,
    }


def generated_hashes(root: Path) -> dict[str, str]:
    """Return hashes for the complete generated artifact set."""

    paths = [
        root / "dashboards.json",
        root / "manifest.yaml",
        root / "dbt_project" / "dbt_project.yml",
        root / "dbt_project" / "models" / "staging" / "sources.yml",
        root / "query_logs" / "query_logs.parquet",
        *sorted((root / "dbt_project" / "models").rglob("*.sql")),
    ]
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }


def generate(root: Path, *, progress: bool = True) -> dict[str, Any]:
    """Generate and verify a complete deterministic fixture under ``root``."""

    root = root.resolve()
    if progress:
        print("[lineageiq] generate stage=1/5 name=dbt_models status=running")
    _write_dbt_project(root)

    dashboards = dashboard_fixture()
    if progress:
        print("[lineageiq] generate stage=2/5 name=dashboards status=running")
    _write_json(root / "dashboards.json", dashboards)

    if progress:
        print("[lineageiq] generate stage=3/5 name=query_logs status=running")
    _write_query_logs(root, dashboards)

    if progress:
        print("[lineageiq] generate stage=4/5 name=manifest status=running")
    _write_yaml(root / "manifest.yaml", manifest_fixture())

    if progress:
        print("[lineageiq] generate stage=5/5 name=manifest_verification status=running")
    verification = verify_generated_stack(root)
    if progress:
        passed = sum(verification["checks"].values())
        print(
            f"[lineageiq] generate verification={passed}/{len(verification['checks'])} "
            f"query_log_rows={verification['query_log_rows']} status=passed"
        )
        print(
            "[lineageiq] cost llm_requests=0 input_tokens=0 output_tokens=0 "
            "estimated_cost_usd=0.000000"
        )
    return verification


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Synthetic artifact directory (default: the synthetic package directory).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    generate(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
