"""Dashboard metric aggregation endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import DateTime, case
from sqlalchemy import cast as sql_cast
from sqlalchemy import func
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api.deps import require_org_member
from app.core.config import settings
from app.core.time import utcnow
from app.db.session import get_session
from app.models.activity_events import ActivityEvent
from app.models.agents import Agent
from app.models.approvals import Approval
from app.models.boards import Board
from app.models.tasks import Task
from app.schemas.metrics import (
    DashboardBucketKey,
    DashboardKpis,
    DashboardMetrics,
    DashboardPendingApproval,
    DashboardPendingApprovals,
    DashboardRangeKey,
    DashboardRangeSeries,
    RuntimeOpsMetrics,
    DashboardSeriesPoint,
    DashboardSeriesSet,
    DashboardWipPoint,
    DashboardWipRangeSeries,
    DashboardWipSeriesSet,
)
from app.services.organizations import OrganizationContext, list_accessible_board_ids

router = APIRouter(prefix="/metrics", tags=["metrics"])

ERROR_EVENT_PATTERN = "%failed"
_RUNTIME_TYPE_REFERENCES = (UUID, AsyncSession)
RANGE_QUERY = Query(default="24h")
BOARD_ID_QUERY = Query(default=None)
GROUP_ID_QUERY = Query(default=None)
SESSION_DEP = Depends(get_session)
ORG_MEMBER_DEP = Depends(require_org_member)
RUNTIME_WINDOW_QUERY = Query(
    default=settings.runtime_ops_default_window_minutes,
    ge=15,
    le=24 * 60,
)
RUNTIME_EVENT_LIMIT_QUERY = Query(default=settings.runtime_ops_default_event_limit, ge=5, le=250)
RUNTIME_AGENTS_LIMIT_QUERY = Query(default=18, ge=3, le=200)


@dataclass(frozen=True)
class RangeSpec:
    """Resolved time-range specification for metric aggregation."""

    key: DashboardRangeKey
    start: datetime
    end: datetime
    bucket: DashboardBucketKey
    duration: timedelta


def _resolve_range(range_key: DashboardRangeKey) -> RangeSpec:
    now = utcnow()
    specs: dict[DashboardRangeKey, tuple[timedelta, DashboardBucketKey]] = {
        "24h": (timedelta(hours=24), "hour"),
        "3d": (timedelta(days=3), "day"),
        "7d": (timedelta(days=7), "day"),
        "14d": (timedelta(days=14), "day"),
        "1m": (timedelta(days=30), "day"),
        "3m": (timedelta(days=90), "week"),
        "6m": (timedelta(days=180), "week"),
        "1y": (timedelta(days=365), "month"),
    }
    duration, bucket = specs[range_key]
    return RangeSpec(
        key=range_key,
        start=now - duration,
        end=now,
        bucket=bucket,
        duration=duration,
    )


def _comparison_range(range_spec: RangeSpec) -> RangeSpec:
    return RangeSpec(
        key=range_spec.key,
        start=range_spec.start - range_spec.duration,
        end=range_spec.end - range_spec.duration,
        bucket=range_spec.bucket,
        duration=range_spec.duration,
    )


def _bucket_start(value: datetime, bucket: DashboardBucketKey) -> datetime:
    normalized = value.replace(hour=0, minute=0, second=0, microsecond=0)
    if bucket == "month":
        return normalized.replace(day=1)
    if bucket == "week":
        return normalized - timedelta(days=normalized.weekday())
    if bucket == "day":
        return normalized
    return value.replace(minute=0, second=0, microsecond=0)


def _next_bucket(cursor: datetime, bucket: DashboardBucketKey) -> datetime:
    if bucket == "hour":
        return cursor + timedelta(hours=1)
    if bucket == "day":
        return cursor + timedelta(days=1)
    if bucket == "week":
        return cursor + timedelta(days=7)
    next_month = cursor.month + 1
    next_year = cursor.year
    if next_month > 12:
        next_month = 1
        next_year += 1
    return cursor.replace(year=next_year, month=next_month, day=1)


def _build_buckets(range_spec: RangeSpec) -> list[datetime]:
    cursor = _bucket_start(range_spec.start, range_spec.bucket)
    buckets: list[datetime] = []
    while cursor <= range_spec.end:
        buckets.append(cursor)
        cursor = _next_bucket(cursor, range_spec.bucket)
    return buckets


def _series_from_mapping(
    range_spec: RangeSpec,
    mapping: dict[datetime, float],
) -> DashboardRangeSeries:
    points = [
        DashboardSeriesPoint(period=bucket, value=float(mapping.get(bucket, 0)))
        for bucket in _build_buckets(range_spec)
    ]
    return DashboardRangeSeries(
        range=range_spec.key,
        bucket=range_spec.bucket,
        points=points,
    )


def _wip_series_from_mapping(
    range_spec: RangeSpec,
    mapping: dict[datetime, dict[str, int]],
) -> DashboardWipRangeSeries:
    points: list[DashboardWipPoint] = []
    for bucket in _build_buckets(range_spec):
        values = mapping.get(bucket, {})
        points.append(
            DashboardWipPoint(
                period=bucket,
                inbox=values.get("inbox", 0),
                in_progress=values.get("in_progress", 0),
                review=values.get("review", 0),
                done=values.get("done", 0),
            ),
        )
    return DashboardWipRangeSeries(
        range=range_spec.key,
        bucket=range_spec.bucket,
        points=points,
    )


async def _query_throughput(
    session: AsyncSession,
    range_spec: RangeSpec,
    board_ids: list[UUID],
) -> DashboardRangeSeries:
    bucket_col = func.date_trunc(range_spec.bucket, Task.updated_at).label("bucket")
    statement = (
        select(bucket_col, func.count())
        .where(col(Task.status) == "done")
        .where(col(Task.updated_at) >= range_spec.start)
        .where(col(Task.updated_at) <= range_spec.end)
    )
    if not board_ids:
        return _series_from_mapping(range_spec, {})
    statement = (
        statement.where(col(Task.board_id).in_(board_ids)).group_by(bucket_col).order_by(bucket_col)
    )
    results = (await session.exec(statement)).all()
    mapping = {row[0]: float(row[1]) for row in results}
    return _series_from_mapping(range_spec, mapping)


async def _query_cycle_time(
    session: AsyncSession,
    range_spec: RangeSpec,
    board_ids: list[UUID],
) -> DashboardRangeSeries:
    bucket_col = func.date_trunc(range_spec.bucket, Task.updated_at).label("bucket")
    in_progress = sql_cast(Task.in_progress_at, DateTime)
    duration_hours = func.extract("epoch", Task.updated_at - in_progress) / 3600.0
    statement = (
        select(bucket_col, func.avg(duration_hours))
        .where(col(Task.status) == "review")
        .where(col(Task.in_progress_at).is_not(None))
        .where(col(Task.updated_at) >= range_spec.start)
        .where(col(Task.updated_at) <= range_spec.end)
    )
    if not board_ids:
        return _series_from_mapping(range_spec, {})
    statement = (
        statement.where(col(Task.board_id).in_(board_ids)).group_by(bucket_col).order_by(bucket_col)
    )
    results = (await session.exec(statement)).all()
    mapping = {row[0]: float(row[1] or 0) for row in results}
    return _series_from_mapping(range_spec, mapping)


async def _query_error_rate(
    session: AsyncSession,
    range_spec: RangeSpec,
    board_ids: list[UUID],
) -> DashboardRangeSeries:
    bucket_col = func.date_trunc(
        range_spec.bucket,
        ActivityEvent.created_at,
    ).label("bucket")
    error_case = case(
        (
            col(ActivityEvent.event_type).like(ERROR_EVENT_PATTERN),
            1,
        ),
        else_=0,
    )
    statement = (
        select(bucket_col, func.sum(error_case), func.count())
        .join(Task, col(ActivityEvent.task_id) == col(Task.id))
        .where(col(ActivityEvent.created_at) >= range_spec.start)
        .where(col(ActivityEvent.created_at) <= range_spec.end)
    )
    if not board_ids:
        return _series_from_mapping(range_spec, {})
    statement = (
        statement.where(col(Task.board_id).in_(board_ids)).group_by(bucket_col).order_by(bucket_col)
    )
    results = (await session.exec(statement)).all()
    mapping: dict[datetime, float] = {}
    for bucket, errors, total in results:
        total_count = float(total or 0)
        error_count = float(errors or 0)
        rate = (error_count / total_count) * 100 if total_count > 0 else 0.0
        mapping[bucket] = rate
    return _series_from_mapping(range_spec, mapping)


async def _query_wip(
    session: AsyncSession,
    range_spec: RangeSpec,
    board_ids: list[UUID],
) -> DashboardWipRangeSeries:
    if not board_ids:
        return _wip_series_from_mapping(range_spec, {})

    inbox_bucket_col = func.date_trunc(range_spec.bucket, Task.created_at).label("inbox_bucket")
    inbox_statement = (
        select(inbox_bucket_col, func.count())
        .where(col(Task.status) == "inbox")
        .where(col(Task.created_at) >= range_spec.start)
        .where(col(Task.created_at) <= range_spec.end)
        .where(col(Task.board_id).in_(board_ids))
        .group_by(inbox_bucket_col)
        .order_by(inbox_bucket_col)
    )
    inbox_results = (await session.exec(inbox_statement)).all()

    status_bucket_col = func.date_trunc(range_spec.bucket, Task.updated_at).label("status_bucket")
    progress_case = case((col(Task.status) == "in_progress", 1), else_=0)
    review_case = case((col(Task.status) == "review", 1), else_=0)
    done_case = case((col(Task.status) == "done", 1), else_=0)
    status_statement = (
        select(
            status_bucket_col,
            func.sum(progress_case),
            func.sum(review_case),
            func.sum(done_case),
        )
        .where(col(Task.updated_at) >= range_spec.start)
        .where(col(Task.updated_at) <= range_spec.end)
        .where(col(Task.board_id).in_(board_ids))
        .group_by(status_bucket_col)
        .order_by(status_bucket_col)
    )
    status_results = (await session.exec(status_statement)).all()

    mapping: dict[datetime, dict[str, int]] = {}
    for bucket, inbox in inbox_results:
        values = mapping.setdefault(bucket, {})
        values["inbox"] = int(inbox or 0)
    for bucket, in_progress, review, done in status_results:
        values = mapping.setdefault(bucket, {})
        values["in_progress"] = int(in_progress or 0)
        values["review"] = int(review or 0)
        values["done"] = int(done or 0)
    return _wip_series_from_mapping(range_spec, mapping)


async def _median_cycle_time_for_range(
    session: AsyncSession,
    range_spec: RangeSpec,
    board_ids: list[UUID],
) -> float | None:
    in_progress = sql_cast(Task.in_progress_at, DateTime)
    duration_hours = func.extract("epoch", Task.updated_at - in_progress) / 3600.0
    statement = (
        select(func.percentile_cont(0.5).within_group(duration_hours))
        .where(col(Task.status) == "review")
        .where(col(Task.in_progress_at).is_not(None))
        .where(col(Task.updated_at) >= range_spec.start)
        .where(col(Task.updated_at) <= range_spec.end)
    )
    if not board_ids:
        return None
    statement = statement.where(col(Task.board_id).in_(board_ids))
    value = (await session.exec(statement)).one_or_none()
    if value is None:
        return None
    if isinstance(value, tuple):
        value = value[0]
    if value is None:
        return None
    return float(value)


async def _error_rate_kpi(
    session: AsyncSession,
    range_spec: RangeSpec,
    board_ids: list[UUID],
) -> float:
    error_case = case(
        (
            col(ActivityEvent.event_type).like(ERROR_EVENT_PATTERN),
            1,
        ),
        else_=0,
    )
    statement = (
        select(func.sum(error_case), func.count())
        .join(Task, col(ActivityEvent.task_id) == col(Task.id))
        .where(col(ActivityEvent.created_at) >= range_spec.start)
        .where(col(ActivityEvent.created_at) <= range_spec.end)
    )
    if not board_ids:
        return 0.0
    statement = statement.where(col(Task.board_id).in_(board_ids))
    result = (await session.exec(statement)).one_or_none()
    if result is None:
        return 0.0
    errors, total = result
    total_count = float(total or 0)
    error_count = float(errors or 0)
    return (error_count / total_count) * 100 if total_count > 0 else 0.0


async def _active_agents(
    session: AsyncSession,
    range_spec: RangeSpec,
    board_ids: list[UUID],
) -> int:
    statement = select(func.count()).where(
        col(Agent.last_seen_at).is_not(None),
        col(Agent.last_seen_at) >= range_spec.start,
        col(Agent.last_seen_at) <= range_spec.end,
    )
    if not board_ids:
        return 0
    statement = statement.where(col(Agent.board_id).in_(board_ids))
    result = (await session.exec(statement)).one()
    return int(result)


async def _task_status_counts(
    session: AsyncSession,
    board_ids: list[UUID],
) -> dict[str, int]:
    if not board_ids:
        return {
            "inbox": 0,
            "in_progress": 0,
            "review": 0,
            "done": 0,
        }
    statement = (
        select(col(Task.status), func.count())
        .where(col(Task.board_id).in_(board_ids))
        .group_by(col(Task.status))
    )
    results = (await session.exec(statement)).all()
    counts = {
        "inbox": 0,
        "in_progress": 0,
        "review": 0,
        "done": 0,
    }
    for status_value, total in results:
        key = str(status_value)
        if key in counts:
            counts[key] = int(total or 0)
    return counts


async def _pending_approvals_snapshot(
    session: AsyncSession,
    board_ids: list[UUID],
    *,
    limit: int = 10,
) -> DashboardPendingApprovals:
    if not board_ids:
        return DashboardPendingApprovals(total=0, items=[])

    total_statement = (
        select(func.count(col(Approval.id)))
        .where(col(Approval.board_id).in_(board_ids))
        .where(col(Approval.status) == "pending")
    )
    total = int((await session.exec(total_statement)).one() or 0)
    if total == 0:
        return DashboardPendingApprovals(total=0, items=[])

    rows = (
        await session.exec(
            select(Approval, Board, Task)
            .join(Board, col(Board.id) == col(Approval.board_id))
            .outerjoin(Task, col(Task.id) == col(Approval.task_id))
            .where(col(Approval.board_id).in_(board_ids))
            .where(col(Approval.status) == "pending")
            .order_by(col(Approval.created_at).desc())
            .limit(limit)
        )
    ).all()

    items = [
        DashboardPendingApproval(
            approval_id=approval.id,
            board_id=approval.board_id,
            board_name=board.name,
            action_type=approval.action_type,
            confidence=float(approval.confidence),
            created_at=approval.created_at,
            task_title=task.title if task is not None else None,
        )
        for approval, board, task in rows
    ]
    return DashboardPendingApprovals(total=total, items=items)


async def _resolve_dashboard_board_ids(
    session: AsyncSession,
    *,
    ctx: OrganizationContext,
    board_id: UUID | None,
    group_id: UUID | None,
) -> list[UUID]:
    board_ids = await list_accessible_board_ids(session, member=ctx.member, write=False)
    if not board_ids:
        return []
    allowed = set(board_ids)

    if board_id is not None and board_id not in allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)

    if group_id is None:
        return [board_id] if board_id is not None else board_ids

    group_board_ids = list(
        await session.exec(
            select(Board.id)
            .where(col(Board.organization_id) == ctx.member.organization_id)
            .where(col(Board.board_group_id) == group_id)
            .where(col(Board.id).in_(board_ids)),
        ),
    )
    if board_id is not None:
        return [board_id] if board_id in set(group_board_ids) else []
    return group_board_ids


@router.get("/dashboard", response_model=DashboardMetrics)
async def dashboard_metrics(
    range_key: DashboardRangeKey = RANGE_QUERY,
    board_id: UUID | None = BOARD_ID_QUERY,
    group_id: UUID | None = GROUP_ID_QUERY,
    session: AsyncSession = SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> DashboardMetrics:
    """Return dashboard KPIs and time-series data for accessible boards."""
    primary = _resolve_range(range_key)
    comparison = _comparison_range(primary)
    board_ids = await _resolve_dashboard_board_ids(
        session,
        ctx=ctx,
        board_id=board_id,
        group_id=group_id,
    )

    throughput_primary = await _query_throughput(session, primary, board_ids)
    throughput_comparison = await _query_throughput(session, comparison, board_ids)
    throughput = DashboardSeriesSet(
        primary=throughput_primary,
        comparison=throughput_comparison,
    )
    cycle_time_primary = await _query_cycle_time(session, primary, board_ids)
    cycle_time_comparison = await _query_cycle_time(session, comparison, board_ids)
    cycle_time = DashboardSeriesSet(
        primary=cycle_time_primary,
        comparison=cycle_time_comparison,
    )
    error_rate_primary = await _query_error_rate(session, primary, board_ids)
    error_rate_comparison = await _query_error_rate(session, comparison, board_ids)
    error_rate = DashboardSeriesSet(
        primary=error_rate_primary,
        comparison=error_rate_comparison,
    )
    wip_primary = await _query_wip(session, primary, board_ids)
    wip_comparison = await _query_wip(session, comparison, board_ids)
    wip = DashboardWipSeriesSet(
        primary=wip_primary,
        comparison=wip_comparison,
    )
    task_status_counts = await _task_status_counts(session, board_ids)
    pending_approvals = await _pending_approvals_snapshot(session, board_ids, limit=10)

    kpis = DashboardKpis(
        active_agents=await _active_agents(session, primary, board_ids),
        tasks_in_progress=task_status_counts["in_progress"],
        inbox_tasks=task_status_counts["inbox"],
        in_progress_tasks=task_status_counts["in_progress"],
        review_tasks=task_status_counts["review"],
        done_tasks=task_status_counts["done"],
        error_rate_pct=await _error_rate_kpi(session, primary, board_ids),
        median_cycle_time_hours_7d=await _median_cycle_time_for_range(
            session,
            primary,
            board_ids,
        ),
    )

    return DashboardMetrics(
        range=primary.key,
        generated_at=utcnow(),
        kpis=kpis,
        throughput=throughput,
        cycle_time=cycle_time,
        error_rate=error_rate,
        wip=wip,
        pending_approvals=pending_approvals,
    )


def _runtime_safe_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return default
    return default


def _runtime_safe_str(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return None


def _runtime_safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _runtime_safe_dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


async def _runtime_fetch_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        response = await client.get(url, params=params)
    except httpx.HTTPError as exc:
        return None, f"{url} request failed: {exc}"

    if response.status_code != status.HTTP_200_OK:
        detail = response.text.strip()
        detail_text = f" ({detail[:200]})" if detail else ""
        return None, f"{url} returned {response.status_code}{detail_text}"

    try:
        payload = response.json()
    except ValueError:
        return None, f"{url} returned invalid JSON"

    if not isinstance(payload, dict):
        return None, f"{url} returned non-object JSON"
    return payload, None


def _runtime_agent_health(agent: dict[str, Any]) -> str:
    errors_15m = _runtime_safe_int(agent.get("errors_15m"))
    events_15m = _runtime_safe_int(agent.get("events_15m"))
    if errors_15m > 0:
        return "bad"
    if events_15m == 0:
        return "warn"
    return "ok"


@router.get("/runtime", response_model=RuntimeOpsMetrics)
async def runtime_metrics(
    window_minutes: int = RUNTIME_WINDOW_QUERY,
    event_limit: int = RUNTIME_EVENT_LIMIT_QUERY,
    agents_limit: int = RUNTIME_AGENTS_LIMIT_QUERY,
    _: AsyncSession = SESSION_DEP,
    __: OrganizationContext = ORG_MEMBER_DEP,
) -> RuntimeOpsMetrics:
    """Return runtime telemetry bridged from the local OpenClaw ops dashboard API."""
    source_url = settings.runtime_ops_source_url
    collected_at = utcnow()
    if not source_url:
        return RuntimeOpsMetrics(
            enabled=False,
            status="unavailable",
            source_url=None,
            collected_at=collected_at,
            window_minutes=window_minutes,
            notes=[
                "Runtime bridge is disabled. Set RUNTIME_OPS_SOURCE_URL to enable live OpenClaw telemetry.",
            ],
        )

    headers = {"User-Agent": "openclaw-mission-control/1.0"}
    if settings.runtime_ops_read_token.strip():
        headers["Authorization"] = f"Bearer {settings.runtime_ops_read_token.strip()}"

    timeout = httpx.Timeout(
        timeout=settings.runtime_ops_timeout_seconds,
        connect=min(settings.runtime_ops_timeout_seconds, 5.0),
    )
    notes: list[str] = []

    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        health_payload, health_error = await _runtime_fetch_json(client, f"{source_url}/health")
        if health_error:
            notes.append(health_error)

        auth_payload, auth_error = await _runtime_fetch_json(
            client,
            f"{source_url}/api/v1/auth/status",
        )
        if auth_error:
            notes.append(auth_error)

        overview_payload, overview_error = await _runtime_fetch_json(
            client,
            f"{source_url}/api/v1/ops/overview",
        )
        if overview_error:
            notes.append(overview_error)

        agents_payload, agents_error = await _runtime_fetch_json(
            client,
            f"{source_url}/api/v1/agents/status",
            params={"limit": agents_limit},
        )
        if agents_error:
            notes.append(agents_error)

        signatures_payload, signatures_error = await _runtime_fetch_json(
            client,
            f"{source_url}/api/v1/errors/signatures",
            params={"window_minutes": window_minutes, "limit": 12},
        )
        if signatures_error:
            notes.append(signatures_error)

        live_probe_payload, live_probe_error = await _runtime_fetch_json(
            client,
            f"{source_url}/api/v1/live/events",
            params={"after": 0, "limit": 1},
        )
        if live_probe_error:
            notes.append(live_probe_error)

        live_events_payload: dict[str, Any] | None = None
        if live_probe_payload:
            probe_window = _runtime_safe_dict(live_probe_payload.get("window"))
            window_max = _runtime_safe_int(probe_window.get("max"))
            recent_after = max(0, window_max - event_limit)
            live_events_payload, live_events_error = await _runtime_fetch_json(
                client,
                f"{source_url}/api/v1/live/events",
                params={"after": recent_after, "limit": event_limit},
            )
            if live_events_error:
                notes.append(live_events_error)

    overview = _runtime_safe_dict(overview_payload)
    latest_reliability = _runtime_safe_dict(overview.get("latest_reliability"))
    incidents = _runtime_safe_dict_list(overview.get("open_incidents"))[:8]
    open_incidents_count = _runtime_safe_int(overview.get("open_incidents_count"))
    errors_15m = _runtime_safe_int(overview.get("errors_15m"))
    commands_1h = _runtime_safe_int(overview.get("commands_1h"))
    command_failures_1h = _runtime_safe_int(overview.get("command_failures_1h"))

    agents_raw = _runtime_safe_dict_list(_runtime_safe_dict(agents_payload).get("agents"))
    agents: list[dict[str, Any]] = []
    for agent in agents_raw:
        agent_out = {
            "agent_id": _runtime_safe_str(agent.get("agent_id")) or "unknown",
            "last_seen": _runtime_safe_str(agent.get("last_seen")),
            "events_15m": _runtime_safe_int(agent.get("events_15m")),
            "errors_15m": _runtime_safe_int(agent.get("errors_15m")),
            "last_event_type": _runtime_safe_str(agent.get("last_event_type")),
            "last_severity": _runtime_safe_str(agent.get("last_severity")),
            "last_run_id": _runtime_safe_str(agent.get("last_run_id")),
            "last_session_id": _runtime_safe_str(agent.get("last_session_id")),
            "health": _runtime_agent_health(agent),
        }
        agents.append(agent_out)
    agents.sort(
        key=lambda item: (
            _runtime_safe_int(item.get("errors_15m"), 0),
            _runtime_safe_int(item.get("events_15m"), 0),
        ),
        reverse=True,
    )

    signatures_data = _runtime_safe_dict(signatures_payload)
    signatures = _runtime_safe_dict_list(signatures_data.get("signatures"))[:12]
    providers = _runtime_safe_dict_list(signatures_data.get("providers"))[:10]
    provider_probe = _runtime_safe_dict(signatures_data.get("provider_probe"))

    live_events_source = live_events_payload or live_probe_payload or {}
    live_events = _runtime_safe_dict_list(live_events_source.get("events"))
    live_events = list(reversed(live_events[-event_limit:]))

    health_ok = bool(_runtime_safe_dict(health_payload).get("ok"))
    auth_mode = _runtime_safe_str(_runtime_safe_dict(auth_payload).get("mode"))

    runtime_status: Literal["ok", "degraded", "unavailable"]
    if not any([overview_payload, agents_payload, signatures_payload, live_events_payload]):
        runtime_status = "unavailable"
    elif (
        not health_ok
        or open_incidents_count > 0
        or any(agent.get("health") == "bad" for agent in agents)
        or len(notes) > 0
    ):
        runtime_status = "degraded"
    else:
        runtime_status = "ok"

    return RuntimeOpsMetrics(
        enabled=True,
        status=runtime_status,
        source_url=source_url,
        collected_at=collected_at,
        window_minutes=window_minutes,
        health_ok=health_ok,
        auth_mode=auth_mode,
        open_incidents_count=open_incidents_count,
        errors_15m=errors_15m,
        commands_1h=commands_1h,
        command_failures_1h=command_failures_1h,
        latest_reliability=latest_reliability,
        provider_probe=provider_probe,
        providers=providers,
        agents=agents,
        incidents=incidents,
        signatures=signatures,
        live_events=live_events,
        notes=notes,
    )
