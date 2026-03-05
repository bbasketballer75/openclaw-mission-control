"""Dashboard metrics schemas for KPI and time-series API responses."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from sqlmodel import Field, SQLModel

RUNTIME_ANNOTATION_TYPES = (datetime, UUID)
DashboardRangeKey = Literal["24h", "3d", "7d", "14d", "1m", "3m", "6m", "1y"]
DashboardBucketKey = Literal["hour", "day", "week", "month"]


class DashboardSeriesPoint(SQLModel):
    """Single numeric time-series point."""

    period: datetime
    value: float


class DashboardWipPoint(SQLModel):
    """Work-in-progress point split by task status buckets."""

    period: datetime
    inbox: int
    in_progress: int
    review: int
    done: int


class DashboardRangeSeries(SQLModel):
    """Series payload for a single range/bucket combination."""

    range: DashboardRangeKey
    bucket: DashboardBucketKey
    points: list[DashboardSeriesPoint]


class DashboardWipRangeSeries(SQLModel):
    """WIP series payload for a single range/bucket combination."""

    range: DashboardRangeKey
    bucket: DashboardBucketKey
    points: list[DashboardWipPoint]


class DashboardSeriesSet(SQLModel):
    """Primary vs comparison pair for generic series metrics."""

    primary: DashboardRangeSeries
    comparison: DashboardRangeSeries


class DashboardWipSeriesSet(SQLModel):
    """Primary vs comparison pair for WIP status series metrics."""

    primary: DashboardWipRangeSeries
    comparison: DashboardWipRangeSeries


class DashboardKpis(SQLModel):
    """Topline dashboard KPI summary values."""

    active_agents: int
    tasks_in_progress: int
    inbox_tasks: int
    in_progress_tasks: int
    review_tasks: int
    done_tasks: int
    error_rate_pct: float
    median_cycle_time_hours_7d: float | None


class DashboardPendingApproval(SQLModel):
    """Single pending approval item for cross-board dashboard listing."""

    approval_id: UUID
    board_id: UUID
    board_name: str
    action_type: str
    confidence: float
    created_at: datetime
    task_title: str | None = None


class DashboardPendingApprovals(SQLModel):
    """Pending approval snapshot used on the dashboard."""

    total: int
    items: list[DashboardPendingApproval]


class DashboardMetrics(SQLModel):
    """Complete dashboard metrics response payload."""

    range: DashboardRangeKey
    generated_at: datetime
    kpis: DashboardKpis
    throughput: DashboardSeriesSet
    cycle_time: DashboardSeriesSet
    error_rate: DashboardSeriesSet
    wip: DashboardWipSeriesSet
    pending_approvals: DashboardPendingApprovals


class RuntimeOpsMetrics(SQLModel):
    """Runtime telemetry bridge payload used by Mission Control dashboard."""

    enabled: bool
    status: Literal["ok", "degraded", "unavailable"]
    source_url: str | None = None
    collected_at: datetime
    window_minutes: int
    health_ok: bool = False
    auth_mode: str | None = None
    open_incidents_count: int = 0
    errors_15m: int = 0
    commands_1h: int = 0
    command_failures_1h: int = 0
    latest_reliability: dict[str, Any] = Field(default_factory=dict)
    provider_probe: dict[str, Any] = Field(default_factory=dict)
    providers: list[dict[str, Any]] = Field(default_factory=list)
    agents: list[dict[str, Any]] = Field(default_factory=list)
    incidents: list[dict[str, Any]] = Field(default_factory=list)
    signatures: list[dict[str, Any]] = Field(default_factory=list)
    live_events: list[dict[str, Any]] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
