"use client";

export const dynamic = "force-dynamic";

import { type KeyboardEvent, type MouseEvent, useCallback, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";

import { SignedIn, SignedOut, useAuth } from "@/auth/clerk";
import {
  Activity,
  ArrowUpRight,
  Bot,
  Check,
  ClipboardCopy,
  ExternalLink,
  Info,
  LayoutGrid,
  Radio,
  Shield,
  TriangleAlert,
  Timer,
} from "lucide-react";

import { DashboardSidebar } from "@/components/organisms/DashboardSidebar";
import { DashboardShell } from "@/components/templates/DashboardShell";
import { Markdown } from "@/components/atoms/Markdown";
import { SignedOutPanel } from "@/components/auth/SignedOutPanel";
import { ApiError, customFetch } from "@/api/mutator";
import {
  type dashboardMetricsApiV1MetricsDashboardGetResponse,
  useDashboardMetricsApiV1MetricsDashboardGet,
} from "@/api/generated/metrics/metrics";
import {
  gatewaysStatusApiV1GatewaysStatusGet,
} from "@/api/generated/gateways/gateways";
import type { GatewaysStatusResponse } from "@/api/generated/model/gatewaysStatusResponse";
import {
  type listAgentsApiV1AgentsGetResponse,
  useListAgentsApiV1AgentsGet,
} from "@/api/generated/agents/agents";
import {
  type listBoardsApiV1BoardsGetResponse,
  useListBoardsApiV1BoardsGet,
} from "@/api/generated/boards/boards";
import {
  type listActivityApiV1ActivityGetResponse,
  useListActivityApiV1ActivityGet,
} from "@/api/generated/activity/activity";
import type { ActivityEventRead } from "@/api/generated/model";
import {
  formatRelativeTimestamp,
  formatTimestamp,
  parseTimestamp,
} from "@/lib/formatters";

type SessionSummary = {
  key: string;
  title: string;
  subtitle: string;
  usage: string;
  lastSeenAt: string | null;
  isMain: boolean;
};

type SummaryRow = {
  label: string;
  value: string;
  tone?: "default" | "success" | "warning" | "danger";
};

type GatewayTarget = {
  gatewayId: string;
  boardId: string;
  boardName: string;
};

type GatewaySnapshot = GatewayTarget & {
  connected: boolean;
  gatewayUrl: string | null;
  sessionsCount: number;
  sessions: unknown[];
  mainSession: unknown | null;
  mainSessionError: string | null;
  error: string | null;
  requestError: string | null;
};

type RuntimeOpsStatus = "ok" | "degraded" | "unavailable";

type RuntimeOpsResponse = {
  enabled: boolean;
  status: RuntimeOpsStatus;
  source_url: string | null;
  collected_at: string;
  window_minutes: number;
  health_ok: boolean;
  auth_mode: string | null;
  open_incidents_count: number;
  errors_15m: number;
  commands_1h: number;
  command_failures_1h: number;
  latest_reliability: Record<string, unknown>;
  provider_probe: Record<string, unknown>;
  providers: Array<Record<string, unknown>>;
  agents: Array<Record<string, unknown>>;
  incidents: Array<Record<string, unknown>>;
  signatures: Array<Record<string, unknown>>;
  live_events: Array<Record<string, unknown>>;
  notes: string[];
};

const DASH = "—";
const DASHBOARD_RANGE = "7d";
const DASHBOARD_RANGE_DAYS = 7;
const DASHBOARD_RANGE_LABEL = "7 days";

const numberFormatter = new Intl.NumberFormat("en-US");
const SESSION_ID_KEYS = ["key", "id", "session_key", "sessionKey", "sessionId"];

const toRecord = (value: unknown): Record<string, unknown> | null => {
  if (!value || Array.isArray(value) || typeof value !== "object") return null;
  return value as Record<string, unknown>;
};

const readString = (
  record: Record<string, unknown> | null,
  keys: string[],
): string | null => {
  if (!record) return null;
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
};

const readNumber = (
  record: Record<string, unknown> | null,
  keys: string[],
): number | null => {
  if (!record) return null;
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string") {
      const cleaned = value.replace(/[^0-9.-]/g, "");
      const parsed = Number.parseFloat(cleaned);
      if (Number.isFinite(parsed)) return parsed;
    }
  }
  return null;
};

const readStringFromRecords = (
  records: Array<Record<string, unknown> | null>,
  keys: string[],
): string | null => {
  for (const record of records) {
    const value = readString(record, keys);
    if (value) return value;
  }
  return null;
};

const readNumberFromRecords = (
  records: Array<Record<string, unknown> | null>,
  keys: string[],
): number | null => {
  for (const record of records) {
    const value = readNumber(record, keys);
    if (value !== null) return value;
  }
  return null;
};

const normalizeEpochMs = (value: number): number => {
  if (value >= 1_000_000_000_000) return value;
  if (value >= 1_000_000_000) return value * 1000;
  return value;
};

const readTimestamp = (
  record: Record<string, unknown> | null,
  keys: string[],
): string | null => {
  if (!record) return null;
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      const date = new Date(normalizeEpochMs(value));
      if (!Number.isNaN(date.getTime())) return date.toISOString();
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (!trimmed) continue;
      const numeric = Number.parseFloat(trimmed);
      if (Number.isFinite(numeric)) {
        const date = new Date(normalizeEpochMs(numeric));
        if (!Number.isNaN(date.getTime())) return date.toISOString();
      }
      const parsed = parseTimestamp(trimmed);
      if (parsed) return parsed.toISOString();
    }
  }
  return null;
};

const readTimestampFromRecords = (
  records: Array<Record<string, unknown> | null>,
  keys: string[],
): string | null => {
  for (const record of records) {
    const value = readTimestamp(record, keys);
    if (value) return value;
  }
  return null;
};

const sessionIdentifiers = (record: Record<string, unknown> | null): string[] => {
  if (!record) return [];
  const ids = SESSION_ID_KEYS.map((key) => readString(record, [key])).filter(Boolean) as string[];
  return [...new Set(ids)];
};

const sharesSessionIdentity = (left: string[], right: string[]): boolean =>
  left.some((value) => right.includes(value));

const compactNumber = (value: number): string => {
  if (!Number.isFinite(value)) return DASH;
  if (Math.abs(value) >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}m`;
  }
  if (Math.abs(value) >= 1_000) {
    return `${(value / 1_000).toFixed(1)}k`;
  }
  return numberFormatter.format(value);
};

const formatCount = (value: number): string =>
  Number.isFinite(value) ? numberFormatter.format(Math.max(0, Math.round(value))) : "0";

const formatPercent = (value: number): string =>
  Number.isFinite(value) ? `${value.toFixed(1)}%` : DASH;

const formatPerDay = (total: number, days: number): string => {
  if (!Number.isFinite(total) || !Number.isFinite(days) || days <= 0) return DASH;
  return `${(total / days).toFixed(1)}/day`;
};

const linesToAiPayload = (title: string, lines: string[]): string => {
  const timestamp = new Date().toISOString();
  return [
    `Mission Control Dashboard :: ${title}`,
    `Captured: ${timestamp}`,
    ...lines,
  ].join("\n");
};

const toSessionSummaries = (
  sessions: unknown[] | null | undefined,
  mainSession: unknown,
): SessionSummary[] => {
  const sessionRecords = (sessions ?? []).map(toRecord).filter(Boolean) as Array<
    Record<string, unknown>
  >;
  const mainRecord = toRecord(mainSession);
  const mainIdentifiers = sessionIdentifiers(mainRecord);

  if (mainRecord && mainIdentifiers.length > 0) {
    const exists = sessionRecords.some(
      (entry) => sharesSessionIdentity(sessionIdentifiers(entry), mainIdentifiers),
    );
    if (!exists) sessionRecords.unshift(mainRecord);
  }

  const uniqueRecords: Record<string, unknown>[] = [];
  const seenIdentifiers = new Set<string>();

  for (const entry of sessionRecords) {
    const identifiers = sessionIdentifiers(entry);
    if (identifiers.length > 0 && identifiers.some((value) => seenIdentifiers.has(value))) {
      continue;
    }
    uniqueRecords.push(entry);
    identifiers.forEach((value) => seenIdentifiers.add(value));
  }

  return uniqueRecords.map((entry, index) => {
    const usageRecord = toRecord(entry.usage);
    const statsRecord = toRecord(entry.stats);
    const metricsRecord = toRecord(entry.metrics);
    const originRecord = toRecord(entry.origin);
    const candidateRecords = [entry, usageRecord, statsRecord, metricsRecord];

    const identifiers = sessionIdentifiers(entry);
    const key =
      readString(entry, ["key", "session_key", "sessionKey", "id", "sessionId"]) ??
      `session-${index}`;
    const label = readString(entry, ["label", "name", "title"]) ?? key;
    const channel = readStringFromRecords([entry, originRecord], [
      "channel",
      "source",
      "kind",
      "chatType",
    ]);
    const model = readString(entry, ["model", "model_name", "provider", "engine"]);
    const modelProvider = readString(entry, ["modelProvider", "model_provider", "provider"]);
    const lastSeenAt = readTimestampFromRecords(candidateRecords, [
      "updated_at",
      "updatedAt",
      "last_updated_at",
      "lastUpdatedAt",
      "last_seen_at",
      "lastSeen",
      "last_seen",
      "last_active_at",
      "lastActiveAt",
      "lastActivityAt",
      "activityAt",
      "created_at",
      "createdAt",
    ]);

    const usedTokens = readNumberFromRecords(candidateRecords, [
      "used",
      "used_tokens",
      "tokens",
      "current",
      "token_count",
      "tokenCount",
      "totalTokens",
      "total_tokens",
      "inputTokens",
      "input_tokens",
    ]);
    const maxTokens = readNumberFromRecords(candidateRecords, [
      "max",
      "limit",
      "token_limit",
      "capacity",
      "max_tokens",
      "maxTokens",
      "context_window",
      "contextWindow",
      "contextTokens",
      "context_tokens",
      "maxContextTokens",
      "max_context_tokens",
    ]);

    const pctFromPayload = readNumberFromRecords(candidateRecords, [
      "pct",
      "percent",
      "ratio_pct",
      "ratioPct",
      "token_pct",
      "usage_pct",
      "percentUsed",
      "contextPercent",
    ]);
    const usagePct = Number.isFinite(pctFromPayload ?? NaN)
      ? Math.max(0, Math.min(100, Math.round(pctFromPayload ?? 0)))
      : usedTokens !== null && maxTokens !== null && maxTokens > 0
        ? Math.max(0, Math.min(100, Math.round((usedTokens / maxTokens) * 100)))
        : 0;

    const usage =
      usedTokens !== null && maxTokens !== null
        ? `${compactNumber(usedTokens)}/${compactNumber(maxTokens)} (${usagePct}%)`
        : usedTokens !== null
          ? `${compactNumber(usedTokens)} tokens`
          : DASH;

    const subtitleBits = [channel, model].filter(Boolean) as string[];
    const subtitle = subtitleBits.length > 0 ? subtitleBits.join(" · ") : "Session";
    const modelWithProvider =
      modelProvider && model && modelProvider !== model ? `${model} · ${modelProvider}` : model;
    const subtitleWithProvider = [channel, modelWithProvider].filter(Boolean).join(" · ");

    return {
      key,
      title: label,
      subtitle: subtitleWithProvider || subtitle,
      usage,
      lastSeenAt,
      isMain:
        mainIdentifiers.length > 0 &&
        sharesSessionIdentity(identifiers, mainIdentifiers),
    };
  });
};

function TopMetricCard({
  title,
  value,
  secondary,
  infoText,
  icon,
  accent,
}: {
  title: string;
  value: string;
  secondary?: string;
  infoText?: string;
  icon: React.ReactNode;
  accent: "blue" | "green" | "violet" | "emerald";
}) {
  const iconTone =
    accent === "blue"
      ? "bg-blue-50 text-blue-600"
      : accent === "green"
        ? "bg-emerald-50 text-emerald-600"
        : accent === "violet"
          ? "bg-violet-50 text-violet-600"
          : "bg-green-50 text-green-600";

  return (
    <section className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-1.5">
            <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              {title}
            </p>
            {infoText ? (
              <span
                className="inline-flex text-slate-400"
                title={infoText}
                aria-label={infoText}
              >
                <Info className="h-3.5 w-3.5" />
              </span>
            ) : null}
          </div>
          <div className="mt-2 flex items-end gap-2">
            <p className="font-heading text-4xl font-bold text-slate-900">{value}</p>
            {secondary ? (
              <p className="pb-1 text-xs text-slate-500">{secondary}</p>
            ) : null}
          </div>
        </div>
        <div className={`rounded-lg p-2 ${iconTone}`}>
          {icon}
        </div>
      </div>
    </section>
  );
}

function SectionCopyButton({
  sectionKey,
  payload,
  copiedSection,
  onCopy,
}: {
  sectionKey: string;
  payload: string;
  copiedSection: string | null;
  onCopy: (section: string, payload: string) => void;
}) {
  const copied = copiedSection === sectionKey;
  return (
    <button
      type="button"
      onClick={() => onCopy(sectionKey, payload)}
      className={`inline-flex items-center gap-1 rounded-md border px-2 py-1 text-[11px] font-medium transition ${
        copied
          ? "border-emerald-300 bg-emerald-50 text-emerald-700"
          : "border-slate-300 bg-white text-slate-600 hover:border-slate-400 hover:text-slate-800"
      }`}
      title="Copy section snapshot for AI handoff"
      aria-label="Copy section snapshot for AI handoff"
    >
      {copied ? <Check className="h-3.5 w-3.5" /> : <ClipboardCopy className="h-3.5 w-3.5" />}
      {copied ? "Copied" : "Copy for AI"}
    </button>
  );
}

function InfoBlock({
  title,
  badge,
  infoText,
  action,
  rows,
}: {
  title: string;
  badge?: { text: string; tone: "online" | "offline" | "neutral" };
  infoText?: string;
  action?: React.ReactNode;
  rows: SummaryRow[];
}) {
  return (
    <section className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
      <div className="mb-4 flex items-center justify-between gap-3">
        <div className="flex items-center gap-1.5">
          <h3 className="text-lg font-semibold text-slate-900">{title}</h3>
          {infoText ? (
            <span
              className="inline-flex text-slate-400"
              title={infoText}
              aria-label={infoText}
            >
              <Info className="h-3.5 w-3.5" />
            </span>
            ) : null}
        </div>
        <div className="flex items-center gap-2">
          {action}
          {badge ? (
            <span
              className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium ${
                badge.tone === "online"
                  ? "bg-emerald-100 text-emerald-700"
                  : badge.tone === "offline"
                    ? "bg-rose-100 text-rose-700"
                    : "bg-slate-200 text-slate-700"
              }`}
            >
              {badge.text}
            </span>
          ) : null}
        </div>
      </div>
      <div className="divide-y divide-slate-100 rounded-lg border border-slate-200 bg-white">
        {rows.map((row) => (
          <div key={`${row.label}-${row.value}`} className="flex items-start justify-between gap-3 px-3 py-2">
            <span className="min-w-0 text-sm text-slate-500">{row.label}</span>
            <span
              className={`max-w-[65%] break-words text-right text-sm font-medium leading-5 ${
                row.tone === "success"
                  ? "text-emerald-700"
                  : row.tone === "warning"
                    ? "text-amber-700"
                    : row.tone === "danger"
                      ? "text-rose-700"
                      : "text-slate-800"
              }`}
            >
              {row.value}
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}

export default function DashboardPage() {
  const router = useRouter();
  const { isSignedIn } = useAuth();
  const [copiedSection, setCopiedSection] = useState<string | null>(null);

  const copySectionForAi = useCallback((section: string, payload: string) => {
    const copyWithFallback = () => {
      const textarea = document.createElement("textarea");
      textarea.value = payload;
      textarea.setAttribute("readonly", "true");
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      document.body.removeChild(textarea);
    };

    const markCopied = () => {
      setCopiedSection(section);
      window.setTimeout(() => {
        setCopiedSection((active) => (active === section ? null : active));
      }, 2200);
    };

    if (navigator.clipboard?.writeText) {
      navigator.clipboard
        .writeText(payload)
        .then(markCopied)
        .catch(() => {
          copyWithFallback();
          markCopied();
        });
      return;
    }

    copyWithFallback();
    markCopied();
  }, []);

  const boardsQuery = useListBoardsApiV1BoardsGet<listBoardsApiV1BoardsGetResponse, ApiError>(
    { limit: 200 },
    {
      query: {
        enabled: Boolean(isSignedIn),
        refetchInterval: 30_000,
        refetchOnMount: "always",
      },
    },
  );

  const agentsQuery = useListAgentsApiV1AgentsGet<listAgentsApiV1AgentsGetResponse, ApiError>(
    { limit: 200 },
    {
      query: {
        enabled: Boolean(isSignedIn),
        refetchInterval: 15_000,
        refetchOnMount: "always",
      },
    },
  );

  const metricsQuery = useDashboardMetricsApiV1MetricsDashboardGet<
    dashboardMetricsApiV1MetricsDashboardGetResponse,
    ApiError
  >(
    {
      range_key: DASHBOARD_RANGE,
    },
    {
      query: {
        enabled: Boolean(isSignedIn),
        refetchInterval: 15_000,
        refetchOnMount: "always",
      },
    },
  );

  const activityQuery = useListActivityApiV1ActivityGet<listActivityApiV1ActivityGetResponse, ApiError>(
    { limit: 200 },
    {
      query: {
        enabled: Boolean(isSignedIn),
        refetchInterval: 15_000,
        refetchOnMount: "always",
      },
    },
  );

  const runtimeOpsQuery = useQuery<RuntimeOpsResponse, ApiError>({
    queryKey: ["dashboard", "runtime-ops"],
    enabled: Boolean(isSignedIn),
    refetchInterval: 20_000,
    refetchOnMount: "always",
    queryFn: async () => {
      const response = await customFetch<{
        data: RuntimeOpsResponse;
        status: number;
        headers: Headers;
      }>("/api/v1/metrics/runtime", { method: "GET" });
      return response.data;
    },
  });

  const boards = useMemo(
    () =>
      boardsQuery.data?.status === 200
        ? [...(boardsQuery.data.data.items ?? [])].sort((a, b) => a.name.localeCompare(b.name))
        : [],
    [boardsQuery.data],
  );

  const agents = useMemo(
    () =>
      agentsQuery.data?.status === 200
        ? [...(agentsQuery.data.data.items ?? [])].sort((a, b) => a.name.localeCompare(b.name))
        : [],
    [agentsQuery.data],
  );

  const metrics = metricsQuery.data?.status === 200 ? metricsQuery.data.data : null;

  const onlineAgents = useMemo(
    () => agents.filter((agent) => (agent.status ?? "").toLowerCase() === "online").length,
    [agents],
  );
  const gatewayTargets = useMemo<GatewayTarget[]>(() => {
    const byGateway = new Map<string, GatewayTarget>();
    for (const board of boards) {
      const gatewayId = board.gateway_id;
      if (!gatewayId) continue;
      if (byGateway.has(gatewayId)) continue;
      byGateway.set(gatewayId, {
        gatewayId,
        boardId: board.id,
        boardName: board.name,
      });
    }
    return [...byGateway.values()].sort((a, b) => a.boardName.localeCompare(b.boardName));
  }, [boards]);
  const hasConfiguredGateways = gatewayTargets.length > 0;

  const gatewayStatusesQuery = useQuery<GatewaySnapshot[], ApiError>({
    queryKey: [
      "dashboard",
      "gateway-statuses",
      gatewayTargets.map((target) => `${target.gatewayId}:${target.boardId}`),
    ],
    enabled: Boolean(isSignedIn && hasConfiguredGateways),
    refetchInterval: 15_000,
    refetchOnMount: "always",
    queryFn: async ({ signal }) => {
      return Promise.all(
        gatewayTargets.map(async (target): Promise<GatewaySnapshot> => {
          try {
            const response = await gatewaysStatusApiV1GatewaysStatusGet(
              { board_id: target.boardId },
              { signal },
            );
            if (response.status !== 200) {
              return {
                ...target,
                connected: false,
                gatewayUrl: null,
                sessionsCount: 0,
                sessions: [],
                mainSession: null,
                mainSessionError: null,
                error: null,
                requestError: `Gateway status request failed (${response.status})`,
              };
            }
            const payload: GatewaysStatusResponse = response.data;
            return {
              ...target,
              connected: Boolean(payload.connected),
              gatewayUrl: payload.gateway_url ?? null,
              sessionsCount: Number(payload.sessions_count ?? 0),
              sessions: Array.isArray(payload.sessions) ? payload.sessions : [],
              mainSession: payload.main_session ?? null,
              mainSessionError: payload.main_session_error ?? null,
              error: payload.error ?? null,
              requestError: null,
            };
          } catch (error) {
            if (signal.aborted) throw error;
            return {
              ...target,
              connected: false,
              gatewayUrl: null,
              sessionsCount: 0,
              sessions: [],
              mainSession: null,
              mainSessionError: null,
              error: null,
              requestError:
                error instanceof Error ? error.message : "Gateway status request failed.",
            };
          }
        }),
      );
    },
  });

  const gatewaySnapshots = useMemo(
    () => gatewayStatusesQuery.data ?? [],
    [gatewayStatusesQuery.data],
  );
  const sessionSummaries = useMemo(
    () =>
      gatewaySnapshots.flatMap((snapshot) => {
        if (snapshot.requestError) return [];
        const sourceLabel = snapshot.gatewayUrl || snapshot.boardName;
        return toSessionSummaries(snapshot.sessions, snapshot.mainSession).map((session) => ({
          ...session,
          key: `${snapshot.gatewayId}:${session.key}`,
          subtitle: `${sourceLabel} · ${session.subtitle}`,
        }));
      }),
    [gatewaySnapshots],
  );

  const activityEvents = useMemo(
    () =>
      activityQuery.data?.status === 200
        ? [...(activityQuery.data.data.items ?? [])]
        : [],
    [activityQuery.data],
  );

  const orderedActivityEvents = useMemo(
    () =>
      [...activityEvents].sort((a, b) => {
        const left = parseTimestamp(a.created_at)?.getTime() ?? 0;
        const right = parseTimestamp(b.created_at)?.getTime() ?? 0;
        return right - left;
      }),
    [activityEvents],
  );

  const recentLogs = orderedActivityEvents.slice(0, 8);

  const latestThroughputPoint =
    metrics?.throughput.primary.points?.[metrics.throughput.primary.points.length - 1] ?? null;
  const throughputTotal = (metrics?.throughput.primary.points ?? []).reduce(
    (sum, point) => sum + Number(point.value ?? 0),
    0,
  );
  const completionDaysCount = (metrics?.throughput.primary.points ?? []).reduce(
    (sum, point) => sum + (Number(point.value ?? 0) > 0 ? 1 : 0),
    0,
  );

  const inboxTasksMetric = metrics?.kpis.inbox_tasks ?? 0;
  const inProgressTasksMetric = metrics?.kpis.in_progress_tasks ?? 0;
  const reviewTasksMetric = metrics?.kpis.review_tasks ?? 0;
  const doneTasksMetric = metrics?.kpis.done_tasks ?? 0;

  const activeAgentsMetric = onlineAgents;
  const tasksTotal = inboxTasksMetric + inProgressTasksMetric + reviewTasksMetric + doneTasksMetric;
  const tasksInProgressMetric = metrics?.kpis.tasks_in_progress ?? inProgressTasksMetric;
  const errorRateMetric = Number(metrics?.kpis.error_rate_pct ?? 0);
  const reviewBacklogRatio =
    inProgressTasksMetric > 0 ? reviewTasksMetric / inProgressTasksMetric : null;

  const gatewayConnectedCount = gatewaySnapshots.filter(
    (snapshot) => !snapshot.requestError && snapshot.connected,
  ).length;
  const gatewayDisconnectedCount = gatewaySnapshots.filter(
    (snapshot) => !snapshot.requestError && !snapshot.connected,
  ).length;
  const gatewayUnavailableCount = gatewaySnapshots.filter(
    (snapshot) => Boolean(snapshot.requestError),
  ).length;
  const gatewayHealthErrorCount = gatewaySnapshots.filter(
    (snapshot) => Boolean(snapshot.error || snapshot.mainSessionError),
  ).length;

  const countedSessions = gatewaySnapshots.reduce(
    (sum, snapshot) => sum + Math.max(0, snapshot.sessionsCount),
    0,
  );
  const activeSessions = Math.max(countedSessions, sessionSummaries.length);

  const gatewayStatusLabel = !hasConfiguredGateways
    ? "Not configured"
    : gatewayStatusesQuery.isLoading
      ? "Checking"
      : gatewayConnectedCount === gatewayTargets.length
        ? "All connected"
        : gatewayConnectedCount > 0
          ? "Partially connected"
          : gatewayUnavailableCount === gatewayTargets.length
            ? "Unavailable"
            : "Disconnected";
  const gatewayBadgeTone: "online" | "offline" | "neutral" =
    gatewayStatusLabel === "All connected"
      ? "online"
      : gatewayStatusLabel === "Partially connected" ||
          gatewayStatusLabel === "Disconnected" ||
          gatewayStatusLabel === "Unavailable"
        ? "offline"
        : "neutral";
  const gatewayStatusTone: SummaryRow["tone"] =
    gatewayStatusLabel === "All connected"
      ? "success"
      : gatewayStatusLabel === "Checking" || gatewayStatusLabel === "Not configured"
        ? "default"
        : gatewayStatusLabel === "Partially connected" || gatewayStatusLabel === "Disconnected"
          ? "warning"
          : "danger";

  const workloadRows: SummaryRow[] = [
    {
      label: "Total work items",
      value: formatCount(tasksTotal),
    },
    {
      label: "Inbox",
      value: formatCount(inboxTasksMetric),
    },
    {
      label: "In progress",
      value: formatCount(inProgressTasksMetric),
      tone: inProgressTasksMetric > 0 ? "warning" : "default",
    },
    {
      label: "In review",
      value: formatCount(reviewTasksMetric),
    },
    {
      label: "Completed",
      value: formatCount(doneTasksMetric),
      tone: doneTasksMetric > 0 ? "success" : "default",
    },
  ];

  const throughputRows: SummaryRow[] = [
    {
      label: "Completed tasks",
      value: formatCount(throughputTotal),
    },
    { label: "Average throughput", value: formatPerDay(throughputTotal, DASHBOARD_RANGE_DAYS) },
    {
      label: "Error rate",
      value: formatPercent(errorRateMetric),
      tone: errorRateMetric > 0 ? "warning" : "success",
    },
    {
      label: "Completion consistency",
      value: `${formatCount(completionDaysCount)} active days`,
      tone: completionDaysCount >= Math.ceil(DASHBOARD_RANGE_DAYS * 0.75) ? "success" : "default",
    },
    {
      label: "Review backlog ratio",
      value:
        reviewBacklogRatio !== null
          ? `${reviewBacklogRatio.toFixed(2)}x`
          : reviewTasksMetric > 0
            ? "∞"
            : "0.00x",
      tone:
        reviewBacklogRatio !== null
          ? reviewBacklogRatio > 1
            ? "warning"
            : "success"
          : reviewTasksMetric > 0
            ? "warning"
            : "success",
    },
  ];

  const gatewayRows: SummaryRow[] = [
    { label: "Gateway status", value: gatewayStatusLabel, tone: gatewayStatusTone },
    { label: "Configured gateways", value: formatCount(gatewayTargets.length) },
    {
      label: "Connected gateways",
      value: formatCount(gatewayConnectedCount),
      tone: gatewayConnectedCount > 0 ? "success" : "default",
    },
    {
      label: "Unavailable gateways",
      value: formatCount(gatewayUnavailableCount),
      tone: gatewayUnavailableCount > 0 ? "danger" : "default",
    },
    {
      label: "Gateways with issues",
      value: formatCount(gatewayHealthErrorCount + gatewayDisconnectedCount),
      tone: gatewayHealthErrorCount + gatewayDisconnectedCount > 0 ? "warning" : "success",
    },
  ];

  const runtimeOps = runtimeOpsQuery.data ?? null;
  const runtimeAgents = useMemo(
    () =>
      (runtimeOps?.agents ?? [])
        .map(toRecord)
        .filter(Boolean)
        .map((record) => {
          const health = readString(record, ["health"]) ?? "warn";
          const normalizedHealth =
            health === "ok" || health === "bad" || health === "warn" ? health : "warn";
          return {
            agentId: readString(record, ["agent_id"]) ?? "unknown",
            health: normalizedHealth,
            errors15m: readNumber(record, ["errors_15m"]) ?? 0,
            events15m: readNumber(record, ["events_15m"]) ?? 0,
            lastSeen: readTimestamp(record, ["last_seen"]),
            lastEventType: readString(record, ["last_event_type"]) ?? DASH,
          };
        }),
    [runtimeOps],
  );
  const runtimeSignatures = useMemo(
    () =>
      (runtimeOps?.signatures ?? [])
        .map(toRecord)
        .filter(Boolean)
        .map((record) => ({
          signature: readString(record, ["signature"]) ?? "unknown signature",
          source: readString(record, ["source"]) ?? "unknown source",
          category: readString(record, ["category"]) ?? "unknown",
          provider: readString(record, ["provider"]) ?? "unknown",
          count: readNumber(record, ["count"]) ?? 0,
          sample: readString(record, ["sample"]) ?? DASH,
        })),
    [runtimeOps],
  );
  const runtimeLiveEvents = useMemo(
    () =>
      (runtimeOps?.live_events ?? [])
        .map(toRecord)
        .filter(Boolean)
        .map((record) => ({
          ts: readTimestamp(record, ["ts"]),
          source: readString(record, ["source"]) ?? "unknown",
          eventType: readString(record, ["event_type"]) ?? "event",
          severity: readString(record, ["severity"]) ?? "info",
          agentId: readString(record, ["agent_id"]),
        })),
    [runtimeOps],
  );

  const runtimeStatusLabel = !runtimeOps
    ? runtimeOpsQuery.isLoading
      ? "Loading"
      : "Unavailable"
    : runtimeOps.status === "ok"
      ? "Healthy"
      : runtimeOps.status === "degraded"
        ? "Degraded"
        : "Unavailable";
  const runtimeBadgeTone: "online" | "offline" | "neutral" =
    runtimeStatusLabel === "Healthy"
      ? "online"
      : runtimeStatusLabel === "Degraded" || runtimeStatusLabel === "Unavailable"
        ? "offline"
        : "neutral";
  const runtimeBadAgentCount = runtimeAgents.filter((agent) => agent.health === "bad").length;
  const runtimeWarnAgentCount = runtimeAgents.filter((agent) => agent.health === "warn").length;
  const runtimeHealthyAgentCount = runtimeAgents.filter((agent) => agent.health === "ok").length;
  const runtimeProviderProbe = toRecord(runtimeOps?.provider_probe ?? null);
  const providerProbePass = readNumber(runtimeProviderProbe, ["pass_count"]) ?? 0;
  const providerProbeTotal = readNumber(runtimeProviderProbe, ["total"]) ?? 0;
  const providerProbeTime = readTimestamp(runtimeProviderProbe, ["time"]);
  const runtimeNotes = runtimeOps?.notes ?? [];
  const runtimeSourceUrl = runtimeOps?.source_url ?? null;

  const workloadSharePayload = useMemo(
    () =>
      linesToAiPayload(
        "Workload",
        workloadRows.map((row) => `${row.label}: ${row.value}`),
      ),
    [workloadRows],
  );
  const throughputSharePayload = useMemo(
    () =>
      linesToAiPayload(
        "Throughput",
        throughputRows.map((row) => `${row.label}: ${row.value}`),
      ),
    [throughputRows],
  );
  const gatewaySharePayload = useMemo(
    () =>
      linesToAiPayload(
        "Gateway Health",
        gatewayRows.map((row) => `${row.label}: ${row.value}`),
      ),
    [gatewayRows],
  );

  const pendingApprovalItems = metrics?.pending_approvals.items ?? [];
  const pendingApprovalsTotal = metrics?.pending_approvals.total ?? 0;
  const hasPendingApprovals = pendingApprovalItems.length > 0;
  const pendingApprovalsSharePayload = useMemo(
    () =>
      linesToAiPayload("Pending Approvals", [
        `Total pending approvals: ${formatCount(pendingApprovalsTotal)}`,
        ...pendingApprovalItems.slice(0, 10).map((item) => {
          const title = item.task_title?.trim() || "Pending approval";
          return `- ${item.board_name}: ${title} | confidence=${item.confidence}% | created=${item.created_at}`;
        }),
      ]),
    [pendingApprovalItems, pendingApprovalsTotal],
  );

  const sessionsSharePayload = useMemo(
    () =>
      linesToAiPayload("Sessions", [
        `Active sessions: ${formatCount(activeSessions)}`,
        `Configured gateways: ${formatCount(gatewayTargets.length)}`,
        `Connected gateways: ${formatCount(gatewayConnectedCount)}`,
        ...sessionSummaries.slice(0, 12).map(
          (session) =>
            `- ${session.title} | ${session.subtitle} | usage=${session.usage} | last_seen=${session.lastSeenAt ?? "unknown"}`,
        ),
      ]),
    [activeSessions, gatewayConnectedCount, gatewayTargets.length, sessionSummaries],
  );

  const recentActivitySharePayload = useMemo(
    () =>
      linesToAiPayload("Recent Activity", [
        `Activity events shown: ${formatCount(recentLogs.length)}`,
        ...recentLogs.map(
          (event) =>
            `- ${event.event_type} | ${event.created_at} | ${(event.message ?? event.event_type).trim().slice(0, 180)}`,
        ),
      ]),
    [recentLogs],
  );

  const runtimeSharePayload = useMemo(
    () =>
      linesToAiPayload("Runtime Ops", [
        `Status: ${runtimeStatusLabel}`,
        `Source URL: ${runtimeSourceUrl ?? "not configured"}`,
        `Open incidents: ${formatCount(runtimeOps?.open_incidents_count ?? 0)}`,
        `Errors (15m): ${formatCount(runtimeOps?.errors_15m ?? 0)}`,
        `Commands (1h): ${formatCount(runtimeOps?.commands_1h ?? 0)}`,
        `Command failures (1h): ${formatCount(runtimeOps?.command_failures_1h ?? 0)}`,
        `Agents healthy/warn/bad: ${formatCount(runtimeHealthyAgentCount)}/${formatCount(runtimeWarnAgentCount)}/${formatCount(runtimeBadAgentCount)}`,
        ...runtimeSignatures.slice(0, 8).map(
          (item) =>
            `- ${item.provider}/${item.category} (${item.count}) :: ${item.sample.slice(0, 160)}`,
        ),
      ]),
    [
      runtimeBadAgentCount,
      runtimeHealthyAgentCount,
      runtimeOps,
      runtimeSignatures,
      runtimeSourceUrl,
      runtimeStatusLabel,
      runtimeWarnAgentCount,
    ],
  );

  const activityFeedHref = "/activity";

  const shouldIgnoreRowNavigation = (target: EventTarget | null): boolean => {
    if (!(target instanceof HTMLElement)) return false;
    return Boolean(target.closest("a"));
  };

  const buildActivityEventHref = (event: ActivityEventRead): string => {
    const routeName = event.route_name ?? null;
    const routeParams = event.route_params ?? {};

    if (routeName === "board.approvals") {
      const boardId = routeParams.boardId;
      if (boardId) {
        return `/boards/${encodeURIComponent(boardId)}/approvals`;
      }
    }

    if (routeName === "board") {
      const boardId = routeParams.boardId;
      if (boardId) {
        const params = new URLSearchParams();
        Object.entries(routeParams).forEach(([key, value]) => {
          if (key !== "boardId") params.set(key, value);
        });
        const query = params.toString();
        return query
          ? `/boards/${encodeURIComponent(boardId)}?${query}`
          : `/boards/${encodeURIComponent(boardId)}`;
      }
    }

    const params = new URLSearchParams(
      Object.keys(routeParams).length > 0
        ? routeParams
        : {
            eventId: event.id,
            eventType: event.event_type,
            createdAt: event.created_at,
          },
    );
    if (event.task_id && !params.has("taskId")) {
      params.set("taskId", event.task_id);
    }
    return `${activityFeedHref}?${params.toString()}`;
  };

  const navigateToActivityFeed = (href: string) => {
    router.push(href);
  };

  const handleLogRowClick = (
    event: MouseEvent<HTMLDivElement>,
    href: string,
  ) => {
    if (shouldIgnoreRowNavigation(event.target)) return;
    navigateToActivityFeed(href);
  };

  const handleLogRowKeyDown = (
    event: KeyboardEvent<HTMLDivElement>,
    href: string,
  ) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    if (shouldIgnoreRowNavigation(event.target)) return;
    event.preventDefault();
    navigateToActivityFeed(href);
  };

  return (
    <DashboardShell>
      <SignedOut>
        <SignedOutPanel
          message="Sign in to access the dashboard."
          forceRedirectUrl="/onboarding"
          signUpForceRedirectUrl="/onboarding"
        />
      </SignedOut>
      <SignedIn>
        <DashboardSidebar />
        <main className="flex-1 overflow-y-auto bg-slate-50">
          <div className="p-8">
            {metricsQuery.error ? (
              <div className="mb-4 rounded-lg border border-rose-300 bg-rose-50 p-3 text-sm text-rose-700">
                {metricsQuery.error.message}
              </div>
            ) : null}
            {runtimeOpsQuery.error ? (
              <div className="mb-4 rounded-lg border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
                Runtime telemetry bridge is temporarily unavailable: {runtimeOpsQuery.error.message}
              </div>
            ) : null}

            <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
              <TopMetricCard
                title="Online Agents"
                value={formatCount(activeAgentsMetric)}
                secondary={`${formatCount(agents.length)} total`}
                icon={<Bot className="h-4 w-4" />}
                accent="blue"
              />
              <TopMetricCard
                title="Tasks In Progress"
                value={formatCount(tasksInProgressMetric)}
                secondary={`${formatCount(tasksTotal)} total`}
                icon={<LayoutGrid className="h-4 w-4" />}
                accent="green"
              />
              <TopMetricCard
                title="Error Rate"
                value={formatPercent(errorRateMetric)}
                secondary={`${formatCount(Number(latestThroughputPoint?.value ?? 0))} completed (latest)`}
                icon={<Activity className="h-4 w-4" />}
                accent="violet"
              />
              <TopMetricCard
                title="Completion Speed"
                value={formatPerDay(throughputTotal, DASHBOARD_RANGE_DAYS)}
                secondary={`${formatCount(throughputTotal)} completed`}
                infoText={`Based on ${DASHBOARD_RANGE_LABEL}`}
                icon={<Timer className="h-4 w-4" />}
                accent="emerald"
              />
            </div>

            <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-3">
              <InfoBlock
                title="Workload"
                action={
                  <SectionCopyButton
                    sectionKey="workload"
                    payload={workloadSharePayload}
                    copiedSection={copiedSection}
                    onCopy={copySectionForAi}
                  />
                }
                rows={workloadRows}
              />
              <InfoBlock
                title="Throughput"
                infoText={`All throughput values are calculated for ${DASHBOARD_RANGE_LABEL}`}
                action={
                  <SectionCopyButton
                    sectionKey="throughput"
                    payload={throughputSharePayload}
                    copiedSection={copiedSection}
                    onCopy={copySectionForAi}
                  />
                }
                rows={throughputRows}
              />
              <InfoBlock
                title="Gateway Health"
                action={
                  <SectionCopyButton
                    sectionKey="gateway-health"
                    payload={gatewaySharePayload}
                    copiedSection={copiedSection}
                    onCopy={copySectionForAi}
                  />
                }
                badge={{
                  text: gatewayStatusLabel,
                  tone: gatewayBadgeTone,
                }}
                rows={gatewayRows}
              />
            </div>

            <section className="mt-4 overflow-hidden rounded-2xl border border-slate-200 bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-6 text-slate-100 shadow-sm">
              <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
                <div>
                  <div className="flex items-center gap-2">
                    <h3 className="text-lg font-semibold tracking-tight text-white">Runtime Operations</h3>
                    <span
                      className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium ${
                        runtimeBadgeTone === "online"
                          ? "bg-emerald-500/20 text-emerald-200"
                          : runtimeBadgeTone === "offline"
                            ? "bg-rose-500/20 text-rose-200"
                            : "bg-slate-500/30 text-slate-200"
                      }`}
                    >
                      <Radio className="mr-1 h-3 w-3" />
                      {runtimeStatusLabel}
                    </span>
                  </div>
                  <p className="mt-1 text-xs text-slate-300">
                    Bridged from local OpenClaw ops telemetry with fresh tail events.
                  </p>
                </div>
                <div className="flex items-center gap-2">
                  <SectionCopyButton
                    sectionKey="runtime-ops"
                    payload={runtimeSharePayload}
                    copiedSection={copiedSection}
                    onCopy={copySectionForAi}
                  />
                  {runtimeSourceUrl ? (
                    <a
                      href={runtimeSourceUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex items-center gap-1 rounded-md border border-slate-500/70 bg-slate-700/40 px-2 py-1 text-[11px] font-medium text-slate-200 transition hover:border-slate-400 hover:text-white"
                    >
                      Open source
                      <ExternalLink className="h-3.5 w-3.5" />
                    </a>
                  ) : null}
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                <div className="rounded-xl border border-slate-600/60 bg-slate-900/35 p-3">
                  <p className="text-[11px] uppercase tracking-wider text-slate-300">Open Incidents</p>
                  <p className="mt-1 text-2xl font-semibold text-white">
                    {formatCount(runtimeOps?.open_incidents_count ?? 0)}
                  </p>
                </div>
                <div className="rounded-xl border border-slate-600/60 bg-slate-900/35 p-3">
                  <p className="text-[11px] uppercase tracking-wider text-slate-300">Errors (15m)</p>
                  <p className="mt-1 text-2xl font-semibold text-white">{formatCount(runtimeOps?.errors_15m ?? 0)}</p>
                </div>
                <div className="rounded-xl border border-slate-600/60 bg-slate-900/35 p-3">
                  <p className="text-[11px] uppercase tracking-wider text-slate-300">Agents (ok/warn/bad)</p>
                  <p className="mt-1 text-2xl font-semibold text-white">
                    {formatCount(runtimeHealthyAgentCount)}/{formatCount(runtimeWarnAgentCount)}/
                    {formatCount(runtimeBadAgentCount)}
                  </p>
                </div>
                <div className="rounded-xl border border-slate-600/60 bg-slate-900/35 p-3">
                  <p className="text-[11px] uppercase tracking-wider text-slate-300">Provider Probe</p>
                  <p className="mt-1 text-2xl font-semibold text-white">
                    {formatCount(providerProbePass)}/{formatCount(providerProbeTotal)}
                  </p>
                  <p className="mt-1 text-[11px] text-slate-300">
                    {providerProbeTime ? formatRelativeTimestamp(providerProbeTime) : "No probe timestamp"}
                  </p>
                </div>
              </div>

              <div className="mt-3 grid grid-cols-1 gap-3 xl:grid-cols-3">
                <section className="rounded-xl border border-slate-600/60 bg-slate-900/35 p-3">
                  <h4 className="text-sm font-semibold text-white">Hot Agents</h4>
                  <div className="mt-2 space-y-2">
                    {runtimeAgents.slice(0, 6).map((agent) => (
                      <div
                        key={agent.agentId}
                        className="flex items-center justify-between gap-2 rounded-lg border border-slate-700/70 bg-slate-950/20 px-2 py-1.5"
                      >
                        <div className="min-w-0">
                          <p className="truncate text-xs font-medium text-slate-100">{agent.agentId}</p>
                          <p className="truncate text-[11px] text-slate-300">{agent.lastEventType}</p>
                        </div>
                        <div className="shrink-0 text-right text-[11px]">
                          <p
                            className={
                              agent.health === "bad"
                                ? "text-rose-300"
                                : agent.health === "warn"
                                  ? "text-amber-300"
                                  : "text-emerald-300"
                            }
                          >
                            e15m {formatCount(agent.errors15m)}
                          </p>
                          <p className="text-slate-300">{agent.lastSeen ? formatRelativeTimestamp(agent.lastSeen) : DASH}</p>
                        </div>
                      </div>
                    ))}
                    {runtimeAgents.length === 0 ? (
                      <p className="text-xs text-slate-300">No agent telemetry available yet.</p>
                    ) : null}
                  </div>
                </section>

                <section className="rounded-xl border border-slate-600/60 bg-slate-900/35 p-3">
                  <h4 className="text-sm font-semibold text-white">Top Error Signatures</h4>
                  <div className="mt-2 space-y-2">
                    {runtimeSignatures.slice(0, 6).map((item) => (
                      <div
                        key={`${item.source}-${item.signature}`}
                        className="rounded-lg border border-slate-700/70 bg-slate-950/20 px-2 py-1.5"
                      >
                        <p className="truncate text-xs font-medium text-slate-100">
                          {item.provider} · {item.category} · {formatCount(item.count)}
                        </p>
                        <p className="truncate text-[11px] text-slate-300">{item.sample}</p>
                      </div>
                    ))}
                    {runtimeSignatures.length === 0 ? (
                      <p className="text-xs text-slate-300">No recurring signatures in this window.</p>
                    ) : null}
                  </div>
                </section>

                <section className="rounded-xl border border-slate-600/60 bg-slate-900/35 p-3">
                  <h4 className="text-sm font-semibold text-white">Fresh Live Feed</h4>
                  <div className="mt-2 space-y-2">
                    {runtimeLiveEvents.slice(0, 6).map((event) => (
                      <div
                        key={`${event.ts ?? "na"}-${event.source}-${event.eventType}`}
                        className="rounded-lg border border-slate-700/70 bg-slate-950/20 px-2 py-1.5"
                      >
                        <p className="truncate text-xs font-medium text-slate-100">
                          {event.eventType} · {event.severity}
                        </p>
                        <p className="truncate text-[11px] text-slate-300">
                          {event.source}
                          {event.agentId ? ` · ${event.agentId}` : ""}
                        </p>
                        <p className="text-[11px] text-slate-400">
                          {event.ts ? formatRelativeTimestamp(event.ts) : DASH}
                        </p>
                      </div>
                    ))}
                    {runtimeLiveEvents.length === 0 ? (
                      <p className="text-xs text-slate-300">No recent live events yet.</p>
                    ) : null}
                  </div>
                </section>
              </div>

              {runtimeNotes.length > 0 ? (
                <div className="mt-3 rounded-lg border border-amber-400/30 bg-amber-500/10 p-3 text-xs text-amber-100">
                  <p className="mb-1 inline-flex items-center gap-1 font-semibold">
                    <TriangleAlert className="h-3.5 w-3.5" />
                    Bridge notes
                  </p>
                  <ul className="space-y-1">
                    {runtimeNotes.slice(0, 4).map((note) => (
                      <li key={note}>- {note}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </section>

            <section className="mt-4 rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
              <div className="mb-3 flex items-center justify-between gap-3">
                <h3 className="text-lg font-semibold text-slate-900">Pending Approvals</h3>
                <div className="flex items-center gap-2">
                  <SectionCopyButton
                    sectionKey="pending-approvals"
                    payload={pendingApprovalsSharePayload}
                    copiedSection={copiedSection}
                    onCopy={copySectionForAi}
                  />
                  <Link
                    href="/approvals"
                    className="inline-flex items-center gap-1 text-xs text-slate-500 transition hover:text-slate-700"
                  >
                    Open global approvals
                    <ArrowUpRight className="h-3.5 w-3.5" />
                  </Link>
                </div>
              </div>

              {!metrics && metricsQuery.isLoading ? (
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-500">
                  Loading pending approvals...
                </div>
              ) : !metrics && metricsQuery.error ? (
                <div className="rounded-lg border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
                  Pending approvals are temporarily unavailable.
                </div>
              ) : hasPendingApprovals ? (
                <div className="space-y-2">
                  <div className="divide-y divide-slate-100 rounded-lg border border-slate-200 bg-white">
                    {pendingApprovalItems.map((item) => (
                      <Link
                        key={item.approval_id}
                        href={`/boards/${item.board_id}/approvals`}
                        className="flex items-center justify-between gap-3 px-3 py-2 transition hover:bg-slate-50"
                      >
                        <span className="min-w-0 text-sm text-slate-700">
                          <span className="block truncate font-medium text-slate-800">
                            {item.task_title || "Pending approval"}
                          </span>
                          <span className="block truncate text-xs text-slate-500">
                            {item.board_name} · {item.confidence}% score
                          </span>
                        </span>
                        <span className="shrink-0 text-xs text-slate-500">
                          {formatRelativeTimestamp(item.created_at)}
                        </span>
                      </Link>
                    ))}
                  </div>
                  {pendingApprovalsTotal > pendingApprovalItems.length ? (
                    <p className="text-xs text-slate-500">
                      Showing latest {formatCount(pendingApprovalItems.length)} of{" "}
                      {formatCount(pendingApprovalsTotal)} pending approvals.
                    </p>
                  ) : null}
                </div>
              ) : (
                <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-700">
                  No pending approvals across your boards.
                </div>
              )}
            </section>

            <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2">
              <section className="min-w-0 overflow-hidden rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <h3 className="text-lg font-semibold text-slate-900">Sessions</h3>
                  <div className="flex items-center gap-2">
                    <SectionCopyButton
                      sectionKey="sessions"
                      payload={sessionsSharePayload}
                      copiedSection={copiedSection}
                      onCopy={copySectionForAi}
                    />
                    <span className="text-xs text-slate-500">{formatCount(activeSessions)}</span>
                  </div>
                </div>
                <div className="max-h-[310px] space-y-2 overflow-x-hidden overflow-y-auto pr-1">
                  {!hasConfiguredGateways ? (
                    <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-500">
                      No gateways are configured for any board yet.
                    </div>
                  ) : gatewayStatusesQuery.isLoading ? (
                    <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-500">
                      Loading sessions...
                    </div>
                  ) : sessionSummaries.length > 0 ? (
                    <>
                      {gatewayUnavailableCount > 0 ? (
                        <div className="rounded-lg border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
                          {formatCount(gatewayUnavailableCount)} gateway
                          {gatewayUnavailableCount === 1 ? "" : "s"} unavailable; showing sessions
                          from reachable gateways.
                        </div>
                      ) : null}
                      {sessionSummaries.map((session) => (
                        <div
                          key={session.key}
                          className="overflow-hidden rounded-lg border border-slate-200 bg-white px-3 py-2"
                        >
                          <div className="flex items-center justify-between gap-3">
                            <div className="min-w-0 flex-1">
                              <p className="truncate text-sm font-medium text-slate-900">
                                <span
                                  className={`mr-2 inline-block h-2 w-2 rounded-full ${
                                    session.isMain ? "bg-emerald-500" : "bg-slate-400"
                                  }`}
                                />
                                {session.title}
                              </p>
                              <p className="mt-0.5 truncate text-xs text-slate-500">{session.subtitle}</p>
                            </div>
                            <div className="min-w-0 max-w-[45%] text-right">
                              <p className="truncate text-xs font-medium text-slate-700">
                                {session.usage === DASH ? "Usage unavailable" : session.usage}
                              </p>
                              <p className="text-[11px] text-slate-500">
                                {session.lastSeenAt
                                  ? formatRelativeTimestamp(session.lastSeenAt)
                                  : "Activity unavailable"}
                              </p>
                            </div>
                          </div>
                        </div>
                      ))}
                    </>
                  ) : gatewayUnavailableCount === gatewayTargets.length ? (
                    <div className="rounded-lg border border-rose-300 bg-rose-50 p-3 text-sm text-rose-700">
                      Session data is unavailable for all configured gateways.
                    </div>
                  ) : (
                    <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-500">
                      No active sessions detected.
                    </div>
                  )}
                </div>
              </section>

              <section className="min-w-0 overflow-hidden rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <h3 className="text-lg font-semibold text-slate-900">Recent Activity</h3>
                  <div className="flex items-center gap-2">
                    <SectionCopyButton
                      sectionKey="recent-activity"
                      payload={recentActivitySharePayload}
                      copiedSection={copiedSection}
                      onCopy={copySectionForAi}
                    />
                    <Link
                      href={activityFeedHref}
                      className="inline-flex items-center gap-1 text-xs text-slate-500 transition hover:text-slate-700"
                    >
                      Open feed
                      <ArrowUpRight className="h-3.5 w-3.5" />
                    </Link>
                  </div>
                </div>
                <div className="max-h-[310px] space-y-2 overflow-x-hidden overflow-y-auto pr-1">
                  {recentLogs.length > 0 ? (
                    recentLogs.map((event) => {
                      const eventHref = buildActivityEventHref(event);
                      return (
                        <div
                          key={event.id}
                          role="link"
                          tabIndex={0}
                        aria-label={`Open related context for ${event.event_type} activity`}
                          onClick={(interactionEvent) =>
                            handleLogRowClick(interactionEvent, eventHref)
                          }
                          onKeyDown={(interactionEvent) =>
                            handleLogRowKeyDown(interactionEvent, eventHref)
                          }
                          className="cursor-pointer overflow-hidden rounded-lg border border-slate-200 bg-white px-3 py-2 transition hover:border-slate-300 focus-visible:border-slate-400 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300"
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0 flex-1 overflow-hidden">
                              <div className="break-words text-sm font-medium text-slate-900 [&_ol]:mb-0 [&_p]:mb-0 [&_pre]:my-1 [&_pre]:max-w-full [&_pre]:overflow-x-auto [&_ul]:mb-0">
                                <Markdown
                                  content={event.message?.trim() || event.event_type}
                                  variant="comment"
                                />
                              </div>
                              <p className="mt-0.5 text-xs uppercase tracking-wider text-slate-500">
                                {event.event_type}
                              </p>
                            </div>
                            <div className="shrink-0 text-right text-[11px] text-slate-500">
                              <p>{formatRelativeTimestamp(event.created_at)}</p>
                              <p>{formatTimestamp(event.created_at)}</p>
                            </div>
                          </div>
                        </div>
                      );
                    })
                  ) : (
                    <div className="flex h-[240px] flex-col items-center justify-center rounded-lg border border-slate-200 bg-white text-sm text-slate-500">
                      <Shield className="mb-2 h-5 w-5 text-slate-400" />
                      No activity yet
                      <p className="mt-1 text-xs text-slate-500">Activity appears here when events are emitted.</p>
                    </div>
                  )}
                </div>
              </section>
            </div>
          </div>
        </main>
      </SignedIn>
    </DashboardShell>
  );
}
