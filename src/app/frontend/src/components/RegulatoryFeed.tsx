import { useState } from "react";
import { useProfile } from "../contexts/ProfileContext";
import { useFetch, ErrorBanner } from "../hooks/useFetch";

interface FeedItem {
  action_id: string;
  action_date: string;
  source: string;
  action_type: string;
  title: string;
  description: string | null;
  company_name: string;
  filing_url: string | null;
  entity_id: string | null;
  industry: string | null;
  cik_number: string | null;
  jurisdiction: string | null;
  overall_risk_level: string | null;
  risk_signal_count: number | null;
  latest_signal_date: string | null;
  is_subscribed: boolean;
}

interface FeedSummary {
  total_actions: number;
  total_entities: number;
  total_risk_signals: number;
}

const SOURCE_STYLES: Record<string, { bg: string; text: string }> = {
  SEC: { bg: "bg-blue-100", text: "text-blue-700" },
  FDA: { bg: "bg-emerald-100", text: "text-emerald-700" },
  USPTO: { bg: "bg-violet-100", text: "text-violet-700" },
};

const RISK_STYLES: Record<string, { dot: string; text: string; label: string }> = {
  elevated: { dot: "bg-amber-500", text: "text-amber-700", label: "Elevated" },
  high: { dot: "bg-red-500", text: "text-red-700", label: "High" },
  critical: { dot: "bg-red-600", text: "text-red-800", label: "Critical" },
  normal: { dot: "bg-green-500", text: "text-green-700", label: "Normal" },
  low: { dot: "bg-green-400", text: "text-green-600", label: "Low" },
};

function groupByDate(items: FeedItem[]): Map<string, FeedItem[]> {
  const groups = new Map<string, FeedItem[]>();
  for (const item of items) {
    const date = item.action_date?.split("T")[0] ?? "Unknown";
    const group = groups.get(date) ?? [];
    group.push(item);
    groups.set(date, group);
  }
  return groups;
}

function formatDate(dateStr: string): string {
  try {
    return new Date(dateStr + "T00:00:00").toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  } catch {
    return dateStr;
  }
}

function FeedSkeleton() {
  return (
    <div className="space-y-6">
      <div className="flex gap-4">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="rounded-xl border bg-white p-4 shadow-sm">
            <div className="h-3 w-16 animate-pulse rounded bg-gray-200" />
            <div className="mt-2 h-6 w-20 animate-pulse rounded bg-gray-200" />
          </div>
        ))}
      </div>
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} className="rounded-xl border bg-white p-5 shadow-sm">
          <div className="flex items-start gap-3">
            <div className="h-6 w-12 animate-pulse rounded-full bg-gray-200" />
            <div className="flex-1 space-y-2">
              <div className="h-4 w-3/4 animate-pulse rounded bg-gray-200" />
              <div className="h-3 w-1/2 animate-pulse rounded bg-gray-200" />
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function EntityPanel({ item }: { item: FeedItem }) {
  const risk = RISK_STYLES[item.overall_risk_level?.toLowerCase() ?? ""] ?? RISK_STYLES.normal;

  return (
    <div className="mt-3 rounded-lg border bg-gray-50 p-4">
      <div className="grid grid-cols-2 gap-3 text-xs">
        {item.industry && (
          <div>
            <span className="font-medium text-gray-500">Industry</span>
            <p className="mt-0.5 text-gray-800">{item.industry}</p>
          </div>
        )}
        {item.jurisdiction && (
          <div>
            <span className="font-medium text-gray-500">Jurisdiction</span>
            <p className="mt-0.5 text-gray-800">{item.jurisdiction}</p>
          </div>
        )}
        {item.cik_number && (
          <div>
            <span className="font-medium text-gray-500">CIK</span>
            <p className="mt-0.5 font-mono text-gray-800">{item.cik_number}</p>
          </div>
        )}
        {item.risk_signal_count != null && (
          <div>
            <span className="font-medium text-gray-500">Risk Signals</span>
            <p className="mt-0.5 text-gray-800">
              {item.risk_signal_count} signal{item.risk_signal_count !== 1 ? "s" : ""}
              {item.latest_signal_date && (
                <span className="ml-1 text-gray-400">
                  (latest: {item.latest_signal_date.split("T")[0]})
                </span>
              )}
            </p>
          </div>
        )}
      </div>
      {item.description && (
        <p className="mt-3 border-t pt-3 text-xs leading-relaxed text-gray-600">
          {item.description}
        </p>
      )}
      {item.filing_url && (
        <a
          href={item.filing_url}
          target="_blank"
          rel="noopener noreferrer"
          className="mt-2 inline-block text-xs font-medium text-meridian-600 hover:text-meridian-700"
        >
          View filing &rarr;
        </a>
      )}
    </div>
  );
}

export default function RegulatoryFeed() {
  const { activeProfile } = useProfile();
  const tier = activeProfile?.subscription_tier ?? "sec_only";
  const { data: feed, loading: loadingFeed, error: errFeed, refetch } = useFetch<FeedItem[]>(`/api/catalog/feed?subscription_tier=${tier}&limit=30`);
  const { data: summary, loading: loadingSummary } = useFetch<FeedSummary>(`/api/catalog/feed/summary?subscription_tier=${tier}`);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const loading = loadingFeed || loadingSummary;
  if (loading) return <FeedSkeleton />;
  if (errFeed) return <ErrorBanner message="Failed to load regulatory feed" onRetry={refetch} />;

  const grouped = groupByDate(feed ?? []);

  const toggleExpand = (id: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="font-display text-lg font-semibold text-gray-900">
              Regulatory Intelligence Feed
            </h2>
            <p className="mt-1 text-sm text-gray-500">
              {activeProfile?.role?.split(",").pop()?.trim() ?? "Customer"} — {tier.replace("_", " ").replace(/\b\w/g, (c) => c.toUpperCase())} Tier
            </p>
          </div>
          <span className="text-xs text-gray-400">Last 90 Days</span>
        </div>

        {summary && (
          <div className="mt-4 flex gap-4">
            {[
              { label: "Actions", value: summary.total_actions },
              { label: "Entities", value: summary.total_entities },
              { label: "Risk Signals", value: summary.total_risk_signals },
            ].map((s) => (
              <div
                key={s.label}
                className="flex-1 rounded-lg bg-gray-50 px-4 py-3 text-center"
              >
                <p className="text-xl font-bold text-meridian-900">
                  {s.value.toLocaleString()}
                </p>
                <p className="text-xs text-gray-500">{s.label}</p>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Timeline */}
      {Array.from(grouped.entries()).map(([date, items]) => (
        <div key={date}>
          <div className="mb-3 flex items-center gap-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-gray-400">
              {formatDate(date)}
            </span>
            <div className="h-px flex-1 bg-gray-200" />
          </div>

          <div className="space-y-3">
            {items.map((item) => {
              const src = SOURCE_STYLES[item.source] ?? SOURCE_STYLES.SEC;
              const risk = item.overall_risk_level
                ? RISK_STYLES[item.overall_risk_level.toLowerCase()]
                : null;
              const isExpanded = expanded.has(item.action_id);

              if (!item.is_subscribed) {
                return (
                  <div
                    key={item.action_id}
                    className="rounded-xl border border-dashed border-gray-300 bg-gray-50/50 p-5 opacity-60"
                  >
                    <div className="flex items-start gap-3">
                      <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${src.bg} ${src.text}`}>
                        {item.source}
                      </span>
                      <div className="flex-1">
                        <p className="text-sm font-medium text-gray-500">
                          {item.company_name} — {item.action_type}
                        </p>
                        <div className="mt-2 flex items-center gap-2">
                          <svg className="h-4 w-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                          </svg>
                          <span className="text-xs text-gray-400">
                            Upgrade to {item.source} tier to access full details
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                );
              }

              return (
                <div
                  key={item.action_id}
                  className="rounded-xl border bg-white p-5 shadow-sm transition-shadow hover:shadow-md"
                >
                  <div className="flex items-start gap-3">
                    <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${src.bg} ${src.text}`}>
                      {item.source}
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="flex items-start justify-between gap-2">
                        <p className="text-sm font-medium text-gray-900">
                          {item.company_name}{" "}
                          <span className="font-normal text-gray-500">
                            — {item.title || item.action_type}
                          </span>
                        </p>
                        {risk && (
                          <span className={`flex flex-shrink-0 items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium ${risk.text}`}>
                            <span className={`h-1.5 w-1.5 rounded-full ${risk.dot}`} />
                            {risk.label}
                          </span>
                        )}
                      </div>

                      <div className="mt-1.5 flex flex-wrap items-center gap-2 text-xs text-gray-400">
                        {item.cik_number && (
                          <span className="font-mono">CIK: {item.cik_number}</span>
                        )}
                        {item.risk_signal_count != null && item.risk_signal_count > 0 && (
                          <span>{item.risk_signal_count} signal{item.risk_signal_count !== 1 ? "s" : ""}</span>
                        )}
                      </div>

                      <button
                        onClick={() => toggleExpand(item.action_id)}
                        className="mt-2 flex items-center gap-1 text-xs font-medium text-meridian-600 hover:text-meridian-700"
                      >
                        <svg
                          className={`h-3 w-3 transition-transform ${isExpanded ? "rotate-90" : ""}`}
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                        >
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                        </svg>
                        {isExpanded ? "Hide" : "View"} entity profile
                      </button>

                      {isExpanded && <EntityPanel item={item} />}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}
