import { useState } from "react";
import { useFetch, ErrorBanner } from "../hooks/useFetch";

interface Highlight {
  icon: "trend-up" | "alert" | "chart" | "insight";
  title: string;
  detail: string;
  sentiment: "positive" | "warning" | "negative" | "neutral";
}

interface BriefData {
  highlights: Highlight[];
  generated_at: string;
  error?: string;
}

const SENTIMENT_STYLES: Record<string, string> = {
  positive: "border-l-green-500 bg-green-50/40",
  warning: "border-l-amber-500 bg-amber-50/40",
  negative: "border-l-red-500 bg-red-50/40",
  neutral: "border-l-blue-500 bg-blue-50/40",
};

const ICON_MAP: Record<string, JSX.Element> = {
  "trend-up": (
    <svg className="h-5 w-5 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
    </svg>
  ),
  alert: (
    <svg className="h-5 w-5 text-amber-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
    </svg>
  ),
  chart: (
    <svg className="h-5 w-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
    </svg>
  ),
  insight: (
    <svg className="h-5 w-5 text-purple-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
    </svg>
  ),
};

function ShimmerCard() {
  return (
    <div className="rounded-xl border-l-4 border-l-gray-200 bg-gray-50 p-5">
      <div className="flex items-start gap-3">
        <div className="h-8 w-8 animate-pulse rounded-lg bg-gray-200" />
        <div className="flex-1 space-y-2">
          <div className="h-4 w-3/5 animate-pulse rounded bg-gray-200" />
          <div className="h-3 w-full animate-pulse rounded bg-gray-200" />
          <div className="h-3 w-4/5 animate-pulse rounded bg-gray-200" />
        </div>
      </div>
    </div>
  );
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins} min ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

export default function BusinessBrief() {
  const { data, loading, error, refetch } = useFetch<BriefData>("/api/analytics/business-brief");
  const [refreshing, setRefreshing] = useState(false);

  const handleRefresh = () => {
    setRefreshing(true);
    refetch();
    setTimeout(() => setRefreshing(false), 1000);
  };

  if (error) return <ErrorBanner message="Failed to load business brief" onRetry={refetch} />;

  return (
    <div className="mb-6 rounded-xl border bg-white p-5 shadow-sm">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-meridian-100">
            <svg className="h-4 w-4 text-meridian-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
            </svg>
          </div>
          <h3 className="text-sm font-semibold text-gray-900">AI Business Brief</h3>
          <span className="rounded-full bg-meridian-100 px-2 py-0.5 text-[10px] font-medium text-meridian-700">
            Foundation Model API
          </span>
        </div>
        <div className="flex items-center gap-3">
          {data?.generated_at && (
            <span className="text-xs text-gray-400">
              Generated {timeAgo(data.generated_at)}
            </span>
          )}
          <button
            onClick={handleRefresh}
            disabled={loading || refreshing}
            className="flex items-center gap-1 rounded-lg border px-2.5 py-1.5 text-xs font-medium text-gray-600 transition-colors hover:bg-gray-50 disabled:opacity-50"
          >
            <svg
              className={`h-3.5 w-3.5 ${refreshing ? "animate-spin" : ""}`}
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Refresh
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        {loading
          ? Array.from({ length: 4 }).map((_, i) => <ShimmerCard key={i} />)
          : (data?.highlights ?? []).map((h, i) => (
              <div
                key={i}
                className={`rounded-xl border-l-4 p-5 transition-shadow hover:shadow-md ${SENTIMENT_STYLES[h.sentiment] ?? SENTIMENT_STYLES.neutral}`}
              >
                <div className="flex items-start gap-3">
                  <div className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-lg bg-white/80">
                    {ICON_MAP[h.icon] ?? ICON_MAP.insight}
                  </div>
                  <div className="min-w-0 flex-1">
                    <p className="text-sm font-semibold text-gray-900">{h.title}</p>
                    <p className="mt-1 text-sm leading-relaxed text-gray-600">{h.detail}</p>
                  </div>
                </div>
              </div>
            ))}
      </div>
    </div>
  );
}
