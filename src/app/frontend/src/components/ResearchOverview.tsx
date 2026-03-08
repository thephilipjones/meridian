import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { useFetch, ErrorBanner } from "../hooks/useFetch";
import { SkeletonKPICard, SkeletonChart } from "./Skeleton";

interface OverviewData {
  total_articles: number;
  total_citations: number;
  total_authors: number;
  preprint_ratio: number;
  peer_reviewed_pct: number;
  publication_trend: { year: number; count: number }[];
  top_journals: { journal: string; count: number }[];
  top_authors: { name: string; h_index: number; articles: number }[];
}

interface MeshTerm {
  mesh_term: string;
  article_count: number;
  [key: string]: unknown;
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function OverviewSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <SkeletonKPICard key={i} />
        ))}
      </div>
      <div className="grid grid-cols-2 gap-6">
        <SkeletonChart />
        <SkeletonChart />
      </div>
      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <div className="mb-4 h-3 w-36 animate-pulse rounded bg-gray-200" />
        <div className="grid grid-cols-4 gap-3">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="h-10 animate-pulse rounded-lg bg-gray-200" />
          ))}
        </div>
      </div>
    </div>
  );
}

export default function ResearchOverview() {
  const { data: overview, loading: loadingOverview, error: errOverview, refetch } = useFetch<OverviewData>("/api/research/overview");
  const { data: meshTerms, loading: loadingMesh } = useFetch<MeshTerm[]>("/api/research/mesh-terms?limit=20");

  const loading = loadingOverview || loadingMesh;
  if (loading) return <OverviewSkeleton />;
  if (errOverview) return <ErrorBanner message="Failed to load research overview" onRetry={refetch} />;
  if (!overview) return null;

  const maxMeshCount = Math.max(...(meshTerms ?? []).map((m) => m.article_count), 1);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <h2 className="font-display text-lg font-semibold text-gray-900">
          Research Corpus Overview
        </h2>
        <p className="mt-1 text-sm text-gray-500">
          Bibliometric landscape powered by Meridian's curated research data products
        </p>
      </div>

      {/* KPI Ribbon */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: "Articles", value: formatNumber(overview.total_articles), icon: "doc" },
          { label: "Citations", value: formatNumber(overview.total_citations), icon: "cite" },
          { label: "Authors", value: formatNumber(overview.total_authors), icon: "person" },
          { label: "Peer-Reviewed", value: `${overview.peer_reviewed_pct}%`, icon: "check" },
        ].map((kpi) => (
          <div key={kpi.label} className="rounded-xl border bg-white p-5 shadow-sm">
            <p className="text-sm text-gray-500">{kpi.label}</p>
            <p className="mt-1 text-2xl font-bold text-meridian-900">{kpi.value}</p>
          </div>
        ))}
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-2 gap-6">
        {/* Publication Trend */}
        <div className="rounded-xl border bg-white p-5 shadow-sm">
          <h3 className="mb-4 text-sm font-semibold text-gray-700">
            Publication Trend
          </h3>
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={overview.publication_trend}>
              <defs>
                <linearGradient id="pubGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#486581" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#486581" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="year" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Area
                type="monotone"
                dataKey="count"
                stroke="#334e68"
                strokeWidth={2}
                fill="url(#pubGrad)"
                name="Articles"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Top Journals + Top Authors */}
        <div className="space-y-6">
          <div className="rounded-xl border bg-white p-5 shadow-sm">
            <h3 className="mb-3 text-sm font-semibold text-gray-700">
              Top Journals
            </h3>
            <div className="space-y-2">
              {overview.top_journals.map((j, i) => (
                <div key={j.journal} className="flex items-center gap-3">
                  <span className="w-5 text-right text-xs font-bold text-gray-400">
                    {i + 1}.
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center justify-between">
                      <span className="truncate text-sm text-gray-800">
                        {j.journal}
                      </span>
                      <span className="ml-2 flex-shrink-0 text-xs text-gray-400">
                        ({j.count})
                      </span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-xl border bg-white p-5 shadow-sm">
            <h3 className="mb-3 text-sm font-semibold text-gray-700">
              Top Authors by h-index
            </h3>
            <div className="space-y-2">
              {overview.top_authors.map((a) => (
                <div key={a.name} className="flex items-center justify-between">
                  <span className="truncate text-sm text-gray-800">{a.name}</span>
                  <div className="ml-2 flex flex-shrink-0 items-center gap-3 text-xs text-gray-500">
                    <span className="rounded bg-meridian-50 px-1.5 py-0.5 font-medium text-meridian-700">
                      h={a.h_index}
                    </span>
                    <span>{a.articles} articles</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* MeSH Topic Grid */}
      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <h3 className="mb-4 text-sm font-semibold text-gray-700">
          Research Topics (MeSH)
        </h3>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
          {(meshTerms ?? []).map((term) => {
            const pct = Math.max((term.article_count / maxMeshCount) * 100, 8);
            return (
              <div
                key={term.mesh_term}
                className="group relative overflow-hidden rounded-lg border bg-gray-50 px-3 py-2.5 transition-colors hover:border-meridian-300 hover:bg-meridian-50"
              >
                <div
                  className="absolute inset-y-0 left-0 bg-meridian-200/40 transition-all group-hover:bg-meridian-300/40"
                  style={{ width: `${pct}%` }}
                />
                <div className="relative flex items-center justify-between">
                  <span className="truncate text-sm font-medium text-gray-800">
                    {term.mesh_term}
                  </span>
                  <span className="ml-2 flex-shrink-0 text-xs text-gray-500">
                    {term.article_count}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
