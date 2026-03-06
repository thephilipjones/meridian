import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
  Legend,
} from "recharts";
import GenieEmbed from "./GenieEmbed";
import { useFetch, ErrorBanner } from "../hooks/useFetch";
import {
  SalesDashboardSkeleton,
  PlatformAnalyticsSkeleton,
  ProductUsageSkeleton,
} from "./Skeleton";
import type { SalesPipeline, RevenueSummary, CustomerHealth } from "../types";

interface Props {
  activeTab: string;
}

interface QueryActivity {
  event_date: string;
  user_email: string;
  action_name: string;
  query_count: number;
}

interface TableAccess {
  schema_name: string;
  table_name: string;
  unique_users: number;
  access_count: number;
  last_accessed: string;
}

interface ComputeConsumption {
  usage_date: string;
  sku_name: string;
  usage_unit: string;
  total_dbus: number;
}

function formatCurrency(val: number): string {
  if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(1)}M`;
  if (val >= 1_000) return `$${(val / 1_000).toFixed(0)}K`;
  return `$${val.toFixed(0)}`;
}

function SalesDashboard() {
  const { data: pipeline, loading: loadingPipeline, error: errPipeline, refetch: retryPipeline } = useFetch<SalesPipeline[]>("/api/analytics/sales-pipeline");
  const { data: revenue, loading: loadingRevenue } = useFetch<RevenueSummary[]>("/api/analytics/revenue");
  const { data: health, loading: loadingHealth } = useFetch<CustomerHealth[]>("/api/analytics/customer-health?limit=10");

  const loading = loadingPipeline || loadingRevenue || loadingHealth;
  if (loading) return <SalesDashboardSkeleton />;
  if (errPipeline) return <ErrorBanner message="Failed to load sales data" onRetry={retryPipeline} />;

  const stageData = (pipeline ?? []).reduce(
    (acc, row) => {
      const existing = acc.find((a) => a.stage === row.stage);
      if (existing) {
        existing.deal_count += row.deal_count;
        existing.total_amount += row.total_amount;
        existing.total_arr += row.total_arr;
      } else {
        acc.push({ ...row });
      }
      return acc;
    },
    [] as SalesPipeline[]
  );

  const revByQuarter = (revenue ?? []).reduce(
    (acc, row) => {
      const key = `FY${row.fiscal_year} ${row.fiscal_quarter}`;
      const existing = acc.find((a) => a.period === key);
      if (existing) {
        existing.revenue += row.revenue;
      } else {
        acc.push({ period: key, revenue: row.revenue });
      }
      return acc;
    },
    [] as { period: string; revenue: number }[]
  );

  return (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: "Total Pipeline", value: formatCurrency(stageData.reduce((s, r) => s + r.total_amount, 0)) },
          { label: "Total ARR", value: formatCurrency(stageData.reduce((s, r) => s + r.total_arr, 0)) },
          { label: "Active Deals", value: stageData.reduce((s, r) => s + r.deal_count, 0).toLocaleString() },
          { label: "Avg Deal Size", value: formatCurrency(stageData.reduce((s, r) => s + r.total_amount, 0) / Math.max(stageData.reduce((s, r) => s + r.deal_count, 0), 1)) },
        ].map((kpi) => (
          <div key={kpi.label} className="rounded-xl border bg-white p-5 shadow-sm">
            <p className="text-sm text-gray-500">{kpi.label}</p>
            <p className="mt-1 text-2xl font-bold text-meridian-900">{kpi.value}</p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-2 gap-6">
        {/* Pipeline by Stage */}
        <div className="rounded-xl border bg-white p-5 shadow-sm">
          <h3 className="mb-4 text-sm font-semibold text-gray-700">Pipeline by Stage</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={stageData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="stage" tick={{ fontSize: 11 }} />
              <YAxis tickFormatter={(v) => formatCurrency(v)} />
              <Tooltip formatter={(v: number) => formatCurrency(v)} />
              <Bar dataKey="total_amount" fill="#3b82f6" name="Amount" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Revenue Trend */}
        <div className="rounded-xl border bg-white p-5 shadow-sm">
          <h3 className="mb-4 text-sm font-semibold text-gray-700">Revenue by Quarter</h3>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={revByQuarter}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="period" tick={{ fontSize: 11 }} />
              <YAxis tickFormatter={(v) => formatCurrency(v)} />
              <Tooltip formatter={(v: number) => formatCurrency(v)} />
              <Legend />
              <Line type="monotone" dataKey="revenue" stroke="#1d4ed8" strokeWidth={2} name="Revenue" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Top Accounts by Health */}
      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <h3 className="mb-4 text-sm font-semibold text-gray-700">Top Accounts by ARR</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-gray-500">
                <th className="pb-2 font-medium">Account</th>
                <th className="pb-2 font-medium">ARR</th>
                <th className="pb-2 font-medium">Products</th>
                <th className="pb-2 font-medium">API Calls (30d)</th>
                <th className="pb-2 font-medium">Health</th>
              </tr>
            </thead>
            <tbody>
              {(health ?? []).map((h) => (
                <tr key={h.account_name} className="border-b last:border-0">
                  <td className="py-2 font-medium">{h.account_name}</td>
                  <td className="py-2">{formatCurrency(h.arr)}</td>
                  <td className="py-2">{h.products_subscribed}</td>
                  <td className="py-2">{h.api_calls_30d.toLocaleString()}</td>
                  <td className="py-2">
                    <span
                      className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${
                        h.health_tier === "Healthy"
                          ? "bg-green-100 text-green-700"
                          : h.health_tier === "At Risk"
                            ? "bg-yellow-100 text-yellow-700"
                            : "bg-red-100 text-red-700"
                      }`}
                    >
                      {h.health_tier}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function PlatformAnalytics() {
  const { data: queryActivity, loading: loadingQA, error: errQA, refetch: retryQA } = useFetch<QueryActivity[]>("/api/analytics/query-activity?days=30");
  const { data: tableAccess, loading: loadingTA } = useFetch<TableAccess[]>("/api/analytics/table-access?limit=15");
  const { data: compute, loading: loadingCompute } = useFetch<ComputeConsumption[]>("/api/analytics/compute-consumption?days=30");

  const loading = loadingQA || loadingTA || loadingCompute;
  if (loading) return <PlatformAnalyticsSkeleton />;
  if (errQA) return <ErrorBanner message="Failed to load platform analytics" onRetry={retryQA} />;

  const dailyQueries = (queryActivity ?? []).reduce(
    (acc, row) => {
      const existing = acc.find((a) => a.date === row.event_date);
      if (existing) {
        existing.queries += row.query_count;
      } else {
        acc.push({ date: row.event_date, queries: row.query_count });
      }
      return acc;
    },
    [] as { date: string; queries: number }[]
  ).sort((a, b) => a.date.localeCompare(b.date));

  const totalQueries = (queryActivity ?? []).reduce((s, r) => s + r.query_count, 0);
  const uniqueUsers = new Set((queryActivity ?? []).map((r) => r.user_email)).size;
  const totalDBUs = (compute ?? []).reduce((s, r) => s + r.total_dbus, 0);

  return (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-3 gap-4">
        {[
          { label: "Queries (30d)", value: totalQueries.toLocaleString() },
          { label: "Unique Users", value: uniqueUsers.toLocaleString() },
          { label: "Total DBUs (30d)", value: totalDBUs.toFixed(1) },
        ].map((kpi) => (
          <div key={kpi.label} className="rounded-xl border bg-white p-5 shadow-sm">
            <p className="text-sm text-gray-500">{kpi.label}</p>
            <p className="mt-1 text-2xl font-bold text-meridian-900">{kpi.value}</p>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-2 gap-6">
        {/* Query Activity Over Time */}
        <div className="rounded-xl border bg-white p-5 shadow-sm">
          <h3 className="mb-4 text-sm font-semibold text-gray-700">Query Activity (Daily)</h3>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={dailyQueries}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fontSize: 10 }} />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="queries" stroke="#6366f1" strokeWidth={2} name="Queries" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Top Tables */}
        <div className="rounded-xl border bg-white p-5 shadow-sm">
          <h3 className="mb-4 text-sm font-semibold text-gray-700">Most Accessed Tables</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={(tableAccess ?? []).slice(0, 10)} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis dataKey="table_name" type="category" tick={{ fontSize: 10 }} width={120} />
              <Tooltip />
              <Bar dataKey="access_count" fill="#8b5cf6" name="Accesses" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Compute Consumption */}
      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <h3 className="mb-4 text-sm font-semibold text-gray-700">Compute Consumption by SKU</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-gray-500">
                <th className="pb-2 font-medium">Date</th>
                <th className="pb-2 font-medium">SKU</th>
                <th className="pb-2 font-medium">Unit</th>
                <th className="pb-2 font-medium">DBUs</th>
              </tr>
            </thead>
            <tbody>
              {(compute ?? []).slice(0, 20).map((c, i) => (
                <tr key={i} className="border-b last:border-0">
                  <td className="py-2">{c.usage_date}</td>
                  <td className="py-2 font-medium">{c.sku_name}</td>
                  <td className="py-2">{c.usage_unit}</td>
                  <td className="py-2">{c.total_dbus.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function ProductUsageView() {
  const { data: usage, loading, error, refetch } = useFetch<
    { account_name: string; product: string; api_calls: number; error_rate: number }[]
  >("/api/analytics/product-usage?limit=50");

  if (loading) return <ProductUsageSkeleton />;
  if (error) return <ErrorBanner message="Failed to load product usage" onRetry={refetch} />;

  return (
    <div className="rounded-xl border bg-white p-5 shadow-sm">
      <h3 className="mb-4 text-sm font-semibold text-gray-700">Product Usage by Account</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-gray-500">
              <th className="pb-2 font-medium">Account</th>
              <th className="pb-2 font-medium">Product</th>
              <th className="pb-2 font-medium">API Calls</th>
              <th className="pb-2 font-medium">Error Rate</th>
            </tr>
          </thead>
          <tbody>
            {(usage ?? []).map((u, i) => (
              <tr key={i} className="border-b last:border-0">
                <td className="py-2 font-medium">{u.account_name}</td>
                <td className="py-2">{u.product}</td>
                <td className="py-2">{u.api_calls.toLocaleString()}</td>
                <td className="py-2">{(u.error_rate * 100).toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function InternalView({ activeTab }: Props) {
  switch (activeTab) {
    case "Sales Dashboard":
      return <SalesDashboard />;
    case "Product Usage":
      return <ProductUsageView />;
    case "Platform Analytics":
      return <PlatformAnalytics />;
    case "Genie":
      return <GenieEmbed />;
    default:
      return <SalesDashboard />;
  }
}
