import { useEffect, useState } from "react";
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
import type { SalesPipeline, RevenueSummary, CustomerHealth } from "../types";

interface Props {
  activeTab: string;
}

function formatCurrency(val: number): string {
  if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(1)}M`;
  if (val >= 1_000) return `$${(val / 1_000).toFixed(0)}K`;
  return `$${val.toFixed(0)}`;
}

function SalesDashboard() {
  const [pipeline, setPipeline] = useState<SalesPipeline[]>([]);
  const [revenue, setRevenue] = useState<RevenueSummary[]>([]);
  const [health, setHealth] = useState<CustomerHealth[]>([]);

  useEffect(() => {
    fetch("/api/analytics/sales-pipeline").then((r) => r.json()).then(setPipeline).catch(() => {});
    fetch("/api/analytics/revenue").then((r) => r.json()).then(setRevenue).catch(() => {});
    fetch("/api/analytics/customer-health?limit=10").then((r) => r.json()).then(setHealth).catch(() => {});
  }, []);

  const stageData = pipeline.reduce(
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

  const revByQuarter = revenue.reduce(
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
              {health.map((h) => (
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

function ProductUsageView() {
  const [usage, setUsage] = useState<
    { account_name: string; product: string; api_calls: number; error_rate: number }[]
  >([]);

  useEffect(() => {
    fetch("/api/analytics/product-usage?limit=50").then((r) => r.json()).then(setUsage).catch(() => {});
  }, []);

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
            {usage.map((u, i) => (
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
    case "Genie":
      return <GenieEmbed />;
    default:
      return <SalesDashboard />;
  }
}
