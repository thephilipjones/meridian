import { useEffect, useState } from "react";
import type { DataProduct, DataProductDetail } from "../types";

export default function DataCatalog() {
  const [products, setProducts] = useState<DataProduct[]>([]);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [detail, setDetail] = useState<DataProductDetail | null>(null);

  useEffect(() => {
    fetch("/api/catalog/products?subscription_tier=sec_only")
      .then((r) => r.json())
      .then(setProducts)
      .catch(() => {});
  }, []);

  const handleExpand = (tableName: string) => {
    if (expanded === tableName) {
      setExpanded(null);
      setDetail(null);
      return;
    }
    setExpanded(tableName);
    fetch(`/api/catalog/products/${tableName}?subscription_tier=sec_only`)
      .then((r) => r.json())
      .then(setDetail)
      .catch(() => {});
  };

  const productDisplayNames: Record<string, string> = {
    regulatory_actions: "Regulatory Actions",
    patent_landscape: "Patent Landscape",
    company_entities: "Company Entities",
    company_risk_signals: "Company Risk Signals",
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">
            Data Product Catalog
          </h2>
          <p className="text-sm text-gray-500">
            Browse available regulatory data products. Contact sales to expand
            your subscription.
          </p>
        </div>
        <span className="rounded-full bg-meridian-100 px-3 py-1 text-xs font-medium text-meridian-700">
          Acme Bank &mdash; SEC Tier
        </span>
      </div>

      <div className="grid gap-4">
        {products.map((product) => (
          <div
            key={product.table_name}
            className={`rounded-xl border shadow-sm transition-all ${
              product.is_subscribed
                ? "bg-white hover:shadow-md"
                : "bg-gray-50 opacity-75"
            }`}
          >
            <button
              onClick={() =>
                product.is_subscribed && handleExpand(product.table_name)
              }
              className={`w-full p-5 text-left ${
                product.is_subscribed
                  ? "cursor-pointer"
                  : "cursor-default"
              }`}
            >
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <h3 className="font-semibold text-gray-900">
                      {productDisplayNames[product.table_name] ??
                        product.table_name}
                    </h3>
                    {product.is_subscribed ? (
                      <span className="rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700">
                        Subscribed
                      </span>
                    ) : (
                      <span className="rounded-full bg-gray-200 px-2 py-0.5 text-xs font-medium text-gray-500">
                        Contact Sales
                      </span>
                    )}
                  </div>
                  <p className="mt-1 text-sm text-gray-500">
                    {product.comment ?? "No description available"}
                  </p>
                </div>
                <div className="ml-4 flex flex-col items-end gap-1 text-xs text-gray-400">
                  <span>{product.column_count} columns</span>
                  {product.freshness && (
                    <span>Updated: {new Date(product.freshness).toLocaleDateString()}</span>
                  )}
                </div>
              </div>
            </button>

            {expanded === product.table_name && detail && (
              <div className="border-t bg-gray-50 p-5">
                <div className="mb-4 flex items-center justify-between">
                  <h4 className="text-sm font-semibold text-gray-700">
                    Schema &mdash; {detail.row_count.toLocaleString()} rows
                  </h4>
                </div>

                <div className="mb-4 overflow-x-auto rounded-lg border bg-white">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b bg-gray-50 text-left text-gray-500">
                        <th className="px-3 py-2 font-medium">Column</th>
                        <th className="px-3 py-2 font-medium">Type</th>
                        <th className="px-3 py-2 font-medium">Description</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.schema.map((col) => (
                        <tr
                          key={col.column_name}
                          className="border-b last:border-0"
                        >
                          <td className="px-3 py-1.5 font-mono text-meridian-700">
                            {col.column_name}
                          </td>
                          <td className="px-3 py-1.5 text-gray-500">
                            {col.data_type}
                          </td>
                          <td className="px-3 py-1.5 text-gray-400">
                            {col.comment ?? "—"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {detail.sample_rows.length > 0 && (
                  <div>
                    <h4 className="mb-2 text-sm font-semibold text-gray-700">
                      Sample Data
                    </h4>
                    <div className="overflow-x-auto rounded-lg border bg-white">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="border-b bg-gray-50 text-left text-gray-500">
                            {Object.keys(detail.sample_rows[0]).map((col) => (
                              <th
                                key={col}
                                className="whitespace-nowrap px-3 py-2 font-medium"
                              >
                                {col}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {detail.sample_rows.map((row, i) => (
                            <tr key={i} className="border-b last:border-0">
                              {Object.values(row).map((val, j) => (
                                <td
                                  key={j}
                                  className="max-w-[200px] truncate whitespace-nowrap px-3 py-1.5 text-gray-600"
                                >
                                  {String(val ?? "—")}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
