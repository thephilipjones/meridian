import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ErrorBanner } from "../hooks/useFetch";
import { SkeletonTable } from "./Skeleton";
import type { Article } from "../types";

export default function PaperBrowser() {
  const [urlParams, setUrlParams] = useSearchParams();
  const [articles, setArticles] = useState<Article[]>([]);
  const [search, setSearch] = useState(urlParams.get("search") ?? "");
  const [typeFilter, setTypeFilter] = useState("");
  const [yearFilter, setYearFilter] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const meshParam = urlParams.get("search");
    if (meshParam) {
      setSearch(meshParam);
      setUrlParams({}, { replace: true });
    }
  }, [urlParams, setUrlParams]);

  useEffect(() => {
    fetchArticles();
  }, [typeFilter, yearFilter, search]);

  const fetchArticles = async () => {
    setLoading(true);
    const params = new URLSearchParams();
    if (search) params.set("search", search);
    if (typeFilter) params.set("publication_type", typeFilter);
    if (yearFilter) params.set("year", yearFilter);
    params.set("limit", "100");

    setError(null);
    try {
      const resp = await fetch(`/api/research/articles?${params}`);
      if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
      const data = await resp.json();
      setArticles(Array.isArray(data) ? data : []);
    } catch (e: unknown) {
      setArticles([]);
      setError(e instanceof Error ? e.message : "Failed to load articles");
    }
    setLoading(false);
  };

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex flex-wrap gap-3 rounded-xl border bg-white p-4 shadow-sm">
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && fetchArticles()}
          placeholder="Search titles and abstracts..."
          className="flex-1 rounded-lg border px-3 py-2 text-sm focus:border-meridian-500 focus:outline-none"
        />
        <select
          value={typeFilter}
          onChange={(e) => setTypeFilter(e.target.value)}
          className="rounded-lg border px-3 py-2 text-sm"
        >
          <option value="">All types</option>
          <option value="Journal Article">Journal Article</option>
          <option value="Meta-Analysis">Meta-Analysis</option>
          <option value="RCT">RCT</option>
          <option value="Review">Review</option>
          <option value="Clinical Trial">Clinical Trial</option>
        </select>
        <select
          value={yearFilter}
          onChange={(e) => setYearFilter(e.target.value)}
          className="rounded-lg border px-3 py-2 text-sm"
        >
          <option value="">All years</option>
          <option value="2026">2026</option>
          <option value="2025">2025</option>
          <option value="2024">2024</option>
        </select>
        <button
          onClick={fetchArticles}
          className="rounded-lg bg-meridian-600 px-4 py-2 text-sm font-medium text-white hover:bg-meridian-700"
        >
          Filter
        </button>
      </div>

      {/* Results Table */}
      {error && <ErrorBanner message={error} onRetry={fetchArticles} />}

      <div className="rounded-xl border bg-white shadow-sm">
        {loading ? (
          <div className="p-5">
            <SkeletonTable rows={8} cols={5} />
          </div>
        ) : articles.length === 0 ? (
          <div className="p-8 text-center text-gray-500">
            No articles found. Try adjusting your filters.
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b bg-gray-50 text-left text-gray-500">
                <th className="px-4 py-3 font-medium">Title</th>
                <th className="px-4 py-3 font-medium">Journal</th>
                <th className="px-4 py-3 font-medium">Year</th>
                <th className="px-4 py-3 font-medium">Type</th>
                <th className="px-4 py-3 font-medium text-right">Citations</th>
              </tr>
            </thead>
            <tbody>
              {articles.map((a) => (
                <>
                  <tr
                    key={a.article_id}
                    onClick={() =>
                      setExpanded(
                        expanded === a.article_id ? null : a.article_id
                      )
                    }
                    className="cursor-pointer border-b transition-colors hover:bg-gray-50"
                  >
                    <td className="max-w-md px-4 py-3">
                      <div className="flex items-center gap-2">
                        {a.is_preprint === "true" && (
                          <span className="flex-shrink-0 rounded bg-amber-100 px-1.5 py-0.5 text-[10px] font-medium text-amber-700">
                            PRE
                          </span>
                        )}
                        <span className="line-clamp-2 font-medium text-gray-900">
                          {a.title}
                        </span>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-gray-600">
                      {a.journal ?? "—"}
                    </td>
                    <td className="px-4 py-3 text-gray-600">
                      {a.publication_year ?? "—"}
                    </td>
                    <td className="px-4 py-3 text-gray-600">
                      {a.publication_type ?? "—"}
                    </td>
                    <td className="px-4 py-3 text-right font-medium">
                      {a.citation_count}
                    </td>
                  </tr>
                  {expanded === a.article_id && (
                    <tr key={`${a.article_id}-detail`}>
                      <td colSpan={5} className="bg-gray-50 px-6 py-4">
                        {a.doi && (
                          <p className="mb-2 text-xs text-gray-500">
                            DOI:{" "}
                            <span className="font-mono text-meridian-600">
                              {a.doi}
                            </span>
                          </p>
                        )}
                        <p className="text-sm text-gray-600">
                          {a.abstract ?? "No abstract available."}
                        </p>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
