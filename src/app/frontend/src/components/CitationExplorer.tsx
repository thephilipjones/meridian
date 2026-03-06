import { useEffect, useState } from "react";
import { ErrorBanner } from "../hooks/useFetch";
import { SkeletonTable } from "./Skeleton";

interface Citation {
  citing_doi: string;
  cited_doi: string;
  citing_title: string | null;
  cited_title: string | null;
  citing_year: number | null;
  cited_year: number | null;
}

export default function CitationExplorer() {
  const [query, setQuery] = useState("");
  const [citations, setCitations] = useState<Citation[]>([]);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [searched, setSearched] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/research/citations?limit=25")
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .then(setCitations)
      .catch((e) => setError(e.message ?? "Failed to load citations"))
      .finally(() => setInitialLoading(false));
  }, []);

  const handleSearch = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setSearched(true);
    setError(null);
    try {
      const resp = await fetch(
        `/api/research/citations?title=${encodeURIComponent(query)}&limit=50`
      );
      if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
      setCitations(await resp.json());
    } catch (e: unknown) {
      setCitations([]);
      setError(e instanceof Error ? e.message : "Search failed");
    }
    setLoading(false);
  };

  const doiLabel = (doi: string) => (
    <span className="font-mono text-meridian-600">{doi}</span>
  );

  return (
    <div className="space-y-6">
      <div className="rounded-xl border bg-white p-6 shadow-sm">
        <h2 className="font-display mb-2 text-lg font-semibold text-meridian-900">
          Citation Explorer
        </h2>
        <p className="mb-4 text-sm text-gray-500">
          Explore citation relationships between research articles.
          Search by title keyword to find which papers cite each other.
        </p>
        <div className="flex gap-3">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSearch()}
            placeholder="e.g., CRISPR, immunotherapy, breast cancer..."
            className="flex-1 rounded-lg border border-gray-300 px-4 py-2.5 text-sm focus:border-meridian-500 focus:outline-none focus:ring-1 focus:ring-meridian-500"
          />
          <button
            onClick={handleSearch}
            disabled={loading}
            className="rounded-lg bg-meridian-600 px-6 py-2.5 text-sm font-medium text-white transition-colors hover:bg-meridian-700 disabled:opacity-50"
          >
            {loading ? "Searching..." : "Search"}
          </button>
        </div>
      </div>

      {error && <ErrorBanner message={error} />}

      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-sm font-semibold text-gray-700">
            {searched
              ? `${citations.length} citation links found`
              : `Recent citations (${citations.length})`}
          </h3>
        </div>
        {initialLoading ? (
          <SkeletonTable rows={5} cols={5} />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-500">
                  <th className="pb-2 pr-4 font-medium">Citing Paper</th>
                  <th className="pb-2 pr-4 font-medium">Year</th>
                  <th className="pb-2 pr-4 font-medium">Cites</th>
                  <th className="pb-2 pr-4 font-medium">Year</th>
                  <th className="pb-2 font-medium">DOI Links</th>
                </tr>
              </thead>
              <tbody>
                {citations.map((c, i) => (
                  <tr key={i} className="border-b last:border-0">
                    <td className="max-w-xs truncate py-2.5 pr-4 font-medium">
                      {c.citing_title ?? c.citing_doi}
                    </td>
                    <td className="py-2.5 pr-4 text-gray-500">
                      {c.citing_year ?? "—"}
                    </td>
                    <td className="max-w-xs truncate py-2.5 pr-4">
                      {c.cited_title ?? c.cited_doi}
                    </td>
                    <td className="py-2.5 pr-4 text-gray-500">
                      {c.cited_year ?? "—"}
                    </td>
                    <td className="py-2.5 text-xs">
                      <div className="flex gap-2">
                        {doiLabel(c.citing_doi)}
                        <span className="text-gray-300">→</span>
                        {doiLabel(c.cited_doi)}
                      </div>
                    </td>
                  </tr>
                ))}
                {citations.length === 0 && (
                  <tr>
                    <td colSpan={5} className="py-8 text-center text-gray-400">
                      No citation links found for this query.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
