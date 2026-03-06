import PaperBrowser from "./PaperBrowser";
import CitationExplorer from "./CitationExplorer";
import { useState } from "react";
import type { Article } from "../types";

interface Props {
  activeTab: string;
}

interface SearchResult extends Article {
  similarity_score?: number;
}

function ResearchQA() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [searchMode, setSearchMode] = useState<"semantic" | "keyword" | null>(null);
  const [searching, setSearching] = useState(false);

  const handleSearch = async () => {
    if (!query.trim()) return;
    setSearching(true);
    try {
      const resp = await fetch(
        `/api/research/semantic-search?q=${encodeURIComponent(query)}&limit=10`
      );
      const data = await resp.json();
      setResults(data.results ?? []);
      setSearchMode(data.mode ?? "keyword");
    } catch {
      setResults([]);
      setSearchMode(null);
    }
    setSearching(false);
  };

  return (
    <div className="space-y-6">
      {/* Search Box */}
      <div className="rounded-xl border bg-white p-6 shadow-sm">
        <h2 className="mb-2 text-lg font-semibold text-meridian-900">
          Research Q&A
        </h2>
        <p className="mb-4 text-sm text-gray-500">
          Ask a research question in natural language — results use semantic
          search over article abstracts, powered by Vector Search.
        </p>
        <div className="flex gap-3">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSearch()}
            placeholder="e.g., What are the latest findings on CRISPR off-target effects?"
            className="flex-1 rounded-lg border border-gray-300 px-4 py-2.5 text-sm focus:border-meridian-500 focus:outline-none focus:ring-1 focus:ring-meridian-500"
          />
          <button
            onClick={handleSearch}
            disabled={searching}
            className="rounded-lg bg-meridian-600 px-6 py-2.5 text-sm font-medium text-white transition-colors hover:bg-meridian-700 disabled:opacity-50"
          >
            {searching ? "Searching..." : "Search"}
          </button>
        </div>
      </div>

      {/* Results */}
      {results.length > 0 && (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-medium text-gray-500">
              {results.length} results found
            </h3>
            {searchMode && (
              <span
                className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${
                  searchMode === "semantic"
                    ? "bg-indigo-100 text-indigo-700"
                    : "bg-gray-100 text-gray-600"
                }`}
              >
                {searchMode === "semantic" ? "Semantic Search" : "Keyword Search"}
              </span>
            )}
          </div>
          {results.map((article) => (
            <div
              key={article.article_id}
              className="rounded-xl border bg-white p-5 shadow-sm"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1">
                  <h4 className="font-medium text-meridian-900">
                    {article.title}
                  </h4>
                  <div className="mt-1 flex flex-wrap gap-2 text-xs text-gray-500">
                    {article.journal && <span>{article.journal}</span>}
                    {article.publication_year && (
                      <span>({article.publication_year})</span>
                    )}
                    {article.doi && (
                      <span className="font-mono text-meridian-600">
                        DOI: {article.doi}
                      </span>
                    )}
                  </div>
                </div>
                <div className="flex flex-shrink-0 flex-col items-end gap-1">
                  {article.similarity_score != null && (
                    <span className="rounded-full bg-indigo-50 px-2 py-0.5 text-xs font-medium text-indigo-600">
                      {(article.similarity_score * 100).toFixed(0)}% match
                    </span>
                  )}
                  <span
                    className={`rounded-full px-2 py-0.5 text-xs font-medium ${
                      article.is_preprint === "true"
                        ? "bg-amber-100 text-amber-700"
                        : "bg-green-100 text-green-700"
                    }`}
                  >
                    {article.is_preprint === "true"
                      ? "Preprint"
                      : "Peer-reviewed"}
                  </span>
                  {article.publication_type && (
                    <span className="rounded-full bg-gray-100 px-2 py-0.5 text-xs text-gray-600">
                      {article.publication_type}
                    </span>
                  )}
                  <span className="text-xs text-gray-400">
                    {article.citation_count} citations
                  </span>
                </div>
              </div>
              {article.abstract && (
                <p className="mt-3 text-sm leading-relaxed text-gray-600">
                  {article.abstract.length > 400
                    ? article.abstract.slice(0, 400) + "..."
                    : article.abstract}
                </p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default function ResearchView({ activeTab }: Props) {
  switch (activeTab) {
    case "Research Q&A":
      return <ResearchQA />;
    case "Paper Browser":
      return <PaperBrowser />;
    case "Citation Explorer":
      return <CitationExplorer />;
    default:
      return <ResearchQA />;
  }
}
