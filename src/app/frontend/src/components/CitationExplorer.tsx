/**
 * Citation Explorer — tabular view of citation relationships.
 * Phase 1: table-based display. Phase 3: visual citation graph.
 */

export default function CitationExplorer() {
  return (
    <div className="rounded-xl border bg-white p-8 shadow-sm">
      <div className="flex flex-col items-center justify-center py-12">
        <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-gray-100">
          <svg
            className="h-8 w-8 text-gray-400"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={1.5}
              d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
            />
          </svg>
        </div>
        <h3 className="text-lg font-semibold text-gray-900">
          Citation Explorer
        </h3>
        <p className="mt-2 max-w-md text-center text-sm text-gray-500">
          Citation relationships will be available once the Crossref data
          pipeline is active (Phase 2). The citation graph visualization is
          planned for Phase 3.
        </p>
        <p className="mt-4 text-xs text-gray-400">
          Tables: meridian.research.citations
        </p>
      </div>
    </div>
  );
}
