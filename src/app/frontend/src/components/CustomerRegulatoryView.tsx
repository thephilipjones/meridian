import { useEffect, useState } from "react";
import DataCatalog from "./DataCatalog";
import GenieEmbed from "./GenieEmbed";
import type { ConnectionInfo, CodeSnippet } from "../types";

interface Props {
  activeTab: string;
}

function ConnectEnvironment() {
  const [connInfo, setConnInfo] = useState<ConnectionInfo | null>(null);
  const [snippets, setSnippets] = useState<Record<string, CodeSnippet>>({});
  const [activePlatform, setActivePlatform] = useState("databricks");

  useEffect(() => {
    fetch("/api/sharing/connection-info")
      .then((r) => r.json())
      .then(setConnInfo)
      .catch(() => {});
    fetch("/api/sharing/code-snippets")
      .then((r) => r.json())
      .then(setSnippets)
      .catch(() => {});
  }, []);

  const platforms = Object.keys(snippets);
  const activeSnippet = snippets[activePlatform];

  return (
    <div className="space-y-6">
      {connInfo && (
        <div className="rounded-xl border bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-gray-900">
            Delta Sharing Connection
          </h2>
          <p className="mt-1 text-sm text-gray-500">{connInfo.instructions}</p>

          <div className="mt-4 grid grid-cols-2 gap-4">
            <div className="rounded-lg bg-gray-50 p-4">
              <p className="text-xs font-medium text-gray-400">Share Name</p>
              <p className="mt-1 font-mono text-sm text-gray-800">
                {connInfo.share_name}
              </p>
            </div>
            <div className="rounded-lg bg-gray-50 p-4">
              <p className="text-xs font-medium text-gray-400">Recipient</p>
              <p className="mt-1 font-mono text-sm text-gray-800">
                {connInfo.recipient_name}
              </p>
            </div>
            <div className="rounded-lg bg-gray-50 p-4">
              <p className="text-xs font-medium text-gray-400">Provider</p>
              <p className="mt-1 text-sm text-gray-800">{connInfo.provider}</p>
            </div>
            <div className="rounded-lg bg-gray-50 p-4">
              <p className="text-xs font-medium text-gray-400">Status</p>
              <p className="mt-1 text-sm">
                <span className="inline-flex items-center gap-1 rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700">
                  <span className="h-1.5 w-1.5 rounded-full bg-green-500" />
                  {connInfo.activation_status}
                </span>
              </p>
            </div>
          </div>

          <div className="mt-4">
            <p className="text-xs font-medium text-gray-400">Shared Tables</p>
            <div className="mt-2 space-y-2">
              {connInfo.shared_tables.map((t) => (
                <div
                  key={t.table}
                  className="flex items-center justify-between rounded-lg border bg-white px-4 py-2"
                >
                  <span className="font-mono text-sm text-meridian-700">
                    {t.schema}.{t.table}
                  </span>
                  <span className="text-xs text-gray-400">
                    {t.description}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      <div className="rounded-xl border bg-white shadow-sm">
        <div className="border-b px-6 pt-4">
          <h3 className="mb-3 text-sm font-semibold text-gray-700">
            Code Snippets
          </h3>
          <nav className="flex gap-1">
            {platforms.map((p) => (
              <button
                key={p}
                onClick={() => setActivePlatform(p)}
                className={`rounded-t-lg px-3 py-1.5 text-xs font-medium transition-colors ${
                  p === activePlatform
                    ? "bg-gray-100 text-meridian-700"
                    : "text-gray-500 hover:text-gray-700"
                }`}
              >
                {snippets[p]?.label ?? p}
              </button>
            ))}
          </nav>
        </div>
        {activeSnippet && (
          <div className="p-6">
            <pre className="overflow-x-auto rounded-lg bg-gray-900 p-4 text-sm text-gray-100">
              <code>{activeSnippet.code}</code>
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}

export default function CustomerRegulatoryView({ activeTab }: Props) {
  switch (activeTab) {
    case "Data Catalog":
      return <DataCatalog />;
    case "Genie":
      return <GenieEmbed />;
    case "Connect Your Environment":
      return <ConnectEnvironment />;
    default:
      return <DataCatalog />;
  }
}
