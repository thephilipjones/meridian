import { useState } from "react";
import { useProfile } from "../contexts/ProfileContext";

interface GenieResult {
  conversation_id: string;
  message_id: string;
  status: string;
  sql_query: string | null;
  result_columns: string[];
  result_rows: (string | number | null)[][];
  description: string | null;
}

interface ChatEntry {
  role: "user" | "genie";
  text: string;
  result?: GenieResult;
  loading?: boolean;
}

const SUGGESTED_QUESTIONS: Record<string, string[]> = {
  internal: [
    "What is total pipeline value by stage?",
    "Which accounts have the highest ARR?",
    "Show revenue trend by quarter",
  ],
  regulatory: [
    "How many SEC filings were there this year?",
    "Which companies have the most risk signals?",
    "Show FDA recall actions by classification",
  ],
  research: [
    "What are the most cited articles?",
    "Show publication counts by journal",
    "Which authors have the highest h-index?",
  ],
};

export default function GenieEmbed() {
  const { activeProfile } = useProfile();
  const [entries, setEntries] = useState<ChatEntry[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);

  const genieSpaceId = activeProfile?.genie_space_id;
  const bu = activeProfile?.business_unit ?? "internal";

  if (!genieSpaceId) {
    return (
      <div className="flex flex-col items-center justify-center rounded-xl border bg-white p-12 shadow-sm">
        <h3 className="text-lg font-semibold text-gray-900">
          Genie Space Not Configured
        </h3>
        <p className="mt-2 max-w-md text-center text-sm text-gray-500">
          The Genie space for <strong>{activeProfile?.name}</strong> has not
          been set up yet.
        </p>
      </div>
    );
  }

  const ask = async (question: string) => {
    if (!question.trim() || loading) return;

    const userEntry: ChatEntry = { role: "user", text: question };
    const loadingEntry: ChatEntry = {
      role: "genie",
      text: "Thinking...",
      loading: true,
    };

    setEntries((prev) => [...prev, userEntry, loadingEntry]);
    setInput("");
    setLoading(true);

    try {
      const resp = await fetch("/api/genie/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          space_id: genieSpaceId,
          question,
          conversation_id: conversationId,
        }),
      });

      if (!resp.ok) {
        const errBody = await resp.json().catch(() => null);
        const errMsg =
          errBody?.detail || `Server error (${resp.status})`;
        setEntries((prev) => {
          const updated = prev.slice(0, -1);
          updated.push({ role: "genie", text: errMsg });
          return updated;
        });
        setLoading(false);
        return;
      }

      const data: GenieResult = await resp.json();

      if (data.conversation_id) {
        setConversationId(data.conversation_id);
      }

      setEntries((prev) => {
        const updated = prev.slice(0, -1);
        updated.push({
          role: "genie",
          text: data.description || "Here are the results:",
          result: data,
        });
        return updated;
      });
    } catch {
      setEntries((prev) => {
        const updated = prev.slice(0, -1);
        updated.push({
          role: "genie",
          text: "Sorry, something went wrong. Please try again.",
        });
        return updated;
      });
    }
    setLoading(false);
  };

  const suggestions = SUGGESTED_QUESTIONS[bu] ?? SUGGESTED_QUESTIONS.internal;

  return (
    <div className="flex h-[650px] flex-col rounded-xl border bg-white shadow-sm">
      {/* Header */}
      <div className="flex items-center justify-between border-b bg-gray-50 px-5 py-3">
        <div className="flex items-center gap-2">
          <div className="flex h-7 w-7 items-center justify-center rounded-full bg-meridian-600 text-xs font-bold text-white">
            G
          </div>
          <span className="text-sm font-medium text-gray-700">
            Genie — {activeProfile?.persona}
          </span>
        </div>
        {entries.length > 0 && (
          <button
            onClick={() => {
              setEntries([]);
              setConversationId(null);
            }}
            className="text-xs text-gray-400 hover:text-gray-600"
          >
            New conversation
          </button>
        )}
      </div>

      {/* Chat area */}
      <div className="flex-1 overflow-y-auto px-5 py-4">
        {entries.length === 0 ? (
          <div className="flex h-full flex-col items-center justify-center">
            <div className="mb-6 flex h-14 w-14 items-center justify-center rounded-full bg-meridian-100">
              <span className="text-2xl">✨</span>
            </div>
            <h3 className="mb-1 text-base font-semibold text-gray-800">
              Ask a question about your data
            </h3>
            <p className="mb-6 max-w-sm text-center text-sm text-gray-500">
              Genie translates your question into SQL, runs it, and returns the
              results.
            </p>
            <div className="flex flex-wrap justify-center gap-2">
              {suggestions.map((q) => (
                <button
                  key={q}
                  onClick={() => ask(q)}
                  className="rounded-lg border bg-gray-50 px-3 py-2 text-left text-xs text-gray-600 transition-colors hover:border-meridian-300 hover:bg-meridian-50"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            {entries.map((entry, i) => (
              <div key={i}>
                {entry.role === "user" ? (
                  <div className="flex justify-end">
                    <div className="max-w-[75%] rounded-2xl rounded-br-md bg-meridian-600 px-4 py-2.5 text-sm text-white">
                      {entry.text}
                    </div>
                  </div>
                ) : (
                  <div className="flex gap-2">
                    <div className="mt-0.5 flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-meridian-100 text-[10px] font-bold text-meridian-700">
                      G
                    </div>
                    <div className="max-w-[85%] space-y-2">
                      {entry.loading ? (
                        <div className="flex items-center gap-2 rounded-2xl rounded-bl-md bg-gray-100 px-4 py-2.5 text-sm text-gray-500">
                          <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-meridian-400" />
                          Thinking...
                        </div>
                      ) : (
                        <>
                          <div className="rounded-2xl rounded-bl-md bg-gray-100 px-4 py-2.5 text-sm text-gray-800">
                            {entry.text}
                          </div>
                          {entry.result?.sql_query && (
                            <details className="rounded-lg border bg-gray-50 text-xs">
                              <summary className="cursor-pointer px-3 py-1.5 font-medium text-gray-500 hover:text-gray-700">
                                SQL Query
                              </summary>
                              <pre className="overflow-x-auto border-t bg-gray-900 p-3 text-[11px] text-green-300">
                                {entry.result.sql_query}
                              </pre>
                            </details>
                          )}
                          {entry.result &&
                            (entry.result.result_columns?.length ?? 0) > 0 &&
                            (entry.result.result_rows?.length ?? 0) > 0 && (
                              <div className="overflow-x-auto rounded-lg border">
                                <table className="w-full text-xs">
                                  <thead>
                                    <tr className="bg-gray-50 text-left text-gray-500">
                                      {entry.result.result_columns.map((c) => (
                                        <th
                                          key={c}
                                          className="px-3 py-1.5 font-medium"
                                        >
                                          {c}
                                        </th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {entry.result.result_rows
                                      .slice(0, 20)
                                      .map((row, ri) => (
                                        <tr
                                          key={ri}
                                          className="border-t text-gray-700"
                                        >
                                          {row.map((cell, ci) => (
                                            <td key={ci} className="px-3 py-1.5">
                                              {cell ?? "—"}
                                            </td>
                                          ))}
                                        </tr>
                                      ))}
                                  </tbody>
                                </table>
                                {(entry.result.result_rows?.length ?? 0) > 20 && (
                                  <div className="border-t bg-gray-50 px-3 py-1 text-[10px] text-gray-400">
                                    Showing 20 of{" "}
                                    {entry.result.result_rows!.length} rows
                                  </div>
                                )}
                              </div>
                            )}
                        </>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Input */}
      <div className="border-t bg-gray-50 px-4 py-3">
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && ask(input)}
            placeholder="Ask a question about your data..."
            disabled={loading}
            className="flex-1 rounded-lg border border-gray-300 px-4 py-2.5 text-sm focus:border-meridian-500 focus:outline-none focus:ring-1 focus:ring-meridian-500 disabled:opacity-50"
          />
          <button
            onClick={() => ask(input)}
            disabled={loading || !input.trim()}
            className="rounded-lg bg-meridian-600 px-5 py-2.5 text-sm font-medium text-white transition-colors hover:bg-meridian-700 disabled:opacity-50"
          >
            Ask
          </button>
        </div>
      </div>
    </div>
  );
}
