import PaperBrowser from "./PaperBrowser";
import CitationExplorer from "./CitationExplorer";
import { useState, useRef, useEffect } from "react";
import type { Article } from "../types";

interface Props {
  activeTab: string;
}

interface Source {
  index: number;
  article_id: string;
  title: string;
  doi: string | null;
  journal: string | null;
  publication_year: number | null;
  is_preprint: string;
  publication_type: string | null;
  citation_count: number;
  similarity_score: number;
}

interface QAMessage {
  role: "user" | "assistant";
  content: string;
  sources?: Source[];
}

function ResearchQA() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<QAMessage[]>([]);
  const [asking, setAsking] = useState(false);
  const [expandedSources, setExpandedSources] = useState<Set<number>>(new Set());
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleAsk = async () => {
    if (!query.trim() || asking) return;
    const question = query.trim();
    setQuery("");
    setAsking(true);
    setExpandedSources(new Set());

    const userMsg: QAMessage = { role: "user", content: question };
    setMessages((prev) => [...prev, userMsg]);

    const history = messages.map((m) => ({
      role: m.role,
      content: m.content,
    }));

    try {
      const resp = await fetch("/api/research/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, history }),
      });
      const data = await resp.json();
      const assistantMsg: QAMessage = {
        role: "assistant",
        content: data.answer ?? "No response received.",
        sources: data.sources ?? [],
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: "Something went wrong. Please try again.",
        },
      ]);
    }
    setAsking(false);
  };

  const toggleSources = (msgIndex: number) => {
    setExpandedSources((prev) => {
      const next = new Set(prev);
      if (next.has(msgIndex)) next.delete(msgIndex);
      else next.add(msgIndex);
      return next;
    });
  };

  return (
    <div className="flex flex-col" style={{ height: "calc(100vh - 180px)" }}>
      {/* Header */}
      <div className="mb-4 rounded-xl border bg-white p-5 shadow-sm">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-meridian-100">
            <svg className="h-5 w-5 text-meridian-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
            </svg>
          </div>
          <div>
            <h2 className="font-display text-lg font-semibold text-meridian-900">AI Research Assistant</h2>
            <p className="text-sm text-gray-500">
              Ask research questions in natural language. Answers are generated from
              36M+ articles using Vector Search and Foundation Model API, with cited sources.
            </p>
          </div>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 space-y-4 overflow-y-auto rounded-xl border bg-gray-50 p-4">
        {messages.length === 0 && (
          <div className="flex h-full flex-col items-center justify-center text-center">
            <div className="mb-6 text-gray-400">
              <svg className="mx-auto h-12 w-12" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
              </svg>
            </div>
            <p className="mb-4 text-sm text-gray-500">Try asking a research question:</p>
            <div className="flex flex-wrap justify-center gap-2">
              {[
                "What are the latest findings on CRISPR off-target effects?",
                "Summarize recent meta-analyses on gut microbiome and depression",
                "What is known about mRNA vaccine stability?",
              ].map((suggestion) => (
                <button
                  key={suggestion}
                  onClick={() => { setQuery(suggestion); }}
                  className="rounded-lg border bg-white px-3 py-2 text-left text-xs text-gray-600 shadow-sm transition-colors hover:border-meridian-400 hover:bg-meridian-50"
                >
                  {suggestion}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
            <div
              className={`max-w-[85%] rounded-xl px-4 py-3 ${
                msg.role === "user"
                  ? "bg-meridian-600 text-white"
                  : "border bg-white text-gray-800 shadow-sm"
              }`}
            >
              <div className="whitespace-pre-wrap text-sm leading-relaxed">
                {msg.content}
              </div>

              {msg.sources && msg.sources.length > 0 && (
                <div className="mt-3 border-t pt-3">
                  <button
                    onClick={() => toggleSources(i)}
                    className="mb-2 flex items-center gap-1 text-xs font-medium text-meridian-600 hover:text-meridian-700"
                  >
                    <svg
                      className={`h-3 w-3 transition-transform ${expandedSources.has(i) ? "rotate-90" : ""}`}
                      fill="none" stroke="currentColor" viewBox="0 0 24 24"
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                    {msg.sources.length} source{msg.sources.length !== 1 ? "s" : ""} cited
                  </button>

                  {expandedSources.has(i) && (
                    <div className="space-y-2">
                      {msg.sources.map((s) => (
                        <div
                          key={s.index}
                          className="rounded-lg bg-gray-50 p-3 text-xs"
                        >
                          <div className="flex items-start justify-between gap-2">
                            <div className="flex-1">
                              <span className="mr-1.5 inline-flex h-5 w-5 items-center justify-center rounded bg-meridian-100 text-[10px] font-bold text-meridian-700">
                                {s.index}
                              </span>
                              <span className="font-medium text-gray-900">
                                {s.title}
                              </span>
                            </div>
                            <div className="flex flex-shrink-0 flex-col items-end gap-0.5">
                              <span
                                className={`rounded-full px-1.5 py-0.5 text-[10px] font-medium ${
                                  String(s.is_preprint).toLowerCase() === "true"
                                    ? "bg-amber-100 text-amber-700"
                                    : "bg-green-100 text-green-700"
                                }`}
                              >
                                {String(s.is_preprint).toLowerCase() === "true" ? "Preprint" : "Peer-reviewed"}
                              </span>
                            </div>
                          </div>
                          <div className="mt-1 flex flex-wrap gap-2 text-gray-500">
                            {s.journal && <span>{s.journal}</span>}
                            {s.publication_year && <span>({s.publication_year})</span>}
                            {s.doi && (
                              <span className="font-mono text-meridian-600">DOI: {s.doi}</span>
                            )}
                            <span>{s.citation_count} citations</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        ))}

        {asking && (
          <div className="flex justify-start">
            <div className="max-w-[85%] rounded-xl border bg-white px-4 py-3 shadow-sm">
              <div className="flex items-center gap-2 text-sm text-gray-500">
                <div className="flex gap-1">
                  <span className="h-2 w-2 animate-bounce rounded-full bg-meridian-400" style={{ animationDelay: "0ms" }} />
                  <span className="h-2 w-2 animate-bounce rounded-full bg-meridian-400" style={{ animationDelay: "150ms" }} />
                  <span className="h-2 w-2 animate-bounce rounded-full bg-meridian-400" style={{ animationDelay: "300ms" }} />
                </div>
                Searching articles and generating answer...
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="mt-3 flex gap-3">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleAsk()}
          placeholder="Ask a research question..."
          disabled={asking}
          className="flex-1 rounded-lg border border-gray-300 px-4 py-2.5 text-sm focus:border-meridian-500 focus:outline-none focus:ring-1 focus:ring-meridian-500 disabled:bg-gray-50"
        />
        <button
          onClick={handleAsk}
          disabled={asking || !query.trim()}
          className="rounded-lg bg-meridian-600 px-6 py-2.5 text-sm font-medium text-white transition-colors hover:bg-meridian-700 disabled:opacity-50"
        >
          Ask
        </button>
      </div>
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
