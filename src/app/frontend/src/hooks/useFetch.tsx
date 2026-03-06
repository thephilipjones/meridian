import { useEffect, useState, useCallback, useRef } from "react";

interface UseFetchResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useFetch<T>(url: string | null): UseFetchResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const fetchData = useCallback(() => {
    if (!url) {
      setLoading(false);
      return;
    }

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setError(null);

    fetch(url, { signal: controller.signal })
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .then((d: T) => {
        if (!controller.signal.aborted) {
          setData(d);
          setLoading(false);
        }
      })
      .catch((e) => {
        if (e.name === "AbortError") return;
        setError(e.message ?? "Fetch failed");
        setLoading(false);
      });
  }, [url]);

  useEffect(() => {
    fetchData();
    return () => abortRef.current?.abort();
  }, [fetchData]);

  return { data, loading, error, refetch: fetchData };
}

interface ErrorBannerProps {
  message: string;
  onRetry?: () => void;
}

export function ErrorBanner({ message, onRetry }: ErrorBannerProps) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-red-200 bg-red-50 px-4 py-3">
      <div className="flex items-center gap-2 text-sm text-red-700">
        <svg className="h-4 w-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
        </svg>
        {message}
      </div>
      {onRetry && (
        <button
          onClick={onRetry}
          className="rounded px-3 py-1 text-xs font-medium text-red-700 transition-colors hover:bg-red-100"
        >
          Retry
        </button>
      )}
    </div>
  );
}

const prefetchCache = new Map<string, Promise<unknown>>();

export function prefetchUrls(urls: string[]) {
  for (const url of urls) {
    if (!prefetchCache.has(url)) {
      const promise = fetch(url)
        .then((r) => r.json())
        .catch(() => null);
      prefetchCache.set(url, promise);
      setTimeout(() => prefetchCache.delete(url), 30_000);
    }
  }
}
