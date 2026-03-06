interface SkeletonLineProps {
  width?: string;
  className?: string;
}

export function SkeletonLine({ width = "w-full", className = "" }: SkeletonLineProps) {
  return <div className={`h-4 animate-pulse rounded bg-gray-200 ${width} ${className}`} />;
}

interface SkeletonBlockProps {
  className?: string;
}

export function SkeletonBlock({ className = "h-32" }: SkeletonBlockProps) {
  return <div className={`animate-pulse rounded-xl bg-gray-200 ${className}`} />;
}

interface SkeletonTableProps {
  rows?: number;
  cols?: number;
}

export function SkeletonTable({ rows = 5, cols = 4 }: SkeletonTableProps) {
  return (
    <div className="w-full">
      <div className="flex gap-4 border-b pb-3">
        {Array.from({ length: cols }).map((_, i) => (
          <div key={i} className="h-3 flex-1 animate-pulse rounded bg-gray-200" />
        ))}
      </div>
      {Array.from({ length: rows }).map((_, r) => (
        <div key={r} className="flex gap-4 border-b py-3 last:border-0">
          {Array.from({ length: cols }).map((_, c) => (
            <div
              key={c}
              className="h-3 flex-1 animate-pulse rounded bg-gray-200"
              style={{ opacity: 0.6 + ((r + c) % 4) * 0.1 }}
            />
          ))}
        </div>
      ))}
    </div>
  );
}

export function SkeletonKPICard() {
  return (
    <div className="rounded-xl border bg-white p-5 shadow-sm">
      <div className="h-3 w-24 animate-pulse rounded bg-gray-200" />
      <div className="mt-3 h-7 w-28 animate-pulse rounded bg-gray-200" />
    </div>
  );
}

export function SkeletonChart({ height = "h-[280px]" }: { height?: string }) {
  return (
    <div className="rounded-xl border bg-white p-5 shadow-sm">
      <div className="mb-4 h-3 w-36 animate-pulse rounded bg-gray-200" />
      <div className={`animate-pulse rounded-lg bg-gray-100 ${height}`}>
        <div className="flex h-full items-end justify-around px-6 pb-6 pt-8">
          {[40, 65, 45, 80, 55, 70, 50].map((h, i) => (
            <div
              key={i}
              className="w-6 rounded-t bg-gray-200"
              style={{ height: `${h}%` }}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

export function SkeletonCatalogCard() {
  return (
    <div className="rounded-xl border bg-white p-5 shadow-sm">
      <div className="flex items-start justify-between">
        <div className="flex-1 space-y-2">
          <div className="flex items-center gap-2">
            <div className="h-4 w-40 animate-pulse rounded bg-gray-200" />
            <div className="h-5 w-20 animate-pulse rounded-full bg-gray-200" />
          </div>
          <div className="h-3 w-3/4 animate-pulse rounded bg-gray-200" />
        </div>
        <div className="ml-4 flex flex-col items-end gap-1">
          <div className="h-3 w-16 animate-pulse rounded bg-gray-200" />
          <div className="h-3 w-24 animate-pulse rounded bg-gray-200" />
        </div>
      </div>
    </div>
  );
}

export function SkeletonConnectionInfo() {
  return (
    <div className="space-y-6">
      <div className="rounded-xl border bg-white p-6 shadow-sm">
        <div className="mb-2 h-5 w-52 animate-pulse rounded bg-gray-200" />
        <div className="mb-4 h-3 w-96 animate-pulse rounded bg-gray-200" />
        <div className="grid grid-cols-2 gap-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="rounded-lg bg-gray-50 p-4">
              <div className="h-2.5 w-16 animate-pulse rounded bg-gray-200" />
              <div className="mt-2 h-4 w-32 animate-pulse rounded bg-gray-200" />
            </div>
          ))}
        </div>
      </div>
      <div className="rounded-xl border bg-white p-6 shadow-sm">
        <div className="mb-3 h-4 w-28 animate-pulse rounded bg-gray-200" />
        <SkeletonBlock className="h-48" />
      </div>
    </div>
  );
}

export function SalesDashboardSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <SkeletonKPICard key={i} />
        ))}
      </div>
      <div className="grid grid-cols-2 gap-6">
        <SkeletonChart />
        <SkeletonChart />
      </div>
      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <div className="mb-4 h-3 w-36 animate-pulse rounded bg-gray-200" />
        <SkeletonTable rows={5} cols={5} />
      </div>
    </div>
  );
}

export function PlatformAnalyticsSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        {Array.from({ length: 3 }).map((_, i) => (
          <SkeletonKPICard key={i} />
        ))}
      </div>
      <div className="grid grid-cols-2 gap-6">
        <SkeletonChart />
        <SkeletonChart />
      </div>
      <div className="rounded-xl border bg-white p-5 shadow-sm">
        <div className="mb-4 h-3 w-40 animate-pulse rounded bg-gray-200" />
        <SkeletonTable rows={5} cols={4} />
      </div>
    </div>
  );
}

export function ProductUsageSkeleton() {
  return (
    <div className="rounded-xl border bg-white p-5 shadow-sm">
      <div className="mb-4 h-3 w-44 animate-pulse rounded bg-gray-200" />
      <SkeletonTable rows={8} cols={4} />
    </div>
  );
}

export function AppSkeleton() {
  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-meridian-900 text-white shadow-lg">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3">
          <div className="flex items-center gap-3">
            <div className="h-9 w-9 animate-pulse rounded-lg bg-meridian-700" />
            <div className="space-y-1.5">
              <div className="h-4 w-32 animate-pulse rounded bg-meridian-700" />
              <div className="h-2.5 w-56 animate-pulse rounded bg-meridian-800" />
            </div>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 animate-pulse rounded-full bg-meridian-700" />
            <div className="space-y-1">
              <div className="h-3 w-24 animate-pulse rounded bg-meridian-700" />
              <div className="h-2.5 w-16 animate-pulse rounded bg-meridian-800" />
            </div>
          </div>
        </div>
        <div className="mx-auto max-w-7xl px-4">
          <div className="flex gap-1 pb-2">
            {Array.from({ length: 3 }).map((_, i) => (
              <div key={i} className="h-8 w-28 animate-pulse rounded-t-lg bg-meridian-800" />
            ))}
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-4 py-6">
        <div className="space-y-6">
          <div className="grid grid-cols-4 gap-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <SkeletonKPICard key={i} />
            ))}
          </div>
          <div className="grid grid-cols-2 gap-6">
            <SkeletonChart />
            <SkeletonChart />
          </div>
        </div>
      </main>
    </div>
  );
}
