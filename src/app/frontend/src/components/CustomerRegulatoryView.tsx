/**
 * Customer Regulatory View — Phase 2 stub.
 *
 * TODO Phase 2: Implement data catalog, scoped Genie, and Delta Sharing
 * connection info for the James Rivera (Acme Bank) persona.
 */

interface Props {
  activeTab: string;
}

export default function CustomerRegulatoryView({ activeTab }: Props) {
  return (
    <div className="rounded-xl border bg-white p-8 shadow-sm">
      <div className="flex flex-col items-center justify-center py-12">
        <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-blue-100">
          <svg
            className="h-8 w-8 text-meridian-600"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={1.5}
              d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
            />
          </svg>
        </div>
        <h3 className="text-lg font-semibold text-gray-900">
          {activeTab}
        </h3>
        <p className="mt-2 max-w-md text-center text-sm text-gray-500">
          The customer regulatory portal is coming in Phase 2. This view will
          include a browsable data product catalog, a scoped regulatory Genie
          space, and Delta Sharing connection instructions for the James Rivera
          (Acme Bank) persona.
        </p>
      </div>
    </div>
  );
}
