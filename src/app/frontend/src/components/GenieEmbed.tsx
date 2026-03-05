import { useProfile } from "../contexts/ProfileContext";

export default function GenieEmbed() {
  const { activeProfile } = useProfile();

  const genieSpaceId = activeProfile?.genie_space_id;

  if (!genieSpaceId) {
    return (
      <div className="flex flex-col items-center justify-center rounded-xl border bg-white p-12 shadow-sm">
        <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-meridian-100">
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
              d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
        </div>
        <h3 className="text-lg font-semibold text-gray-900">
          Genie Space Not Configured
        </h3>
        <p className="mt-2 max-w-md text-center text-sm text-gray-500">
          The Genie space for the <strong>{activeProfile?.name}</strong> profile
          has not been set up yet. Set the <code>genie_space_id</code> in the
          profile configuration after creating the Genie space in your
          Databricks workspace.
        </p>
      </div>
    );
  }

  // Genie embedding via iframe — URL pattern may change; update as needed
  const genieUrl = `/genie/spaces/${genieSpaceId}`;

  return (
    <div className="overflow-hidden rounded-xl border bg-white shadow-sm">
      <div className="border-b bg-gray-50 px-4 py-2">
        <p className="text-xs text-gray-500">
          Genie — {activeProfile?.persona}
        </p>
      </div>
      <iframe
        src={genieUrl}
        className="h-[600px] w-full border-0"
        title={`Genie Space — ${activeProfile?.name}`}
      />
    </div>
  );
}
