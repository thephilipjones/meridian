import { useState, useRef, useEffect } from "react";
import { useProfile } from "../contexts/ProfileContext";

export default function ProfileSwitcher() {
  const { profiles, activeProfile, setActiveProfile } = useProfile();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  if (!activeProfile) return null;

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 rounded-lg px-3 py-2 transition-colors hover:bg-meridian-800"
      >
        <div className="flex h-8 w-8 items-center justify-center rounded-full bg-meridian-500 text-xs font-bold">
          {activeProfile.avatar_initials}
        </div>
        <div className="text-left">
          <div className="text-sm font-medium">{activeProfile.name}</div>
          <div className="text-xs text-meridian-300">{activeProfile.role}</div>
        </div>
        <svg
          className={`h-4 w-4 transition-transform ${open ? "rotate-180" : ""}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="absolute right-0 z-50 mt-1 w-72 rounded-lg border border-gray-200 bg-white py-1 shadow-xl">
          <div className="border-b px-3 py-2">
            <p className="text-xs font-medium uppercase tracking-wider text-gray-400">
              Switch Demo Profile
            </p>
          </div>
          {profiles.map((profile) => (
            <button
              key={profile.id}
              onClick={() => {
                setActiveProfile(profile);
                setOpen(false);
              }}
              className={`flex w-full items-center gap-3 px-3 py-3 text-left transition-colors hover:bg-gray-50 ${
                profile.id === activeProfile.id ? "bg-blue-50" : ""
              }`}
            >
              <div
                className={`flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-full text-sm font-bold text-white ${
                  profile.id === activeProfile.id
                    ? "bg-meridian-600"
                    : "bg-gray-400"
                }`}
              >
                {profile.avatar_initials}
              </div>
              <div>
                <div className="text-sm font-medium text-gray-900">
                  {profile.name}
                </div>
                <div className="text-xs text-gray-500">{profile.role}</div>
                <div className="mt-0.5 text-xs text-meridian-600">
                  {profile.persona}
                </div>
              </div>
              {profile.id === activeProfile.id && (
                <svg
                  className="ml-auto h-5 w-5 text-meridian-600"
                  fill="currentColor"
                  viewBox="0 0 20 20"
                >
                  <path
                    fillRule="evenodd"
                    d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                    clipRule="evenodd"
                  />
                </svg>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
