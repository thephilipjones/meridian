import React, { createContext, useContext, useEffect, useState } from "react";
import type { Profile } from "../types";

interface ProfileContextType {
  profiles: Profile[];
  activeProfile: Profile | null;
  setActiveProfile: (profile: Profile) => void;
  loading: boolean;
}

const ProfileContext = createContext<ProfileContextType>({
  profiles: [],
  activeProfile: null,
  setActiveProfile: () => {},
  loading: true,
});

export function ProfileProvider({ children }: { children: React.ReactNode }) {
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [activeProfile, setActiveProfile] = useState<Profile | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/profiles")
      .then((r) => r.json())
      .then((data: Profile[]) => {
        setProfiles(data);
        if (data.length > 0) {
          setActiveProfile(data[0]);
        }
        setLoading(false);
      })
      .catch(() => {
        // Fallback profiles for local dev without backend
        const fallback: Profile[] = [
          {
            id: "sarah",
            name: "Sarah Chen",
            role: "RevOps Analyst",
            persona: "Internal",
            avatar_initials: "SC",
            nav_tabs: ["Sales Dashboard", "Product Usage", "Genie"],
            business_unit: "internal",
            genie_space_id: null,
          },
          {
            id: "james",
            name: "James Rivera",
            role: "Data Engineering Lead, Acme Bank",
            persona: "External Customer (Regulatory)",
            avatar_initials: "JR",
            nav_tabs: ["Data Catalog", "Genie", "Connect Your Environment"],
            business_unit: "regulatory",
            genie_space_id: null,
          },
          {
            id: "anika",
            name: "Dr. Anika Park",
            role: "Research Director, NIH",
            persona: "External Customer (Research)",
            avatar_initials: "AP",
            nav_tabs: [
              "Research Q&A",
              "Paper Browser",
            ],
            business_unit: "research",
            genie_space_id: null,
          },
        ];
        setProfiles(fallback);
        setActiveProfile(fallback[0]);
        setLoading(false);
      });
  }, []);

  return (
    <ProfileContext.Provider
      value={{ profiles, activeProfile, setActiveProfile, loading }}
    >
      {children}
    </ProfileContext.Provider>
  );
}

export function useProfile() {
  return useContext(ProfileContext);
}
