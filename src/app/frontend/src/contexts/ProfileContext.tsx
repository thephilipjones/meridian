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
    function pickInitialProfile(list: Profile[]): Profile {
      const seg = window.location.pathname.split("/").filter(Boolean)[0];
      return list.find((p) => p.business_unit === seg) ?? list[0];
    }

    fetch("/api/profiles")
      .then((r) => r.json())
      .then((data: Profile[]) => {
        setProfiles(data);
        if (data.length > 0) {
          setActiveProfile(pickInitialProfile(data));
        }
        setLoading(false);
      })
      .catch(() => {
        const fallback: Profile[] = [
          {
            id: "sarah",
            name: "Sarah Chen",
            role: "VP, Product Analytics",
            persona: "Internal",
            avatar_initials: "SC",
            nav_tabs: ["Sales Dashboard", "Product Usage", "Platform Analytics", "Genie"],
            business_unit: "internal",
            genie_space_id: "01f118c5ddb81e3dba76005e6020b2bc",
            subscription_tier: null,
          },
          {
            id: "james",
            name: "James Rivera",
            role: "VP, Regulatory Affairs, Acme Bank",
            persona: "External Customer (Regulatory)",
            avatar_initials: "JR",
            nav_tabs: ["Regulatory Feed", "Data Catalog", "Genie", "Connect Your Environment"],
            business_unit: "regulatory",
            genie_space_id: "01f118ce34db1cfeb9085c37cea33f8d",
            subscription_tier: "sec_only",
          },
          {
            id: "anika",
            name: "Dr. Anika Park",
            role: "Research Director, NIH — Oncology Informatics",
            persona: "External Customer (Research)",
            avatar_initials: "AP",
            nav_tabs: ["Research Overview", "Research Q&A", "Paper Browser", "Citation Explorer"],
            business_unit: "research",
            genie_space_id: "01f118c5d7e01a32a58159a70e55160c",
            subscription_tier: null,
          },
        ];
        setProfiles(fallback);
        setActiveProfile(pickInitialProfile(fallback));
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
