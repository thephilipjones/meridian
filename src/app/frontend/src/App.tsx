import { useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useProfile } from "./contexts/ProfileContext";
import ProfileSwitcher from "./components/ProfileSwitcher";
import InternalView from "./components/InternalView";
import ResearchView from "./components/ResearchView";
import CustomerRegulatoryView from "./components/CustomerRegulatoryView";
import { AppSkeleton } from "./components/Skeleton";

export const toSlug = (name: string) =>
  name.toLowerCase().replace(/\s+/g, "-");

function fromSlug(slug: string | undefined, tabs: string[]): string {
  if (!slug) return tabs[0];
  return tabs.find((t) => toSlug(t) === slug) ?? tabs[0];
}

function App() {
  const { activeProfile, loading } = useProfile();
  const location = useLocation();
  const navigate = useNavigate();

  const segments = location.pathname.split("/").filter(Boolean);
  const urlTabSlug = segments[1];

  const tabs = activeProfile?.nav_tabs ?? [];
  const activeTab = fromSlug(urlTabSlug, tabs);

  useEffect(() => {
    if (!activeProfile) return;
    const [urlBu, urlTab] = location.pathname.split("/").filter(Boolean);
    const validBu = urlBu === activeProfile.business_unit;
    const validTab = urlTab && tabs.some((t) => toSlug(t) === urlTab);
    if (!validBu || !validTab) {
      navigate(
        `/${activeProfile.business_unit}/${toSlug(tabs[0])}`,
        { replace: true },
      );
    }
  }, [activeProfile, location.pathname, navigate, tabs]);

  if (loading) {
    return <AppSkeleton />;
  }

  const renderView = () => {
    if (!activeProfile) return null;

    switch (activeProfile.business_unit) {
      case "internal":
        return <InternalView activeTab={activeTab} />;
      case "research":
        return <ResearchView activeTab={activeTab} />;
      case "regulatory":
        return <CustomerRegulatoryView activeTab={activeTab} />;
      default:
        return <div className="p-8 text-gray-500">Unknown view</div>;
    }
  };

  const handleTabClick = (tab: string) => {
    if (!activeProfile) return;
    navigate(`/${activeProfile.business_unit}/${toSlug(tab)}`);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-meridian-900 text-white shadow-lg">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-meridian-500 font-bold text-white">
              M
            </div>
            <div>
              <h1 className="text-lg font-semibold leading-tight">
                Meridian Portal
              </h1>
              <p className="text-xs text-meridian-300">
                Regulatory Intelligence & Research Analytics
              </p>
            </div>
          </div>
          <ProfileSwitcher />
        </div>

        <div className="mx-auto max-w-7xl px-4">
          <nav className="flex gap-1">
            {tabs.map((tab) => (
              <button
                key={tab}
                onClick={() => handleTabClick(tab)}
                className={`rounded-t-lg px-4 py-2 text-sm font-medium transition-colors ${
                  tab === activeTab
                    ? "bg-gray-50 text-meridian-900"
                    : "text-meridian-200 hover:bg-meridian-800 hover:text-white"
                }`}
              >
                {tab}
              </button>
            ))}
          </nav>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-6">{renderView()}</main>
    </div>
  );
}

export default App;
