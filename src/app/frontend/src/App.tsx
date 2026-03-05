import { useState } from "react";
import { useProfile } from "./contexts/ProfileContext";
import ProfileSwitcher from "./components/ProfileSwitcher";
import InternalView from "./components/InternalView";
import ResearchView from "./components/ResearchView";
import CustomerRegulatoryView from "./components/CustomerRegulatoryView";

function App() {
  const { activeProfile, loading } = useProfile();
  const [activeTab, setActiveTab] = useState(0);

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-lg text-gray-500">Loading Meridian Portal...</div>
      </div>
    );
  }

  const tabs = activeProfile?.nav_tabs ?? [];

  const renderView = () => {
    if (!activeProfile) return null;

    switch (activeProfile.business_unit) {
      case "internal":
        return <InternalView activeTab={tabs[activeTab]} />;
      case "research":
        return <ResearchView activeTab={tabs[activeTab]} />;
      case "regulatory":
        return <CustomerRegulatoryView activeTab={tabs[activeTab]} />;
      default:
        return <div className="p-8 text-gray-500">Unknown view</div>;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Top Navigation */}
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

        {/* Tab Bar */}
        <div className="mx-auto max-w-7xl px-4">
          <nav className="flex gap-1">
            {tabs.map((tab, i) => (
              <button
                key={tab}
                onClick={() => setActiveTab(i)}
                className={`rounded-t-lg px-4 py-2 text-sm font-medium transition-colors ${
                  i === activeTab
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

      {/* Content */}
      <main className="mx-auto max-w-7xl px-4 py-6">{renderView()}</main>
    </div>
  );
}

export default App;
