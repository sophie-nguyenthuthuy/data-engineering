import { useState } from "react";
import { Shield, LayoutDashboard, Send, Sliders, Database } from "lucide-react";
import BudgetDashboard from "./components/BudgetDashboard";
import QueryGateway from "./components/QueryGateway";
import BudgetAllocationPanel from "./components/BudgetAllocation";
import DatasetManager from "./components/DatasetManager";

type Tab = "dashboard" | "query" | "allocations" | "manage";

const tabs: { id: Tab; label: string; icon: React.ElementType }[] = [
  { id: "dashboard", label: "Dashboard", icon: LayoutDashboard },
  { id: "query", label: "Query Gateway", icon: Send },
  { id: "allocations", label: "Budget Allocations", icon: Sliders },
  { id: "manage", label: "Datasets & Analysts", icon: Database },
];

export default function App() {
  const [active, setActive] = useState<Tab>("dashboard");

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 flex items-center justify-between h-14">
          <div className="flex items-center gap-2.5">
            <Shield className="w-6 h-6 text-indigo-600" />
            <span className="font-bold text-gray-900 text-lg">DP Budget Manager</span>
            <span className="hidden sm:inline text-xs bg-indigo-100 text-indigo-700 px-2 py-0.5 rounded-full ml-1">
              Differential Privacy
            </span>
          </div>
          <nav className="flex gap-1">
            {tabs.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                onClick={() => setActive(id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition ${
                  active === id
                    ? "bg-indigo-50 text-indigo-700"
                    : "text-gray-500 hover:text-gray-800 hover:bg-gray-100"
                }`}
              >
                <Icon className="w-4 h-4" />
                <span className="hidden md:inline">{label}</span>
              </button>
            ))}
          </nav>
        </div>
      </header>

      {/* Page content */}
      <main className="max-w-6xl mx-auto px-4 sm:px-6 py-6">
        <div className="mb-5">
          <h2 className="text-xl font-bold text-gray-900">
            {tabs.find((t) => t.id === active)?.label}
          </h2>
          <p className="text-sm text-gray-500 mt-0.5">
            {active === "dashboard" && "Monitor cumulative ε consumption across all analyst–dataset pairs."}
            {active === "query" && "Submit queries through the privacy gateway — noise is added automatically."}
            {active === "allocations" && "Data owners: grant, adjust, and reset privacy budgets per analyst."}
            {active === "manage" && "Register datasets and analysts in the system."}
          </p>
        </div>

        {active === "dashboard" && <BudgetDashboard />}
        {active === "query" && <QueryGateway />}
        {active === "allocations" && <BudgetAllocationPanel />}
        {active === "manage" && <DatasetManager />}
      </main>
    </div>
  );
}
