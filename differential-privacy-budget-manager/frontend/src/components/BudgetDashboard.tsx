import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import { ShieldAlert, ShieldCheck, Database, Activity } from "lucide-react";
import { getBudgetSummary, getQueryLogs } from "../api/client";
import type { BudgetSummary, QueryLog } from "../types";
import BudgetGauge from "./BudgetGauge";

export default function BudgetDashboard() {
  const [summaries, setSummaries] = useState<BudgetSummary[]>([]);
  const [logs, setLogs] = useState<QueryLog[]>([]);
  const [loading, setLoading] = useState(true);

  const load = async () => {
    setLoading(true);
    const [s, l] = await Promise.all([getBudgetSummary(), getQueryLogs({ limit: 20 })]);
    setSummaries(s);
    setLogs(l);
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const exhausted = summaries.filter((s) => s.is_exhausted).length;
  const totalQueries = summaries.reduce((acc, s) => acc + s.query_count, 0);
  const chartData = summaries.map((s) => ({
    name: `${s.analyst_username}/${s.dataset_name.slice(0, 10)}`,
    consumed: +s.consumed_epsilon.toFixed(3),
    remaining: +s.remaining_epsilon.toFixed(3),
    exhausted: s.is_exhausted,
  }));

  if (loading) return <div className="flex items-center justify-center h-64 text-gray-400">Loading…</div>;

  return (
    <div className="space-y-6">
      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { icon: Database, label: "Allocations", value: summaries.length, color: "text-blue-600" },
          { icon: ShieldAlert, label: "Exhausted", value: exhausted, color: "text-red-500" },
          { icon: ShieldCheck, label: "Active", value: summaries.length - exhausted, color: "text-green-500" },
          { icon: Activity, label: "Total Queries", value: totalQueries, color: "text-purple-500" },
        ].map(({ icon: Icon, label, value, color }) => (
          <div key={label} className="bg-white rounded-xl shadow-sm p-4 flex items-center gap-3">
            <Icon className={`w-8 h-8 ${color}`} />
            <div>
              <p className="text-xs text-gray-500">{label}</p>
              <p className="text-2xl font-bold text-gray-800">{value}</p>
            </div>
          </div>
        ))}
      </div>

      {/* Bar chart */}
      <div className="bg-white rounded-xl shadow-sm p-5">
        <h3 className="font-semibold text-gray-700 mb-4">Privacy Budget Consumption (ε)</h3>
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={chartData} margin={{ top: 4, right: 8, bottom: 40, left: 0 }}>
            <XAxis dataKey="name" tick={{ fontSize: 11 }} angle={-30} textAnchor="end" />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => v.toFixed(4)} />
            <Bar dataKey="consumed" name="Consumed ε" radius={[4, 4, 0, 0]}>
              {chartData.map((d, i) => (
                <Cell key={i} fill={d.exhausted ? "#ef4444" : "#6366f1"} />
              ))}
            </Bar>
            <Bar dataKey="remaining" name="Remaining ε" fill="#e0e7ff" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Per-allocation gauges */}
      <div className="bg-white rounded-xl shadow-sm p-5">
        <h3 className="font-semibold text-gray-700 mb-4">Allocation Details</h3>
        <div className="space-y-4">
          {summaries.map((s) => (
            <div key={`${s.dataset_id}-${s.analyst_id}`} className="border rounded-lg p-3">
              <div className="flex justify-between items-start mb-2">
                <div>
                  <span className="font-medium text-gray-800">{s.analyst_username}</span>
                  <span className="text-gray-400 mx-1">→</span>
                  <span className="text-gray-600">{s.dataset_name}</span>
                </div>
                <div className="flex items-center gap-2">
                  {s.is_exhausted && (
                    <span className="text-xs bg-red-100 text-red-700 px-2 py-0.5 rounded-full">Exhausted</span>
                  )}
                  <span className="text-xs text-gray-400">{s.query_count} queries</span>
                  <span className={`text-xs px-2 py-0.5 rounded-full ${s.exhaustion_policy === "block" ? "bg-gray-100 text-gray-600" : "bg-yellow-100 text-yellow-700"}`}>
                    {s.exhaustion_policy === "block" ? "Block" : "Inject noise"}
                  </span>
                </div>
              </div>
              <BudgetGauge consumed={s.consumed_epsilon} total={s.total_epsilon} />
            </div>
          ))}
        </div>
      </div>

      {/* Recent query log */}
      <div className="bg-white rounded-xl shadow-sm p-5">
        <h3 className="font-semibold text-gray-700 mb-3">Recent Query Activity</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-500 border-b">
                <th className="pb-2 pr-4">Time</th>
                <th className="pb-2 pr-4">Query</th>
                <th className="pb-2 pr-4">Type</th>
                <th className="pb-2 pr-4">ε cost</th>
                <th className="pb-2 pr-4">Mechanism</th>
                <th className="pb-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {logs.map((log) => (
                <tr key={log.id} className="border-b last:border-0 hover:bg-gray-50">
                  <td className="py-2 pr-4 text-gray-400 whitespace-nowrap">
                    {new Date(log.created_at).toLocaleTimeString()}
                  </td>
                  <td className="py-2 pr-4 text-gray-700 max-w-xs truncate">{log.query_text}</td>
                  <td className="py-2 pr-4">
                    <span className="text-xs bg-indigo-50 text-indigo-700 px-2 py-0.5 rounded">{log.query_type}</span>
                  </td>
                  <td className="py-2 pr-4 font-mono">{log.epsilon_requested.toFixed(3)}</td>
                  <td className="py-2 pr-4 text-gray-500">{log.mechanism_used}</td>
                  <td className="py-2">
                    <span className={`text-xs px-2 py-0.5 rounded-full ${
                      log.status === "allowed" ? "bg-green-100 text-green-700" :
                      log.status === "blocked" ? "bg-red-100 text-red-700" :
                      "bg-yellow-100 text-yellow-700"
                    }`}>
                      {log.status}
                    </span>
                  </td>
                </tr>
              ))}
              {logs.length === 0 && (
                <tr><td colSpan={6} className="py-8 text-center text-gray-400">No queries yet</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="text-right">
        <button onClick={load} className="text-sm text-indigo-600 hover:underline">Refresh</button>
      </div>
    </div>
  );
}
