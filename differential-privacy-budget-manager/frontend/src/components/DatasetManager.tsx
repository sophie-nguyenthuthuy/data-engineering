import { useEffect, useState } from "react";
import { Plus, Database } from "lucide-react";
import { getDatasets, getAnalysts, createDataset, createAnalyst } from "../api/client";
import type { Dataset, Analyst } from "../types";

export default function DatasetManager() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [analysts, setAnalysts] = useState<Analyst[]>([]);
  const [tab, setTab] = useState<"datasets" | "analysts">("datasets");
  const [showForm, setShowForm] = useState(false);
  const [status, setStatus] = useState("");

  const [dsForm, setDsForm] = useState({ name: "", description: "", owner_id: "owner-1", sensitivity: "1.0", data_range_min: "", data_range_max: "" });
  const [anaForm, setAnaForm] = useState({ username: "", email: "", role: "analyst" });

  const load = async () => {
    const [d, a] = await Promise.all([getDatasets(), getAnalysts()]);
    setDatasets(d);
    setAnalysts(a);
  };
  useEffect(() => { load(); }, []);

  const handleCreateDataset = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await createDataset({
        ...dsForm,
        sensitivity: parseFloat(dsForm.sensitivity),
        data_range_min: dsForm.data_range_min ? parseFloat(dsForm.data_range_min) : null,
        data_range_max: dsForm.data_range_max ? parseFloat(dsForm.data_range_max) : null,
      });
      setShowForm(false);
      setStatus("Dataset created.");
      load();
    } catch (err: any) { setStatus(`Error: ${err.message}`); }
  };

  const handleCreateAnalyst = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await createAnalyst(anaForm);
      setShowForm(false);
      setStatus("Analyst created.");
      load();
    } catch (err: any) { setStatus(`Error: ${err.message}`); }
  };

  const inputCls = "w-full border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300";

  return (
    <div className="space-y-5">
      <div className="flex justify-between items-center">
        <div className="flex gap-1 bg-gray-100 rounded-lg p-1">
          {(["datasets", "analysts"] as const).map((t) => (
            <button key={t} onClick={() => { setTab(t); setShowForm(false); }}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition ${tab === t ? "bg-white shadow text-indigo-700" : "text-gray-500 hover:text-gray-700"}`}>
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          ))}
        </div>
        <button onClick={() => setShowForm(!showForm)}
          className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-700 text-white text-sm px-3 py-1.5 rounded-lg transition">
          <Plus className="w-4 h-4" /> Add {tab === "datasets" ? "Dataset" : "Analyst"}
        </button>
      </div>

      {status && <p className="text-sm text-indigo-700 bg-indigo-50 rounded p-2">{status}</p>}

      {showForm && tab === "datasets" && (
        <form onSubmit={handleCreateDataset} className="bg-gray-50 rounded-xl p-4 space-y-3 border border-gray-200">
          <p className="text-sm font-medium text-gray-700">New Dataset</p>
          <div className="grid grid-cols-2 gap-3">
            <input className={inputCls} placeholder="Name" required value={dsForm.name} onChange={(e) => setDsForm((f) => ({ ...f, name: e.target.value }))} />
            <input className={inputCls} placeholder="Owner ID" required value={dsForm.owner_id} onChange={(e) => setDsForm((f) => ({ ...f, owner_id: e.target.value }))} />
            <input className={inputCls + " col-span-2"} placeholder="Description" value={dsForm.description} onChange={(e) => setDsForm((f) => ({ ...f, description: e.target.value }))} />
            <input className={inputCls} placeholder="Sensitivity (global)" type="number" step="any" value={dsForm.sensitivity} onChange={(e) => setDsForm((f) => ({ ...f, sensitivity: e.target.value }))} />
            <div className="flex gap-2">
              <input className={inputCls} placeholder="Range min" type="number" step="any" value={dsForm.data_range_min} onChange={(e) => setDsForm((f) => ({ ...f, data_range_min: e.target.value }))} />
              <input className={inputCls} placeholder="Range max" type="number" step="any" value={dsForm.data_range_max} onChange={(e) => setDsForm((f) => ({ ...f, data_range_max: e.target.value }))} />
            </div>
          </div>
          <div className="flex gap-2">
            <button type="submit" className="bg-indigo-600 text-white text-sm px-4 py-1.5 rounded-lg hover:bg-indigo-700">Create</button>
            <button type="button" onClick={() => setShowForm(false)} className="text-gray-600 text-sm px-3 py-1.5 rounded-lg hover:bg-gray-100">Cancel</button>
          </div>
        </form>
      )}

      {showForm && tab === "analysts" && (
        <form onSubmit={handleCreateAnalyst} className="bg-gray-50 rounded-xl p-4 space-y-3 border border-gray-200">
          <p className="text-sm font-medium text-gray-700">New Analyst</p>
          <div className="grid grid-cols-2 gap-3">
            <input className={inputCls} placeholder="Username" required value={anaForm.username} onChange={(e) => setAnaForm((f) => ({ ...f, username: e.target.value }))} />
            <input className={inputCls} placeholder="Email" type="email" required value={anaForm.email} onChange={(e) => setAnaForm((f) => ({ ...f, email: e.target.value }))} />
            <select className={inputCls + " col-span-2"} value={anaForm.role} onChange={(e) => setAnaForm((f) => ({ ...f, role: e.target.value }))}>
              {["analyst", "researcher", "data_scientist", "auditor"].map((r) => <option key={r}>{r}</option>)}
            </select>
          </div>
          <div className="flex gap-2">
            <button type="submit" className="bg-indigo-600 text-white text-sm px-4 py-1.5 rounded-lg hover:bg-indigo-700">Create</button>
            <button type="button" onClick={() => setShowForm(false)} className="text-gray-600 text-sm px-3 py-1.5 rounded-lg hover:bg-gray-100">Cancel</button>
          </div>
        </form>
      )}

      {tab === "datasets" && (
        <div className="grid md:grid-cols-2 gap-3">
          {datasets.map((d) => (
            <div key={d.id} className="bg-white rounded-xl shadow-sm p-4">
              <div className="flex items-start gap-3">
                <Database className="w-8 h-8 text-indigo-400 mt-0.5 shrink-0" />
                <div className="min-w-0">
                  <p className="font-medium text-gray-800">{d.name}</p>
                  <p className="text-sm text-gray-500 truncate">{d.description || "No description"}</p>
                  <div className="flex flex-wrap gap-2 mt-2">
                    <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded">Δf = {d.sensitivity}</span>
                    {d.data_range_min !== null && (
                      <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded">
                        range [{d.data_range_min}, {d.data_range_max}]
                      </span>
                    )}
                    <span className="text-xs bg-indigo-50 text-indigo-600 px-2 py-0.5 rounded">owner: {d.owner_id}</span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {tab === "analysts" && (
        <div className="grid md:grid-cols-2 gap-3">
          {analysts.map((a) => (
            <div key={a.id} className="bg-white rounded-xl shadow-sm p-4 flex items-center gap-3">
              <div className="w-10 h-10 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold text-lg shrink-0">
                {a.username[0].toUpperCase()}
              </div>
              <div>
                <p className="font-medium text-gray-800">{a.username}</p>
                <p className="text-sm text-gray-500">{a.email}</p>
                <span className="text-xs bg-purple-50 text-purple-700 px-2 py-0.5 rounded">{a.role}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
