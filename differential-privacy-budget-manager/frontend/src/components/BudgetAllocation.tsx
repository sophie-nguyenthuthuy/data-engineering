import { useEffect, useState } from "react";
import { Plus, RefreshCw, Edit2, Check, X } from "lucide-react";
import {
  getDatasets, getAnalysts, getBudgets,
  createBudget, updateBudget, resetBudget,
} from "../api/client";
import type { Dataset, Analyst, BudgetAllocation as BA } from "../types";
import BudgetGauge from "./BudgetGauge";

export default function BudgetAllocationPanel() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [analysts, setAnalysts] = useState<Analyst[]>([]);
  const [allocations, setAllocations] = useState<BA[]>([]);
  const [editId, setEditId] = useState<string | null>(null);
  const [editEps, setEditEps] = useState("");
  const [showNew, setShowNew] = useState(false);
  const [newForm, setNewForm] = useState({
    dataset_id: "",
    analyst_id: "",
    total_epsilon: "5",
    total_delta: "0.00001",
    exhaustion_policy: "block",
    default_mechanism: "laplace",
  });
  const [status, setStatus] = useState("");

  const load = async () => {
    const [d, a, b] = await Promise.all([getDatasets(), getAnalysts(), getBudgets()]);
    setDatasets(d);
    setAnalysts(a);
    setAllocations(b);
    if (d[0] && !newForm.dataset_id) setNewForm((f) => ({ ...f, dataset_id: d[0].id }));
    if (a[0] && !newForm.analyst_id) setNewForm((f) => ({ ...f, analyst_id: a[0].id }));
  };

  useEffect(() => { load(); }, []);

  const datasetName = (id: string) => datasets.find((d) => d.id === id)?.name ?? id;
  const analystName = (id: string) => analysts.find((a) => a.id === id)?.username ?? id;

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await createBudget({
        ...newForm,
        total_epsilon: parseFloat(newForm.total_epsilon),
        total_delta: parseFloat(newForm.total_delta),
      });
      setShowNew(false);
      setStatus("Allocation created.");
      load();
    } catch (err: any) { setStatus(`Error: ${err.message}`); }
  };

  const handleSaveEdit = async (id: string) => {
    await updateBudget(id, { total_epsilon: parseFloat(editEps) });
    setEditId(null);
    load();
  };

  const handleReset = async (id: string) => {
    if (!confirm("Reset budget consumption to 0?")) return;
    await resetBudget(id);
    load();
  };

  const inputCls = "border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300";

  return (
    <div className="space-y-5">
      <div className="flex justify-between items-center">
        <h3 className="font-semibold text-gray-800">Budget Allocations</h3>
        <button onClick={() => setShowNew(!showNew)}
          className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-700 text-white text-sm px-3 py-1.5 rounded-lg transition">
          <Plus className="w-4 h-4" /> New Allocation
        </button>
      </div>

      {status && <p className="text-sm text-indigo-700 bg-indigo-50 rounded p-2">{status}</p>}

      {showNew && (
        <form onSubmit={handleCreate} className="bg-indigo-50 rounded-xl p-4 space-y-3 border border-indigo-100">
          <p className="text-sm font-medium text-indigo-800">New Allocation</p>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-xs text-gray-600 mb-1 block">Dataset</label>
              <select className={inputCls + " w-full"} value={newForm.dataset_id}
                onChange={(e) => setNewForm((f) => ({ ...f, dataset_id: e.target.value }))}>
                {datasets.map((d) => <option key={d.id} value={d.id}>{d.name}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-600 mb-1 block">Analyst</label>
              <select className={inputCls + " w-full"} value={newForm.analyst_id}
                onChange={(e) => setNewForm((f) => ({ ...f, analyst_id: e.target.value }))}>
                {analysts.map((a) => <option key={a.id} value={a.id}>{a.username}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-600 mb-1 block">Total ε</label>
              <input className={inputCls + " w-full"} type="number" step="0.1" min="0.01"
                value={newForm.total_epsilon} onChange={(e) => setNewForm((f) => ({ ...f, total_epsilon: e.target.value }))} />
            </div>
            <div>
              <label className="text-xs text-gray-600 mb-1 block">Total δ</label>
              <input className={inputCls + " w-full"} type="number" step="any"
                value={newForm.total_delta} onChange={(e) => setNewForm((f) => ({ ...f, total_delta: e.target.value }))} />
            </div>
            <div>
              <label className="text-xs text-gray-600 mb-1 block">On exhaustion</label>
              <select className={inputCls + " w-full"} value={newForm.exhaustion_policy}
                onChange={(e) => setNewForm((f) => ({ ...f, exhaustion_policy: e.target.value }))}>
                <option value="block">Block queries</option>
                <option value="inject_noise">Inject heavy noise</option>
              </select>
            </div>
            <div>
              <label className="text-xs text-gray-600 mb-1 block">Default mechanism</label>
              <select className={inputCls + " w-full"} value={newForm.default_mechanism}
                onChange={(e) => setNewForm((f) => ({ ...f, default_mechanism: e.target.value }))}>
                <option value="laplace">Laplace</option>
                <option value="gaussian">Gaussian</option>
              </select>
            </div>
          </div>
          <div className="flex gap-2">
            <button type="submit" className="bg-indigo-600 text-white text-sm px-4 py-1.5 rounded-lg hover:bg-indigo-700">Create</button>
            <button type="button" onClick={() => setShowNew(false)} className="text-gray-600 text-sm px-3 py-1.5 rounded-lg hover:bg-gray-100">Cancel</button>
          </div>
        </form>
      )}

      <div className="space-y-3">
        {allocations.map((alloc) => (
          <div key={alloc.id} className={`bg-white rounded-xl shadow-sm p-4 border-l-4 ${alloc.is_exhausted ? "border-red-400" : "border-green-400"}`}>
            <div className="flex justify-between items-start mb-3">
              <div>
                <span className="font-medium text-gray-800">{analystName(alloc.analyst_id)}</span>
                <span className="text-gray-400 mx-1.5">→</span>
                <span className="text-indigo-600 font-medium">{datasetName(alloc.dataset_id)}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className={`text-xs px-2 py-0.5 rounded-full ${alloc.default_mechanism === "laplace" ? "bg-blue-50 text-blue-700" : "bg-purple-50 text-purple-700"}`}>
                  {alloc.default_mechanism}
                </span>
                <span className={`text-xs px-2 py-0.5 rounded-full ${alloc.exhaustion_policy === "block" ? "bg-gray-100 text-gray-600" : "bg-orange-50 text-orange-700"}`}>
                  {alloc.exhaustion_policy === "block" ? "block" : "inject noise"}
                </span>
                {alloc.is_exhausted && (
                  <span className="text-xs bg-red-100 text-red-700 px-2 py-0.5 rounded-full font-medium">EXHAUSTED</span>
                )}
              </div>
            </div>

            <BudgetGauge consumed={alloc.consumed_epsilon} total={alloc.total_epsilon} />

            <div className="flex justify-between items-center mt-3">
              <div className="text-xs text-gray-400">
                δ consumed: {alloc.consumed_delta.toExponential(2)} / {alloc.total_delta.toExponential(2)}
              </div>
              <div className="flex items-center gap-2">
                {editId === alloc.id ? (
                  <>
                    <span className="text-xs text-gray-500">New ε total:</span>
                    <input className="border rounded px-2 py-0.5 text-sm w-20" type="number" step="0.1"
                      value={editEps} onChange={(e) => setEditEps(e.target.value)} />
                    <button onClick={() => handleSaveEdit(alloc.id)} className="text-green-600 hover:text-green-800"><Check className="w-4 h-4" /></button>
                    <button onClick={() => setEditId(null)} className="text-gray-400 hover:text-gray-600"><X className="w-4 h-4" /></button>
                  </>
                ) : (
                  <button onClick={() => { setEditId(alloc.id); setEditEps(String(alloc.total_epsilon)); }}
                    className="text-xs text-indigo-600 hover:underline flex items-center gap-1">
                    <Edit2 className="w-3 h-3" /> Adjust ε
                  </button>
                )}
                <button onClick={() => handleReset(alloc.id)}
                  className="text-xs text-gray-500 hover:text-red-600 flex items-center gap-1">
                  <RefreshCw className="w-3 h-3" /> Reset
                </button>
              </div>
            </div>
          </div>
        ))}
        {allocations.length === 0 && (
          <p className="text-center text-gray-400 py-8">No allocations yet</p>
        )}
      </div>
    </div>
  );
}
