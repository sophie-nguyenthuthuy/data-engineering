import { useEffect, useState } from "react";
import { Send, Lock, ShieldCheck, AlertTriangle } from "lucide-react";
import { getDatasets, getAnalysts, submitQuery } from "../api/client";
import type { Dataset, Analyst, QueryResponse } from "../types";

export default function QueryGateway() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [analysts, setAnalysts] = useState<Analyst[]>([]);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [form, setForm] = useState({
    dataset_id: "",
    analyst_id: "",
    query_type: "count" as const,
    query_text: "",
    true_result: "",
    epsilon_requested: "0.5",
    delta_requested: "0.00001",
    sensitivity: "",
    mechanism: "" as "" | "laplace" | "gaussian",
  });

  useEffect(() => {
    Promise.all([getDatasets(), getAnalysts()]).then(([d, a]) => {
      setDatasets(d);
      setAnalysts(a);
      if (d[0]) setForm((f) => ({ ...f, dataset_id: d[0].id }));
      if (a[0]) setForm((f) => ({ ...f, analyst_id: a[0].id }));
    });
  }, []);

  const set = (k: string, v: string) => setForm((f) => ({ ...f, [k]: v }));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setResult(null);
    setLoading(true);
    try {
      const res = await submitQuery({
        dataset_id: form.dataset_id,
        analyst_id: form.analyst_id,
        query_type: form.query_type as any,
        query_text: form.query_text,
        true_result: parseFloat(form.true_result),
        epsilon_requested: parseFloat(form.epsilon_requested),
        delta_requested: parseFloat(form.delta_requested),
        sensitivity: form.sensitivity ? parseFloat(form.sensitivity) : undefined,
        mechanism: form.mechanism || undefined,
      });
      setResult(res);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const inputCls = "w-full border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300";
  const labelCls = "block text-xs font-medium text-gray-600 mb-1";

  return (
    <div className="grid md:grid-cols-2 gap-6">
      {/* Form */}
      <div className="bg-white rounded-xl shadow-sm p-5">
        <h3 className="font-semibold text-gray-800 mb-4 flex items-center gap-2">
          <Send className="w-4 h-4 text-indigo-500" /> Submit Query
        </h3>
        <form onSubmit={handleSubmit} className="space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className={labelCls}>Dataset</label>
              <select className={inputCls} value={form.dataset_id} onChange={(e) => set("dataset_id", e.target.value)} required>
                {datasets.map((d) => <option key={d.id} value={d.id}>{d.name}</option>)}
              </select>
            </div>
            <div>
              <label className={labelCls}>Analyst</label>
              <select className={inputCls} value={form.analyst_id} onChange={(e) => set("analyst_id", e.target.value)} required>
                {analysts.map((a) => <option key={a.id} value={a.id}>{a.username}</option>)}
              </select>
            </div>
          </div>

          <div>
            <label className={labelCls}>Query description</label>
            <input className={inputCls} value={form.query_text} onChange={(e) => set("query_text", e.target.value)}
              placeholder="e.g. COUNT patients with diabetes" required />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className={labelCls}>Query type</label>
              <select className={inputCls} value={form.query_type} onChange={(e) => set("query_type", e.target.value)}>
                {["count", "sum", "mean", "histogram"].map((t) => <option key={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label className={labelCls}>True result (unnoised)</label>
              <input className={inputCls} type="number" step="any" value={form.true_result}
                onChange={(e) => set("true_result", e.target.value)} required />
            </div>
          </div>

          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className={labelCls}>ε requested</label>
              <input className={inputCls} type="number" step="0.001" min="0.001" value={form.epsilon_requested}
                onChange={(e) => set("epsilon_requested", e.target.value)} required />
            </div>
            <div>
              <label className={labelCls}>δ (Gaussian only)</label>
              <input className={inputCls} type="number" step="any" value={form.delta_requested}
                onChange={(e) => set("delta_requested", e.target.value)} />
            </div>
            <div>
              <label className={labelCls}>Sensitivity (opt)</label>
              <input className={inputCls} type="number" step="any" value={form.sensitivity}
                onChange={(e) => set("sensitivity", e.target.value)} placeholder="auto" />
            </div>
          </div>

          <div>
            <label className={labelCls}>Mechanism override</label>
            <select className={inputCls} value={form.mechanism} onChange={(e) => set("mechanism", e.target.value)}>
              <option value="">Use allocation default</option>
              <option value="laplace">Laplace</option>
              <option value="gaussian">Gaussian</option>
            </select>
          </div>

          {error && <p className="text-sm text-red-600 bg-red-50 rounded p-2">{error}</p>}

          <button type="submit" disabled={loading}
            className="w-full bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg py-2 text-sm font-medium transition disabled:opacity-50">
            {loading ? "Submitting…" : "Submit Query"}
          </button>
        </form>
      </div>

      {/* Result */}
      <div className="bg-white rounded-xl shadow-sm p-5">
        <h3 className="font-semibold text-gray-800 mb-4">Query Result</h3>
        {!result ? (
          <div className="flex flex-col items-center justify-center h-64 text-gray-300">
            <Send className="w-12 h-12 mb-2" />
            <p className="text-sm">Submit a query to see the result</p>
          </div>
        ) : (
          <div className="space-y-4">
            {/* Status banner */}
            <div className={`flex items-center gap-3 p-3 rounded-lg ${
              result.status === "allowed" ? "bg-green-50 text-green-800" :
              result.status === "blocked" ? "bg-red-50 text-red-800" :
              "bg-yellow-50 text-yellow-800"
            }`}>
              {result.status === "allowed" ? <ShieldCheck className="w-5 h-5" /> :
               result.status === "blocked" ? <Lock className="w-5 h-5" /> :
               <AlertTriangle className="w-5 h-5" />}
              <div>
                <p className="font-semibold capitalize">{result.status}</p>
                <p className="text-sm">{result.message}</p>
              </div>
            </div>

            {result.result !== null && (
              <div className="grid grid-cols-2 gap-3">
                <div className="bg-gray-50 rounded-lg p-3">
                  <p className="text-xs text-gray-500 mb-1">Privatized Result</p>
                  <p className="text-2xl font-mono font-bold text-gray-800">{result.result.toFixed(4)}</p>
                </div>
                <div className="bg-gray-50 rounded-lg p-3">
                  <p className="text-xs text-gray-500 mb-1">Noise Added</p>
                  <p className="text-2xl font-mono font-bold text-gray-800">
                    {result.noise_added !== null ? result.noise_added.toFixed(4) : "—"}
                  </p>
                </div>
              </div>
            )}

            <div className="grid grid-cols-3 gap-3 text-center">
              {[
                { label: "ε Consumed", value: result.epsilon_consumed.toFixed(4) },
                { label: "ε Remaining", value: result.budget_remaining.toFixed(4) },
                { label: "Mechanism", value: result.mechanism_used },
              ].map(({ label, value }) => (
                <div key={label} className="border rounded-lg p-2">
                  <p className="text-xs text-gray-500">{label}</p>
                  <p className="font-semibold text-sm text-gray-700">{value}</p>
                </div>
              ))}
            </div>

            <p className="text-xs text-gray-400">Query ID: {result.query_id}</p>
          </div>
        )}
      </div>
    </div>
  );
}
