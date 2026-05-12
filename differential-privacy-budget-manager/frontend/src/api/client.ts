import type {
  Dataset,
  Analyst,
  BudgetAllocation,
  BudgetSummary,
  QueryRequest,
  QueryResponse,
  QueryLog,
} from "../types";

const BASE = "/api";

async function req<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail ?? "Request failed");
  }
  return res.json();
}

// Datasets
export const getDatasets = () => req<Dataset[]>("/datasets/");
export const createDataset = (data: Omit<Dataset, "id" | "created_at">) =>
  req<Dataset>("/datasets/", { method: "POST", body: JSON.stringify(data) });

// Analysts
export const getAnalysts = () => req<Analyst[]>("/analysts/");
export const createAnalyst = (data: Omit<Analyst, "id" | "created_at">) =>
  req<Analyst>("/analysts/", { method: "POST", body: JSON.stringify(data) });

// Budget allocations
export const getBudgets = (params?: { dataset_id?: string; analyst_id?: string }) => {
  const qs = new URLSearchParams(params as Record<string, string>).toString();
  return req<BudgetAllocation[]>(`/budgets/${qs ? "?" + qs : ""}`);
};
export const createBudget = (data: {
  dataset_id: string;
  analyst_id: string;
  total_epsilon: number;
  total_delta: number;
  exhaustion_policy: string;
  default_mechanism: string;
}) => req<BudgetAllocation>("/budgets/", { method: "POST", body: JSON.stringify(data) });

export const updateBudget = (
  id: string,
  data: Partial<Pick<BudgetAllocation, "total_epsilon" | "total_delta" | "exhaustion_policy" | "default_mechanism">>
) => req<BudgetAllocation>(`/budgets/${id}`, { method: "PATCH", body: JSON.stringify(data) });

export const resetBudget = (id: string) =>
  req<{ detail: string }>(`/budgets/${id}/reset`, { method: "POST" });

export const getBudgetSummary = () => req<BudgetSummary[]>("/budgets/summary/all");

// Queries
export const submitQuery = (data: QueryRequest) =>
  req<QueryResponse>("/queries/", { method: "POST", body: JSON.stringify(data) });

export const getQueryLogs = (params?: { dataset_id?: string; analyst_id?: string; limit?: number }) => {
  const qs = new URLSearchParams(
    Object.fromEntries(Object.entries(params ?? {}).filter(([, v]) => v !== undefined).map(([k, v]) => [k, String(v)]))
  ).toString();
  return req<QueryLog[]>(`/queries/logs${qs ? "?" + qs : ""}`);
};
