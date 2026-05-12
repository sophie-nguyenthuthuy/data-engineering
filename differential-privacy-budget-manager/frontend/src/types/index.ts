export type Mechanism = "laplace" | "gaussian";
export type QueryType = "count" | "sum" | "mean" | "histogram";
export type ExhaustionPolicy = "block" | "inject_noise";

export interface Dataset {
  id: string;
  name: string;
  description: string;
  owner_id: string;
  sensitivity: number;
  data_range_min: number | null;
  data_range_max: number | null;
  created_at: string;
}

export interface Analyst {
  id: string;
  username: string;
  email: string;
  role: string;
  created_at: string;
}

export interface BudgetAllocation {
  id: string;
  dataset_id: string;
  analyst_id: string;
  total_epsilon: number;
  consumed_epsilon: number;
  remaining_epsilon: number;
  total_delta: number;
  consumed_delta: number;
  percent_used?: number;
  is_exhausted: boolean;
  exhaustion_policy: ExhaustionPolicy;
  default_mechanism: Mechanism;
  created_at: string;
  updated_at: string | null;
}

export interface BudgetSummary {
  dataset_id: string;
  dataset_name: string;
  analyst_id: string;
  analyst_username: string;
  total_epsilon: number;
  consumed_epsilon: number;
  remaining_epsilon: number;
  percent_used: number;
  is_exhausted: boolean;
  query_count: number;
  exhaustion_policy: ExhaustionPolicy;
}

export interface QueryRequest {
  dataset_id: string;
  analyst_id: string;
  query_type: QueryType;
  query_text: string;
  true_result: number;
  epsilon_requested: number;
  delta_requested: number;
  sensitivity?: number;
  mechanism?: Mechanism;
}

export interface QueryResponse {
  query_id: string;
  status: "allowed" | "noised" | "blocked";
  result: number | null;
  noise_added: number | null;
  epsilon_consumed: number;
  budget_remaining: number;
  mechanism_used: Mechanism;
  message: string;
}

export interface QueryLog {
  id: string;
  dataset_id: string;
  analyst_id: string;
  query_type: QueryType;
  query_text: string;
  noisy_result: number | null;
  epsilon_requested: number;
  mechanism_used: Mechanism;
  status: string;
  budget_remaining_after: number | null;
  created_at: string;
}
