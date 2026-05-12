#pragma once
#include "plan/physical.h"
#include "executor/interpreter.h"
#include "codegen/llvm_codegen.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <functional>

namespace qc {

// ─── Speculative (adaptive) query execution ───────────────────────────────────
//
// Strategy:
//  1. Parse and plan the query.
//  2. Start the Volcano interpreter immediately — it begins producing partial
//     aggregate accumulators row by row.
//  3. Concurrently, LLVM compiles a type-specialized native function on a
//     background thread.
//  4. If LLVM finishes before the interpreter exhausts all rows:
//       a. Record the interpreter's current scan position (checkpoint).
//       b. Discard partial accumulator from the interpreter.
//       c. Run the JIT function from row 0 through all rows — it's fast enough
//          that re-scanning is cheaper than merging partial state.
//     Otherwise: the interpreter finishes first; use its result directly.
//  5. Return results plus timing metadata.
//
// The hot-swap is safe because:
//  - Scan operators expose their current row index via rows_consumed().
//  - The JIT function takes a start_row parameter so it can resume mid-table.
//  - Aggregates are idempotent re-runs (no external side effects).

struct ExecutionStats {
    std::chrono::microseconds parse_plan_time{0};
    std::chrono::microseconds interp_time{0};
    std::chrono::microseconds compile_time{0};
    std::chrono::microseconds jit_exec_time{0};
    std::chrono::microseconds total_time{0};
    bool  jit_used{false};
    bool  hot_swap_occurred{false};
    int64_t rows_interpreted{0};
    int64_t rows_jit{0};
    int64_t total_rows{0};
};

class SpeculativeEngine {
public:
    SpeculativeEngine();
    ~SpeculativeEngine();

    // Execute a SQL query with speculative JIT compilation.
    Result execute(const std::string& sql, ExecutionStats* stats = nullptr);

    // Execute a pre-built physical plan
    Result execute_plan(const plan::PhysicalNode& plan, ExecutionStats* stats = nullptr);

    // Warm up LLVM (initialize target, passes) — call once at startup
    static void warmup();

private:
    Result run_interpreter_only(const plan::PhysicalNode& plan,
                                 ExecutionStats* stats);

    Result run_speculative(const plan::PhysicalNode& plan,
                            ExecutionStats* stats);

    bool supports_jit(const plan::PhysicalNode& plan);

    // Merge interpreter partial aggregates with JIT-computed remainder
    Result merge_agg_results(
        const Result& interp_partial,
        const JitResult& jit_remainder,
        const plan::PhysicalHashAggregate& agg_info);
};

// ─── Pretty-print helpers ─────────────────────────────────────────────────────

void print_result(const Result& r, const plan::Schema& schema, int max_rows = 20);
void print_stats(const ExecutionStats& s);

} // namespace qc
