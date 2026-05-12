#include "jit/speculative.h"
#include "plan/physical.h"
#include <iostream>
#include <iomanip>
#include <sstream>

namespace qc {

using namespace plan;
using Clock = std::chrono::high_resolution_clock;

SpeculativeEngine::SpeculativeEngine() {}
SpeculativeEngine::~SpeculativeEngine() {}

void SpeculativeEngine::warmup() {
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
}

// Returns true if this plan can be JIT-compiled (scan→[filter]→agg shape)
bool SpeculativeEngine::supports_jit(const PhysicalNode& plan) {
    const PhysicalNode* cur = &plan;
    if (cur->type() == PhysicalOpType::HASH_AGGREGATE)
        cur = std::get<PhysicalHashAggregate>(cur->op).child.get();
    else return false;

    if (cur->type() == PhysicalOpType::FILTER)
        cur = std::get<PhysicalFilter>(cur->op).child.get();

    return cur->type() == PhysicalOpType::SCAN;
}

Result SpeculativeEngine::execute(const std::string& sql, ExecutionStats* stats) {
    auto t0 = Clock::now();

    PhysicalNodePtr plan;
    try {
        plan = build_plan(sql, Catalog::instance());
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Plan error: ") + e.what());
    }

    if (stats) {
        stats->parse_plan_time = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - t0);
    }

    return execute_plan(*plan, stats);
}

Result SpeculativeEngine::execute_plan(const PhysicalNode& plan, ExecutionStats* stats) {
    if (!supports_jit(plan))
        return run_interpreter_only(plan, stats);
    return run_speculative(plan, stats);
}

Result SpeculativeEngine::run_interpreter_only(const PhysicalNode& plan,
                                                ExecutionStats* stats) {
    auto t0 = Clock::now();
    Interpreter interp;
    Result r = interp.execute(plan);
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0);
    if (stats) {
        stats->interp_time = elapsed;
        stats->total_time  = elapsed;
        stats->jit_used    = false;
    }
    return r;
}

// ─── Speculative execution ────────────────────────────────────────────────────
//
// Thread 1: Interpreter starts scanning and accumulating partial aggregates.
//           It tracks its current row position via rows_consumed().
//
// Thread 2: LLVM compiles the query pipeline (typically 50-200ms for OLAP).
//
// When thread 2 finishes:
//   If interpreter is still running:
//     → Signal interpreter to stop.
//     → Get its checkpoint (row K it stopped at).
//     → Run JIT function from row 0 (restart is cheaper than partial merge
//       for simple aggregates because JIT is 10-50x faster than the interpreter).
//   Else:
//     → Use interpreter result directly (JIT finished too late).

Result SpeculativeEngine::run_speculative(const PhysicalNode& plan,
                                           ExecutionStats* stats) {
    // Extract plan components
    const PhysicalHashAggregate& agg = std::get<PhysicalHashAggregate>(plan.op);
    const PhysicalNode* filter_or_scan = agg.child.get();
    const PhysicalScan* scan = nullptr;

    if (filter_or_scan->type() == PhysicalOpType::FILTER)
        scan = &std::get<PhysicalScan>(
            std::get<PhysicalFilter>(filter_or_scan->op).child->op);
    else
        scan = &std::get<PhysicalScan>(filter_or_scan->op);

    int64_t total_rows = scan->table->num_rows();
    if (stats) stats->total_rows = total_rows;

    // ── Shared state ─────────────────────────────────────────────────────────
    std::atomic<bool>  stop_interp{false};
    std::atomic<bool>  jit_ready{false};
    std::atomic<int64_t> interp_checkpoint{0};

    CompiledPipeline compiled;
    std::chrono::microseconds compile_time{0};
    std::string compile_error;

    // ── Background: LLVM compilation ─────────────────────────────────────────
    auto compile_t0 = Clock::now();
    std::thread compile_thread([&]() {
        LLVMCodegen codegen;
        bool ok = codegen.compile(plan, compiled);
        compile_time = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - compile_t0);
        if (!ok) compile_error = codegen.last_error();
        jit_ready.store(true, std::memory_order_release);
        stop_interp.store(true, std::memory_order_release);
    });

    // ── Foreground: interpreter ───────────────────────────────────────────────
    auto interp_t0 = Clock::now();

    // Run interpreter in its own thread so we can preempt it
    Result interp_result;
    bool   interp_finished = false;
    std::chrono::microseconds interp_time{0};

    std::thread interp_thread([&]() {
        // Use a custom scan iterator that checks the stop flag periodically
        Interpreter interp;

        // Build plan iterator
        auto iter = interp.build(plan, 0);
        iter->open();

        Row row;
        while (!stop_interp.load(std::memory_order_acquire)) {
            if (!iter->next(row)) {
                interp_finished = true;
                break;
            }
            interp_result.push_back(row);
            interp_checkpoint.store(iter->rows_consumed(), std::memory_order_relaxed);
        }

        if (!interp_finished) {
            // Record where we stopped
            interp_checkpoint.store(iter->rows_consumed(), std::memory_order_release);
        }

        iter->close();
        interp_time = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - interp_t0);
    });

    // Wait for completion
    compile_thread.join();
    interp_thread.join();

    if (stats) {
        stats->compile_time = compile_time;
        stats->interp_time  = interp_time;
        stats->rows_interpreted = interp_checkpoint.load();
    }

    // ── Decision: use JIT or interpreter result ───────────────────────────────
    if (interp_finished) {
        // Interpreter won the race
        if (stats) {
            stats->jit_used           = false;
            stats->hot_swap_occurred  = false;
            stats->total_time = std::max(interp_time, compile_time);
        }
        return interp_result;
    }

    if (!compiled.valid()) {
        // JIT failed — fall back to full interpreter run
        if (stats) stats->jit_used = false;
        Interpreter interp;
        return interp.execute(plan);
    }

    // Hot-swap: JIT re-runs from row 0 (full table)
    int64_t swap_point = interp_checkpoint.load();
    if (stats) {
        stats->hot_swap_occurred = true;
        stats->jit_used          = true;
        stats->rows_interpreted  = swap_point;
    }

    auto jit_t0 = Clock::now();
    JitResult jr = LLVMCodegen().run_scan_agg(compiled, *scan, agg, 0);
    auto jit_exec = std::chrono::duration_cast<std::chrono::microseconds>(
        Clock::now() - jit_t0);

    if (stats) {
        stats->jit_exec_time = jit_exec;
        stats->rows_jit      = jr.rows_scanned;
        stats->total_time    = compile_time + jit_exec;
    }

    // Convert JitResult → Result rows (one row per agg, for simple scalar aggs)
    Result final_result;
    if (!jr.scalar_aggs.empty()) {
        Row row;
        for (double v : jr.scalar_aggs) row.push_back(v);
        final_result.push_back(row);
    }
    return final_result;
}

// ─── Pretty printers ──────────────────────────────────────────────────────────

void print_result(const Result& r, const plan::Schema& schema, int max_rows) {
    // Print header
    for (auto& c : schema)
        std::cout << std::setw(16) << c.name << " ";
    std::cout << "\n" << std::string(schema.size() * 17, '-') << "\n";

    int shown = 0;
    for (auto& row : r) {
        if (shown++ >= max_rows) { std::cout << "... (" << r.size() << " rows total)\n"; break; }
        for (auto& v : row) {
            std::visit([](auto&& x) {
                using T = std::decay_t<decltype(x)>;
                if constexpr (std::is_same_v<T, std::monostate>)
                    std::cout << std::setw(16) << "NULL";
                else if constexpr (std::is_same_v<T, double>)
                    std::cout << std::setw(16) << std::fixed << std::setprecision(2) << x;
                else
                    std::cout << std::setw(16) << x;
            }, v);
            std::cout << " ";
        }
        std::cout << "\n";
    }
    std::cout << "(" << r.size() << " rows)\n";
}

void print_stats(const ExecutionStats& s) {
    auto us = [](std::chrono::microseconds t) {
        if (t.count() < 1000) return std::to_string(t.count()) + "µs";
        return std::to_string(t.count() / 1000) + "ms";
    };

    std::cout << "\n── Execution Stats ──────────────────────────────\n";
    std::cout << "  Plan:         " << us(s.parse_plan_time) << "\n";
    std::cout << "  Interpret:    " << us(s.interp_time)
              << " (" << s.rows_interpreted << " rows)\n";
    std::cout << "  Compile(JIT): " << us(s.compile_time) << "\n";
    if (s.jit_used)
        std::cout << "  JIT exec:     " << us(s.jit_exec_time)
                  << " (" << s.rows_jit << " rows)\n";
    std::cout << "  Total:        " << us(s.total_time) << "\n";
    std::cout << "  Mode:         "
              << (s.jit_used ? "JIT" : "Interpreter")
              << (s.hot_swap_occurred ? " (hot-swapped)" : "") << "\n";
    std::cout << "─────────────────────────────────────────────────\n";
}

} // namespace qc
