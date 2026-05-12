#pragma once
#include "plan/physical.h"
#include "storage/table.h"
#include <memory>
#include <functional>
#include <cstdint>

// Forward-declare LLVM types to avoid polluting headers with LLVM includes
namespace llvm {
class LLVMContext;
class Module;
class Function;
class Value;
template<typename T> class IRBuilder;
class BasicBlock;
namespace orc { class LLJIT; }
}

namespace qc {

// ─── Compiled query function signatures ──────────────────────────────────────
//
// After JIT compilation a query plan is lowered to one or more native functions.
// For simple scan+filter+aggregate pipelines (TPC-H Q1, Q6) we generate:
//
//   typedef void (*ScanAggFn)(
//       void** col_ptrs,      // array of raw column pointers
//       int64_t nrows,
//       double* agg_results   // output aggregates
//   );
//
// For hash joins we split into build + probe functions:
//
//   typedef void (*BuildFn)(void** col_ptrs, int64_t nrows, void* ht);
//   typedef void (*ProbeFn)(void** col_ptrs, int64_t nrows, void* ht, double* out);
//
// The codegen emits specialized code for the exact column types, inlining hash
// functions and comparison predicates — zero virtual dispatch in the hot path.

struct CompiledPipeline {
    // Opaque function pointers — cast based on the FnKind
    void*       fn_ptr{nullptr};
    std::string fn_name;

    // Metadata for partial execution during hot-swap
    int64_t start_row{0};   // resume from this row

    bool valid() const { return fn_ptr != nullptr; }
};

// ─── Code generation result ───────────────────────────────────────────────────

struct JitResult {
    // For scalar aggregate queries (Q1, Q6): one double[] per aggregate
    std::vector<double>      scalar_aggs;
    // For relational queries: row buffer
    std::vector<std::vector<Value>> rows;
    int64_t rows_scanned{0};
};

// Function types for compiled pipelines
using ScanAggFn   = void (*)(void** cols, int64_t start, int64_t end, double* aggs);
using BuildHashFn = void (*)(void** cols, int64_t nrows, void* ht_data);
using ProbeHashFn = void (*)(void** probe_cols, int64_t nrows,
                              void* ht_data,
                              void** build_col_data,
                              double* agg_out);

// ─── The JIT compiler ─────────────────────────────────────────────────────────

class LLVMCodegen {
public:
    LLVMCodegen();
    ~LLVMCodegen();

    // Compile a physical plan. Returns false on error.
    // On success, compiled_fn is populated and can be called immediately.
    bool compile(const plan::PhysicalNode& plan, CompiledPipeline& out);

    // Run a previously compiled scan+aggregate pipeline
    JitResult run_scan_agg(const CompiledPipeline& cp,
                           const plan::PhysicalScan& scan_info,
                           const plan::PhysicalHashAggregate& agg_info,
                           int64_t start_row = 0);

    std::string last_error() const { return last_error_; }

private:
    // IR generation helpers
    struct GenCtx;
    bool gen_scan_filter_agg(const plan::PhysicalNode& plan, GenCtx& ctx);
    bool gen_hash_join_probe(const plan::PhysicalNode& plan, GenCtx& ctx);

    // Emit specialized comparison + arithmetic based on TypeTag
    llvm::Value* emit_predicate(const plan::PlanExpr& expr,
                                llvm::IRBuilder<llvm::ConstantFolder>& builder,
                                GenCtx& ctx);

    // Inline hash: Wang hash for int64, FNV for varchar
    llvm::Value* emit_hash(llvm::Value* val, TypeTag t,
                            llvm::IRBuilder<llvm::ConstantFolder>& builder);

    std::unique_ptr<llvm::orc::LLJIT>  jit_;
    std::string                        last_error_;
};

} // namespace qc
