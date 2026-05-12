#include "codegen/llvm_codegen.h"
#include "plan/physical.h"
#include "storage/table.h"

#include "llvm/ADT/StringRef.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ExecutionEngine/Orc/ThreadSafeModule.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Passes/PassBuilder.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Vectorize/LoopVectorize.h"
#include "llvm/Transforms/Vectorize/SLPVectorizer.h"

#include <cassert>
#include <cstdio>
#include <stdexcept>

namespace qc {

using namespace plan;
using namespace llvm;

// ─── Optimization pipeline ───────────────────────────────────────────────────

static void optimize_module(Module& M, int opt_level = 2) {
    LoopAnalysisManager     LAM;
    FunctionAnalysisManager FAM;
    CGSCCAnalysisManager    CGAM;
    ModuleAnalysisManager   MAM;

    PassBuilder PB;
    PB.registerModuleAnalyses(MAM);
    PB.registerCGSCCAnalyses(CGAM);
    PB.registerFunctionAnalyses(FAM);
    PB.registerLoopAnalyses(LAM);
    PB.crossRegisterProxies(LAM, FAM, CGAM, MAM);

    OptimizationLevel OL = (opt_level >= 2) ? OptimizationLevel::O2
                                             : OptimizationLevel::O1;
    ModulePassManager MPM = PB.buildPerModuleDefaultPipeline(OL);
    MPM.run(M, MAM);
}

// ─── Code generation context ──────────────────────────────────────────────────

struct LLVMCodegen::GenCtx {
    LLVMContext&  ctx;
    Module&       mod;
    IRBuilder<>   builder;
    Function*     fn{nullptr};

    // Column pointers: for a scan, these are the raw data arrays
    // cols_arg is the void** argument; we GEP into it for each column
    Value* cols_arg{nullptr};
    Value* start_arg{nullptr};
    Value* end_arg{nullptr};
    Value* agg_out_arg{nullptr};

    // Loop induction variable
    Value* row_idx{nullptr};

    // Names for the current row's loaded column values (by output col index)
    std::vector<Value*> col_vals;

    GenCtx(LLVMContext& c, Module& m)
        : ctx(c), mod(m), builder(c) {}
};

// ─── LLVM type mapping ────────────────────────────────────────────────────────

static llvm::Type* llvm_type_for(TypeTag t, LLVMContext& ctx) {
    switch (t) {
    case TypeTag::BOOL:    return llvm::Type::getInt1Ty(ctx);
    case TypeTag::INT32:
    case TypeTag::DATE:    return llvm::Type::getInt32Ty(ctx);
    case TypeTag::INT64:   return llvm::Type::getInt64Ty(ctx);
    case TypeTag::FLOAT64: return llvm::Type::getDoubleTy(ctx);
    case TypeTag::VARCHAR: return llvm::PointerType::getUnqual(ctx); // opaque ptr
    default:               return llvm::Type::getInt64Ty(ctx);
    }
}

// ─── Inline Wang hash for int64 ───────────────────────────────────────────────
// uint64_t wang_hash(uint64_t x) {
//   x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
//   x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
//   return x ^ (x >> 31);
// }
static Value* emit_wang_hash(Value* x, IRBuilder<>& B, LLVMContext& ctx) {
    auto* u64 = B.getInt64Ty();
    // cast to uint64
    Value* h = B.CreateIntCast(x, u64, false, "hash_input");

    auto xorshift = [&](Value* v, int shift, uint64_t mul) -> Value* {
        Value* s = B.CreateLShr(v, ConstantInt::get(u64, shift), "xsr");
        Value* x2 = B.CreateXor(v, s, "xor");
        return B.CreateMul(x2, ConstantInt::get(u64, mul), "hmul");
    };

    h = xorshift(h, 30, 0xbf58476d1ce4e5b9ULL);
    h = xorshift(h, 27, 0x94d049bb133111ebULL);
    Value* s = B.CreateLShr(h, ConstantInt::get(u64, 31), "xsr2");
    h = B.CreateXor(h, s, "hash_final");
    return h;
}

// ─── Expression code generation ───────────────────────────────────────────────

static Value* emit_expr(const PlanExpr& expr, GenCtx& gctx) {
    auto& B   = gctx.builder;
    auto& ctx = gctx.ctx;

    return std::visit([&](const auto& node) -> Value* {
        using T = std::decay_t<decltype(node)>;

        if constexpr (std::is_same_v<T, PColRef>) {
            assert(node.col_idx < (int)gctx.col_vals.size());
            return gctx.col_vals[node.col_idx];
        }

        if constexpr (std::is_same_v<T, PLiteral>) {
            return std::visit([&](const auto& v) -> Value* {
                using V = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<V, bool>)
                    return ConstantInt::get(B.getInt1Ty(), v ? 1 : 0);
                if constexpr (std::is_same_v<V, int32_t>)
                    return ConstantInt::get(B.getInt32Ty(), (uint64_t)(int64_t)v, true);
                if constexpr (std::is_same_v<V, int64_t>)
                    return ConstantInt::get(B.getInt64Ty(), (uint64_t)v, true);
                if constexpr (std::is_same_v<V, double>)
                    return ConstantFP::get(B.getDoubleTy(), v);
                // monostate / string → zero
                return ConstantInt::get(B.getInt64Ty(), 0);
            }, node.value);
        }

        if constexpr (std::is_same_v<T, PBinExpr>) {
            Value* lv = emit_expr(*node.left,  gctx);
            Value* rv = emit_expr(*node.right, gctx);

            // Type coercion: if one is float, cast both to float
            auto promote = [&](Value* a, TypeTag ta, Value* b, TypeTag tb)
                -> std::pair<Value*, Value*> {
                if (ta == TypeTag::FLOAT64 || tb == TypeTag::FLOAT64) {
                    auto to_f = [&](Value* v, TypeTag t) -> Value* {
                        if (t == TypeTag::FLOAT64) return v;
                        if (is_integral(t)) return B.CreateSIToFP(v, B.getDoubleTy(), "itof");
                        return v;
                    };
                    return {to_f(a, ta), to_f(b, tb)};
                }
                // both integral — widen to i64
                auto widen = [&](Value* v, TypeTag t) -> Value* {
                    if (t == TypeTag::INT64) return v;
                    return B.CreateSExt(v, B.getInt64Ty(), "sext");
                };
                return {widen(a, ta), widen(b, tb)};
            };

            TypeTag lt = node.left->type;
            TypeTag rt_tag = node.right->type;
            bool is_fp = (lt == TypeTag::FLOAT64 || rt_tag == TypeTag::FLOAT64);

            switch (node.op) {
            case PBinOp::ADD: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFAdd(la, ra, "fadd") : B.CreateAdd(la, ra, "iadd");
            }
            case PBinOp::SUB: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFSub(la, ra, "fsub") : B.CreateSub(la, ra, "isub");
            }
            case PBinOp::MUL: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFMul(la, ra, "fmul") : B.CreateMul(la, ra, "imul");
            }
            case PBinOp::DIV: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFDiv(la, ra, "fdiv") : B.CreateSDiv(la, ra, "idiv");
            }
            case PBinOp::EQ: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFCmpOEQ(la, ra, "feq") : B.CreateICmpEQ(la, ra, "ieq");
            }
            case PBinOp::NEQ: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFCmpONE(la, ra, "fne") : B.CreateICmpNE(la, ra, "ine");
            }
            case PBinOp::LT: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFCmpOLT(la, ra, "flt") : B.CreateICmpSLT(la, ra, "ilt");
            }
            case PBinOp::LE: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFCmpOLE(la, ra, "fle") : B.CreateICmpSLE(la, ra, "ile");
            }
            case PBinOp::GT: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFCmpOGT(la, ra, "fgt") : B.CreateICmpSGT(la, ra, "igt");
            }
            case PBinOp::GE: {
                auto [la, ra] = promote(lv, lt, rv, rt_tag);
                return is_fp ? B.CreateFCmpOGE(la, ra, "fge") : B.CreateICmpSGE(la, ra, "ige");
            }
            case PBinOp::AND:
                return B.CreateAnd(lv, rv, "band");
            case PBinOp::OR:
                return B.CreateOr(lv, rv, "bor");
            }
        }

        // Fallback
        return ConstantInt::get(llvm::Type::getInt64Ty(ctx), 0);
    }, expr.node);
}

// ─── Core: scan + filter + hash-aggregate pipeline ───────────────────────────
//
// Generates:
//   void query_fn(void** col_ptrs, int64_t start, int64_t end, double* agg_out)
//
// The inner loop looks like (for TPC-H Q6):
//
//   for (int64_t i = start; i < end; i++) {
//       int32_t shipdate  = ((int32_t*)col_ptrs[10])[i];
//       double  discount  = ((double*)col_ptrs[6])[i];
//       double  quantity  = ((double*)col_ptrs[4])[i];
//       double  extprice  = ((double*)col_ptrs[5])[i];
//       if (shipdate >= DATE_LO && shipdate < DATE_HI
//           && discount >= 0.05 && discount <= 0.07
//           && quantity < 24.0) {
//           agg0 += extprice * discount;
//       }
//   }

static bool gen_scan_filter_agg(
    const PhysicalScan&          scan,
    const PlanExpr*              predicate,   // may be null
    const PhysicalHashAggregate& agg,
    LLVMContext& ctx, Module& mod,
    std::string& fn_name,
    std::string& error)
{
    fn_name = "query_scan_agg";

    // Function type: void(void** cols, i64 start, i64 end, double* agg_out)
    auto* void_ty  = llvm::Type::getVoidTy(ctx);
    auto* i64_ty   = llvm::Type::getInt64Ty(ctx);
    auto* ptr_ty   = llvm::PointerType::getUnqual(ctx); // opaque ptr (LLVM 15+)
    llvm::Type* param_types[] = {ptr_ty, i64_ty, i64_ty, ptr_ty};
    auto* fn_type = FunctionType::get(void_ty, param_types, false);
    auto* fn = Function::Create(fn_type, Function::ExternalLinkage, fn_name, mod);
    fn->setDoesNotThrow();

    // Mark args
    auto arg_it = fn->arg_begin();
    Value* cols_arg  = &*arg_it++; cols_arg->setName("cols");
    Value* start_arg = &*arg_it++; start_arg->setName("start");
    Value* end_arg   = &*arg_it++; end_arg->setName("end");
    Value* agg_arg   = &*arg_it++; agg_arg->setName("agg_out");

    // Add noalias / dereferenceable hints
    fn->addParamAttr(0, Attribute::NoAlias);
    fn->addParamAttr(3, Attribute::NoAlias);

    auto* entry  = BasicBlock::Create(ctx, "entry",   fn);
    auto* header = BasicBlock::Create(ctx, "header",  fn);
    auto* body   = BasicBlock::Create(ctx, "body",    fn);
    auto* filter = BasicBlock::Create(ctx, "filter",  fn);
    auto* update = BasicBlock::Create(ctx, "update",  fn);
    auto* latch  = BasicBlock::Create(ctx, "latch",   fn);
    auto* exit_b = BasicBlock::Create(ctx, "exit",    fn);

    IRBuilder<> B(ctx);

    // ── Entry: allocate accumulators ──────────────────────────────────────────
    B.SetInsertPoint(entry);
    size_t n_aggs = agg.agg_vals.size();
    std::vector<Value*> acc_ptrs;
    for (size_t i = 0; i < n_aggs; i++) {
        auto* acc = B.CreateAlloca(B.getDoubleTy(), nullptr, "acc" + std::to_string(i));
        // Initialize: COUNT=0, others=0
        B.CreateStore(ConstantFP::get(B.getDoubleTy(), 0.0), acc);
        acc_ptrs.push_back(acc);
    }
    B.CreateBr(header);

    // ── Loop header: phi for loop index ──────────────────────────────────────
    B.SetInsertPoint(header);
    PHINode* phi = B.CreatePHI(i64_ty, 2, "i");
    phi->addIncoming(start_arg, entry);

    Value* cond = B.CreateICmpSLT(phi, end_arg, "loop_cond");
    B.CreateCondBr(cond, body, exit_b);

    // ── Body: load column values ──────────────────────────────────────────────
    B.SetInsertPoint(body);

    // Build col_vals: for each output column of scan, load from the right array
    const auto& out_schema = scan.table->schema();
    std::vector<Value*> col_vals;
    col_vals.resize(out_schema.columns.size() * 2); // short + qualified names

    // The scan emits pairs: (qualified_name, short_name), both pointing to same col
    for (int ci = 0; ci < (int)out_schema.columns.size(); ci++) {
        TypeTag ct = out_schema.columns[ci].type;
        auto* elem_ty = llvm_type_for(ct, ctx);

        // cols_arg[ci] is the pointer to the ci-th column array
        // GEP: void** cols_arg, index ci → void*, then bitcast to elem_ty*
        Value* col_ptr_ptr = B.CreateGEP(
            ptr_ty, cols_arg,
            ConstantInt::get(i64_ty, ci), "cpp");
        Value* col_ptr = B.CreateLoad(ptr_ty, col_ptr_ptr, "cp");

        // Load element at phi
        Value* elem_ptr = B.CreateGEP(elem_ty, col_ptr, phi, "ep");
        Value* val = B.CreateLoad(elem_ty, elem_ptr, "v" + std::to_string(ci));

        // Store in both positions (qualified and short name share same data)
        col_vals[ci * 2]     = val;  // position for qualified name
        col_vals[ci * 2 + 1] = val;  // position for short name
    }

    // Now we need to figure out which col_vals index each column ref uses.
    // The planner puts pairs: qualified at 2i, short at 2i+1 in output_schema.
    // So col_idx 0 = qual of col 0, col_idx 1 = short of col 0, etc.
    // This matches how ScanIterator emits rows.

    GenCtx gctx(ctx, mod);
    gctx.fn       = fn;
    gctx.col_vals = col_vals;
    gctx.cols_arg = cols_arg;
    gctx.row_idx  = phi;
    gctx.builder.SetInsertPoint(body); // will be updated per block

    // ── Filter check ──────────────────────────────────────────────────────────
    if (predicate) {
        gctx.builder.SetInsertPoint(body);
        Value* passes = emit_expr(*predicate, gctx);
        B.SetInsertPoint(body);
        B.CreateCondBr(passes, filter, latch);
    } else {
        B.SetInsertPoint(body);
        B.CreateBr(filter);
    }

    // ── Aggregate update ──────────────────────────────────────────────────────
    B.SetInsertPoint(filter);
    gctx.builder.SetInsertPoint(filter);

    for (size_t ai = 0; ai < n_aggs; ai++) {
        auto& av = agg.agg_vals[ai];
        Value* cur = B.CreateLoad(B.getDoubleTy(), acc_ptrs[ai], "cur");

        switch (av.agg) {
        case ast::AggFunc::COUNT_STAR:
        case ast::AggFunc::COUNT: {
            Value* inc = B.CreateFAdd(cur, ConstantFP::get(B.getDoubleTy(), 1.0), "cnt");
            B.CreateStore(inc, acc_ptrs[ai]);
            break;
        }
        case ast::AggFunc::SUM:
        case ast::AggFunc::AVG: {
            Value* arg_val = nullptr;
            if (av.arg) {
                gctx.builder.SetInsertPoint(filter);
                arg_val = emit_expr(*av.arg, gctx);
                B.SetInsertPoint(filter);
                // Cast to double if needed
                if (av.arg->type != TypeTag::FLOAT64)
                    arg_val = B.CreateSIToFP(arg_val, B.getDoubleTy(), "acast");
            }
            if (arg_val) {
                Value* sum = B.CreateFAdd(cur, arg_val, "sum");
                B.CreateStore(sum, acc_ptrs[ai]);
            }
            break;
        }
        case ast::AggFunc::MIN: {
            if (av.arg) {
                gctx.builder.SetInsertPoint(filter);
                Value* arg_val = emit_expr(*av.arg, gctx);
                B.SetInsertPoint(filter);
                if (av.arg->type != TypeTag::FLOAT64)
                    arg_val = B.CreateSIToFP(arg_val, B.getDoubleTy(), "acast");
                Value* is_less = B.CreateFCmpOLT(arg_val, cur, "islt");
                Value* new_min = B.CreateSelect(is_less, arg_val, cur, "newmin");
                B.CreateStore(new_min, acc_ptrs[ai]);
            }
            break;
        }
        case ast::AggFunc::MAX: {
            if (av.arg) {
                gctx.builder.SetInsertPoint(filter);
                Value* arg_val = emit_expr(*av.arg, gctx);
                B.SetInsertPoint(filter);
                if (av.arg->type != TypeTag::FLOAT64)
                    arg_val = B.CreateSIToFP(arg_val, B.getDoubleTy(), "acast");
                Value* is_gt = B.CreateFCmpOGT(arg_val, cur, "isgt");
                Value* new_max = B.CreateSelect(is_gt, arg_val, cur, "newmax");
                B.CreateStore(new_max, acc_ptrs[ai]);
            }
            break;
        }
        }
    }
    B.CreateBr(latch);

    // ── Latch: increment i ───────────────────────────────────────────────────
    B.SetInsertPoint(latch);
    Value* next_i = B.CreateAdd(phi, ConstantInt::get(i64_ty, 1), "i_next");
    phi->addIncoming(next_i, latch);
    B.CreateBr(header);

    // ── Exit: store accumulators to output ────────────────────────────────────
    B.SetInsertPoint(exit_b);
    for (size_t ai = 0; ai < n_aggs; ai++) {
        Value* val = B.CreateLoad(B.getDoubleTy(), acc_ptrs[ai], "final");
        Value* out_ptr = B.CreateGEP(B.getDoubleTy(), agg_arg,
                                     ConstantInt::get(i64_ty, ai), "outp");
        B.CreateStore(val, out_ptr);
    }
    B.CreateRetVoid();

    // Add loop metadata for auto-vectorization hint
    {
        MDNode* loop_id = MDNode::getDistinct(ctx, {});
        MDNode* vectorize_enable = MDNode::get(ctx, {
            MDString::get(ctx, "llvm.loop.vectorize.enable"),
            ConstantInt::getTrue(ctx)
        });
        MDNode* loop_meta = MDNode::get(ctx, {loop_id, vectorize_enable});
        loop_id->replaceOperandWith(0, loop_meta);
        auto* latch_br = cast<BranchInst>(latch->getTerminator());
        latch_br->setMetadata("llvm.loop", loop_meta);
    }

    std::string err;
    raw_string_ostream errs(err);
    if (verifyFunction(*fn, &errs)) {
        error = "IR verification failed: " + err;
        return false;
    }
    return true;
}

// ─── LLVMCodegen implementation ───────────────────────────────────────────────

LLVMCodegen::LLVMCodegen() {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    InitializeNativeTargetAsmParser();

    auto jit_result = orc::LLJITBuilder().create();
    if (!jit_result) {
        last_error_ = toString(jit_result.takeError());
        return;
    }
    jit_ = std::move(*jit_result);
}

LLVMCodegen::~LLVMCodegen() = default;

bool LLVMCodegen::compile(const PhysicalNode& plan, CompiledPipeline& out) {
    // We specialize for scan → [filter] → hash_agg pipelines
    // (covers TPC-H Q1, Q6, and the leaf pipelines of Q3)

    const PhysicalHashAggregate* agg = nullptr;
    const PhysicalFilter*        flt = nullptr;
    const PhysicalScan*          scn = nullptr;

    const PhysicalNode* cur = &plan;

    if (cur->type() == PhysicalOpType::HASH_AGGREGATE) {
        agg = &std::get<PhysicalHashAggregate>(cur->op);
        cur = agg->child.get();
    }
    if (cur->type() == PhysicalOpType::FILTER) {
        flt = &std::get<PhysicalFilter>(cur->op);
        cur = flt->child.get();
    }
    if (cur->type() == PhysicalOpType::SCAN) {
        scn = &std::get<PhysicalScan>(cur->op);
    }

    if (!agg || !scn) {
        last_error_ = "JIT only supports scan→[filter]→aggregate pipelines";
        return false;
    }

    auto ctx = std::make_unique<LLVMContext>();
    auto mod = std::make_unique<Module>("query_jit", *ctx);

    std::string fn_name, error;
    bool ok = gen_scan_filter_agg(
        *scn,
        flt ? flt->predicate.get() : nullptr,
        *agg,
        *ctx, *mod, fn_name, error);

    if (!ok) { last_error_ = error; return false; }

    // Optimize
    optimize_module(*mod, 2);

    // JIT compile
    auto tsm = orc::ThreadSafeModule(std::move(mod), std::move(ctx));
    auto err = jit_->addIRModule(std::move(tsm));
    if (err) {
        last_error_ = toString(std::move(err));
        return false;
    }

    auto sym = jit_->lookup(fn_name);
    if (!sym) {
        last_error_ = toString(sym.takeError());
        return false;
    }

    out.fn_ptr   = reinterpret_cast<void*>(sym->getValue());
    out.fn_name  = fn_name;
    return true;
}

JitResult LLVMCodegen::run_scan_agg(
    const CompiledPipeline& cp,
    const PhysicalScan& scan,
    const PhysicalHashAggregate& agg,
    int64_t start_row)
{
    JitResult r;
    if (!cp.valid()) return r;

    auto fn = reinterpret_cast<ScanAggFn>(cp.fn_ptr);

    // Build col_ptrs array: same ordering as gen_scan_filter_agg
    const auto& tbl = *scan.table;
    std::vector<void*> col_ptrs(tbl.num_columns());
    for (int i = 0; i < (int)tbl.num_columns(); i++)
        col_ptrs[i] = const_cast<void*>(tbl.column(i).raw_ptr());

    int64_t nrows = tbl.num_rows();
    r.rows_scanned = nrows - start_row;

    size_t n_aggs = agg.agg_vals.size();
    r.scalar_aggs.resize(n_aggs, 0.0);

    fn(col_ptrs.data(), start_row, nrows, r.scalar_aggs.data());

    return r;
}

} // namespace qc
