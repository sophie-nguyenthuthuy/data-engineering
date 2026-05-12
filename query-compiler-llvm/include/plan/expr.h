#pragma once
#include "common/types.h"
#include "parser/ast.h"
#include <memory>
#include <string>
#include <vector>

namespace qc {
namespace plan {

// ─── Typed plan expressions ──────────────────────────────────────────────────
// Unlike AST nodes, plan expressions carry resolved type information.

struct PlanExpr;
using PlanExprPtr = std::unique_ptr<PlanExpr>;

struct PColRef {
    int     col_idx;   // index into operator's output schema
    TypeTag type;
    std::string col_name;
};

struct PLiteral {
    Value   value;
    TypeTag type;
};

enum class PBinOp { EQ, NEQ, LT, LE, GT, GE, ADD, SUB, MUL, DIV, AND, OR };

struct PBinExpr {
    PBinOp      op;
    TypeTag     result_type;
    PlanExprPtr left;
    PlanExprPtr right;
};

struct PAggExpr {
    ast::AggFunc agg;
    TypeTag      result_type;
    PlanExprPtr  arg;  // null for COUNT(*)
};

struct PCastExpr {
    PlanExprPtr from;
    TypeTag     from_type;
    TypeTag     to_type;
};

using PlanExprNode = std::variant<PColRef, PLiteral, PBinExpr, PAggExpr, PCastExpr>;

struct PlanExpr {
    PlanExprNode node;
    TypeTag      type;  // resolved result type

    explicit PlanExpr(PlanExprNode n, TypeTag t) : node(std::move(n)), type(t) {}
};

inline PlanExprPtr make_col_ref(int idx, TypeTag t, std::string name) {
    return std::make_unique<PlanExpr>(PColRef{idx, t, std::move(name)}, t);
}
inline PlanExprPtr make_p_literal(Value v, TypeTag t) {
    return std::make_unique<PlanExpr>(PLiteral{std::move(v), t}, t);
}
inline PlanExprPtr make_p_binop(PBinOp op, TypeTag rt, PlanExprPtr l, PlanExprPtr r) {
    return std::make_unique<PlanExpr>(PBinExpr{op, rt, std::move(l), std::move(r)}, rt);
}

// Clone an expression tree
PlanExprPtr clone_expr(const PlanExpr& e);

// ─── Output column descriptor ────────────────────────────────────────────────

struct OutputCol {
    std::string name;
    TypeTag     type;
};

using Schema = std::vector<OutputCol>;

} // namespace plan
} // namespace qc
