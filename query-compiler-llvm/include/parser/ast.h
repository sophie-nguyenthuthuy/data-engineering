#pragma once
#include "common/types.h"
#include <memory>
#include <string>
#include <vector>
#include <variant>

namespace qc {
namespace ast {

// ─── Expressions ─────────────────────────────────────────────────────────────

struct Expr;
using ExprPtr = std::unique_ptr<Expr>;

struct Literal {
    Value value;
};

struct ColumnRef {
    std::string table;  // may be empty
    std::string column;
};

enum class BinOp {
    EQ, NEQ, LT, LE, GT, GE,
    ADD, SUB, MUL, DIV,
    AND, OR,
};

struct BinaryExpr {
    BinOp    op;
    ExprPtr  left;
    ExprPtr  right;
};

struct UnaryExpr {
    enum class Op { NOT, NEG };
    Op      op;
    ExprPtr expr;
};

struct BetweenExpr {
    ExprPtr value;
    ExprPtr lo;
    ExprPtr hi;
};

enum class AggFunc { COUNT, SUM, AVG, MIN, MAX, COUNT_STAR };

struct AggExpr {
    AggFunc agg;
    bool    distinct{false};
    ExprPtr arg;  // null for COUNT(*)
};

struct CastExpr {
    ExprPtr  expr;
    TypeTag  target;
};

using ExprNode = std::variant<
    Literal,
    ColumnRef,
    BinaryExpr,
    UnaryExpr,
    BetweenExpr,
    AggExpr,
    CastExpr
>;

struct Expr {
    ExprNode node;
    explicit Expr(ExprNode n) : node(std::move(n)) {}
};

inline ExprPtr make_literal(Value v) {
    return std::make_unique<Expr>(Literal{std::move(v)});
}
inline ExprPtr make_col(std::string tbl, std::string col) {
    return std::make_unique<Expr>(ColumnRef{std::move(tbl), std::move(col)});
}
inline ExprPtr make_binop(BinOp op, ExprPtr l, ExprPtr r) {
    return std::make_unique<Expr>(BinaryExpr{op, std::move(l), std::move(r)});
}

// ─── Sort key ────────────────────────────────────────────────────────────────

struct SortKey {
    ExprPtr expr;
    bool    ascending{true};
    bool    nulls_first{true};
};

// ─── Select items ────────────────────────────────────────────────────────────

struct SelectItem {
    ExprPtr     expr;
    std::string alias;
};

// ─── FROM clause ─────────────────────────────────────────────────────────────

struct TableRef {
    std::string name;
    std::string alias;
};

enum class JoinType { INNER, LEFT, RIGHT, FULL };

struct JoinClause;
using JoinClausePtr = std::unique_ptr<JoinClause>;

struct FromItem {
    TableRef              table;
    std::vector<JoinClausePtr> joins;
};

struct JoinClause {
    JoinType  type{JoinType::INNER};
    TableRef  table;
    ExprPtr   condition;
};

// ─── Full SELECT statement ───────────────────────────────────────────────────

struct SelectStmt {
    bool                    distinct{false};
    std::vector<SelectItem> select_list;
    std::vector<FromItem>   from_list;
    ExprPtr                 where_clause;   // may be null
    std::vector<ExprPtr>    group_by;
    ExprPtr                 having;         // may be null
    std::vector<SortKey>    order_by;
    int64_t                 limit{-1};
    int64_t                 offset{0};
};

} // namespace ast
} // namespace qc
