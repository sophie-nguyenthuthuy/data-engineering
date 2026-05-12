#include "plan/physical.h"
#include "parser/parser.h"
#include "parser/ast.h"
#include <stdexcept>
#include <cassert>
#include <unordered_map>
#include <algorithm>

namespace qc {
namespace plan {

// ─── Expression type resolver ─────────────────────────────────────────────────

struct ColumnEnv {
    // Maps qualified column name (or unqualified) to (index, type) in the current schema
    std::unordered_map<std::string, std::pair<int, TypeTag>> cols;

    void add(int idx, const OutputCol& c, const std::string& table_alias = "") {
        cols[c.name] = {idx, c.type};
        if (!table_alias.empty())
            cols[table_alias + "." + c.name] = {idx, c.type};
    }
};

// Determine result type of a binary expression
static TypeTag binop_result_type(ast::BinOp op, TypeTag l, TypeTag r) {
    using B = ast::BinOp;
    switch (op) {
    case B::EQ: case B::NEQ: case B::LT: case B::LE: case B::GT: case B::GE:
    case B::AND: case B::OR:
        return TypeTag::BOOL;
    case B::ADD: case B::SUB: case B::MUL: case B::DIV:
        if (l == TypeTag::FLOAT64 || r == TypeTag::FLOAT64) return TypeTag::FLOAT64;
        if (l == TypeTag::INT64   || r == TypeTag::INT64)   return TypeTag::INT64;
        return TypeTag::INT32;
    }
    return TypeTag::INVALID;
}

static PBinOp to_plan_binop(ast::BinOp op) {
    switch (op) {
    case ast::BinOp::EQ:  return PBinOp::EQ;
    case ast::BinOp::NEQ: return PBinOp::NEQ;
    case ast::BinOp::LT:  return PBinOp::LT;
    case ast::BinOp::LE:  return PBinOp::LE;
    case ast::BinOp::GT:  return PBinOp::GT;
    case ast::BinOp::GE:  return PBinOp::GE;
    case ast::BinOp::ADD: return PBinOp::ADD;
    case ast::BinOp::SUB: return PBinOp::SUB;
    case ast::BinOp::MUL: return PBinOp::MUL;
    case ast::BinOp::DIV: return PBinOp::DIV;
    case ast::BinOp::AND: return PBinOp::AND;
    case ast::BinOp::OR:  return PBinOp::OR;
    }
    return PBinOp::EQ;
}

static PlanExprPtr lower_expr(const ast::Expr& e, const ColumnEnv& env);

static PlanExprPtr lower_expr(const ast::Expr& e, const ColumnEnv& env) {
    return std::visit([&](const auto& node) -> PlanExprPtr {
        using T = std::decay_t<decltype(node)>;

        if constexpr (std::is_same_v<T, ast::Literal>) {
            TypeTag t = value_type(node.value);
            if (t == TypeTag::INVALID) t = TypeTag::INT32; // NULL
            return make_p_literal(node.value, t);
        }

        if constexpr (std::is_same_v<T, ast::ColumnRef>) {
            std::string key = node.column;
            if (!node.table.empty()) key = node.table + "." + node.column;
            auto it = env.cols.find(key);
            if (it == env.cols.end())
                throw std::runtime_error("Unknown column: " + key);
            return make_col_ref(it->second.first, it->second.second, node.column);
        }

        if constexpr (std::is_same_v<T, ast::BinaryExpr>) {
            auto l = lower_expr(*node.left,  env);
            auto r = lower_expr(*node.right, env);
            TypeTag rt = binop_result_type(node.op, l->type, r->type);
            return make_p_binop(to_plan_binop(node.op), rt, std::move(l), std::move(r));
        }

        if constexpr (std::is_same_v<T, ast::BetweenExpr>) {
            // x BETWEEN lo AND hi  →  x >= lo AND x <= hi
            auto lx  = lower_expr(*node.value, env);
            auto llo = lower_expr(*node.lo,    env);
            auto lhi = lower_expr(*node.hi,    env);
            TypeTag t = lx->type;
            auto clx = clone_expr(*lx);
            auto ge = make_p_binop(PBinOp::GE, TypeTag::BOOL, std::move(lx),  std::move(llo));
            auto le = make_p_binop(PBinOp::LE, TypeTag::BOOL, std::move(clx), std::move(lhi));
            return make_p_binop(PBinOp::AND, TypeTag::BOOL, std::move(ge), std::move(le));
        }

        if constexpr (std::is_same_v<T, ast::AggExpr>) {
            TypeTag rt = TypeTag::FLOAT64;
            if (node.agg == ast::AggFunc::COUNT || node.agg == ast::AggFunc::COUNT_STAR)
                rt = TypeTag::INT64;
            PlanExprPtr arg;
            if (node.arg) arg = lower_expr(*node.arg, env);
            return std::make_unique<PlanExpr>(PAggExpr{node.agg, rt, std::move(arg)}, rt);
        }

        if constexpr (std::is_same_v<T, ast::UnaryExpr>) {
            auto inner = lower_expr(*node.expr, env);
            if (node.op == ast::UnaryExpr::Op::NEG) {
                // Represent -x as (0 - x)
                auto zero = make_p_literal(Value{static_cast<int64_t>(0)}, TypeTag::INT64);
                return make_p_binop(PBinOp::SUB, inner->type, std::move(zero), std::move(inner));
            }
            // NOT: wrap as boolean negation — represent as (x == false)
            auto fv = make_p_literal(Value{false}, TypeTag::BOOL);
            return make_p_binop(PBinOp::EQ, TypeTag::BOOL, std::move(inner), std::move(fv));
        }

        throw std::runtime_error("Unsupported expression node in lowering");
    }, e.node);
}

PlanExprPtr clone_expr(const PlanExpr& e) {
    return std::visit([&](const auto& node) -> PlanExprPtr {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, PColRef>)
            return std::make_unique<PlanExpr>(node, e.type);
        if constexpr (std::is_same_v<T, PLiteral>)
            return std::make_unique<PlanExpr>(node, e.type);
        if constexpr (std::is_same_v<T, PBinExpr>) {
            PBinExpr copy{node.op, node.result_type,
                          clone_expr(*node.left), clone_expr(*node.right)};
            return std::make_unique<PlanExpr>(std::move(copy), e.type);
        }
        if constexpr (std::is_same_v<T, PAggExpr>) {
            PAggExpr copy{node.agg, node.result_type,
                          node.arg ? clone_expr(*node.arg) : nullptr};
            return std::make_unique<PlanExpr>(std::move(copy), e.type);
        }
        if constexpr (std::is_same_v<T, PCastExpr>) {
            return std::make_unique<PlanExpr>(
                PCastExpr{clone_expr(*node.from), node.from_type, node.to_type}, e.type);
        }
        return std::make_unique<PlanExpr>(node, e.type);
    }, e.node);
}

// ─── Planner ─────────────────────────────────────────────────────────────────

class Planner {
public:
    explicit Planner(const Catalog& cat) : cat_(cat) {}

    PhysicalNodePtr plan(const ast::SelectStmt& stmt) {
        // Build FROM (handle cross joins and explicit JOINs)
        if (stmt.from_list.empty())
            throw std::runtime_error("No FROM clause");

        PhysicalNodePtr root = build_from(stmt.from_list);

        // WHERE
        if (stmt.where_clause) {
            ColumnEnv env = build_env(root->output_schema);
            auto pred = lower_expr(*stmt.where_clause, env);
            auto filter = std::make_unique<PhysicalNode>();
            filter->op = PhysicalFilter{std::move(root), std::move(pred)};
            filter->output_schema = std::get<PhysicalFilter>(filter->op).child->output_schema;
            root = std::move(filter);
        }

        // GROUP BY / aggregates
        bool has_agg = false;
        for (auto& si : stmt.select_list) {
            if (has_aggregate(*si.expr)) { has_agg = true; break; }
        }
        if (!stmt.group_by.empty() || has_agg) {
            root = build_aggregate(std::move(root), stmt);
        } else {
            // Plain projection
            root = build_project(std::move(root), stmt.select_list);
        }

        // ORDER BY
        if (!stmt.order_by.empty()) {
            root = build_sort(std::move(root), stmt.order_by);
        }

        // LIMIT
        if (stmt.limit >= 0) {
            auto lim = std::make_unique<PhysicalNode>();
            Schema out = root->output_schema;
            lim->op = PhysicalLimit{std::move(root), stmt.limit, stmt.offset};
            lim->output_schema = out;
            root = std::move(lim);
        }

        return root;
    }

private:
    const Catalog& cat_;

    // Build environment from schema
    ColumnEnv build_env(const Schema& schema, const std::string& table_alias = "") {
        ColumnEnv env;
        for (int i = 0; i < (int)schema.size(); i++)
            env.add(i, schema[i], table_alias);
        return env;
    }

    PhysicalNodePtr build_scan(const std::string& table_name,
                               const std::string& alias) {
        auto tbl = cat_.find(table_name);
        if (!tbl) throw std::runtime_error("Unknown table: " + table_name);

        Schema out;
        std::vector<int> col_idxs;
        const auto& schema = tbl->schema();
        for (int i = 0; i < (int)schema.columns.size(); i++) {
            const auto& c = schema.columns[i];
            std::string cname = (alias.empty() ? table_name : alias) + "." + c.name;
            out.push_back({cname, c.type});
            // Also short name
            out.push_back({c.name, c.type});
            col_idxs.push_back(i);
            col_idxs.push_back(i);
        }

        auto node = std::make_unique<PhysicalNode>();
        node->op = PhysicalScan{tbl, col_idxs};
        node->output_schema = out;
        return node;
    }

    // Handle FROM clause: cross joins and explicit JOINs
    PhysicalNodePtr build_from(const std::vector<ast::FromItem>& from_list) {
        // Start with first table
        PhysicalNodePtr root = build_scan(from_list[0].table.name,
                                          from_list[0].table.alias);

        // Apply explicit JOINs on first table
        for (auto& jc : from_list[0].joins) {
            root = build_join(std::move(root), jc);
        }

        // Cross-join remaining tables
        for (size_t i = 1; i < from_list.size(); i++) {
            auto rhs = build_scan(from_list[i].table.name, from_list[i].table.alias);
            for (auto& jc : from_list[i].joins)
                rhs = build_join(std::move(rhs), jc);
            root = build_cross_join(std::move(root), std::move(rhs));
        }
        return root;
    }

    PhysicalNodePtr build_cross_join(PhysicalNodePtr left, PhysicalNodePtr right) {
        // Nested-loop cross join — represented as a hash join with no keys for now
        // For simplicity in the interpreter we keep schema merged
        Schema merged = left->output_schema;
        merged.insert(merged.end(), right->output_schema.begin(), right->output_schema.end());

        // Build as hash join with empty key sets (degenerate cross join)
        auto node = std::make_unique<PhysicalNode>();
        node->op = PhysicalHashJoin{std::move(left), std::move(right), {}, {}, nullptr};
        node->output_schema = merged;
        return node;
    }

    PhysicalNodePtr build_join(PhysicalNodePtr left,
                               const ast::JoinClause& jc) {
        auto right = build_scan(jc.table.name, jc.table.alias);

        // Merge schemas to resolve join condition
        Schema merged = left->output_schema;
        merged.insert(merged.end(), right->output_schema.begin(), right->output_schema.end());
        ColumnEnv env = build_env(merged);

        PlanExprPtr cond = lower_expr(*jc.condition, env);

        // Detect equi-join keys: look for chains of col_a = col_b
        std::vector<int> left_keys, right_keys;
        PlanExprPtr extra;

        extract_equi_keys(*cond, (int)left->output_schema.size(),
                          left_keys, right_keys, extra);

        auto node = std::make_unique<PhysicalNode>();
        node->op = PhysicalHashJoin{std::move(left), std::move(right),
                                    left_keys, right_keys, std::move(extra)};
        node->output_schema = merged;
        return node;
    }

    // Extract equi-join keys from a condition like col_a = col_b AND ...
    void extract_equi_keys(const PlanExpr& expr, int left_ncols,
                           std::vector<int>& lkeys, std::vector<int>& rkeys,
                           PlanExprPtr& extra) {
        const auto* bin = std::get_if<PBinExpr>(&expr.node);
        if (!bin) { extra = clone_expr(expr); return; }

        if (bin->op == PBinOp::AND) {
            PlanExprPtr ex1, ex2;
            extract_equi_keys(*bin->left,  left_ncols, lkeys, rkeys, ex1);
            extract_equi_keys(*bin->right, left_ncols, lkeys, rkeys, ex2);
            if (ex1 && ex2)
                extra = make_p_binop(PBinOp::AND, TypeTag::BOOL,
                                     std::move(ex1), std::move(ex2));
            else if (ex1) extra = std::move(ex1);
            else if (ex2) extra = std::move(ex2);
            return;
        }

        if (bin->op == PBinOp::EQ) {
            const auto* lc = std::get_if<PColRef>(&bin->left->node);
            const auto* rc = std::get_if<PColRef>(&bin->right->node);
            if (lc && rc) {
                int li = lc->col_idx, ri = rc->col_idx;
                // Ensure left ref is from left side
                if (li >= left_ncols) std::swap(li, ri);
                if (li < left_ncols && ri >= left_ncols) {
                    lkeys.push_back(li);
                    rkeys.push_back(ri - left_ncols);
                    return;
                }
            }
        }
        extra = clone_expr(expr);
    }

    PhysicalNodePtr build_aggregate(PhysicalNodePtr child,
                                    const ast::SelectStmt& stmt) {
        ColumnEnv env = build_env(child->output_schema);

        std::vector<PlanExprPtr> group_keys;
        for (auto& gk : stmt.group_by)
            group_keys.push_back(lower_expr(*gk, env));

        std::vector<AggregateVal> agg_vals;
        Schema out;

        // Group key columns in output
        for (size_t i = 0; i < group_keys.size(); i++) {
            std::string name = "group_" + std::to_string(i);
            // Try to get a nicer name from the expr
            if (const auto* cr = std::get_if<PColRef>(&group_keys[i]->node))
                name = cr->col_name;
            out.push_back({name, group_keys[i]->type});
        }

        // Aggregate expressions from SELECT list
        for (auto& si : stmt.select_list) {
            collect_aggs(*si.expr, env, agg_vals, out, si.alias);
        }

        auto node = std::make_unique<PhysicalNode>();
        node->op = PhysicalHashAggregate{std::move(child),
                                         std::move(group_keys),
                                         std::move(agg_vals)};
        node->output_schema = out;
        return node;
    }

    void collect_aggs(const ast::Expr& e, const ColumnEnv& env,
                      std::vector<AggregateVal>& agg_vals, Schema& out,
                      const std::string& alias) {
        const auto* agg = std::get_if<ast::AggExpr>(&e.node);
        if (agg) {
            AggregateVal av;
            av.agg = agg->agg;
            av.result_type = (agg->agg == ast::AggFunc::COUNT ||
                              agg->agg == ast::AggFunc::COUNT_STAR)
                             ? TypeTag::INT64 : TypeTag::FLOAT64;
            if (agg->arg) av.arg = lower_expr(*agg->arg, env);
            std::string name = alias.empty() ? "agg_" + std::to_string(agg_vals.size()) : alias;
            av.output_name = name;
            out.push_back({name, av.result_type});
            agg_vals.push_back(std::move(av));
            return;
        }
        // Non-aggregate select items (like group keys) are handled elsewhere
    }

    PhysicalNodePtr build_project(PhysicalNodePtr child,
                                  const std::vector<ast::SelectItem>& items) {
        ColumnEnv env = build_env(child->output_schema);
        std::vector<PlanExprPtr> exprs;
        Schema out;

        for (auto& si : items) {
            // SELECT * expansion
            if (const auto* cr = std::get_if<ast::ColumnRef>(&si.expr->node)) {
                if (cr->column == "*") {
                    for (int i = 0; i < (int)child->output_schema.size(); i++) {
                        exprs.push_back(make_col_ref(i, child->output_schema[i].type,
                                                     child->output_schema[i].name));
                        out.push_back(child->output_schema[i]);
                    }
                    continue;
                }
            }
            auto expr = lower_expr(*si.expr, env);
            std::string name = si.alias.empty() ? "col_" + std::to_string(exprs.size())
                                                : si.alias;
            out.push_back({name, expr->type});
            exprs.push_back(std::move(expr));
        }

        auto node = std::make_unique<PhysicalNode>();
        node->op = PhysicalProject{std::move(child), std::move(exprs)};
        node->output_schema = out;
        return node;
    }

    PhysicalNodePtr build_sort(PhysicalNodePtr child,
                               const std::vector<ast::SortKey>& order_by) {
        ColumnEnv env = build_env(child->output_schema);
        std::vector<SortColumn> keys;
        for (auto& sk : order_by) {
            keys.push_back({lower_expr(*sk.expr, env), sk.ascending});
        }
        Schema out = child->output_schema;
        auto node = std::make_unique<PhysicalNode>();
        node->op = PhysicalSort{std::move(child), std::move(keys)};
        node->output_schema = out;
        return node;
    }

    bool has_aggregate(const ast::Expr& e) {
        return std::visit([&](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ast::AggExpr>) return true;
            if constexpr (std::is_same_v<T, ast::BinaryExpr>)
                return has_aggregate(*node.left) || has_aggregate(*node.right);
            if constexpr (std::is_same_v<T, ast::UnaryExpr>)
                return has_aggregate(*node.expr);
            if constexpr (std::is_same_v<T, ast::BetweenExpr>)
                return has_aggregate(*node.value);
            return false;
        }, e.node);
    }
};

PhysicalNodePtr build_plan(const std::string& sql, const Catalog& cat) {
    auto stmt = parse_sql(sql);
    Planner p(cat);
    return p.plan(stmt);
}

} // namespace plan
} // namespace qc
