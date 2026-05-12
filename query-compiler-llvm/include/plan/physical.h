#pragma once
#include "plan/expr.h"
#include "storage/table.h"
#include <memory>
#include <vector>
#include <string>

namespace qc {
namespace plan {

// ─── Physical operator nodes ─────────────────────────────────────────────────
// The planner lowers a SQL AST to a tree of PhysicalNode objects.
// The interpreter walks this tree; the JIT compiler flattens it into pipelines.

enum class PhysicalOpType {
    SCAN,
    FILTER,
    PROJECT,
    HASH_JOIN,
    HASH_AGGREGATE,
    SORT,
    LIMIT,
};

struct PhysicalNode;
using PhysicalNodePtr = std::unique_ptr<PhysicalNode>;

// ─── Concrete operators ───────────────────────────────────────────────────────

struct PhysicalScan {
    std::shared_ptr<Table> table;
    std::vector<int>       col_indices;  // projected columns from table
};

struct PhysicalFilter {
    PhysicalNodePtr child;
    PlanExprPtr     predicate;
};

struct PhysicalProject {
    PhysicalNodePtr        child;
    std::vector<PlanExprPtr> exprs;
};

struct AggregateKey {
    std::vector<PlanExprPtr> keys;
};

struct AggregateVal {
    ast::AggFunc agg;
    PlanExprPtr  arg;  // null for COUNT(*)
    TypeTag      result_type;
    std::string  output_name;
};

struct PhysicalHashAggregate {
    PhysicalNodePtr        child;
    std::vector<PlanExprPtr> group_keys;
    std::vector<AggregateVal> agg_vals;
};

struct PhysicalHashJoin {
    PhysicalNodePtr build_child;  // smaller relation → build hash table
    PhysicalNodePtr probe_child;
    std::vector<int> build_keys;  // column indices in build output
    std::vector<int> probe_keys;  // column indices in probe output
    PlanExprPtr      extra_pred;  // additional filter after join (may be null)
};

struct SortColumn {
    PlanExprPtr expr;
    bool        ascending{true};
};

struct PhysicalSort {
    PhysicalNodePtr    child;
    std::vector<SortColumn> keys;
};

struct PhysicalLimit {
    PhysicalNodePtr child;
    int64_t         limit;
    int64_t         offset{0};
};

using PhysicalOp = std::variant<
    PhysicalScan,
    PhysicalFilter,
    PhysicalProject,
    PhysicalHashAggregate,
    PhysicalHashJoin,
    PhysicalSort,
    PhysicalLimit
>;

struct PhysicalNode {
    PhysicalOp op;
    Schema     output_schema;

    PhysicalOpType type() const {
        return std::visit([](auto&& x) -> PhysicalOpType {
            using T = std::decay_t<decltype(x)>;
            if constexpr (std::is_same_v<T, PhysicalScan>)          return PhysicalOpType::SCAN;
            if constexpr (std::is_same_v<T, PhysicalFilter>)        return PhysicalOpType::FILTER;
            if constexpr (std::is_same_v<T, PhysicalProject>)       return PhysicalOpType::PROJECT;
            if constexpr (std::is_same_v<T, PhysicalHashAggregate>) return PhysicalOpType::HASH_AGGREGATE;
            if constexpr (std::is_same_v<T, PhysicalHashJoin>)      return PhysicalOpType::HASH_JOIN;
            if constexpr (std::is_same_v<T, PhysicalSort>)          return PhysicalOpType::SORT;
            if constexpr (std::is_same_v<T, PhysicalLimit>)         return PhysicalOpType::LIMIT;
            return PhysicalOpType::SCAN;
        }, op);
    }
};

// Build a physical plan from SQL string
PhysicalNodePtr build_plan(const std::string& sql, const Catalog& cat);

} // namespace plan
} // namespace qc
