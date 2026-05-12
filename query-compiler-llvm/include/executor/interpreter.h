#pragma once
#include "plan/physical.h"
#include <vector>
#include <atomic>

namespace qc {

// Result row: vector of typed values
using Row    = std::vector<Value>;
using Result = std::vector<Row>;

// ─── Volcano-model iterator interface ────────────────────────────────────────

class RowIterator {
public:
    virtual ~RowIterator() = default;
    virtual void open()  = 0;
    virtual bool next(Row& out) = 0;  // returns false when exhausted
    virtual void close() = 0;
    virtual const plan::Schema& schema() const = 0;

    // For speculative hot-swap: report row index that has been consumed
    virtual int64_t rows_consumed() const { return -1; }
};

// ─── Interpreter ─────────────────────────────────────────────────────────────

class Interpreter {
public:
    // Build a Volcano iterator tree for the given physical plan.
    // `start_row` allows resuming a scan from a checkpoint (used during hot-swap).
    std::unique_ptr<RowIterator> build(const plan::PhysicalNode& plan,
                                       int64_t start_row = 0);

    // Execute fully: collect all rows
    Result execute(const plan::PhysicalNode& plan);

    // Evaluate a single expression against a row
    static Value eval_expr(const plan::PlanExpr& expr, const Row& row);
    static bool  eval_predicate(const plan::PlanExpr& expr, const Row& row);
};

// ─── Hash table used by interpreter's HashJoin/HashAgg ───────────────────────

struct InterpHashTable {
    std::vector<Row>              build_rows;
    // key (string representation) → vector of build_rows indices
    std::unordered_map<std::string, std::vector<size_t>> index;

    void insert(const Row& key_cols, size_t row_idx) {
        std::string k = make_key(key_cols);
        index[k].push_back(row_idx);
    }

    const std::vector<size_t>* lookup(const Row& key_cols) const {
        auto it = index.find(make_key(key_cols));
        return it != index.end() ? &it->second : nullptr;
    }

private:
    static std::string make_key(const Row& cols) {
        std::string k;
        for (auto& v : cols) {
            std::visit([&](auto&& x) {
                using T = std::decay_t<decltype(x)>;
                if constexpr (std::is_same_v<T, std::monostate>) k += "N|";
                else if constexpr (std::is_same_v<T, std::string>) k += x + "|";
                else k += std::to_string(x) + "|";
            }, v);
        }
        return k;
    }
};

} // namespace qc
