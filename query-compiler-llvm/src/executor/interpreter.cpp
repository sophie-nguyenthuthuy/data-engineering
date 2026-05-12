#include "executor/interpreter.h"
#include <algorithm>
#include <numeric>
#include <stdexcept>
#include <cassert>
#include <unordered_map>
#include <cmath>

namespace qc {

using namespace plan;

// ─── Expression evaluator ─────────────────────────────────────────────────────

Value Interpreter::eval_expr(const PlanExpr& expr, const Row& row) {
    return std::visit([&](const auto& node) -> Value {
        using T = std::decay_t<decltype(node)>;

        if constexpr (std::is_same_v<T, PLiteral>)
            return node.value;

        if constexpr (std::is_same_v<T, PColRef>) {
            assert(node.col_idx < (int)row.size());
            return row[node.col_idx];
        }

        if constexpr (std::is_same_v<T, PBinExpr>) {
            Value lv = eval_expr(*node.left,  row);
            Value rv = eval_expr(*node.right, row);

            // Arithmetic/comparison helper — coerce to float64 if needed
            auto as_f64 = [](const Value& v) -> double {
                return std::visit([](auto&& x) -> double {
                    using X = std::decay_t<decltype(x)>;
                    if constexpr (std::is_arithmetic_v<X> && !std::is_same_v<X, bool>)
                        return static_cast<double>(x);
                    return 0.0;
                }, v);
            };
            auto as_i64 = [](const Value& v) -> int64_t {
                return std::visit([](auto&& x) -> int64_t {
                    using X = std::decay_t<decltype(x)>;
                    if constexpr (std::is_integral_v<X> && !std::is_same_v<X, bool>)
                        return static_cast<int64_t>(x);
                    if constexpr (std::is_floating_point_v<X>)
                        return static_cast<int64_t>(x);
                    return 0;
                }, v);
            };

            switch (node.op) {
            case PBinOp::ADD:
                if (node.result_type == TypeTag::FLOAT64)
                    return as_f64(lv) + as_f64(rv);
                return as_i64(lv) + as_i64(rv);
            case PBinOp::SUB:
                if (node.result_type == TypeTag::FLOAT64)
                    return as_f64(lv) - as_f64(rv);
                return as_i64(lv) - as_i64(rv);
            case PBinOp::MUL:
                if (node.result_type == TypeTag::FLOAT64)
                    return as_f64(lv) * as_f64(rv);
                return as_i64(lv) * as_i64(rv);
            case PBinOp::DIV:
                if (node.result_type == TypeTag::FLOAT64) {
                    double d = as_f64(rv);
                    return d == 0.0 ? 0.0 : as_f64(lv) / d;
                }
                { int64_t d = as_i64(rv); return d == 0 ? int64_t(0) : as_i64(lv) / d; }
            case PBinOp::EQ:  return lv == rv;
            case PBinOp::NEQ: return lv != rv;
            case PBinOp::LT:  return as_f64(lv) <  as_f64(rv);
            case PBinOp::LE:  return as_f64(lv) <= as_f64(rv);
            case PBinOp::GT:  return as_f64(lv) >  as_f64(rv);
            case PBinOp::GE:  return as_f64(lv) >= as_f64(rv);
            case PBinOp::AND:
                return std::get<bool>(lv) && std::get<bool>(rv);
            case PBinOp::OR:
                return std::get<bool>(lv) || std::get<bool>(rv);
            }
        }

        if constexpr (std::is_same_v<T, PAggExpr>) {
            // Agg expressions are not evaluated row-by-row here
            if (node.arg) return eval_expr(*node.arg, row);
            return null_value();
        }

        return null_value();
    }, expr.node);
}

bool Interpreter::eval_predicate(const PlanExpr& expr, const Row& row) {
    Value v = eval_expr(expr, row);
    if (auto* b = std::get_if<bool>(&v)) return *b;
    return false;
}

// ─── Iterator implementations ─────────────────────────────────────────────────

class ScanIterator : public RowIterator {
public:
    ScanIterator(const PhysicalScan& op, int64_t start_row)
        : table_(op.table), col_indices_(op.col_indices), pos_(start_row) {}

    void open()  override { pos_ = start_; }
    void close() override {}
    int64_t rows_consumed() const override { return pos_; }
    const Schema& schema() const override { return schema_; }

    bool next(Row& out) override {
        if (pos_ >= (int64_t)table_->num_rows()) return false;
        out.resize(col_indices_.size());
        for (int i = 0; i < (int)col_indices_.size(); i++)
            out[i] = table_->column(col_indices_[i]).get(pos_);
        pos_++;
        return true;
    }

    void set_schema(Schema s) { schema_ = std::move(s); }

private:
    std::shared_ptr<Table> table_;
    std::vector<int>       col_indices_;
    int64_t                start_{0};
    int64_t                pos_{0};
    Schema                 schema_;
};

class FilterIterator : public RowIterator {
public:
    FilterIterator(std::unique_ptr<RowIterator> child, const PlanExpr& pred)
        : child_(std::move(child)), pred_(pred) {}

    void open()  override { child_->open(); }
    void close() override { child_->close(); }
    const Schema& schema() const override { return child_->schema(); }

    bool next(Row& out) override {
        while (child_->next(out))
            if (Interpreter::eval_predicate(pred_, out)) return true;
        return false;
    }

private:
    std::unique_ptr<RowIterator> child_;
    const PlanExpr&              pred_;
};

class ProjectIterator : public RowIterator {
public:
    ProjectIterator(std::unique_ptr<RowIterator> child,
                    const std::vector<PlanExprPtr>& exprs,
                    Schema schema)
        : child_(std::move(child)), exprs_(exprs), schema_(std::move(schema)) {}

    void open()  override { child_->open(); }
    void close() override { child_->close(); }
    const Schema& schema() const override { return schema_; }

    bool next(Row& out) override {
        Row in;
        if (!child_->next(in)) return false;
        out.resize(exprs_.size());
        for (int i = 0; i < (int)exprs_.size(); i++)
            out[i] = Interpreter::eval_expr(*exprs_[i], in);
        return true;
    }

private:
    std::unique_ptr<RowIterator> child_;
    const std::vector<PlanExprPtr>& exprs_;
    Schema schema_;
};

// Hash aggregate — materializes all input, then emits groups
class HashAggIterator : public RowIterator {
public:
    HashAggIterator(std::unique_ptr<RowIterator> child,
                    const std::vector<PlanExprPtr>& group_keys,
                    const std::vector<AggregateVal>& agg_vals,
                    Schema schema)
        : child_(std::move(child)),
          group_keys_(group_keys),
          agg_vals_(agg_vals),
          schema_(std::move(schema)) {}

    void open() override {
        child_->open();
        build();
        out_pos_ = 0;
    }
    void close() override { child_->close(); }
    const Schema& schema() const override { return schema_; }

    bool next(Row& out) override {
        if (out_pos_ >= (int64_t)output_.size()) return false;
        out = output_[out_pos_++];
        return true;
    }

private:
    void build() {
        // accumulator: key_str → (key_values, [agg_state])
        struct AggState {
            Row key_vals;
            std::vector<double>  sum_val;
            std::vector<int64_t> count_val;
            std::vector<double>  min_val;
            std::vector<double>  max_val;
        };
        std::unordered_map<std::string, AggState> ht;
        std::vector<std::string> key_order;

        Row in;
        while (child_->next(in)) {
            // Compute group key
            Row key_vals;
            std::string key_str;
            for (auto& ke : group_keys_) {
                Value kv = Interpreter::eval_expr(*ke, in);
                key_vals.push_back(kv);
                std::visit([&](auto&& x) {
                    using X = std::decay_t<decltype(x)>;
                    if constexpr (std::is_same_v<X, std::monostate>) key_str += "N|";
                    else if constexpr (std::is_same_v<X, std::string>) key_str += x + "|";
                    else key_str += std::to_string(x) + "|";
                }, kv);
            }

            auto it = ht.find(key_str);
            if (it == ht.end()) {
                AggState st;
                st.key_vals = key_vals;
                st.sum_val.resize(agg_vals_.size(), 0.0);
                st.count_val.resize(agg_vals_.size(), 0);
                st.min_val.resize(agg_vals_.size(), std::numeric_limits<double>::max());
                st.max_val.resize(agg_vals_.size(), std::numeric_limits<double>::lowest());
                ht[key_str] = std::move(st);
                key_order.push_back(key_str);
                it = ht.find(key_str);
            }

            auto& st = it->second;
            for (int i = 0; i < (int)agg_vals_.size(); i++) {
                auto& av = agg_vals_[i];
                st.count_val[i]++;
                if (av.agg == ast::AggFunc::COUNT_STAR) continue;
                if (!av.arg) continue;
                Value v = Interpreter::eval_expr(*av.arg, in);
                double d = std::visit([](auto&& x) -> double {
                    using X = std::decay_t<decltype(x)>;
                    if constexpr (std::is_arithmetic_v<X> && !std::is_same_v<X, bool>)
                        return static_cast<double>(x);
                    return 0.0;
                }, v);
                st.sum_val[i]  += d;
                st.min_val[i]   = std::min(st.min_val[i], d);
                st.max_val[i]   = std::max(st.max_val[i], d);
            }
        }

        // Finalize and collect output rows
        for (auto& key_str : key_order) {
            auto& st = ht[key_str];
            Row row = st.key_vals;
            for (int i = 0; i < (int)agg_vals_.size(); i++) {
                auto& av = agg_vals_[i];
                switch (av.agg) {
                case ast::AggFunc::COUNT:
                case ast::AggFunc::COUNT_STAR:
                    row.push_back(st.count_val[i]);
                    break;
                case ast::AggFunc::SUM:
                    row.push_back(st.sum_val[i]);
                    break;
                case ast::AggFunc::AVG:
                    row.push_back(st.count_val[i] > 0
                                  ? st.sum_val[i] / st.count_val[i] : 0.0);
                    break;
                case ast::AggFunc::MIN:
                    row.push_back(st.min_val[i]);
                    break;
                case ast::AggFunc::MAX:
                    row.push_back(st.max_val[i]);
                    break;
                }
            }
            output_.push_back(std::move(row));
        }
    }

    std::unique_ptr<RowIterator> child_;
    const std::vector<PlanExprPtr>&  group_keys_;
    const std::vector<AggregateVal>& agg_vals_;
    Schema                           schema_;
    Result                           output_;
    int64_t                          out_pos_{0};
};

// Hash join — builds hash table on left (build) side, probes with right
class HashJoinIterator : public RowIterator {
public:
    HashJoinIterator(std::unique_ptr<RowIterator> build,
                     std::unique_ptr<RowIterator> probe,
                     const std::vector<int>& build_keys,
                     const std::vector<int>& probe_keys,
                     const PlanExpr*          extra_pred,
                     Schema schema)
        : build_(std::move(build)), probe_(std::move(probe)),
          build_keys_(build_keys), probe_keys_(probe_keys),
          extra_pred_(extra_pred), schema_(std::move(schema)) {}

    void open() override {
        build_->open();
        // Materialize build side
        Row row;
        while (build_->next(row)) build_rows_.push_back(row);
        // Build hash index
        for (size_t i = 0; i < build_rows_.size(); i++) {
            std::string k = make_key(build_rows_[i], build_keys_);
            build_ht_[k].push_back(i);
        }
        probe_->open();
        match_pos_ = 0;
        matches_.clear();
    }

    void close() override { build_->close(); probe_->close(); }
    const Schema& schema() const override { return schema_; }

    bool next(Row& out) override {
        while (true) {
            // Drain current matches
            while (match_pos_ < (int)matches_.size()) {
                size_t bi = matches_[match_pos_++];
                // Concatenate probe row + build row
                Row combined = cur_probe_;
                combined.insert(combined.end(),
                                build_rows_[bi].begin(), build_rows_[bi].end());
                if (!extra_pred_ || Interpreter::eval_predicate(*extra_pred_, combined)) {
                    out = std::move(combined);
                    return true;
                }
            }
            // Fetch next probe row
            if (!probe_->next(cur_probe_)) return false;
            // If no join keys, cross join: all build rows match
            if (build_keys_.empty()) {
                matches_.resize(build_rows_.size());
                std::iota(matches_.begin(), matches_.end(), 0);
            } else {
                std::string k = make_key(cur_probe_, probe_keys_);
                auto it = build_ht_.find(k);
                matches_ = (it != build_ht_.end()) ? it->second : std::vector<size_t>{};
            }
            match_pos_ = 0;
        }
    }

private:
    static std::string make_key(const Row& row, const std::vector<int>& keys) {
        std::string k;
        for (int idx : keys) {
            std::visit([&](auto&& x) {
                using X = std::decay_t<decltype(x)>;
                if constexpr (std::is_same_v<X, std::string>) k += x + "|";
                else if constexpr (!std::is_same_v<X, std::monostate>)
                    k += std::to_string(x) + "|";
                else k += "N|";
            }, row[idx]);
        }
        return k;
    }

    std::unique_ptr<RowIterator> build_, probe_;
    const std::vector<int>&      build_keys_, probe_keys_;
    const PlanExpr*              extra_pred_;
    Schema                       schema_;
    Result                       build_rows_;
    std::unordered_map<std::string, std::vector<size_t>> build_ht_;
    Row                          cur_probe_;
    std::vector<size_t>          matches_;
    int                          match_pos_{0};
};

class SortIterator : public RowIterator {
public:
    SortIterator(std::unique_ptr<RowIterator> child,
                 const std::vector<SortColumn>& keys,
                 Schema schema)
        : child_(std::move(child)), keys_(keys), schema_(std::move(schema)) {}

    void open() override {
        child_->open();
        rows_.clear();
        Row r;
        while (child_->next(r)) rows_.push_back(r);
        std::stable_sort(rows_.begin(), rows_.end(), [&](const Row& a, const Row& b) {
            for (auto& k : keys_) {
                Value va = Interpreter::eval_expr(*k.expr, a);
                Value vb = Interpreter::eval_expr(*k.expr, b);
                auto da = std::visit([](auto&& x) -> double {
                    using X = std::decay_t<decltype(x)>;
                    if constexpr (std::is_arithmetic_v<X> && !std::is_same_v<X, bool>)
                        return static_cast<double>(x);
                    return 0.0;
                }, va);
                auto db = std::visit([](auto&& x) -> double {
                    using X = std::decay_t<decltype(x)>;
                    if constexpr (std::is_arithmetic_v<X> && !std::is_same_v<X, bool>)
                        return static_cast<double>(x);
                    return 0.0;
                }, vb);
                if (da != db)
                    return k.ascending ? da < db : da > db;
            }
            return false;
        });
        pos_ = 0;
    }

    void close() override { child_->close(); }
    const Schema& schema() const override { return schema_; }

    bool next(Row& out) override {
        if (pos_ >= (int64_t)rows_.size()) return false;
        out = rows_[pos_++];
        return true;
    }

private:
    std::unique_ptr<RowIterator> child_;
    const std::vector<SortColumn>& keys_;
    Schema schema_;
    Result rows_;
    int64_t pos_{0};
};

class LimitIterator : public RowIterator {
public:
    LimitIterator(std::unique_ptr<RowIterator> child,
                  int64_t limit, int64_t offset, Schema schema)
        : child_(std::move(child)), limit_(limit), offset_(offset),
          schema_(std::move(schema)) {}

    void open() override { child_->open(); skipped_ = emitted_ = 0; }
    void close() override { child_->close(); }
    const Schema& schema() const override { return schema_; }

    bool next(Row& out) override {
        while (skipped_ < offset_) {
            Row tmp;
            if (!child_->next(tmp)) return false;
            skipped_++;
        }
        if (emitted_ >= limit_) return false;
        if (!child_->next(out)) return false;
        emitted_++;
        return true;
    }

private:
    std::unique_ptr<RowIterator> child_;
    int64_t limit_, offset_;
    Schema  schema_;
    int64_t skipped_{0}, emitted_{0};
};

// ─── Interpreter::build ───────────────────────────────────────────────────────

std::unique_ptr<RowIterator>
Interpreter::build(const PhysicalNode& plan, int64_t start_row) {
    return std::visit([&](const auto& op) -> std::unique_ptr<RowIterator> {
        using T = std::decay_t<decltype(op)>;

        if constexpr (std::is_same_v<T, PhysicalScan>) {
            auto it = std::make_unique<ScanIterator>(op, start_row);
            it->set_schema(plan.output_schema);
            return it;
        }

        if constexpr (std::is_same_v<T, PhysicalFilter>) {
            auto child = build(*op.child, start_row);
            return std::make_unique<FilterIterator>(std::move(child), *op.predicate);
        }

        if constexpr (std::is_same_v<T, PhysicalProject>) {
            auto child = build(*op.child, start_row);
            return std::make_unique<ProjectIterator>(
                std::move(child), op.exprs, plan.output_schema);
        }

        if constexpr (std::is_same_v<T, PhysicalHashAggregate>) {
            auto child = build(*op.child, start_row);
            return std::make_unique<HashAggIterator>(
                std::move(child), op.group_keys, op.agg_vals, plan.output_schema);
        }

        if constexpr (std::is_same_v<T, PhysicalHashJoin>) {
            auto build_it  = build(*op.build_child, 0);
            auto probe_it  = build(*op.probe_child, start_row);
            const PlanExpr* ep = op.extra_pred ? op.extra_pred.get() : nullptr;
            return std::make_unique<HashJoinIterator>(
                std::move(build_it), std::move(probe_it),
                op.build_keys, op.probe_keys, ep, plan.output_schema);
        }

        if constexpr (std::is_same_v<T, PhysicalSort>) {
            auto child = build(*op.child, start_row);
            return std::make_unique<SortIterator>(
                std::move(child), op.keys, plan.output_schema);
        }

        if constexpr (std::is_same_v<T, PhysicalLimit>) {
            auto child = build(*op.child, start_row);
            return std::make_unique<LimitIterator>(
                std::move(child), op.limit, op.offset, plan.output_schema);
        }

        throw std::runtime_error("Unhandled operator");
    }, plan.op);
}

Result Interpreter::execute(const PhysicalNode& plan) {
    auto it = build(plan);
    it->open();
    Result result;
    Row row;
    while (it->next(row)) result.push_back(row);
    it->close();
    return result;
}

} // namespace qc
