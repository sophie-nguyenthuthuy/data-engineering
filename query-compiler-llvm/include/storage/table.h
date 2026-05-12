#pragma once
#include "storage/column.h"
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>

namespace qc {

class Table {
public:
    explicit Table(TableSchema schema) : schema_(std::move(schema)) {
        for (auto& col : schema_.columns)
            columns_.emplace_back(col.name, col.type);
    }

    const TableSchema& schema() const { return schema_; }
    const std::string& name()   const { return schema_.name; }
    size_t num_rows()    const { return columns_.empty() ? 0 : columns_[0].size(); }
    size_t num_columns() const { return columns_.size(); }

    Column&       column(int idx)       { return columns_[idx]; }
    const Column& column(int idx) const { return columns_[idx]; }

    Column* find_column(std::string_view n) {
        int idx = schema_.column_index(n);
        return idx >= 0 ? &columns_[idx] : nullptr;
    }

    void append_row(const std::vector<Value>& row) {
        assert(row.size() == columns_.size());
        for (size_t i = 0; i < columns_.size(); i++)
            columns_[i].push_back(row[i]);
    }

    void reserve(size_t n) {
        for (auto& col : columns_) col.reserve(n);
    }

private:
    TableSchema         schema_;
    std::vector<Column> columns_;
};

// Global catalog
class Catalog {
public:
    static Catalog& instance() {
        static Catalog inst;
        return inst;
    }

    void register_table(std::shared_ptr<Table> t) {
        tables_[t->name()] = std::move(t);
    }

    std::shared_ptr<Table> find(const std::string& name) const {
        auto it = tables_.find(name);
        return it != tables_.end() ? it->second : nullptr;
    }

    void clear() { tables_.clear(); }

private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};

// Load a TPC-H table from a pipe-delimited .tbl file
void load_tbl_file(const std::string& path, Table& table);

// Generate small in-memory TPC-H-like data for testing (scale = num_rows multiplier)
void generate_tpch_data(Catalog& cat, int scale = 1000);

} // namespace qc
