#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <stdexcept>

namespace qc {

enum class TypeTag : uint8_t {
    BOOL,
    INT32,
    INT64,
    FLOAT64,
    VARCHAR,
    DATE,      // stored as int32 (days since epoch)
    INVALID,
};

inline std::string type_name(TypeTag t) {
    switch (t) {
        case TypeTag::BOOL:    return "bool";
        case TypeTag::INT32:   return "int32";
        case TypeTag::INT64:   return "int64";
        case TypeTag::FLOAT64: return "float64";
        case TypeTag::VARCHAR: return "varchar";
        case TypeTag::DATE:    return "date";
        default:               return "invalid";
    }
}

inline bool is_numeric(TypeTag t) {
    return t == TypeTag::INT32 || t == TypeTag::INT64 || t == TypeTag::FLOAT64;
}

inline bool is_integral(TypeTag t) {
    return t == TypeTag::INT32 || t == TypeTag::INT64 || t == TypeTag::DATE;
}

// Runtime value (used in interpreter, not in JIT hot path)
using Value = std::variant<std::monostate, bool, int32_t, int64_t, double, std::string>;

inline Value null_value() { return std::monostate{}; }

inline bool is_null(const Value& v) {
    return std::holds_alternative<std::monostate>(v);
}

inline TypeTag value_type(const Value& v) {
    return std::visit([](auto&& x) -> TypeTag {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, std::monostate>) return TypeTag::INVALID;
        if constexpr (std::is_same_v<T, bool>)           return TypeTag::BOOL;
        if constexpr (std::is_same_v<T, int32_t>)        return TypeTag::INT32;
        if constexpr (std::is_same_v<T, int64_t>)        return TypeTag::INT64;
        if constexpr (std::is_same_v<T, double>)         return TypeTag::FLOAT64;
        if constexpr (std::is_same_v<T, std::string>)    return TypeTag::VARCHAR;
        return TypeTag::INVALID;
    }, v);
}

// Parse ISO date string to days since 1970-01-01
int32_t parse_date(std::string_view s);
std::string date_to_string(int32_t days);

// Schema column descriptor
struct ColumnSchema {
    std::string name;
    TypeTag     type;
    bool        nullable{false};
};

struct TableSchema {
    std::string              name;
    std::vector<ColumnSchema> columns;

    int column_index(std::string_view col_name) const {
        for (int i = 0; i < (int)columns.size(); i++)
            if (columns[i].name == col_name) return i;
        return -1;
    }
};

} // namespace qc
