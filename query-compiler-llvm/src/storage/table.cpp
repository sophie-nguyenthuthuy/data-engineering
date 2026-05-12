#include "storage/table.h"
#include "common/types.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <stdexcept>
#include <random>
#include <algorithm>

namespace qc {

// Parse ISO date string "YYYY-MM-DD" → days since 1970-01-01
int32_t parse_date(std::string_view s) {
    if (s.size() < 10) return 0;
    int y = std::stoi(std::string(s.substr(0, 4)));
    int m = std::stoi(std::string(s.substr(5, 2)));
    int d = std::stoi(std::string(s.substr(8, 2)));
    // Zeller-like formula
    int days = 0;
    for (int yr = 1970; yr < y; yr++) {
        bool leap = (yr % 4 == 0 && yr % 100 != 0) || (yr % 400 == 0);
        days += leap ? 366 : 365;
    }
    int month_days[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    if (leap) month_days[1] = 29;
    for (int i = 0; i < m - 1; i++) days += month_days[i];
    days += d - 1;
    return days;
}

std::string date_to_string(int32_t days) {
    int y = 1970, m = 1, d = 1;
    int month_days[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    while (days > 0) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        month_days[1] = leap ? 29 : 28;
        int yd = leap ? 366 : 365;
        if (days >= yd) { days -= yd; y++; continue; }
        for (int i = 0; i < 12; i++) {
            if (days < month_days[i]) { m = i+1; d = days+1; break; }
            days -= month_days[i];
        }
        break;
    }
    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
    return buf;
}

// Load a pipe-delimited TPC-H .tbl file
void load_tbl_file(const std::string& path, Table& table) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("Cannot open: " + path);

    std::string line;
    const auto& schema = table.schema();
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        std::vector<Value> row;
        std::stringstream ss(line);
        std::string field;
        for (int i = 0; i < (int)schema.columns.size(); i++) {
            if (!std::getline(ss, field, '|')) break;
            const auto& col = schema.columns[i];
            switch (col.type) {
            case TypeTag::INT32:   row.push_back(static_cast<int32_t>(std::stoi(field))); break;
            case TypeTag::INT64:   row.push_back(static_cast<int64_t>(std::stoll(field))); break;
            case TypeTag::FLOAT64: row.push_back(std::stod(field)); break;
            case TypeTag::DATE:    row.push_back(parse_date(field)); break;
            case TypeTag::VARCHAR: row.push_back(field); break;
            default:               row.push_back(null_value()); break;
            }
        }
        if ((int)row.size() == (int)schema.columns.size())
            table.append_row(row);
    }
    std::cout << "Loaded " << table.num_rows() << " rows into " << table.name() << "\n";
}

// ─── TPC-H in-memory data generator ──────────────────────────────────────────
// Generates deterministic synthetic data that mimics TPC-H schema and value
// distributions closely enough to produce meaningful benchmark numbers.

static const char* MKTSEGS[] = {"BUILDING","AUTOMOBILE","MACHINERY","HOUSEHOLD","FURNITURE"};
static const char* ORDERSTATUS[] = {"F","O","P"};
static const char* SHIPMODE[]  = {"AIR","RAIL","SHIP","TRUCK","MAIL","FOB","AIR REG"};
static const char* RETURNFLAG[]= {"N","A","R"};
static const char* LINESTATUS[]= {"O","F"};

void generate_tpch_data(Catalog& cat, int scale) {
    std::mt19937 rng(42);
    auto randu = [&](int lo, int hi) { return std::uniform_int_distribution<int>(lo,hi)(rng); };
    auto randf = [&](double lo, double hi) { return std::uniform_real_distribution<double>(lo,hi)(rng); };

    int num_customers = scale;
    int num_orders    = scale * 10;
    int num_lineitem  = scale * 40;

    // ─── CUSTOMER ────────────────────────────────────────────────────────────
    TableSchema cust_schema;
    cust_schema.name = "customer";
    cust_schema.columns = {
        {"c_custkey",   TypeTag::INT64},
        {"c_name",      TypeTag::VARCHAR},
        {"c_address",   TypeTag::VARCHAR},
        {"c_nationkey", TypeTag::INT64},
        {"c_phone",     TypeTag::VARCHAR},
        {"c_acctbal",   TypeTag::FLOAT64},
        {"c_mktsegment",TypeTag::VARCHAR},
        {"c_comment",   TypeTag::VARCHAR},
    };
    auto cust = std::make_shared<Table>(cust_schema);
    cust->reserve(num_customers);
    for (int i = 1; i <= num_customers; i++) {
        cust->append_row({
            static_cast<int64_t>(i),
            std::string("Customer#") + std::to_string(i),
            std::string("addr") + std::to_string(i),
            static_cast<int64_t>(randu(0, 24)),
            std::string("phone") + std::to_string(i),
            randf(-999.0, 9999.0),
            std::string(MKTSEGS[randu(0,4)]),
            std::string("comment"),
        });
    }
    cat.register_table(cust);

    // ─── ORDERS ──────────────────────────────────────────────────────────────
    TableSchema ord_schema;
    ord_schema.name = "orders";
    ord_schema.columns = {
        {"o_orderkey",     TypeTag::INT64},
        {"o_custkey",      TypeTag::INT64},
        {"o_orderstatus",  TypeTag::VARCHAR},
        {"o_totalprice",   TypeTag::FLOAT64},
        {"o_orderdate",    TypeTag::DATE},
        {"o_orderpriority",TypeTag::VARCHAR},
        {"o_clerk",        TypeTag::VARCHAR},
        {"o_shippriority", TypeTag::INT32},
        {"o_comment",      TypeTag::VARCHAR},
    };
    auto orders = std::make_shared<Table>(ord_schema);
    orders->reserve(num_orders);
    // Date range: 1992-01-01 to 1998-12-31
    int32_t date_lo = parse_date("1992-01-01");
    int32_t date_hi = parse_date("1998-12-31");
    for (int i = 1; i <= num_orders; i++) {
        orders->append_row({
            static_cast<int64_t>(i),
            static_cast<int64_t>(randu(1, num_customers)),
            std::string(ORDERSTATUS[randu(0,2)]),
            randf(1000.0, 500000.0),
            static_cast<int32_t>(randu(date_lo, date_hi)),
            std::string("1-URGENT"),
            std::string("Clerk#001"),
            static_cast<int32_t>(0),
            std::string("comment"),
        });
    }
    cat.register_table(orders);

    // ─── LINEITEM ─────────────────────────────────────────────────────────────
    TableSchema li_schema;
    li_schema.name = "lineitem";
    li_schema.columns = {
        {"l_orderkey",      TypeTag::INT64},
        {"l_partkey",       TypeTag::INT64},
        {"l_suppkey",       TypeTag::INT64},
        {"l_linenumber",    TypeTag::INT32},
        {"l_quantity",      TypeTag::FLOAT64},
        {"l_extendedprice", TypeTag::FLOAT64},
        {"l_discount",      TypeTag::FLOAT64},
        {"l_tax",           TypeTag::FLOAT64},
        {"l_returnflag",    TypeTag::VARCHAR},
        {"l_linestatus",    TypeTag::VARCHAR},
        {"l_shipdate",      TypeTag::DATE},
        {"l_commitdate",    TypeTag::DATE},
        {"l_receiptdate",   TypeTag::DATE},
        {"l_shipinstruct",  TypeTag::VARCHAR},
        {"l_shipmode",      TypeTag::VARCHAR},
        {"l_comment",       TypeTag::VARCHAR},
    };
    auto lineitem = std::make_shared<Table>(li_schema);
    lineitem->reserve(num_lineitem);
    int32_t ship_lo = parse_date("1992-01-02");
    int32_t ship_hi = parse_date("1998-12-01");
    for (int i = 0; i < num_lineitem; i++) {
        int32_t shipdate = randu(ship_lo, ship_hi);
        lineitem->append_row({
            static_cast<int64_t>(randu(1, num_orders)),
            static_cast<int64_t>(randu(1, scale * 2)),
            static_cast<int64_t>(randu(1, scale / 10 + 1)),
            static_cast<int32_t>(randu(1, 7)),
            randf(1.0, 50.0),
            randf(900.0, 100000.0),
            randf(0.0, 0.10),
            randf(0.0, 0.08),
            std::string(RETURNFLAG[randu(0,2)]),
            std::string(LINESTATUS[randu(0,1)]),
            static_cast<int32_t>(shipdate),
            static_cast<int32_t>(shipdate + randu(10,90)),
            static_cast<int32_t>(shipdate + randu(30,120)),
            std::string("DELIVER IN PERSON"),
            std::string(SHIPMODE[randu(0,6)]),
            std::string("comment"),
        });
    }
    cat.register_table(lineitem);

    std::cout << "Generated TPC-H data: "
              << num_customers << " customers, "
              << num_orders << " orders, "
              << num_lineitem << " lineitems\n";
}

} // namespace qc
