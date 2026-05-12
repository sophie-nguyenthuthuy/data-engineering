#include "storage/table.h"
#include "executor/interpreter.h"
#include "plan/physical.h"
#include "parser/parser.h"
#include "common/types.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <stdexcept>
#include <string>

// ─── Minimal test harness ─────────────────────────────────────────────────────

static int g_pass = 0, g_fail = 0;

#define ASSERT_EQ(a, b, msg)                                              \
    do {                                                                  \
        if ((a) == (b)) { g_pass++;                                       \
        } else {                                                          \
            std::cerr << "FAIL [" << __LINE__ << "] " << (msg) << ": "   \
                      << (a) << " != " << (b) << "\n"; g_fail++;         \
        }                                                                 \
    } while(0)

#define ASSERT_NEAR(a, b, tol, msg)                                       \
    do {                                                                  \
        if (std::abs((double)(a) - (double)(b)) <= (tol)) { g_pass++;    \
        } else {                                                          \
            std::cerr << "FAIL [" << __LINE__ << "] " << (msg) << ": "   \
                      << (a) << " !≈ " << (b) << "\n"; g_fail++;         \
        }                                                                 \
    } while(0)

#define ASSERT_TRUE(cond, msg) ASSERT_EQ((bool)(cond), true, msg)
#define ASSERT_NO_THROW(expr, msg)                                        \
    do {                                                                  \
        try { expr; g_pass++;                                             \
        } catch (const std::exception& e) {                              \
            std::cerr << "FAIL [" << __LINE__ << "] " << (msg)           \
                      << ": threw " << e.what() << "\n"; g_fail++;       \
        }                                                                 \
    } while(0)

// ─── Fixtures ─────────────────────────────────────────────────────────────────

static void setup_test_data(qc::Catalog& cat) {
    cat.clear();

    qc::TableSchema schema;
    schema.name = "t";
    schema.columns = {
        {"id",    qc::TypeTag::INT64},
        {"price", qc::TypeTag::FLOAT64},
        {"qty",   qc::TypeTag::FLOAT64},
        {"flag",  qc::TypeTag::VARCHAR},
        {"dt",    qc::TypeTag::DATE},
    };

    auto tbl = std::make_shared<qc::Table>(schema);
    // id, price,   qty,  flag, date
    // Rows with flag='A': 3 rows
    tbl->append_row({int64_t(1), 100.0, 5.0,  std::string("A"), int32_t(qc::parse_date("1994-06-01"))});
    tbl->append_row({int64_t(2), 200.0, 3.0,  std::string("A"), int32_t(qc::parse_date("1994-08-15"))});
    tbl->append_row({int64_t(3), 150.0, 8.0,  std::string("A"), int32_t(qc::parse_date("1994-11-30"))});
    // Rows with flag='B': 2 rows
    tbl->append_row({int64_t(4), 300.0, 2.0,  std::string("B"), int32_t(qc::parse_date("1993-01-01"))});
    tbl->append_row({int64_t(5), 250.0, 10.0, std::string("B"), int32_t(qc::parse_date("1995-06-01"))});
    cat.register_table(tbl);
}

// ─── Parser tests ─────────────────────────────────────────────────────────────

static void test_lexer_basic() {
    ASSERT_NO_THROW(qc::parse_sql("SELECT 1"), "parse literal SELECT");
    ASSERT_NO_THROW(qc::parse_sql("SELECT a FROM t"), "parse simple select");
    ASSERT_NO_THROW(
        qc::parse_sql("SELECT a, b FROM t WHERE a > 1"),
        "parse where clause");
}

static void test_parser_tpch_q6() {
    std::string sql = R"(
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= date '1994-01-01'
  AND l_shipdate < date '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24.0
)";
    ASSERT_NO_THROW(qc::parse_sql(sql), "parse TPC-H Q6");
}

static void test_parser_aggregates() {
    ASSERT_NO_THROW(
        qc::parse_sql("SELECT COUNT(*), SUM(price), AVG(qty) FROM t"),
        "parse aggregate functions");
    ASSERT_NO_THROW(
        qc::parse_sql("SELECT COUNT(*) FROM t GROUP BY flag"),
        "parse group by");
}

static void test_parser_order_limit() {
    ASSERT_NO_THROW(
        qc::parse_sql("SELECT id FROM t ORDER BY price DESC LIMIT 10"),
        "parse order by + limit");
}

// ─── Planner tests ────────────────────────────────────────────────────────────

static void test_plan_scan() {
    auto& cat = qc::Catalog::instance();
    setup_test_data(cat);

    auto plan = qc::plan::build_plan("SELECT id FROM t", cat);
    ASSERT_TRUE(plan != nullptr, "plan not null");
}

static void test_plan_filter() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan("SELECT id FROM t WHERE price > 200", cat);
    ASSERT_TRUE(plan != nullptr, "filter plan not null");
}

static void test_plan_agg() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan(
        "SELECT SUM(price) AS total FROM t WHERE price > 100", cat);
    ASSERT_TRUE(plan != nullptr, "agg plan not null");
}

// ─── Interpreter correctness ──────────────────────────────────────────────────

static void test_interp_count_star() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan("SELECT COUNT(*) AS cnt FROM t", cat);
    qc::Interpreter interp;
    auto r = interp.execute(*plan);
    ASSERT_EQ(r.size(), size_t(1), "count(*) returns 1 row");
    // cnt should be 5
    double cnt = std::get<double>(r[0][0]);
    ASSERT_NEAR(cnt, 5.0, 0.001, "count(*) = 5");
}

static void test_interp_filter_count() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan(
        "SELECT COUNT(*) AS cnt FROM t WHERE flag = 'A'", cat);
    qc::Interpreter interp;
    auto r = interp.execute(*plan);
    ASSERT_EQ(r.size(), size_t(1), "filtered count returns 1 row");
    double cnt = std::get<double>(r[0][0]);
    ASSERT_NEAR(cnt, 3.0, 0.001, "count WHERE flag='A' = 3");
}

static void test_interp_sum() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan("SELECT SUM(price) AS total FROM t", cat);
    qc::Interpreter interp;
    auto r = interp.execute(*plan);
    ASSERT_EQ(r.size(), size_t(1), "sum returns 1 row");
    double total = std::get<double>(r[0][0]);
    // 100 + 200 + 150 + 300 + 250 = 1000
    ASSERT_NEAR(total, 1000.0, 0.001, "SUM(price) = 1000");
}

static void test_interp_sum_product() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan(
        "SELECT SUM(price * qty) AS revenue FROM t", cat);
    qc::Interpreter interp;
    auto r = interp.execute(*plan);
    ASSERT_EQ(r.size(), size_t(1), "sum product returns 1 row");
    double rev = std::get<double>(r[0][0]);
    // 100*5 + 200*3 + 150*8 + 300*2 + 250*10 = 500+600+1200+600+2500 = 5400
    ASSERT_NEAR(rev, 5400.0, 0.001, "SUM(price*qty) = 5400");
}

static void test_interp_between() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan(
        "SELECT COUNT(*) AS cnt FROM t WHERE qty BETWEEN 3.0 AND 8.0", cat);
    qc::Interpreter interp;
    auto r = interp.execute(*plan);
    ASSERT_EQ(r.size(), size_t(1), "between filter returns 1 row");
    double cnt = std::get<double>(r[0][0]);
    // qty: 5,3,8,2,10 → 5,3,8 pass → count=3
    ASSERT_NEAR(cnt, 3.0, 0.001, "BETWEEN 3 AND 8 count = 3");
}

static void test_interp_date_filter() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan(
        "SELECT COUNT(*) AS cnt FROM t WHERE dt >= date '1994-01-01' AND dt < date '1995-01-01'",
        cat);
    qc::Interpreter interp;
    auto r = interp.execute(*plan);
    ASSERT_EQ(r.size(), size_t(1), "date filter returns 1 row");
    double cnt = std::get<double>(r[0][0]);
    // dates in 1994: 1994-06-01, 1994-08-15, 1994-11-30 → count=3
    ASSERT_NEAR(cnt, 3.0, 0.001, "date range count = 3");
}

static void test_interp_group_by() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan(
        "SELECT SUM(price) AS total FROM t GROUP BY flag", cat);
    qc::Interpreter interp;
    auto r = interp.execute(*plan);
    // 2 groups: A(100+200+150=450), B(300+250=550)
    ASSERT_EQ(r.size(), size_t(2), "group by flag → 2 groups");
    double sum = 0;
    for (auto& row : r) {
        double v = std::get<double>(row[row.size()-1]);
        sum += v;
    }
    ASSERT_NEAR(sum, 1000.0, 0.001, "group by total = 1000");
}

static void test_interp_order_limit() {
    auto& cat = qc::Catalog::instance();
    auto plan = qc::plan::build_plan(
        "SELECT id FROM t ORDER BY price DESC LIMIT 3", cat);
    qc::Interpreter interp;
    auto r = interp.execute(*plan);
    ASSERT_EQ(r.size(), size_t(3), "limit 3 returns 3 rows");
}

// ─── Date utility tests ───────────────────────────────────────────────────────

static void test_date_parse() {
    int32_t d = qc::parse_date("1970-01-01");
    ASSERT_EQ(d, 0, "epoch = 0");

    int32_t d2 = qc::parse_date("1970-01-02");
    ASSERT_EQ(d2, 1, "day 2 = 1");

    int32_t d3 = qc::parse_date("1994-01-01");
    ASSERT_TRUE(d3 > 0, "1994 > epoch");
    ASSERT_TRUE(d3 < qc::parse_date("1995-01-01"), "1994 < 1995");
}

// ─── Runner ──────────────────────────────────────────────────────────────────

int main() {
    std::cout << "Running tests...\n";

    // Parser
    test_lexer_basic();
    test_parser_tpch_q6();
    test_parser_aggregates();
    test_parser_order_limit();

    // Planner
    test_plan_scan();
    test_plan_filter();
    test_plan_agg();

    // Interpreter
    test_interp_count_star();
    test_interp_filter_count();
    test_interp_sum();
    test_interp_sum_product();
    test_interp_between();
    test_interp_date_filter();
    test_interp_group_by();
    test_interp_order_limit();

    // Dates
    test_date_parse();

    std::cout << "\nResults: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail > 0 ? 1 : 0;
}
