#include "storage/table.h"
#include "executor/interpreter.h"
#include "jit/speculative.h"
#include "plan/physical.h"
#include <iostream>
#include <string>
#include <cstring>

static const char* HELP = R"(
Usage: qc [options] [sql]

Options:
  --scale N       TPC-H row count multiplier (default: 10000)
  --sql  QUERY    Execute a SQL query
  --verbose / -v  Print result rows and detailed stats
  --demo          Run TPC-H Q6 with speculative JIT

Examples:
  qc --demo
  qc --sql "SELECT COUNT(*) AS cnt FROM lineitem WHERE l_discount > 0.05"
  qc --scale 100000 --demo
)";

int main(int argc, char* argv[]) {
    int         scale   = 10000;
    bool        verbose = false;
    bool        demo    = false;
    std::string sql;

    for (int i = 1; i < argc; i++) {
        if      (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0)
            { std::cout << HELP; return 0; }
        else if (strcmp(argv[i], "--scale") == 0 && i+1 < argc)
            scale = std::stoi(argv[++i]);
        else if ((strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0))
            verbose = true;
        else if (strcmp(argv[i], "--demo") == 0)
            demo = true;
        else if (strcmp(argv[i], "--sql") == 0 && i+1 < argc)
            sql = argv[++i];
    }

    qc::SpeculativeEngine::warmup();
    qc::generate_tpch_data(qc::Catalog::instance(), scale);

    if (demo) {
        sql = R"(
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= date '1994-01-01'
  AND l_shipdate < date '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24.0
)";
    }

    if (sql.empty()) {
        std::cout << HELP;
        return 0;
    }

    std::cout << "Query: " << sql << "\n\n";

    qc::SpeculativeEngine engine;
    qc::ExecutionStats stats;
    try {
        auto result = engine.execute(sql, &stats);

        if (verbose) {
            auto plan = qc::plan::build_plan(sql, qc::Catalog::instance());
            qc::print_result(result, plan->output_schema);
        } else {
            std::cout << "Result: " << result.size() << " row(s)\n";
            for (auto& row : result) {
                for (auto& v : row) {
                    std::visit([](auto&& x) {
                        using T = std::decay_t<decltype(x)>;
                        if constexpr (std::is_same_v<T, double>)
                            std::cout << std::fixed << x;
                        else if constexpr (!std::is_same_v<T, std::monostate>)
                            std::cout << x;
                        else
                            std::cout << "NULL";
                        std::cout << "  ";
                    }, v);
                }
                std::cout << "\n";
            }
        }

        qc::print_stats(stats);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
