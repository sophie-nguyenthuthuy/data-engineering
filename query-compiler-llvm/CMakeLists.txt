cmake_minimum_required(VERSION 3.20)
project(query_compiler_llvm VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ─── Build type defaults ──────────────────────────────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ─── Compiler flags ───────────────────────────────────────────────────────────
add_compile_options(
  -Wall -Wextra -Wpedantic
  $<$<CONFIG:Release>:-O3 -march=native -DNDEBUG>
  $<$<CONFIG:Debug>:-g -O0 -fsanitize=address,undefined>
)
add_link_options(
  $<$<CONFIG:Debug>:-fsanitize=address,undefined>
)

# ─── LLVM ─────────────────────────────────────────────────────────────────────
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION} at ${LLVM_DIR}")
message(STATUS "LLVM include dirs: ${LLVM_INCLUDE_DIRS}")

include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

# Components we need: core + JIT + native backend + optimizer
llvm_map_components_to_libnames(LLVM_LIBS
  Core
  Support
  IRReader
  ExecutionEngine
  OrcJIT
  MCJIT
  Passes
  Analysis
  TransformUtils
  InstCombine
  ScalarOpts
  Vectorize
  native            # registers the native target
)

# ─── Threading ────────────────────────────────────────────────────────────────
find_package(Threads REQUIRED)

# ─── Include paths ────────────────────────────────────────────────────────────
include_directories(${CMAKE_SOURCE_DIR}/include)

# ─── Core library ────────────────────────────────────────────────────────────
add_library(qc_core STATIC
  src/parser/lexer.cpp
  src/parser/parser.cpp
  src/storage/table.cpp
  src/plan/planner.cpp
  src/executor/interpreter.cpp
  src/codegen/llvm_codegen.cpp
  src/jit/speculative.cpp
)

target_include_directories(qc_core PUBLIC
  ${CMAKE_SOURCE_DIR}/include
  ${LLVM_INCLUDE_DIRS}
)

target_link_libraries(qc_core PUBLIC
  ${LLVM_LIBS}
  Threads::Threads
)

# Suppress LLVM's own deprecation warnings leaking into our build
target_compile_options(qc_core PRIVATE -Wno-unused-parameter)

# ─── Main executable ──────────────────────────────────────────────────────────
add_executable(qc src/main.cpp)
target_link_libraries(qc PRIVATE qc_core)

# ─── Benchmark executable ────────────────────────────────────────────────────
add_executable(bench bench/bench_main.cpp)
target_link_libraries(bench PRIVATE qc_core)
target_include_directories(bench PRIVATE ${CMAKE_SOURCE_DIR}/bench)

# ─── Tests ───────────────────────────────────────────────────────────────────
enable_testing()
add_executable(run_tests test/test_queries.cpp)
target_link_libraries(run_tests PRIVATE qc_core)
add_test(NAME unit_tests COMMAND run_tests)

# ─── Installation ─────────────────────────────────────────────────────────────
install(TARGETS qc bench DESTINATION bin)
