cmake_minimum_required(VERSION 3.15)
project(art_database C)

# Set C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -msse4.2")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -DDEBUG")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -DNDEBUG")

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/third_party
)

#==============================================================================
# Libraries
#==============================================================================

# In-Memory ART Tree (general-purpose, variable-size keys/values)
add_library(art_database STATIC
    database/src/mem_art.c
    database/src/logger.c
)

# Compact ART — Space-Efficient Index (fixed-size keys/values, arena-allocated)
add_library(compact_art STATIC
    database/src/compact_art.c
)

#==============================================================================
# Tests
#==============================================================================

enable_testing()

# Test: In-Memory ART
add_executable(test_mem_art
    database/tests/test_mem_art.c
)
target_link_libraries(test_mem_art art_database)
add_test(NAME test_mem_art COMMAND test_mem_art)

# Test: mem_art stress (in-memory ART correctness at scale)
add_executable(test_stress_mem_art
    database/tests/test_stress_mem_art.c
)
target_link_libraries(test_stress_mem_art art_database)

# Benchmark: mem_art as index (memory usage measurement)
add_executable(bench_mem_art_index
    database/tests/bench_mem_art_index.c
)
target_link_libraries(bench_mem_art_index art_database)

# Benchmark: compact_art vs mem_art (memory comparison)
add_executable(bench_compact_art_index
    database/tests/bench_compact_art_index.c
)
target_link_libraries(bench_compact_art_index compact_art art_database)

#==============================================================================
# Build Summary
#==============================================================================

message(STATUS "")
message(STATUS "========================================")
message(STATUS "ART Database Configuration")
message(STATUS "========================================")
message(STATUS "Build type:        ${CMAKE_BUILD_TYPE}")
message(STATUS "C Compiler:        ${CMAKE_C_COMPILER}")
message(STATUS "C Flags:           ${CMAKE_C_FLAGS}")
message(STATUS "")
message(STATUS "Libraries:")
message(STATUS "  - art_database (in-memory ART tree)")
message(STATUS "  - compact_art (space-efficient index ART)")
message(STATUS "========================================")
message(STATUS "")
