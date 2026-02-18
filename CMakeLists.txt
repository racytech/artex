cmake_minimum_required(VERSION 3.15)
project(art_database C)

# Set C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -DDEBUG")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -DNDEBUG")

# Find required packages
find_package(PkgConfig REQUIRED)

# Find compression libraries (optional for Phase 1, required for Phase 3)
pkg_check_modules(LZ4 liblz4)
pkg_check_modules(ZSTD libzstd)

if(LZ4_FOUND)
    message(STATUS "LZ4 found: ${LZ4_VERSION}")
    add_definitions(-DHAVE_LZ4)
else()
    message(WARNING "LZ4 not found - compression disabled")
endif()

if(ZSTD_FOUND)
    message(STATUS "ZSTD found: ${ZSTD_VERSION}")
    add_definitions(-DHAVE_ZSTD)
else()
    message(WARNING "ZSTD not found - compression disabled")
endif()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/third_party
    ${LZ4_INCLUDE_DIRS}
    ${ZSTD_INCLUDE_DIRS}
)

#==============================================================================
# Database Library (Persistent ART - Page Manager)
#==============================================================================

add_library(art_page_manager STATIC
    database/src/page_manager.c
    database/src/logger.c
)

target_link_libraries(art_page_manager
    ${LZ4_LIBRARIES}
    ${ZSTD_LIBRARIES}
)

target_include_directories(art_page_manager PRIVATE
    ${LZ4_INCLUDE_DIRS}
    ${ZSTD_INCLUDE_DIRS}
)

#==============================================================================
# Database Library (Buffer Pool - LRU Cache)
#==============================================================================

add_library(art_buffer_pool STATIC
    database/src/buffer_pool.c
    database/src/logger.c
)

target_link_libraries(art_buffer_pool
    art_page_manager
    pthread
)

#==============================================================================
# Database Library (Persistent ART - Disk-backed Tree)
#==============================================================================

add_library(art_persistent STATIC
    database/src/data_art_core.c
    database/src/data_art_overflow.c
    database/src/data_art_insert.c
    database/src/data_art_delete.c
    database/src/data_art_node_ops.c
    database/src/logger.c
)

target_link_libraries(art_persistent
    art_page_manager
    art_buffer_pool
)

#==============================================================================
# Database Library (In-Memory ART Tree)
#==============================================================================

add_library(art_database STATIC
    database/src/mem_art.c
    database/src/logger.c
)

#==============================================================================
# Database Tests
#==============================================================================

enable_testing()

# Test: In-Memory ART
add_executable(test_mem_art
    database/tests/test_mem_art.c
)
target_link_libraries(test_mem_art art_database)
add_test(NAME test_mem_art COMMAND test_mem_art)

# Test: Page Manager
add_executable(test_page_manager
    database/tests/test_page_manager.c
)
target_link_libraries(test_page_manager art_page_manager)
add_test(NAME test_page_manager COMMAND test_page_manager)

# Test: Page Manager Edge Cases
add_executable(test_page_manager_edge_cases
    database/tests/test_page_manager_edge_cases.c
)
target_link_libraries(test_page_manager_edge_cases art_page_manager)
add_test(NAME test_page_manager_edge_cases COMMAND test_page_manager_edge_cases)

# Test: Buffer Pool
add_executable(test_buffer_pool
    database/tests/test_buffer_pool.c
)
target_link_libraries(test_buffer_pool art_buffer_pool)
add_test(NAME test_buffer_pool COMMAND test_buffer_pool)

# Test: Buffer Pool Edge Cases
add_executable(test_buffer_pool_edge
    database/tests/test_buffer_pool_edge.c
)
target_link_libraries(test_buffer_pool_edge art_buffer_pool pthread)
add_test(NAME test_buffer_pool_edge COMMAND test_buffer_pool_edge)

# Test: Persistent ART Basic
add_executable(test_data_art_basic
    database/tests/test_data_art_basic.c
)
target_link_libraries(test_data_art_basic art_persistent pthread)
add_test(NAME test_data_art_basic COMMAND test_data_art_basic)

# Test: Persistent ART Node Growth
add_executable(test_data_art_growth
    database/tests/test_data_art_growth.c
)
target_link_libraries(test_data_art_growth art_persistent pthread)
add_test(NAME test_data_art_growth COMMAND test_data_art_growth)

# Test: Persistent ART Stress Tests
add_executable(test_data_art_stress
    database/tests/test_data_art_stress.c
)
target_link_libraries(test_data_art_stress art_persistent pthread)

# Benchmark: Persistent ART Performance
add_executable(benchmark_data_art
    database/tests/benchmark_data_art.c
)
target_link_libraries(benchmark_data_art art_persistent pthread)

# Test: Persistent ART Edge Cases
add_executable(test_data_art_edge_cases
    database/tests/test_data_art_edge_cases.c
)
target_link_libraries(test_data_art_edge_cases art_persistent pthread)

# Test: Persistent ART Delete Operations
add_executable(test_data_art_delete
    database/tests/test_data_art_delete.c
)
target_link_libraries(test_data_art_delete art_persistent pthread)

# Test: Persistent ART Insert & Search Stress Tests
add_executable(test_data_art_insert_search_stress
    database/tests/test_data_art_insert_search_stress.c
)
target_link_libraries(test_data_art_insert_search_stress art_persistent pthread)

# Test: Full Stress Test (Insert + Search + Delete)
add_executable(test_data_art_full_stress
    database/tests/test_data_art_full_stress.c
)
target_link_libraries(test_data_art_full_stress art_persistent pthread)

# Test: Prefix Key Debug
add_executable(test_prefix_debug
    database/tests/test_prefix_debug.c
)
target_link_libraries(test_prefix_debug art_persistent pthread)

# Test: Reproduce Seed
add_executable(test_reproduce_seed
    database/tests/test_reproduce_seed.c
)
target_link_libraries(test_reproduce_seed art_persistent pthread)

# Test: Persistent ART Node Operations
add_executable(test_data_art_node_ops
    database/tests/test_data_art_node_ops.c
)
target_link_libraries(test_data_art_node_ops art_persistent pthread)
add_test(NAME test_data_art_node_ops COMMAND test_data_art_node_ops)

# Benchmark: ART vs uthash
add_executable(benchmark_art_vs_uthash
    benchmark_art_vs_uthash.c
)
target_link_libraries(benchmark_art_vs_uthash art_database)

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
message(STATUS "Install prefix:    ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")
message(STATUS "Components:")
message(STATUS "  - ART Database Library (in-memory)")
message(STATUS "  - ART Page Manager (persistent)")
message(STATUS "  - ART Buffer Pool (LRU cache)")
message(STATUS "  - Compression: LZ4=${LZ4_FOUND}, ZSTD=${ZSTD_FOUND}")
message(STATUS "")
message(STATUS "Tests:")
message(STATUS "  - test_mem_art (in-memory ART)")
message(STATUS "  - test_page_manager (persistent page manager)")
message(STATUS "  - test_page_manager_edge_cases (edge cases & stress tests)")
message(STATUS "  - test_buffer_pool (buffer pool basic tests)")
message(STATUS "  - test_buffer_pool_edge (buffer pool edge cases)")
message(STATUS "========================================")
message(STATUS "")
