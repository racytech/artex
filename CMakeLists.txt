cmake_minimum_required(VERSION 3.15)
project(art_database C)

# Set C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -msse4.2")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -DDEBUG")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -DNDEBUG")

# Find required packages
find_package(PkgConfig REQUIRED)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/third_party
)

#==============================================================================
# Database Library (CRC32 Utility)
#==============================================================================

add_library(art_crc32 STATIC
    database/src/crc32.c
)

#==============================================================================
# Database Library (Persistent ART - mmap Backend)
#==============================================================================

add_library(art_persistent STATIC
    database/src/data_art_core.c
    database/src/data_art_overflow.c
    database/src/data_art_search.c
    database/src/data_art_insert.c
    database/src/data_art_delete.c
    database/src/data_art_node_ops.c
    database/src/mmap_storage.c
    database/src/txn_buffer.c
    database/src/mvcc.c
    database/src/checkpoint_manager.c
    database/src/data_art_iterator.c
    database/src/data_art_batch.c
    database/src/db_backup.c
    database/src/db_error.c
    database/src/logger.c
)

target_link_libraries(art_persistent
    art_crc32
    pthread
)

#==============================================================================
# Database Library (In-Memory ART Tree)
#==============================================================================

add_library(art_database STATIC
    database/src/mem_art.c
    database/src/logger.c
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

# Test: Transaction-Based Stress Test
add_executable(test_data_art_txn_stress
    database/tests/test_data_art_txn_stress.c
)
target_link_libraries(test_data_art_txn_stress art_persistent pthread)

# Test: Transaction Buffer
add_executable(test_txn_buffer
    database/tests/test_txn_buffer.c
)
target_link_libraries(test_txn_buffer art_persistent pthread)
add_test(NAME test_txn_buffer COMMAND test_txn_buffer)

# Test: MVCC
add_executable(test_mvcc
    database/tests/test_mvcc.c
)
target_link_libraries(test_mvcc art_persistent pthread)
add_test(NAME test_mvcc COMMAND test_mvcc)

# Test: MVCC Edge Cases
add_executable(test_mvcc_edge_cases
    database/tests/test_mvcc_edge_cases.c
)
target_link_libraries(test_mvcc_edge_cases art_persistent pthread)
add_test(NAME test_mvcc_edge_cases COMMAND test_mvcc_edge_cases)

# Test: Snapshot Isolation
add_executable(test_snapshot_isolation
    database/tests/test_snapshot_isolation.c
)
target_link_libraries(test_snapshot_isolation art_persistent pthread)
add_test(NAME test_snapshot_isolation COMMAND test_snapshot_isolation)

# Test: Concurrent Snapshots
add_executable(test_concurrent_snapshots
    database/tests/test_concurrent_snapshots.c
)
target_link_libraries(test_concurrent_snapshots art_persistent pthread)
add_test(NAME test_concurrent_snapshots COMMAND test_concurrent_snapshots)

# Test: Fail-Fast Stress Test (60 seconds)
add_executable(test_fail_fast_stress
    database/tests/test_fail_fast_stress.c
)
target_link_libraries(test_fail_fast_stress art_persistent pthread)
add_test(NAME test_fail_fast_stress COMMAND test_fail_fast_stress)

# Test: Real-World Stress Test (Ethereum access pattern)
add_executable(test_realworld_stress
    database/tests/test_realworld_stress.c
)
target_link_libraries(test_realworld_stress art_persistent pthread)

# Test: Stress Loop (Ethereum workload, reproducible, looped)
add_executable(test_stress_loop
    database/tests/test_stress_loop.c
)
target_link_libraries(test_stress_loop art_persistent pthread)

# Test: Stress Grow (growing DB to target size, determinism checks)
add_executable(test_stress_grow
    database/tests/test_stress_grow.c
)
target_link_libraries(test_stress_grow art_persistent pthread)

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

# Test: Crash Recovery (mmap persistence)
add_executable(test_crash_recovery
    database/tests/test_crash_recovery.c
)
target_link_libraries(test_crash_recovery art_persistent pthread)
add_test(NAME test_crash_recovery COMMAND test_crash_recovery)

# Test: Checkpoint Manager
add_executable(test_checkpoint_manager
    database/tests/test_checkpoint_manager.c
)
target_link_libraries(test_checkpoint_manager art_persistent pthread)
add_test(NAME test_checkpoint_manager COMMAND test_checkpoint_manager)

# Test: Persistent ART Iterator
add_executable(test_data_art_iterator
    database/tests/test_data_art_iterator.c
)
target_link_libraries(test_data_art_iterator art_persistent pthread)
add_test(NAME test_data_art_iterator COMMAND test_data_art_iterator)

# Test: Database Backup (Export/Import)
add_executable(test_db_backup
    database/tests/test_db_backup.c
)
target_link_libraries(test_db_backup art_persistent pthread)
add_test(NAME test_db_backup COMMAND test_db_backup)

# Test: ART Determinism
add_executable(test_determinism
    database/tests/test_determinism.c
)
target_link_libraries(test_determinism art_persistent pthread)

# Test: Simple delete test
add_executable(test_delete_simple
    database/tests/test_delete_simple.c
)
target_link_libraries(test_delete_simple art_persistent pthread)

add_executable(test_delete_debug
    database/tests/test_delete_debug.c
)
target_link_libraries(test_delete_debug art_persistent pthread)

# Test: Deadlock detection (lock ordering)
add_executable(test_deadlock
    database/tests/test_deadlock.c
)
target_link_libraries(test_deadlock art_persistent pthread)
add_test(NAME test_deadlock COMMAND test_deadlock)

# Test: Incremental checkpoint (dirty page tracking)
add_executable(test_incremental_checkpoint
    database/tests/test_incremental_checkpoint.c
)
target_link_libraries(test_incremental_checkpoint art_persistent pthread)
add_test(NAME test_incremental_checkpoint COMMAND test_incremental_checkpoint)

# Benchmark: ART vs LMDB Comparison
find_library(LMDB_LIB lmdb)
find_path(LMDB_INCLUDE lmdb.h)

add_executable(benchmark_comparison
    database/tests/benchmark_comparison.c
)

if(LMDB_LIB AND LMDB_INCLUDE)
    target_include_directories(benchmark_comparison PRIVATE ${LMDB_INCLUDE})
    target_link_libraries(benchmark_comparison art_persistent ${LMDB_LIB} pthread)
    target_compile_definitions(benchmark_comparison PRIVATE HAVE_LMDB=1)
    message(STATUS "LMDB found — benchmark_comparison includes LMDB comparison")
else()
    target_link_libraries(benchmark_comparison art_persistent pthread)
    message(STATUS "LMDB not found — benchmark_comparison runs ART-only")
endif()

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
message(STATUS "")
message(STATUS "Libraries:")
message(STATUS "  - art_persistent (mmap-backed ART tree)")
message(STATUS "  - art_database (in-memory ART tree)")
message(STATUS "  - art_crc32 (CRC32 utility)")
message(STATUS "========================================")
message(STATUS "")
