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
    ${CMAKE_SOURCE_DIR}/state/include
    ${CMAKE_SOURCE_DIR}/third_party/blst/bindings
)

#==============================================================================
# Libraries — Database
#==============================================================================

# In-Memory ART Tree (general-purpose, variable-size keys/values)
add_library(art_database STATIC
    database/src/mem_art.c
)

# Compact ART — Space-Efficient Index (fixed-size keys/values, arena-allocated)
add_library(compact_art STATIC
    database/src/compact_art.c
)

# ART Store (compact_art-indexed persistent record store)
add_library(art_store STATIC
    database/src/art_store.c
)
target_link_libraries(art_store compact_art)

# Disk Hash (zero-RAM disk-backed hash table, pread/pwrite)
add_library(disk_hash STATIC
    database/src/disk_hash.c
)
target_link_libraries(disk_hash pthread)

# Code Store (content-addressed, append-only, deduplicated)
add_library(code_store STATIC
    database/src/code_store.c
)
target_link_libraries(code_store disk_hash pthread)

#==============================================================================
# Libraries — Verkle State
#==============================================================================

# blst (BLS12-381 field arithmetic — pre-built static library)
add_library(blst STATIC IMPORTED)
set_target_properties(blst PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/third_party/blst/libblst.a
)

# Banderwagon curve arithmetic (twisted Edwards over blst_fr)
add_library(banderwagon STATIC
    state/src/banderwagon.c
)
target_link_libraries(banderwagon blst)

# Pedersen commitment (CRS + vector commitment over banderwagon)
add_library(pedersen STATIC
    state/src/pedersen.c
)
target_link_libraries(pedersen banderwagon blst)

# Verkle tree (in-memory width-256 trie with Pedersen commitments)
add_library(verkle STATIC
    state/src/verkle.c
)
target_link_libraries(verkle pedersen banderwagon)

# Verkle key derivation (pedersen hash for address/storage → tree key mapping)
add_library(verkle_key STATIC
    state/src/verkle_key.c
)
target_link_libraries(verkle_key pedersen)

# Verkle state (typed account/storage interface over verkle tree)
add_library(verkle_state STATIC
    state/src/verkle_state.c
)
target_link_libraries(verkle_state verkle verkle_key verkle_flat)

# Verkle commitment store (art_store-backed commitment persistence)
add_library(verkle_commit_store STATIC
    state/src/verkle_commit_store.c
)
target_link_libraries(verkle_commit_store verkle banderwagon art_store)

# Verkle snapshot (binary tree serialization/deserialization)
add_library(verkle_snapshot STATIC
    state/src/verkle_snapshot.c
)
target_link_libraries(verkle_snapshot verkle banderwagon)

# Verkle journal (block revert + crash recovery + background checkpoint)
add_library(verkle_journal STATIC
    state/src/verkle_journal.c
)
target_link_libraries(verkle_journal verkle verkle_snapshot banderwagon)

# Verkle flat updater (disk-backed commitment updates, O(block) RAM)
add_library(verkle_flat STATIC
    state/src/verkle_flat.c
)
target_link_libraries(verkle_flat verkle_commit_store art_store pedersen banderwagon)

#==============================================================================
# Tests — Database
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

# Test: compact_art stress (correctness at scale)
add_executable(test_stress_compact_art
    database/tests/test_stress_compact_art.c
)
target_link_libraries(test_stress_compact_art compact_art)

# Test: Compact ART Iterator Seek (lower-bound)
add_executable(test_compact_art_seek
    database/tests/test_compact_art_seek.c
)
target_link_libraries(test_compact_art_seek compact_art)
add_test(NAME test_compact_art_seek COMMAND test_compact_art_seek)

# Benchmark: compact_art vs mem_art (memory comparison)
add_executable(bench_compact_art_index
    database/tests/bench_compact_art_index.c
)
target_link_libraries(bench_compact_art_index compact_art art_database)

# Benchmark: per-block merge latency at scale
add_executable(bench_per_block_merge
    database/tests/bench_per_block_merge.c
)
target_link_libraries(bench_per_block_merge compact_art art_database)

# Benchmark: dual compact_art RSS (accounts 32B + storage 64B)
add_executable(bench_dual_index
    database/tests/bench_dual_index.c
)
target_link_libraries(bench_dual_index compact_art)

# Benchmark: art_store scale (RAM + disk + throughput at mainnet scale)
add_executable(bench_art_store_scale
    database/tests/bench_art_store_scale.c
)
target_link_libraries(bench_art_store_scale art_store)

# Stress Test: ART Store (correctness + persistence at scale)
add_executable(test_stress_art_store
    database/tests/test_stress_art_store.c
)
target_link_libraries(test_stress_art_store art_store)

# Test: Disk Hash (zero-RAM disk-backed hash table)
add_executable(test_disk_hash
    database/tests/test_disk_hash.c
)
target_link_libraries(test_disk_hash disk_hash)
add_test(NAME test_disk_hash COMMAND test_disk_hash)

# Stress Test: Disk Hash (correctness at scale, fail-fast)
add_executable(test_stress_disk_hash
    database/tests/test_stress_disk_hash.c
)
target_link_libraries(test_stress_disk_hash disk_hash)

# Crash Simulation Test: Disk Hash (fork + kill -9 + recovery)
add_executable(test_crash_disk_hash
    database/tests/test_crash_disk_hash.c
)
target_link_libraries(test_crash_disk_hash disk_hash)

# Multi-Threaded Stress Test: Disk Hash (1 writer + N readers)
add_executable(test_mt_disk_hash
    database/tests/test_mt_disk_hash.c
)
target_link_libraries(test_mt_disk_hash disk_hash pthread)

# Test: Code Store (content-addressed bytecode storage)
add_executable(test_code_store
    database/tests/test_code_store.c
)
target_link_libraries(test_code_store code_store)
add_test(NAME test_code_store COMMAND test_code_store)

#==============================================================================
# Tests — Verkle State
#==============================================================================

# Test: Banderwagon curve arithmetic (twisted Edwards + blst)
add_executable(test_banderwagon
    state/tests/test_banderwagon.c
)
target_link_libraries(test_banderwagon banderwagon)
add_test(NAME test_banderwagon COMMAND test_banderwagon)

# Test: Pedersen commitment (CRS generation + commit/update)
add_executable(test_pedersen
    state/tests/test_pedersen.c
)
target_link_libraries(test_pedersen pedersen)
add_test(NAME test_pedersen COMMAND test_pedersen)

# Test: Verkle tree (in-memory trie with Pedersen commitments)
add_executable(test_verkle
    state/tests/test_verkle.c
)
target_link_libraries(test_verkle verkle)
add_test(NAME test_verkle COMMAND test_verkle)

# Test: Verkle key derivation (pedersen hash → tree key mapping)
add_executable(test_verkle_key
    state/tests/test_verkle_key.c
)
target_link_libraries(test_verkle_key verkle_key)
add_test(NAME test_verkle_key COMMAND test_verkle_key)

# Test: Verkle state (typed account/storage interface)
add_executable(test_verkle_state
    state/tests/test_verkle_state.c
)
target_link_libraries(test_verkle_state verkle_state)
add_test(NAME test_verkle_state COMMAND test_verkle_state)

# Test: Verkle commitment store (art_store-backed persistence)
add_executable(test_verkle_commit_store
    state/tests/test_verkle_commit_store.c
)
target_link_libraries(test_verkle_commit_store verkle_commit_store verkle)
add_test(NAME test_verkle_commit_store COMMAND test_verkle_commit_store)

# Test: Verkle snapshot (binary tree serialization/deserialization)
add_executable(test_verkle_snapshot
    state/tests/test_verkle_snapshot.c
)
target_link_libraries(test_verkle_snapshot verkle_snapshot verkle_state)
add_test(NAME test_verkle_snapshot COMMAND test_verkle_snapshot)

# Test: Verkle journal (block revert + crash recovery + checkpoint)
add_executable(test_verkle_journal
    state/tests/test_verkle_journal.c
)
target_link_libraries(test_verkle_journal verkle_journal verkle_state)
add_test(NAME test_verkle_journal COMMAND test_verkle_journal)

# Benchmark: Verkle state scale (block simulation)
add_executable(bench_verkle_state
    state/tests/bench_verkle_state.c
)
target_link_libraries(bench_verkle_state verkle_journal verkle_state verkle_snapshot)

# Test: Verkle flat updater (disk-backed commitment updates)
add_executable(test_verkle_flat
    state/tests/test_verkle_flat.c
)
target_link_libraries(test_verkle_flat verkle_flat verkle)
add_test(NAME test_verkle_flat COMMAND test_verkle_flat)

# Stress Test: Verkle flat updater (cross-validated, fail-fast)
add_executable(test_stress_verkle_flat
    state/tests/test_stress_verkle_flat.c
)
target_link_libraries(test_stress_verkle_flat verkle_flat verkle verkle_commit_store)

# Benchmark: Verkle flat scale (disk-backed block simulation)
add_executable(bench_verkle_flat
    state/tests/bench_verkle_flat.c
)
target_link_libraries(bench_verkle_flat verkle_state)

# Test: Verkle cross-validation (go-verkle reference vectors)
add_executable(test_verkle_cross_validation
    state/tests/test_verkle_cross_validation.c
)
target_link_libraries(test_verkle_cross_validation verkle_flat verkle)
add_test(NAME test_verkle_cross_validation
    COMMAND test_verkle_cross_validation
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

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
message(STATUS "  - art_store (ART-indexed persistent record store)")
message(STATUS "========================================")
message(STATUS "")
