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
    ${CMAKE_SOURCE_DIR}/common/include
    ${CMAKE_SOURCE_DIR}/third_party
)

#==============================================================================
# Libraries
#==============================================================================

# In-Memory ART Tree (general-purpose, variable-size keys/values)
add_library(art_database STATIC
    database/src/mem_art.c
)

# Compact ART — Space-Efficient Index (fixed-size keys/values, arena-allocated)
add_library(compact_art STATIC
    database/src/compact_art.c
)

# Common Utilities (keccak256, hash, rlp, bytes, uint256, address)
add_library(common STATIC
    common/src/keccak256.c
    common/src/hash.c
    common/src/rlp.c
    common/src/bytes.c
    common/src/uint256.c
    common/src/address.c
)

# Data Layer (hash_store + code store — direct writes, no buffer)
add_library(data_layer STATIC
    database/src/code_store.c
    database/src/data_layer.c
)
target_link_libraries(data_layer hash_store)

# State DB (account state + contract storage — dual data_layer wrapper)
add_library(state_db STATIC
    database/src/state_db.c
)
target_link_libraries(state_db data_layer pthread)

# Account Encoding (shared module — compact variable-length account serialization)
add_library(account STATIC
    database/src/account.c
)
target_link_libraries(account common)

# EVM State (typed in-memory interface over state_db)
add_library(evm_state STATIC
    database/src/evm_state.c
)
target_link_libraries(evm_state state_db account common art_database mpt)

# Hash Store (zero-RAM file-backed hash table for 64B keys)
add_library(hash_store STATIC
    database/src/hash_store.c
)

# MPT Trie (stripped-down nibble trie for MPT hash computation)
add_library(mpt STATIC
    database/src/mpt.c
)
target_link_libraries(mpt common)

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

# Test: compact_art stress (correctness at scale)
add_executable(test_stress_compact_art
    database/tests/test_stress_compact_art.c
)
target_link_libraries(test_stress_compact_art compact_art)

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

# Test: Data Layer Module (library API)
add_executable(test_data_layer
    database/tests/test_data_layer.c
)
target_link_libraries(test_data_layer data_layer)

# Test: Code Store (code.dat + dedup + mixed state/code)
add_executable(test_code_store
    database/tests/test_code_store.c
)
target_link_libraries(test_code_store data_layer)

# Test: Checkpoint & Recovery (index.dat serialization + CRC)
add_executable(test_checkpoint
    database/tests/test_checkpoint.c
)
target_link_libraries(test_checkpoint data_layer)

# Test: Data Layer Integration Stress (state + code + checkpoint + recovery)
add_executable(test_stress_data_layer
    database/tests/test_stress_data_layer.c
)
target_link_libraries(test_stress_data_layer data_layer)

# Scale Test: Data Layer (realistic Ethereum block simulation, long-running)
add_executable(test_scale_data_layer
    database/tests/test_scale_data_layer.c
)
target_link_libraries(test_scale_data_layer data_layer)

# Test: State DB (account state + contract storage + checkpoint)
add_executable(test_state_db
    database/tests/test_state_db.c
)
target_link_libraries(test_state_db state_db)
add_test(NAME test_state_db COMMAND test_state_db)

# Test: EVM State (typed interface over state_db)
add_executable(test_evm_state
    database/tests/test_evm_state.c
)
target_link_libraries(test_evm_state evm_state)
add_test(NAME test_evm_state COMMAND test_evm_state)

# Scale Test: Full-stack EVM (evm_state → sdb → hash_store)
add_executable(test_scale_evm
    database/tests/test_scale_evm.c
)
target_link_libraries(test_scale_evm evm_state)

# Test: Compact ART Iterator Seek (lower-bound)
add_executable(test_compact_art_seek
    database/tests/test_compact_art_seek.c
)
target_link_libraries(test_compact_art_seek compact_art)
add_test(NAME test_compact_art_seek COMMAND test_compact_art_seek)

# Test: MPT Cross-Validation (Python HexaryTrie reference)
add_executable(test_mpt_cross_validation
    database/tests/test_mpt_cross_validation.c
)
target_link_libraries(test_mpt_cross_validation mpt common)
add_test(NAME test_mpt_cross_validation
    COMMAND test_mpt_cross_validation
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Benchmark: uthash vs mem_art (cache performance comparison)
add_executable(bench_uthash_vs_art
    database/tests/bench_uthash_vs_art.c
)
target_link_libraries(bench_uthash_vs_art art_database)

# Benchmark: dual compact_art RSS (accounts 32B + storage 64B)
add_executable(bench_dual_index
    database/tests/bench_dual_index.c
)
target_link_libraries(bench_dual_index compact_art)

# Benchmark: hash_store vs compact_art (64B keys, zero-RAM comparison)
add_executable(bench_hash_store
    database/tests/bench_hash_store.c
)
target_link_libraries(bench_hash_store hash_store compact_art)

# Test: MPT Trie (correctness tests)
add_executable(test_mpt
    database/tests/test_mpt.c
)
target_link_libraries(test_mpt mpt common)
add_test(NAME test_mpt COMMAND test_mpt)

# Scale Test: MPT Trie (incremental blocks, mixed ops, persistence, dirty stress)
add_executable(test_scale_mpt
    database/tests/test_scale_mpt.c
)
target_link_libraries(test_scale_mpt mpt common)

# Benchmark: mpt_trie per-block commitment
add_executable(bench_mpt_trie
    database/tests/bench_mpt_trie.c
)
target_link_libraries(bench_mpt_trie mpt common)

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
message(STATUS "  - common (keccak256, hash, rlp, bytes)")
message(STATUS "  - data_layer (state store + code store + write buffer + merge)")
message(STATUS "  - state_db (account state + contract storage wrapper)")
message(STATUS "  - account (compact variable-length account encoding)")
message(STATUS "  - evm_state (typed in-memory EVM state interface)")
message(STATUS "  - mpt (persistent MPT trie for state root computation)")
message(STATUS "  - hash_store (zero-RAM file-backed hash table for 64B keys)")
message(STATUS "========================================")
message(STATUS "")
