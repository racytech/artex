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
    ${CMAKE_SOURCE_DIR}/state/include
    ${CMAKE_SOURCE_DIR}/third_party
    ${CMAKE_SOURCE_DIR}/third_party/blst/bindings
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

# ART Store (compact_art-indexed persistent record store)
add_library(art_store STATIC
    database/src/art_store.c
)
target_link_libraries(art_store compact_art)

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

# Intermediate Hashes (Erigon-style MPT commitment via flat hash storage)
add_library(intermediate_hashes STATIC
    database/src/intermediate_hashes.c
)
target_link_libraries(intermediate_hashes compact_art common)

# IH Hash Store (intermediate hashes backed by hash_store — zero-RAM, persistent)
add_library(ih_hash_store STATIC
    database/src/ih_hash_store.c
)
target_link_libraries(ih_hash_store hash_store common)

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

# State hash store (self-contained copy for state/ independence from database/)
add_library(state_hash_store STATIC
    state/src/hash_store.c
)

# Verkle commitment store (hash_store-backed commitment persistence)
add_library(verkle_commit_store STATIC
    state/src/verkle_commit_store.c
)
target_link_libraries(verkle_commit_store verkle banderwagon state_hash_store)

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
target_link_libraries(verkle_flat verkle_commit_store state_hash_store pedersen banderwagon)

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

# Stress Test: ART Store (correctness + persistence at scale)
add_executable(test_stress_art_store
    database/tests/test_stress_art_store.c
)
target_link_libraries(test_stress_art_store art_store)

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
target_link_libraries(test_mpt_cross_validation mpt ih_hash_store intermediate_hashes common)
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

# Test: Intermediate Hashes (ih_build + ih_update correctness)
add_executable(test_intermediate_hashes
    database/tests/test_intermediate_hashes.c
)
target_link_libraries(test_intermediate_hashes intermediate_hashes)
add_test(NAME test_intermediate_hashes COMMAND test_intermediate_hashes)

# Test: IH Hash Store (hash_store-backed intermediate hashes)
add_executable(test_ih_hash_store
    database/tests/test_ih_hash_store.c
)
target_link_libraries(test_ih_hash_store ih_hash_store intermediate_hashes)
add_test(NAME test_ih_hash_store COMMAND test_ih_hash_store)

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

# Test: Verkle commitment store (hash_store-backed persistence)
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
message(STATUS "  - common (keccak256, hash, rlp, bytes)")
message(STATUS "  - data_layer (state store + code store + write buffer + merge)")
message(STATUS "  - state_db (account state + contract storage wrapper)")
message(STATUS "  - account (compact variable-length account encoding)")
message(STATUS "  - evm_state (typed in-memory EVM state interface)")
message(STATUS "  - mpt (persistent MPT trie for state root computation)")
message(STATUS "  - hash_store (zero-RAM file-backed hash table for 64B keys)")
message(STATUS "========================================")
message(STATUS "")
