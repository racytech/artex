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

# Common Utilities (keccak256, hash, rlp, bytes)
add_library(common STATIC
    common/src/keccak256.c
    common/src/hash.c
    common/src/rlp.c
    common/src/bytes.c
)

# Data Layer (state store + code store + checkpoint + write buffer + index merge)
add_library(data_layer STATIC
    database/src/state_store.c
    database/src/code_store.c
    database/src/checkpoint.c
    database/src/data_layer.c
)
target_link_libraries(data_layer compact_art art_database)

# State DB (account state + contract storage — dual data_layer wrapper)
add_library(state_db STATIC
    database/src/state_db.c
)
target_link_libraries(state_db data_layer)

# Intermediate Hashes (MPT commitment via flat hash storage)
add_library(intermediate_hashes STATIC
    database/src/intermediate_hashes.c
)
target_link_libraries(intermediate_hashes compact_art common)

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

# PoC: Account Encoding (compact variable-length account serialization)
add_executable(test_account_encoding
    database/tests/test_account_encoding.c
)

# PoC: Data Layer Integration Test (write buffer + index + state.dat)
add_executable(test_data_layer_poc
    database/tests/test_data_layer_poc.c
)
target_link_libraries(test_data_layer_poc compact_art art_database)

# Test: Data Layer Integration Stress (state + code + checkpoint + recovery)
add_executable(test_stress_data_layer
    database/tests/test_stress_data_layer.c
)
target_link_libraries(test_stress_data_layer data_layer)

# Test: State DB (account state + contract storage + checkpoint)
add_executable(test_state_db
    database/tests/test_state_db.c
)
target_link_libraries(test_state_db state_db)
add_test(NAME test_state_db COMMAND test_state_db)

# Test: Intermediate Hashes (MPT commitment)
add_executable(test_intermediate_hashes
    database/tests/test_intermediate_hashes.c
)
target_link_libraries(test_intermediate_hashes intermediate_hashes compact_art common)
add_test(NAME test_intermediate_hashes COMMAND test_intermediate_hashes)

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
message(STATUS "  - intermediate_hashes (MPT commitment via flat hash storage)")
message(STATUS "========================================")
message(STATUS "")
