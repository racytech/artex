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
    common/src/logger.c
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

# Account Encoding (shared module — compact variable-length account serialization)
add_library(account STATIC
    database/src/account.c
)
target_link_libraries(account common)

# EVM State (typed in-memory interface over state_db)
add_library(evm_state STATIC
    database/src/evm_state.c
)
target_link_libraries(evm_state state_db account common art_database intermediate_hashes)

# Intermediate Hashes (MPT commitment via flat hash storage)
add_library(intermediate_hashes STATIC
    database/src/intermediate_hashes.c
)
target_link_libraries(intermediate_hashes compact_art common)

# EVM (Ethereum Virtual Machine)
add_library(evm STATIC
    evm/src/evm.c
    evm/src/evm_stack.c
    evm/src/evm_memory.c
    evm/src/gas.c
    evm/src/interpreter.c
    evm/src/fork.c
    evm/src/transaction.c
    evm/src/opcodes/arithmetic.c
    evm/src/opcodes/comparison.c
    evm/src/opcodes/crypto.c
    evm/src/opcodes/stack.c
    evm/src/opcodes/memory.c
    evm/src/opcodes/logging.c
    evm/src/opcodes/storage.c
    evm/src/opcodes/environmental.c
    evm/src/opcodes/call.c
    evm/src/opcodes/control.c
    evm/src/opcodes/block.c
    evm/src/opcodes/create.c
)
target_include_directories(evm PUBLIC ${CMAKE_SOURCE_DIR}/evm/include)
target_link_libraries(evm evm_state common)

# VM Library (EOF-native, Verkle-targeted)
add_library(vm STATIC
    vm/src/eof.c
    vm/src/vm.c
    vm/src/vm_memory.c
    vm/src/gas.c
)
target_include_directories(vm PUBLIC ${CMAKE_SOURCE_DIR}/vm/include)
target_link_libraries(vm common)

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

# Scale Test: Data Layer (realistic Ethereum block simulation, long-running)
add_executable(test_scale_data_layer
    database/tests/test_scale_data_layer.c
)
target_link_libraries(test_scale_data_layer data_layer)

# Benchmark: MPT commitment (state root computation at scale)
add_executable(bench_mpt
    database/tests/bench_mpt.c
)
target_link_libraries(bench_mpt data_layer intermediate_hashes common)

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

# Test: Intermediate Hashes (MPT commitment)
add_executable(test_intermediate_hashes
    database/tests/test_intermediate_hashes.c
)
target_link_libraries(test_intermediate_hashes intermediate_hashes compact_art common)
add_test(NAME test_intermediate_hashes COMMAND test_intermediate_hashes)

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
target_link_libraries(test_mpt_cross_validation intermediate_hashes compact_art common)
add_test(NAME test_mpt_cross_validation
    COMMAND test_mpt_cross_validation
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Benchmark: uthash vs mem_art (cache performance comparison)
add_executable(bench_uthash_vs_art
    database/tests/bench_uthash_vs_art.c
)
target_link_libraries(bench_uthash_vs_art art_database)

# Test: EOF Validation (EOF container parsing + code validation)
add_executable(test_eof vm/tests/test_eof_validation.c)
target_link_libraries(test_eof vm common)
add_test(NAME test_eof COMMAND test_eof)

#==============================================================================
# Integration Tests (Ethereum test vectors — cJSON required)
#==============================================================================

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(CJSON QUIET libcjson)
endif()
if(NOT CJSON_FOUND)
    find_library(CJSON_LIBRARY NAMES cjson)
    find_path(CJSON_INCLUDE_DIR NAMES cjson/cJSON.h)
    if(CJSON_LIBRARY AND CJSON_INCLUDE_DIR)
        set(CJSON_FOUND TRUE)
        set(CJSON_LIBRARIES ${CJSON_LIBRARY})
        set(CJSON_INCLUDE_DIRS ${CJSON_INCLUDE_DIR})
    endif()
endif()

if(CJSON_FOUND)
    message(STATUS "Found cJSON: ${CJSON_LIBRARIES}")

    file(GLOB_RECURSE INTEGRATION_TEST_SOURCES "integration_tests/src/*.c")
    if(INTEGRATION_TEST_SOURCES)
        add_library(integration_tests STATIC ${INTEGRATION_TEST_SOURCES})
        target_include_directories(integration_tests PUBLIC
            integration_tests/include
            ${CJSON_INCLUDE_DIRS}
        )
        target_link_libraries(integration_tests PUBLIC
            evm
            evm_state
            common
            ${CJSON_LIBRARIES}
        )

        add_executable(test_parser_verify integration_tests/test_parser_verify.c)
        target_link_libraries(test_parser_verify integration_tests)

        add_executable(test_runner_example integration_tests/test_runner_example.c)
        target_link_libraries(test_runner_example integration_tests)

        add_executable(test_runner_batch integration_tests/test_runner_batch_main.c)
        target_link_libraries(test_runner_batch integration_tests)

        add_executable(test_runner_failfast integration_tests/test_runner_failfast_main.c)
        target_link_libraries(test_runner_failfast integration_tests)
    endif()

    # Test: EOF Osaka Vectors (official Ethereum EOF validation tests)
    add_executable(test_eof_osaka vm/tests/test_eof_osaka.c)
    target_include_directories(test_eof_osaka PRIVATE ${CJSON_INCLUDE_DIRS})
    target_link_libraries(test_eof_osaka vm common ${CJSON_LIBRARIES})
else()
    message(STATUS "cJSON not found — skipping integration tests (install: sudo apt-get install libcjson-dev)")
endif()

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
message(STATUS "  - intermediate_hashes (MPT commitment via flat hash storage)")
message(STATUS "  - vm (EOF-native VM for Verkle state)")
message(STATUS "========================================")
message(STATUS "")
