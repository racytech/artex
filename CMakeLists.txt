cmake_minimum_required(VERSION 3.15)

# Default to Release unless the caller asked for something else.
# The project runs 4x slower in the CMake-default empty/Debug mode, so
# we refuse to let it ship unoptimized by accident.
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

project(art_database C)

# Build options (override via -D flags, e.g. -DENABLE_HISTORY=ON)
option(ENABLE_EVM_TRACE "Enable EIP-3155 EVM execution tracing" OFF)
option(ENABLE_DEBUG "Enable debug instrumentation (g_trace_calls, state dump, --no-evict)" OFF)
option(ENABLE_HISTORY "Enable per-block state diff history tracking" OFF)

# Tunable parameters (override via -D flags)
if(NOT DEFINED CODE_CACHE_ENTRIES)
    set(CODE_CACHE_ENTRIES 32768)
endif()
if(NOT DEFINED CHECKPOINT_INTERVAL)
    set(CHECKPOINT_INTERVAL 256)
endif()

# Set C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -msse4.2")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -DDEBUG")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -DNDEBUG -flto=2 -march=native")


# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
    ${CMAKE_SOURCE_DIR}/evm/include
    ${CMAKE_SOURCE_DIR}/executor/include
    ${CMAKE_SOURCE_DIR}/third_party
    ${CMAKE_SOURCE_DIR}/third_party/blst/bindings
)

#==============================================================================
# Libraries — Database
#==============================================================================

# In-Memory ART Tree (general-purpose, variable-size keys/values)
add_library(art_database STATIC
    database/src/mem_art.c
)

# Disk Table (mmap-backed hash table optimized for pre-hashed keys)
add_library(disk_table STATIC database/src/disk_table.c)

# Code Store (content-addressed, append-only, deduplicated)
add_library(code_store STATIC
    database/src/code_store.c
)
target_compile_definitions(code_store PRIVATE CCACHE_DEFAULT_CAP=${CODE_CACHE_ENTRIES})
target_link_libraries(code_store disk_table pthread)

#==============================================================================
# Libraries — Common
#==============================================================================

# Common Utilities (keccak256, hash, rlp, bytes, uint256, address)
add_library(common STATIC
    common/src/keccak256.c
    common/src/hash.c
    common/src/rlp.c
    common/src/bytes.c
    common/src/uint256.c
    common/src/address.c
    common/src/logger.c
    common/src/snappy_decode.c
)
target_link_libraries(common snappy)

# Keccak AVX2 (4-way parallel)
add_library(keccak_avx2 STATIC common/src/keccak256_avx2.c)
target_include_directories(keccak_avx2 PUBLIC ${CMAKE_SOURCE_DIR}/common/include)
target_compile_options(keccak_avx2 PRIVATE -mavx2)

add_executable(bench_keccak_avx2 common/tests/bench_keccak_avx2.c)
target_link_libraries(bench_keccak_avx2 keccak_avx2 common)
target_compile_options(bench_keccak_avx2 PRIVATE -mavx2)

add_executable(test_keccak_avx2 common/tests/test_keccak_avx2.c)
target_link_libraries(test_keccak_avx2 keccak_avx2 common)
target_compile_options(test_keccak_avx2 PRIVATE -mavx2)
add_test(NAME test_keccak_avx2 COMMAND test_keccak_avx2)

# Keccak AVX-512 (8-way parallel)
add_library(keccak_avx512 STATIC common/src/keccak256_avx512.c)
target_include_directories(keccak_avx512 PUBLIC ${CMAKE_SOURCE_DIR}/common/include)
target_compile_options(keccak_avx512 PRIVATE -mavx512f -mavx512vl)

add_executable(bench_keccak_all common/tests/bench_keccak_all.c)
target_link_libraries(bench_keccak_all keccak_avx512 keccak_avx2 common)
target_compile_options(bench_keccak_all PRIVATE -mavx2 -mavx512f -mavx512vl)

add_executable(fuzz_keccak common/tests/fuzz_keccak.c)
target_link_libraries(fuzz_keccak keccak_avx512 keccak_avx2 common)
target_compile_options(fuzz_keccak PRIVATE -mavx2 -mavx512f -mavx512vl)

#==============================================================================
# Libraries — Crypto
#==============================================================================

# blst (BLS12-381 field arithmetic — pre-built static library)
add_library(blst STATIC IMPORTED)
set_target_properties(blst PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/third_party/blst/libblst.a
)

# mem_mpt (batch MPT root computation)
add_library(mem_mpt STATIC database/src/mem_mpt.c)
target_link_libraries(mem_mpt common)


#==============================================================================
# Libraries — EVM
#==============================================================================

# secp256k1 (ECDSA, ECDH — pre-built static library)
add_library(secp256k1 STATIC IMPORTED)
set_target_properties(secp256k1 PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/third_party/secp256k1/build/lib/libsecp256k1.a
)

# State (in-memory, no flat_state/flat_store)
add_library(state STATIC
    state/src/state.c
    state/src/evm_state.c
)
target_include_directories(state PUBLIC
    ${CMAKE_SOURCE_DIR}/state/include
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/evm/include
)
target_link_libraries(state art_database hashed_art storage_hart2 hart_pool code_store common)

# EVM (Ethereum Virtual Machine)
add_library(evm STATIC
    evm/src/evm.c
    evm/src/evm_tracer.c
    evm/src/evm_stack.c
    evm/src/evm_memory.c
    evm/src/gas.c
    evm/src/interpreter.c
    evm/src/fork.c
    evm/src/transaction.c
    evm/src/precompile.c
    evm/vendor/bn256.c
)
target_include_directories(evm PUBLIC
    ${CMAKE_SOURCE_DIR}/evm/include
    ${CMAKE_SOURCE_DIR}/evm/vendor
    ${CMAKE_SOURCE_DIR}/state/include
    ${CMAKE_SOURCE_DIR}/third_party/gmp/include
)
target_link_libraries(evm state art_database common ssl crypto secp256k1 blst code_store
    ${CMAKE_SOURCE_DIR}/third_party/gmp/lib/libgmp.a)
if(ENABLE_EVM_TRACE)
    target_compile_definitions(evm PUBLIC ENABLE_EVM_TRACE=1)
endif()
if(ENABLE_DEBUG)
    target_compile_definitions(evm PUBLIC ENABLE_DEBUG=1)
endif()
if(ENABLE_HISTORY)
    target_compile_definitions(evm PUBLIC ENABLE_HISTORY=1)
    target_compile_definitions(state PRIVATE ENABLE_HISTORY=1)
endif()

#==============================================================================
# Libraries — Block Executor
#==============================================================================

set(BLOCK_EXECUTOR_SOURCES
    executor/src/block.c
    executor/src/tx_decoder.c
    executor/src/tx_pipeline.c
    executor/src/tx_analysis.c
    executor/src/bytecode_scan.c
    executor/src/block_executor.c
    executor/src/dao_fork.c
)
if(ENABLE_HISTORY)
    list(APPEND BLOCK_EXECUTOR_SOURCES executor/src/state_history.c)
endif()
add_library(block_executor STATIC ${BLOCK_EXECUTOR_SOURCES})
target_include_directories(block_executor PUBLIC
    ${CMAKE_SOURCE_DIR}/executor/include
    ${CMAKE_SOURCE_DIR}/state/include
)
target_link_libraries(block_executor evm mem_mpt common secp256k1 pthread)

#==============================================================================
# Libraries — Era Reader (era1/era archive file readers)
#==============================================================================

add_library(era_reader STATIC
    era_reader/src/era1.c
    era_reader/src/era.c
)
target_include_directories(era_reader PUBLIC
    ${CMAKE_SOURCE_DIR}/era_reader/include
    ${CMAKE_SOURCE_DIR}/executor/include
    ${CMAKE_SOURCE_DIR}/state/include
)
target_link_libraries(era_reader common snappy)

#==============================================================================
# cJSON detection (used by libartex, chain_replay, integration tests)
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
else()
    message(FATAL_ERROR "cJSON not found. Install libcjson-dev.")
endif()

#==============================================================================
# Libraries — Public API (libartex)
#==============================================================================

add_library(artex STATIC lib/artex.c)
target_include_directories(artex PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/executor/include
    ${CMAKE_SOURCE_DIR}/evm/include
    ${CMAKE_SOURCE_DIR}/state/include
    ${CMAKE_SOURCE_DIR}/common/include
    ${CJSON_INCLUDE_DIRS}
)
target_link_libraries(artex block_executor evm state common ${CJSON_LIBRARIES})

# Shared library (libartex.so / libartex.dylib)
# Requires ALL dependencies (including third_party/secp256k1) built with -fPIC.
# Build with: cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON ..
if(CMAKE_POSITION_INDEPENDENT_CODE)
    add_library(artex_shared SHARED lib/artex.c)
    target_include_directories(artex_shared PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/executor/include
        ${CMAKE_SOURCE_DIR}/evm/include
        ${CMAKE_SOURCE_DIR}/state/include
        ${CMAKE_SOURCE_DIR}/common/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_link_libraries(artex_shared block_executor evm state common ${CJSON_LIBRARIES})
    target_compile_definitions(artex_shared PRIVATE RX_SHARED RX_BUILD)
    set_target_properties(artex_shared PROPERTIES
        OUTPUT_NAME artex
        VERSION 0.1.0
        SOVERSION 0
    )
endif()

# Install rules
include(GNUInstallDirs)
install(TARGETS artex
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(FILES include/artex.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
if(CMAKE_POSITION_INDEPENDENT_CODE)
    install(TARGETS artex_shared
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()

# pkg-config
configure_file(
    ${CMAKE_SOURCE_DIR}/artex.pc.in
    ${CMAKE_BINARY_DIR}/artex.pc
    @ONLY
)
install(FILES ${CMAKE_BINARY_DIR}/artex.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)

add_executable(test_artex lib/test_artex.c)
target_link_libraries(test_artex artex ${CJSON_LIBRARIES})

add_executable(test_hart_parallel database/tests/test_hart_parallel.c)
target_include_directories(test_hart_parallel PRIVATE
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(test_hart_parallel hashed_art mem_mpt common pthread)

add_executable(test_hart_invalidate_parallel database/tests/test_hart_invalidate_parallel.c)
target_include_directories(test_hart_invalidate_parallel PRIVATE
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(test_hart_invalidate_parallel hashed_art mem_mpt common pthread)
add_test(NAME test_hart_invalidate_parallel COMMAND test_hart_invalidate_parallel)

add_executable(bench_hart_scale database/tests/bench_hart_scale.c)
target_include_directories(bench_hart_scale PRIVATE
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(bench_hart_scale hashed_art common)

add_executable(bench_storage_hart_scale database/tests/bench_storage_hart_scale.c)
target_include_directories(bench_storage_hart_scale PRIVATE
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(bench_storage_hart_scale storage_hart common)

add_executable(test_pool_alloc database/tests/test_pool_alloc.c)
target_include_directories(test_pool_alloc PRIVATE
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(test_pool_alloc storage_hart common)

add_executable(test_tx_analysis executor/tests/test_tx_analysis.c)
target_include_directories(test_tx_analysis PRIVATE
    ${CMAKE_SOURCE_DIR}/executor/include
    ${CMAKE_SOURCE_DIR}/era_reader/include
    ${CMAKE_SOURCE_DIR}/evm/include
    ${CMAKE_SOURCE_DIR}/state/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(test_tx_analysis block_executor era_reader evm state common secp256k1_wrap snappy pthread)

add_executable(bench_parallel_decode executor/tests/bench_parallel_decode.c)
target_include_directories(bench_parallel_decode PRIVATE
    ${CMAKE_SOURCE_DIR}/executor/include
    ${CMAKE_SOURCE_DIR}/evm/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(bench_parallel_decode block_executor evm state common secp256k1_wrap pthread)

#==============================================================================
# Libraries — Crypto utilities
#==============================================================================

# HKDF-SHA256 (RFC 5869 key derivation, uses BLST for SHA-256)
add_library(hkdf STATIC
    common/src/hkdf.c
)
target_link_libraries(hkdf blst)

# secp256k1 wrapper (thin C wrapper over libsecp256k1)
add_library(secp256k1_wrap STATIC
    common/src/secp256k1_wrap.c
)
target_include_directories(secp256k1_wrap PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/secp256k1/include
)
target_link_libraries(secp256k1_wrap secp256k1)

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

add_executable(bench_mem_art_storage
    database/tests/bench_mem_art_storage.c
)
target_link_libraries(bench_mem_art_storage art_database common)

add_executable(bench_mem_art_cache
    database/tests/bench_mem_art_cache.c
)
target_link_libraries(bench_mem_art_cache art_database common)

# Test: Code Store (content-addressed bytecode storage)
add_executable(test_code_store
    database/tests/test_code_store.c
)
target_link_libraries(test_code_store code_store)
add_test(NAME test_code_store COMMAND test_code_store)

# Test: EVM state management audit
add_executable(test_evm_state_audit
    evm/tests/test_evm_state_audit.c
)
target_link_libraries(test_evm_state_audit evm)
add_test(NAME test_evm_state_audit COMMAND test_evm_state_audit)

add_executable(test_state_root state/tests/test_state_root.c)
target_link_libraries(test_state_root state evm)
add_test(NAME test_state_root COMMAND test_state_root)

add_library(hashed_art STATIC database/src/hashed_art.c)
target_include_directories(hashed_art PUBLIC ${CMAKE_SOURCE_DIR}/database/include ${CMAKE_SOURCE_DIR}/common/include)
target_link_libraries(hashed_art common)

add_library(storage_hart STATIC database/src/storage_hart.c)
target_include_directories(storage_hart PUBLIC ${CMAKE_SOURCE_DIR}/database/include ${CMAKE_SOURCE_DIR}/common/include)
target_link_libraries(storage_hart common)

add_library(hart_pool STATIC database/src/hart_pool.c)
target_include_directories(hart_pool PUBLIC ${CMAKE_SOURCE_DIR}/database/include)

add_executable(test_hart_pool database/tests/test_hart_pool.c)
target_link_libraries(test_hart_pool hart_pool)

add_library(storage_hart2 STATIC database/src/storage_hart2.c)
target_include_directories(storage_hart2 PUBLIC
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(storage_hart2 hart_pool common)

add_executable(test_hashed_art database/tests/test_hashed_art.c)
target_link_libraries(test_hashed_art hashed_art mem_mpt common)

add_executable(test_hart_scale database/tests/test_hart_scale.c)
target_link_libraries(test_hart_scale hashed_art common)

add_executable(bench_hart_root database/tests/bench_hart_root.c)
target_link_libraries(bench_hart_root hashed_art common)

add_executable(bench_hart_profile database/tests/bench_hart_profile.c)
target_link_libraries(bench_hart_profile hashed_art common)

add_executable(hart_determinism_test tools/hart_determinism_test.c)
target_link_libraries(hart_determinism_test hashed_art storage_hart2 hart_pool common)


add_executable(test_state_save_load state/tests/test_state_save_load.c)
target_link_libraries(test_state_save_load state evm)
target_include_directories(test_state_save_load PRIVATE ${CMAKE_SOURCE_DIR}/state/include)
add_test(NAME test_state_save_load COMMAND test_state_save_load)

add_executable(test_finalize_bug state/tests/test_finalize_bug.c)
target_link_libraries(test_finalize_bug state evm)
target_include_directories(test_finalize_bug PRIVATE ${CMAKE_SOURCE_DIR}/state/include)

add_executable(test_prune_retouch state/tests/test_prune_retouch.c)
target_link_libraries(test_prune_retouch state evm)
target_include_directories(test_prune_retouch PRIVATE ${CMAKE_SOURCE_DIR}/state/include)

add_executable(test_incremental_root state/tests/test_incremental_root.c)
target_link_libraries(test_incremental_root state evm)
target_include_directories(test_incremental_root PRIVATE ${CMAKE_SOURCE_DIR}/state/include)

add_executable(test_state_compaction state/tests/test_state_compaction.c)
target_link_libraries(test_state_compaction state evm)
target_include_directories(test_state_compaction PRIVATE ${CMAKE_SOURCE_DIR}/state/include)

add_executable(test_state_dup1 state/tests/test_state_dup1.c)
target_link_libraries(test_state_dup1 evm)
target_include_directories(test_state_dup1 PRIVATE ${CMAKE_SOURCE_DIR}/state/include)

# Shared library for Python mpt_fuzz.py differential fuzzing
add_library(mpt_fuzz SHARED tools/mpt_fuzz_lib.c)
target_include_directories(mpt_fuzz PRIVATE
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(mpt_fuzz mem_mpt hashed_art common)

# test_eviction disabled — eviction system replaced by storage_hart pool
# add_executable(test_eviction state/tests/test_eviction.c)
# target_link_libraries(test_eviction state evm)
# target_include_directories(test_eviction PRIVATE ${CMAKE_SOURCE_DIR}/state/include)
# add_test(NAME test_eviction COMMAND test_eviction)

add_executable(test_block_executor
    executor/tests/test_block_executor.c
)
target_link_libraries(test_block_executor block_executor evm common)
add_test(NAME test_block_executor COMMAND test_block_executor)

if(ENABLE_HISTORY)
    add_executable(test_state_history executor/tests/test_state_history.c)
    target_link_libraries(test_state_history block_executor evm common)
    add_test(NAME test_state_history COMMAND test_state_history)

    add_executable(test_apply_diff_parity executor/tests/test_apply_diff_parity.c)
    target_link_libraries(test_apply_diff_parity block_executor evm common)
    add_test(NAME test_apply_diff_parity COMMAND test_apply_diff_parity)

    add_executable(history_dump tools/history_dump.c)
    target_link_libraries(history_dump block_executor evm common)

    # state_reconstruct — disabled (needs rewrite for state_v2)
    # add_executable(state_reconstruct tools/state_reconstruct.c)
    # target_link_libraries(state_reconstruct block_executor evm common ${CJSON_LIBRARIES})

endif()

add_executable(test_uint256_div common/tests/test_uint256_div.c)
target_link_libraries(test_uint256_div common)
add_test(NAME test_uint256_div COMMAND test_uint256_div)

add_executable(test_uint256_ops common/tests/test_uint256_ops.c)
target_link_libraries(test_uint256_ops common)
add_test(NAME test_uint256_ops COMMAND test_uint256_ops)

add_executable(test_keccak256 common/tests/test_keccak256.c)
target_link_libraries(test_keccak256 common)
add_test(NAME test_keccak256 COMMAND test_keccak256)

add_executable(test_byte_conv common/tests/test_byte_conv.c)
target_link_libraries(test_byte_conv common)
add_test(NAME test_byte_conv COMMAND test_byte_conv)

if(CJSON_FOUND)
    # Sync engine (reusable block execution + validation + checkpointing)
    add_library(sync STATIC sync/src/sync.c)
    target_include_directories(sync PUBLIC ${CMAKE_SOURCE_DIR}/sync/include)
    target_include_directories(sync PRIVATE ${CJSON_INCLUDE_DIRS})
    set(SYNC_LIBS block_executor evm common ${CJSON_LIBRARIES} code_store)
    target_link_libraries(sync ${SYNC_LIBS})

    # Chain replay tool
    add_executable(chain_replay tools/chain_replay.c)
    target_compile_definitions(chain_replay PRIVATE
        CHECKPOINT_INTERVAL=${CHECKPOINT_INTERVAL})
    target_include_directories(chain_replay PRIVATE ${CJSON_INCLUDE_DIRS})
    target_link_libraries(chain_replay sync era_reader ${CJSON_LIBRARIES})

    # History query tool — disabled: state_history module not yet implemented
    # add_executable(history_query tools/history_query.c)
    # target_link_libraries(history_query block_executor evm common)

    # Rebuild code_store index with proper sizing
    add_executable(rebuild_code_idx tools/rebuild_code_idx.c)
    target_include_directories(rebuild_code_idx PRIVATE ${CMAKE_SOURCE_DIR}/database/include)
    target_link_libraries(rebuild_code_idx disk_table)

    # Era file parsing verification tool
    add_executable(test_era_parse tools/test_era_parse.c)
    target_include_directories(test_era_parse PRIVATE
        ${CMAKE_SOURCE_DIR}/era_reader/include
        ${CMAKE_SOURCE_DIR}/executor/include
        ${CMAKE_SOURCE_DIR}/common/include)
    target_link_libraries(test_era_parse era_reader block_executor evm common)

    #==============================================================================
    # Integration Tests (Ethereum test vectors)
    #==============================================================================

    file(GLOB_RECURSE INTEGRATION_TEST_SOURCES "integration_tests/src/*.c")
    if(INTEGRATION_TEST_SOURCES)
        add_library(integration_tests STATIC ${INTEGRATION_TEST_SOURCES})
        target_include_directories(integration_tests PUBLIC
            integration_tests/include
            executor/include
            state/include
            ${CJSON_INCLUDE_DIRS}
        )
        target_link_libraries(integration_tests PUBLIC evm block_executor common ${CJSON_LIBRARIES})

        add_executable(test_parser_verify integration_tests/test_parser_verify.c)
        target_link_libraries(test_parser_verify integration_tests)

        add_executable(test_runner_example integration_tests/test_runner_example.c)
        target_link_libraries(test_runner_example integration_tests)

        add_executable(test_runner_batch integration_tests/test_runner_batch_main.c)
        target_link_libraries(test_runner_batch integration_tests)

        add_executable(test_runner_failfast integration_tests/test_runner_failfast_main.c)
        target_link_libraries(test_runner_failfast integration_tests)

        # EVM t8n tool (state transition, geth-compatible)
        add_executable(evm_t8n tools/evm_t8n.c)
        target_include_directories(evm_t8n PRIVATE
        
            ${CMAKE_SOURCE_DIR}/third_party/secp256k1/include
        )
        target_link_libraries(evm_t8n integration_tests secp256k1_wrap)

        # EVM statetest runner (geth/evmone-compatible, for GoEVMLab runtest)
        add_executable(evm_statetest tools/evm_statetest.c)
        target_include_directories(evm_statetest PRIVATE
        
            ${CMAKE_SOURCE_DIR}/third_party/secp256k1/include
        )
        target_link_libraries(evm_statetest integration_tests secp256k1_wrap)
    endif()
else()
    message(STATUS "cJSON not found — skipping integration tests (install: sudo apt-get install libcjson-dev)")
endif()

#==============================================================================
# Build Summary
#==============================================================================

message(STATUS "")
message(STATUS "========================================")
message(STATUS "ART Configuration")
message(STATUS "========================================")
message(STATUS "Build type:        ${CMAKE_BUILD_TYPE}")
message(STATUS "C Compiler:        ${CMAKE_C_COMPILER}")
message(STATUS "C Flags:           ${CMAKE_C_FLAGS}")
message(STATUS "State history:     ${ENABLE_HISTORY}")
message(STATUS "")
message(STATUS "Tuning:")
message(STATUS "  Code cache:      ${CODE_CACHE_ENTRIES} entries")
message(STATUS "  Checkpoint every:${CHECKPOINT_INTERVAL} blocks")
message(STATUS "")
message(STATUS "Libraries:")
message(STATUS "  - art_database (in-memory ART tree)")
message(STATUS "  - code_store (content-addressed bytecode storage)")
message(STATUS "  - evm (Ethereum Virtual Machine)")
message(STATUS "  - block_executor (block decode + execution)")
message(STATUS "  - sync (block execution + validation + checkpointing)")
message(STATUS "========================================")
message(STATUS "")
