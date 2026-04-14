cmake_minimum_required(VERSION 3.15)

# Load project config if present (avoids -D flags on every cmake invocation)
if(EXISTS "${CMAKE_SOURCE_DIR}/config.cmake")
    include("${CMAKE_SOURCE_DIR}/config.cmake")
endif()

project(art_database C)

# Build options (overridable via config.cmake or -D flags)
option(ENABLE_EVM_TRACE "Enable EIP-3155 EVM execution tracing" OFF)
option(ENABLE_DEBUG "Enable debug instrumentation (g_trace_calls, state dump, --no-evict)" OFF)
option(ENABLE_HISTORY "Enable per-block state diff history tracking" OFF)

# Tunable parameters (overridable via config.cmake or -D flags)
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
target_link_libraries(state art_database hashed_art storage_hart code_store common)

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
    evm/vendor/mini-gmp.c
    evm/vendor/bn256.c
)
target_include_directories(evm PUBLIC
    ${CMAKE_SOURCE_DIR}/evm/include
    ${CMAKE_SOURCE_DIR}/evm/vendor
    ${CMAKE_SOURCE_DIR}/state/include
)
target_link_libraries(evm state art_database common ssl crypto secp256k1 blst code_store)
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

add_executable(test_artex lib/test_artex.c)
target_link_libraries(test_artex artex)

add_executable(test_hart_parallel database/tests/test_hart_parallel.c)
target_include_directories(test_hart_parallel PRIVATE
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
)
target_link_libraries(test_hart_parallel hashed_art mem_mpt common pthread)

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

add_executable(test_hashed_art database/tests/test_hashed_art.c)
target_link_libraries(test_hashed_art hashed_art mem_mpt common)

add_executable(test_hart_scale database/tests/test_hart_scale.c)
target_link_libraries(test_hart_scale hashed_art common)

add_executable(bench_hart_root database/tests/bench_hart_root.c)
target_link_libraries(bench_hart_root hashed_art common)

add_executable(bench_hart_profile database/tests/bench_hart_profile.c)
target_link_libraries(bench_hart_profile hashed_art common)


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

#==============================================================================
# Libraries — Engine API (JWT — no cJSON dependency)
#==============================================================================

# Engine JWT (HS256 authentication for Engine API)
add_library(engine_jwt STATIC
    engine/src/engine_jwt.c
)
target_include_directories(engine_jwt PUBLIC
    ${CMAKE_SOURCE_DIR}/engine/include

)
target_link_libraries(engine_jwt hkdf)

# Engine HTTP (minimal TCP server — no cJSON dependency)
add_library(engine_http STATIC
    engine/src/engine_http.c
)
target_include_directories(engine_http PUBLIC ${CMAKE_SOURCE_DIR}/engine/include)

# Test: Engine JWT (HS256 generate/validate/reject)
add_executable(test_engine_jwt
    engine/tests/test_engine_jwt.c
)
target_include_directories(test_engine_jwt PRIVATE ${CMAKE_SOURCE_DIR}/engine/include)
target_link_libraries(test_engine_jwt engine_jwt)
add_test(NAME test_engine_jwt COMMAND test_engine_jwt)

#==============================================================================
# cJSON detection (used by chain_replay + engine API + integration tests)
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

    #==============================================================================
    # Libraries — Engine API (cJSON-dependent)
    #==============================================================================

    # Engine RPC (JSON-RPC 2.0 dispatcher)
    add_library(engine_rpc STATIC
        engine/src/engine_rpc.c
    )
    target_include_directories(engine_rpc PUBLIC
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_link_libraries(engine_rpc ${CJSON_LIBRARIES})

    # Engine Types (ExecutionPayload, PayloadStatus, ForkchoiceState)
    add_library(engine_types STATIC
        engine/src/engine_types.c
    )
    target_include_directories(engine_types PUBLIC
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_link_libraries(engine_types ${CJSON_LIBRARIES})

    # Engine Store (in-memory block storage + fork choice)
    add_library(engine_store STATIC
        engine/src/engine_store.c
    )
    target_include_directories(engine_store PUBLIC
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_link_libraries(engine_store engine_types)

    # Engine Handlers (newPayloadV4, forkchoiceUpdatedV3, getPayloadV4)
    add_library(engine_handlers STATIC
        engine/src/engine_handlers.c
    )
    target_include_directories(engine_handlers PUBLIC
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_include_directories(engine_handlers PRIVATE ${CMAKE_SOURCE_DIR}/sync/include)
    target_link_libraries(engine_handlers engine_rpc engine_types engine_store block_executor ${CJSON_LIBRARIES})

    # Engine (top-level orchestrator)
    add_library(engine STATIC
        engine/src/engine.c
    )
    target_include_directories(engine PUBLIC
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CMAKE_SOURCE_DIR}/sync/include
        ${CJSON_INCLUDE_DIRS}
    )
    # Note: sync must come after engine_handlers for linker symbol resolution
    target_link_libraries(engine engine_http engine_jwt engine_rpc engine_handlers sync engine_store engine_types ${CJSON_LIBRARIES})

    # Standalone Engine API server (for mock CL testing)
    add_executable(engine_server tools/engine_server.c)
    target_include_directories(engine_server PRIVATE
        integration_tests/include
        executor/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_link_libraries(engine_server engine integration_tests block_executor evm ${CJSON_LIBRARIES})

    #==============================================================================
    # Tests — Engine API (cJSON-dependent)
    #==============================================================================

    # Test: Engine RPC + Types + Store
    add_executable(test_engine_rpc
        engine/tests/test_engine_rpc.c
    )
    target_include_directories(test_engine_rpc PRIVATE
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_link_libraries(test_engine_rpc engine_rpc engine_types engine_store ${CJSON_LIBRARIES})
    add_test(NAME test_engine_rpc COMMAND test_engine_rpc)

    # Test: Engine Handlers (full integration)
    add_executable(test_engine_handlers
        engine/tests/test_engine_handlers.c
    )
    target_include_directories(test_engine_handlers PRIVATE
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_link_libraries(test_engine_handlers engine_rpc engine_handlers sync engine_types engine_store block_executor ${CJSON_LIBRARIES})
    add_test(NAME test_engine_handlers COMMAND test_engine_handlers)

    # Sync engine (reusable block execution + validation + checkpointing)
    add_library(sync STATIC sync/src/sync.c)
    target_include_directories(sync PUBLIC ${CMAKE_SOURCE_DIR}/sync/include)
    target_include_directories(sync PRIVATE ${CJSON_INCLUDE_DIRS})
    set(SYNC_LIBS block_executor evm common ${CJSON_LIBRARIES} code_store)
    target_link_libraries(sync ${SYNC_LIBS})

    # Portal Network library
    add_library(portal_net STATIC
        net/src/aes.c
        net/src/discv5.c
        net/src/discv5_codec.c
        net/src/discv5_session.c
        net/src/discv5_table.c
        net/src/enr.c
        net/src/history.c
        net/src/hkdf.c
        net/src/portal.c
        net/src/portal_table.c
        net/src/portal_wire.c
        net/src/secp256k1_wrap.c
        net/src/ssz.c
        net/src/utp.c
    )
    target_include_directories(portal_net PUBLIC
        ${CMAKE_SOURCE_DIR}/net/include
        ${CMAKE_SOURCE_DIR}/third_party/blst/bindings
    )
    target_compile_options(portal_net PRIVATE -maes -msse2)
    target_link_libraries(portal_net blst secp256k1 common)

    # Portal Network tests
    add_executable(test_portal_loopback net/tests/test_loopback.c)
    target_include_directories(test_portal_loopback PRIVATE ${CMAKE_SOURCE_DIR}/net/include)
    target_link_libraries(test_portal_loopback portal_net)
    add_test(NAME test_portal_loopback COMMAND test_portal_loopback)

    # Portal node tool
    add_executable(portal_node tools/portal_node.c)
    target_include_directories(portal_node PRIVATE
        ${CMAKE_SOURCE_DIR}/net/include
        ${CMAKE_SOURCE_DIR}/common/include)
    target_link_libraries(portal_node portal_net)

    # Chain replay tool
    add_executable(chain_replay tools/chain_replay.c)
    target_compile_definitions(chain_replay PRIVATE
        CHECKPOINT_INTERVAL=${CHECKPOINT_INTERVAL}
        ENABLE_ENGINE_API)
    target_include_directories(chain_replay PRIVATE
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CJSON_INCLUDE_DIRS})
    target_link_libraries(chain_replay sync engine era_reader ${CJSON_LIBRARIES})

    # History query tool
    add_executable(history_query tools/history_query.c)
    target_link_libraries(history_query block_executor evm common)

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
