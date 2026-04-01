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
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -DNDEBUG -flto -march=native")

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/common/include
    ${CMAKE_SOURCE_DIR}/evm/include
    ${CMAKE_SOURCE_DIR}/executor/include
    ${CMAKE_SOURCE_DIR}/net/include
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

# Compact ART (in-memory index for flat_store)
add_library(compact_art STATIC database/src/compact_art.c)

# Flat Store (compact_art index + mmap'd data file)
add_library(flat_store STATIC database/src/flat_store.c)
target_link_libraries(flat_store compact_art)

# Flat State (O(1) account/storage lookups via flat_store)
add_library(flat_state STATIC database/src/flat_state.c)
target_link_libraries(flat_state flat_store)

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

#==============================================================================
# Libraries — Crypto
#==============================================================================

# blst (BLS12-381 field arithmetic — pre-built static library)
add_library(blst STATIC IMPORTED)
set_target_properties(blst PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/third_party/blst/libblst.a
)

# ART→MPT (compute MPT hashes from compact_art structure)
add_library(mem_mpt STATIC database/src/mem_mpt.c)
target_link_libraries(mem_mpt common)

add_library(art_mpt STATIC database/src/art_mpt.c)
target_include_directories(art_mpt PUBLIC ${CMAKE_SOURCE_DIR}/database/include)
target_link_libraries(art_mpt compact_art common)

add_library(storage_trie STATIC database/src/storage_trie.c)
target_include_directories(storage_trie PUBLIC ${CMAKE_SOURCE_DIR}/database/include)
target_link_libraries(storage_trie art_mpt flat_store)

add_library(account_trie STATIC database/src/account_trie.c)
target_include_directories(account_trie PUBLIC ${CMAKE_SOURCE_DIR}/database/include)
target_link_libraries(account_trie art_mpt flat_store flat_state)

add_library(state_overlay STATIC database/src/state_overlay.c)
target_include_directories(state_overlay PUBLIC
    ${CMAKE_SOURCE_DIR}/database/include
    ${CMAKE_SOURCE_DIR}/evm/include
)
target_link_libraries(state_overlay art_database flat_state flat_store storage_trie account_trie code_store common)
if(ENABLE_HISTORY)
    target_compile_definitions(state_overlay PUBLIC ENABLE_HISTORY=1)
endif()

#==============================================================================
# Libraries — EVM
#==============================================================================

# secp256k1 (ECDSA, ECDH — pre-built static library)
add_library(secp256k1 STATIC IMPORTED)
set_target_properties(secp256k1 PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/third_party/secp256k1/build/lib/libsecp256k1.a
)

# EVM (Ethereum Virtual Machine)
add_library(evm STATIC
    evm/src/evm.c
    evm/src/evm_state.c
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
)
set(EVM_LINK_LIBS art_database common ssl crypto secp256k1 blst code_store flat_state storage_trie account_trie state_overlay)
target_link_libraries(evm ${EVM_LINK_LIBS})
if(ENABLE_EVM_TRACE)
    target_compile_definitions(evm PUBLIC ENABLE_EVM_TRACE=1)
endif()
if(ENABLE_DEBUG)
    target_compile_definitions(evm PUBLIC ENABLE_DEBUG=1)
endif()
if(ENABLE_HISTORY)
    target_compile_definitions(evm PUBLIC ENABLE_HISTORY=1)
endif()
target_compile_definitions(evm PRIVATE
)

#==============================================================================
# Libraries — Block Executor
#==============================================================================

set(BLOCK_EXECUTOR_SOURCES
    executor/src/block.c
    executor/src/tx_decoder.c
    executor/src/tx_pipeline.c
    executor/src/block_executor.c
    executor/src/dao_fork.c
    executor/src/era1.c
)
if(ENABLE_HISTORY)
    list(APPEND BLOCK_EXECUTOR_SOURCES executor/src/state_history.c)
endif()
add_library(block_executor STATIC ${BLOCK_EXECUTOR_SOURCES})
target_include_directories(block_executor PUBLIC
    ${CMAKE_SOURCE_DIR}/executor/include
)
set(BLOCK_EXECUTOR_LIBS evm mem_mpt common secp256k1 pthread)
target_link_libraries(block_executor ${BLOCK_EXECUTOR_LIBS})

#==============================================================================
# Libraries — Networking / Portal
#==============================================================================

# HKDF-SHA256 (RFC 5869 key derivation, uses BLST for SHA-256)
add_library(hkdf STATIC
    net/src/hkdf.c
)
target_link_libraries(hkdf blst)

# AES-128 CTR + GCM (AES-NI intrinsics)
add_library(aes STATIC
    net/src/aes.c
)
target_compile_options(aes PRIVATE -maes)

# secp256k1 wrapper (thin C wrapper over libsecp256k1)
add_library(secp256k1_wrap STATIC
    net/src/secp256k1_wrap.c
)
target_include_directories(secp256k1_wrap PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/secp256k1/include
)
target_link_libraries(secp256k1_wrap secp256k1)

# SSZ — Simple Serialize (Portal Network subset)
add_library(ssz STATIC
    net/src/ssz.c
)

# ENR — Ethereum Node Record (EIP-778)
add_library(enr STATIC
    net/src/enr.c
)
target_link_libraries(enr common secp256k1_wrap)

# Discv5 Packet Codec (wire protocol encoding/decoding)
add_library(discv5_codec STATIC
    net/src/discv5_codec.c
)
target_link_libraries(discv5_codec hkdf aes secp256k1_wrap blst)

# Discv5 Session Management (handshake state + LRU cache)
add_library(discv5_session STATIC
    net/src/discv5_session.c
)
target_link_libraries(discv5_session discv5_codec secp256k1_wrap blst)

# Discv5 Kademlia Routing Table (256 k-buckets, XOR distance)
add_library(discv5_table STATIC
    net/src/discv5_table.c
)

# Discv5 Protocol Engine (full protocol: codec + sessions + table + UDP)
add_library(discv5 STATIC
    net/src/discv5.c
)
target_link_libraries(discv5 discv5_codec discv5_session discv5_table enr common secp256k1_wrap hkdf aes blst)

# uTP — micro Transport Protocol (BEP29 over Discv5 TALKREQ)
add_library(utp STATIC
    net/src/utp.c
)

# Portal Wire Protocol — message codec (SSZ-encoded)
add_library(portal_wire STATIC
    net/src/portal_wire.c
)
target_link_libraries(portal_wire ssz)

# Portal Overlay Table — Kademlia routing with data_radius
add_library(portal_table STATIC
    net/src/portal_table.c
)
target_link_libraries(portal_table blst)

# History Network — content key codec + content ID derivation
add_library(history STATIC
    net/src/history.c
)

# Portal Overlay Handler — generic sub-protocol integration layer
add_library(portal STATIC
    net/src/portal.c
)
target_link_libraries(portal portal_wire portal_table discv5 utp)

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

add_executable(test_state_overlay_stress
    evm/tests/test_state_overlay_stress.c
)
target_link_libraries(test_state_overlay_stress evm)
add_test(NAME test_state_overlay_stress COMMAND test_state_overlay_stress)

add_executable(test_art_mpt_fuzz database/tests/test_art_mpt_fuzz.c)
target_link_libraries(test_art_mpt_fuzz art_mpt compact_art common)
add_test(NAME test_art_mpt_fuzz COMMAND test_art_mpt_fuzz)

add_executable(test_flat_state_roundtrip
    evm/tests/test_flat_state_roundtrip.c
)
target_link_libraries(test_flat_state_roundtrip evm)
add_test(NAME test_flat_state_roundtrip COMMAND test_flat_state_roundtrip)

# Test: ART→MPT cross-validation against Python reference vectors
add_executable(test_art_mpt_vectors database/tests/test_art_mpt_vectors.c)
target_link_libraries(test_art_mpt_vectors art_mpt compact_art common)
add_test(NAME test_art_mpt_vectors
    COMMAND test_art_mpt_vectors
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

#==============================================================================
# Tests — Networking
#==============================================================================

# Test: HKDF-SHA256 (RFC 5869 + Discv5 key derivation vectors)
add_executable(test_hkdf
    net/tests/test_hkdf.c
)
target_link_libraries(test_hkdf hkdf)
add_test(NAME test_hkdf COMMAND test_hkdf)

# Test: AES-128 CTR + GCM (NIST + Discv5 vectors)
add_executable(test_aes
    net/tests/test_aes.c
)
target_link_libraries(test_aes aes)
target_compile_options(test_aes PRIVATE -maes)
add_test(NAME test_aes COMMAND test_aes)

# Test: secp256k1 wrapper (ECDSA, ECDH, key operations)
add_executable(test_secp256k1_wrap
    net/tests/test_secp256k1_wrap.c
)
target_include_directories(test_secp256k1_wrap PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/secp256k1/include
)
target_link_libraries(test_secp256k1_wrap secp256k1_wrap)
add_test(NAME test_secp256k1_wrap COMMAND test_secp256k1_wrap)

# Test: SSZ (Portal Network subset)
add_executable(test_ssz
    net/tests/test_ssz.c
)
target_link_libraries(test_ssz ssz)
add_test(NAME test_ssz COMMAND test_ssz)

# Test: ENR (Ethereum Node Record — EIP-778)
add_executable(test_enr
    net/tests/test_enr.c
)
target_link_libraries(test_enr enr)
add_test(NAME test_enr COMMAND test_enr)

# Test: Discv5 Packet Codec (wire protocol test vectors)
add_executable(test_discv5_codec
    net/tests/test_discv5_codec.c
)
target_link_libraries(test_discv5_codec discv5_codec enr)
add_test(NAME test_discv5_codec COMMAND test_discv5_codec)

# Test: Discv5 Session Management (handshake state + LRU cache)
add_executable(test_discv5_session
    net/tests/test_discv5_session.c
)
target_link_libraries(test_discv5_session discv5_session)
add_test(NAME test_discv5_session COMMAND test_discv5_session)

# Test: Discv5 Kademlia Routing Table
add_executable(test_discv5_table
    net/tests/test_discv5_table.c
)
target_link_libraries(test_discv5_table discv5_table)
add_test(NAME test_discv5_table COMMAND test_discv5_table)

# Test: Discv5 Protocol Engine
add_executable(test_discv5
    net/tests/test_discv5.c
)
target_link_libraries(test_discv5 discv5)
add_test(NAME test_discv5 COMMAND test_discv5)

# Test: uTP (micro Transport Protocol — BEP29 test vectors + loopback)
add_executable(test_utp
    net/tests/test_utp.c
)
target_link_libraries(test_utp utp)
add_test(NAME test_utp COMMAND test_utp)

# Test: Portal Wire Protocol (message codec test vectors)
add_executable(test_portal_wire
    net/tests/test_portal_wire.c
)
target_link_libraries(test_portal_wire portal_wire ssz)
add_test(NAME test_portal_wire COMMAND test_portal_wire)

# Test: Portal Overlay Table (Kademlia + radius filtering)
add_executable(test_portal_table
    net/tests/test_portal_table.c
)
target_link_libraries(test_portal_table portal_table blst)
add_test(NAME test_portal_table COMMAND test_portal_table)

# Test: History Network (content key codec + content ID derivation)
add_executable(test_history
    net/tests/test_history.c
)
target_link_libraries(test_history history)
add_test(NAME test_history COMMAND test_history)

# Test: Portal Overlay Handler (sub-protocol integration)
add_executable(test_portal
    net/tests/test_portal.c
)
target_link_libraries(test_portal portal history)
add_test(NAME test_portal COMMAND test_portal)

# Test: Roundtrip (encode→decode→verify for all codecs, random inputs)
add_executable(test_roundtrip
    net/tests/test_roundtrip.c
)
target_link_libraries(test_roundtrip common portal_wire ssz discv5 utp history)
add_test(NAME test_roundtrip COMMAND test_roundtrip)

# Test: uTP Stress (concurrent connections, packet loss, large transfers)
add_executable(test_utp_stress
    net/tests/test_utp_stress.c
)
target_link_libraries(test_utp_stress utp)
add_test(NAME test_utp_stress COMMAND test_utp_stress)

# Test: Loopback Integration (full stack — Discv5 + Portal via real UDP on localhost)
add_executable(test_loopback
    net/tests/test_loopback.c
)
target_link_libraries(test_loopback portal discv5 portal_wire ssz history)
add_test(NAME test_loopback COMMAND test_loopback)

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

    add_executable(state_reconstruct tools/state_reconstruct.c)
    target_include_directories(state_reconstruct PRIVATE ${CJSON_INCLUDE_DIRS})
    target_link_libraries(state_reconstruct block_executor evm common ${CJSON_LIBRARIES})

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
    ${CMAKE_SOURCE_DIR}/net/include
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
    set(SYNC_LIBS block_executor evm common ${CJSON_LIBRARIES} code_store flat_state)
    target_link_libraries(sync ${SYNC_LIBS})
    # TUI (ncurses terminal UI, optional)
    find_package(Curses QUIET)
    if(CURSES_FOUND)
        add_library(tui STATIC tui/src/tui.c)
        target_include_directories(tui PUBLIC ${CMAKE_SOURCE_DIR}/tui/include)
        target_link_libraries(tui common ${CURSES_LIBRARIES} pthread)
    endif()

    # Chain replay tool
    add_executable(chain_replay tools/chain_replay.c)
    target_compile_definitions(chain_replay PRIVATE CHECKPOINT_INTERVAL=${CHECKPOINT_INTERVAL})
    target_link_libraries(chain_replay sync)
    if(CURSES_FOUND)
        target_link_libraries(chain_replay tui)
        target_compile_definitions(chain_replay PRIVATE ENABLE_TUI=1)
    endif()

    # History query tool
    add_executable(history_query tools/history_query.c)
    target_link_libraries(history_query block_executor evm common)

    # Full-stack stress test (evm_state → flat_state → account_trie + storage_trie)
    add_executable(stress_fullstack tools/stress_fullstack.c)
    target_link_libraries(stress_fullstack evm)

    # Artex Node — full execution client (era1 replay + CL sync)
    add_executable(artex_node tools/artex_node.c)
    target_include_directories(artex_node PRIVATE
        ${CMAKE_SOURCE_DIR}/engine/include
        ${CJSON_INCLUDE_DIRS}
    )
    target_compile_definitions(artex_node PRIVATE CHECKPOINT_INTERVAL=${CHECKPOINT_INTERVAL})
    target_link_libraries(artex_node sync engine ${CJSON_LIBRARIES})

    #==============================================================================
    # Integration Tests (Ethereum test vectors)
    #==============================================================================

    file(GLOB_RECURSE INTEGRATION_TEST_SOURCES "integration_tests/src/*.c")
    if(INTEGRATION_TEST_SOURCES)
        add_library(integration_tests STATIC ${INTEGRATION_TEST_SOURCES})
        target_include_directories(integration_tests PUBLIC
            integration_tests/include
            executor/include
            ${CJSON_INCLUDE_DIRS}
        )
        set(INTEG_TEST_LIBS evm block_executor flat_state common ${CJSON_LIBRARIES})
        target_link_libraries(integration_tests PUBLIC ${INTEG_TEST_LIBS})

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
            ${CMAKE_SOURCE_DIR}/net/include
            ${CMAKE_SOURCE_DIR}/third_party/secp256k1/include
        )
        target_link_libraries(evm_t8n integration_tests secp256k1_wrap)

        # EVM statetest runner (geth/evmone-compatible, for GoEVMLab runtest)
        add_executable(evm_statetest tools/evm_statetest.c)
        target_include_directories(evm_statetest PRIVATE
            ${CMAKE_SOURCE_DIR}/net/include
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
message(STATUS "  - portal (Portal Network stack)")
message(STATUS "========================================")
message(STATUS "")
