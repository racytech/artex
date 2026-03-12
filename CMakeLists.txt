cmake_minimum_required(VERSION 3.15)

# Load project config if present (avoids -D flags on every cmake invocation)
if(EXISTS "${CMAKE_SOURCE_DIR}/config.cmake")
    include("${CMAKE_SOURCE_DIR}/config.cmake")
endif()

project(art_database C)

# Backend options (overridable via config.cmake or -D flags)
option(ENABLE_MPT "Enable MPT (Merkle Patricia Trie) state root" ON)
option(ENABLE_VERKLE "Enable Verkle state root" OFF)
option(ENABLE_MEM_MPT "Enable in-memory batch MPT for test runner (not needed for chain_replay)" OFF)
option(ENABLE_COMPACTION "Enable periodic storage MPT compaction during chain replay" OFF)
option(ENABLE_EVM_TRACE "Enable EIP-3155 EVM execution tracing" OFF)

if(NOT ENABLE_MPT AND NOT ENABLE_VERKLE)
    message(FATAL_ERROR "At least one state backend must be enabled (ENABLE_MPT or ENABLE_VERKLE)")
endif()

# Tunable parameters (overridable via config.cmake or -D flags)
if(NOT DEFINED MPT_ACCOUNT_CACHE_MB)
    set(MPT_ACCOUNT_CACHE_MB 2048)
endif()
if(NOT DEFINED MPT_STORAGE_CACHE_MB)
    set(MPT_STORAGE_CACHE_MB 2048)
endif()
if(NOT DEFINED MPT_ACCOUNT_CAPACITY)
    set(MPT_ACCOUNT_CAPACITY 500000000)
endif()
if(NOT DEFINED MPT_STORAGE_CAPACITY)
    set(MPT_STORAGE_CAPACITY 500000000)
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
    ${CMAKE_SOURCE_DIR}/state/include
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
target_link_libraries(verkle_flat verkle_commit_store art_store disk_hash pedersen banderwagon)

# In-Memory Batch MPT (testing only — used by test runner for state root verification)
add_library(mem_mpt STATIC
    database/src/mem_mpt.c
)
target_link_libraries(mem_mpt common)

# MPT Store (persistent Merkle Patricia Trie with incremental updates)
add_library(mpt_store STATIC
    database/src/mpt_store.c
)
target_include_directories(mpt_store PUBLIC ${CMAKE_SOURCE_DIR}/database/include)
target_compile_definitions(mpt_store PRIVATE MPT_DEFAULT_CACHE_MB=${MPT_ACCOUNT_CACHE_MB})
target_link_libraries(mpt_store disk_hash common pthread)

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
    evm/src/witness_gas.c
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
set(EVM_LINK_LIBS verkle_key pedersen banderwagon art_database common ssl crypto secp256k1 blst)
if(ENABLE_MEM_MPT)
    list(APPEND EVM_LINK_LIBS mem_mpt)
endif()
if(ENABLE_VERKLE)
    list(APPEND EVM_LINK_LIBS verkle_state)
endif()
if(ENABLE_MPT)
    list(APPEND EVM_LINK_LIBS mpt_store code_store)
endif()
target_link_libraries(evm ${EVM_LINK_LIBS})
if(ENABLE_MPT)
    target_compile_definitions(evm PUBLIC ENABLE_MPT=1)
endif()
if(ENABLE_VERKLE)
    target_compile_definitions(evm PUBLIC ENABLE_VERKLE=1)
endif()
if(ENABLE_MEM_MPT)
    target_compile_definitions(evm PUBLIC ENABLE_MEM_MPT=1)
endif()
if(ENABLE_EVM_TRACE)
    target_compile_definitions(evm PUBLIC ENABLE_EVM_TRACE=1)
endif()
target_compile_definitions(evm PRIVATE
    MPT_STORAGE_CACHE_MB=${MPT_STORAGE_CACHE_MB}
    MPT_ACCOUNT_CAPACITY=${MPT_ACCOUNT_CAPACITY}
    MPT_STORAGE_CAPACITY=${MPT_STORAGE_CAPACITY}
)

#==============================================================================
# Libraries — Block Executor
#==============================================================================

add_library(block_executor STATIC
    executor/src/block.c
    executor/src/tx_decoder.c
    executor/src/block_executor.c
    executor/src/dao_fork.c
    executor/src/era1.c
)
target_include_directories(block_executor PUBLIC
    ${CMAKE_SOURCE_DIR}/executor/include
)
target_link_libraries(block_executor evm common secp256k1)

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

# Test: EVM state management audit
add_executable(test_evm_state_audit
    evm/tests/test_evm_state_audit.c
)
set(TEST_EVM_AUDIT_LIBS evm)
if(ENABLE_VERKLE)
    list(APPEND TEST_EVM_AUDIT_LIBS verkle_state)
endif()
target_link_libraries(test_evm_state_audit ${TEST_EVM_AUDIT_LIBS})
add_test(NAME test_evm_state_audit COMMAND test_evm_state_audit)

# Test: MPT Store cross-validation against Python reference vectors
add_executable(test_mpt_store_vectors
    database/tests/test_mpt_store_vectors.c
)
target_link_libraries(test_mpt_store_vectors mpt_store common)
add_test(NAME test_mpt_store_vectors
    COMMAND test_mpt_store_vectors
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Benchmark: MPT Store at scale (realistic Ethereum workload)
add_executable(bench_mpt_store
    tools/bench_mpt_store.c
)
target_link_libraries(bench_mpt_store mpt_store common)

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
set(TEST_BLOCK_EXEC_LIBS block_executor evm common)
if(ENABLE_VERKLE)
    list(APPEND TEST_BLOCK_EXEC_LIBS verkle_state)
endif()
target_link_libraries(test_block_executor ${TEST_BLOCK_EXEC_LIBS})
add_test(NAME test_block_executor COMMAND test_block_executor)

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
# cJSON detection (used by chain_replay + integration tests)
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

    # Sync engine (reusable block execution + validation + checkpointing)
    add_library(sync STATIC sync/src/sync.c)
    target_include_directories(sync PUBLIC ${CMAKE_SOURCE_DIR}/sync/include)
    target_include_directories(sync PRIVATE ${CJSON_INCLUDE_DIRS})
    set(SYNC_LIBS block_executor evm common ${CJSON_LIBRARIES})
    if(ENABLE_VERKLE)
        list(APPEND SYNC_LIBS verkle_state)
    endif()
    if(ENABLE_MPT)
        list(APPEND SYNC_LIBS code_store)
    endif()
    if(ENABLE_COMPACTION)
        target_compile_definitions(sync PRIVATE ENABLE_COMPACTION=1)
    endif()
    target_link_libraries(sync ${SYNC_LIBS})

    # Chain replay tool
    add_executable(chain_replay tools/chain_replay.c)
    target_compile_definitions(chain_replay PRIVATE CHECKPOINT_INTERVAL=${CHECKPOINT_INTERVAL})
    target_link_libraries(chain_replay sync)

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
        set(INTEG_TEST_LIBS evm block_executor common ${CJSON_LIBRARIES})
        if(ENABLE_VERKLE)
            list(APPEND INTEG_TEST_LIBS verkle_state)
        endif()
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
message(STATUS "MPT backend:       ${ENABLE_MPT}")
message(STATUS "Verkle backend:    ${ENABLE_VERKLE}")
message(STATUS "Compaction:        ${ENABLE_COMPACTION}")
message(STATUS "")
message(STATUS "Tuning:")
message(STATUS "  Account cache:   ${MPT_ACCOUNT_CACHE_MB} MB")
message(STATUS "  Storage cache:   ${MPT_STORAGE_CACHE_MB} MB")
message(STATUS "  Account capacity:${MPT_ACCOUNT_CAPACITY}")
message(STATUS "  Storage capacity:${MPT_STORAGE_CAPACITY}")
message(STATUS "  Checkpoint every:${CHECKPOINT_INTERVAL} blocks")
message(STATUS "")
message(STATUS "Libraries:")
message(STATUS "  - art_database (in-memory ART tree)")
message(STATUS "  - compact_art (space-efficient index ART)")
message(STATUS "  - art_store (ART-indexed persistent record store)")
message(STATUS "  - disk_hash (zero-RAM disk-backed hash table)")
message(STATUS "  - code_store (content-addressed bytecode storage)")
message(STATUS "  - verkle (verkle tree with Pedersen commitments)")
message(STATUS "  - evm (Ethereum Virtual Machine)")
message(STATUS "  - block_executor (block decode + execution)")
message(STATUS "  - sync (block execution + validation + checkpointing)")
message(STATUS "  - portal (Portal Network stack)")
message(STATUS "========================================")
message(STATUS "")
