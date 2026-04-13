/**
 * artex.h — Public C API for the artex Ethereum execution engine.
 *
 * Single header for library consumers. All types are opaque;
 * all functions use plain C types + fixed-width integers.
 * Safe for FFI from any language (Rust, Go, Python, Zig, etc.).
 *
 * Prefix: rx_
 */

#ifndef ARTEX_H
#define ARTEX_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * Opaque handles
 * ======================================================================== */

typedef struct rx_state   rx_state_t;    /* execution state (accounts, storage) */
typedef struct rx_engine  rx_engine_t;   /* sync engine (state + block execution) */

/* ========================================================================
 * Fixed-size types (plain bytes, no internal deps)
 * ======================================================================== */

typedef struct { uint8_t bytes[20]; } rx_address_t;
typedef struct { uint8_t bytes[32]; } rx_hash_t;
typedef struct { uint8_t bytes[32]; } rx_uint256_t;

/* ========================================================================
 * Configuration
 * ======================================================================== */

typedef enum {
    RX_CHAIN_MAINNET = 1,
    RX_CHAIN_SEPOLIA = 11155111,
} rx_chain_id_t;

typedef struct {
    rx_chain_id_t  chain_id;
    uint32_t       checkpoint_interval;  /* validate root every N blocks (0 = never) */
    bool           validate_state_root;  /* compare root against header */
} rx_config_t;

/* ========================================================================
 * Block execution types
 * ======================================================================== */

typedef enum {
    RX_OK = 0,
    RX_ERR_GAS_MISMATCH,
    RX_ERR_ROOT_MISMATCH,
    RX_ERR_INTERNAL,
} rx_error_t;

typedef struct {
    bool        ok;
    rx_error_t  error;
    uint64_t    gas_used;
    size_t      tx_count;
} rx_block_result_t;

/* ========================================================================
 * Engine lifecycle
 * ======================================================================== */

/** Create execution engine. Returns NULL on failure. */
rx_engine_t *rx_engine_create(const rx_config_t *config);

/** Destroy engine and free all resources. */
void rx_engine_destroy(rx_engine_t *engine);

/** Load genesis state from JSON file. Call before first block. */
bool rx_engine_load_genesis(rx_engine_t *engine, const char *path,
                            const rx_hash_t *genesis_hash);

/** Resume from a saved state snapshot. Call instead of load_genesis. */
bool rx_engine_load_state(rx_engine_t *engine, const char *path);

/** Save current state to file. */
bool rx_engine_save_state(rx_engine_t *engine, const char *path);

/* ========================================================================
 * Block execution
 * ======================================================================== */

/**
 * Execute a block from RLP-encoded header + body.
 *
 * This is the primary entry point: hand it raw RLP bytes
 * from any source (p2p, database, file) and get back results.
 */
bool rx_execute_block_rlp(rx_engine_t *engine,
                          const uint8_t *header_rlp, size_t header_len,
                          const uint8_t *body_rlp, size_t body_len,
                          const rx_hash_t *block_hash,
                          rx_block_result_t *result);

/** Compute current state root (Merkle Patricia Trie). */
rx_hash_t rx_compute_state_root(rx_engine_t *engine);

/* ========================================================================
 * State queries (read-only, no journaling)
 * ======================================================================== */

/** Get underlying state handle (for direct queries). */
rx_state_t *rx_engine_get_state(rx_engine_t *engine);

uint64_t    rx_get_nonce(rx_state_t *state, const rx_address_t *addr);
rx_uint256_t rx_get_balance(rx_state_t *state, const rx_address_t *addr);
rx_hash_t   rx_get_code_hash(rx_state_t *state, const rx_address_t *addr);
uint32_t    rx_get_code_size(rx_state_t *state, const rx_address_t *addr);

/**
 * Copy contract bytecode into caller buffer.
 * Returns actual length. If buf is NULL or buf_len too small,
 * returns required size without copying.
 */
uint32_t rx_get_code(rx_state_t *state, const rx_address_t *addr,
                     uint8_t *buf, uint32_t buf_len);

rx_uint256_t rx_get_storage(rx_state_t *state, const rx_address_t *addr,
                            const rx_uint256_t *key);

/** Check if account exists in state. */
bool rx_account_exists(rx_state_t *state, const rx_address_t *addr);

/* ========================================================================
 * Status
 * ======================================================================== */

typedef struct {
    uint64_t last_block;
    uint64_t total_gas;
    uint64_t blocks_executed;
    uint32_t account_count;
} rx_status_t;

rx_status_t rx_get_status(const rx_engine_t *engine);

/** Get last executed block number. */
uint64_t rx_get_block_number(const rx_engine_t *engine);

/* ========================================================================
 * Version
 * ======================================================================== */

/** Returns version string, e.g. "0.1.0" */
const char *rx_version(void);

#ifdef __cplusplus
}
#endif

#endif /* ARTEX_H */
