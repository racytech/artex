/**
 * artex.h — Public C API for the artex Ethereum execution engine.
 *
 * Single header for library consumers. All types are opaque;
 * all functions use plain C types + fixed-width integers.
 * Safe for FFI from any language (Rust, Go, Python, Zig, etc.).
 *
 * Prefix: rx_
 *
 * Minimal usage:
 *   rx_engine_t *e = rx_engine_create(&config);
 *   rx_engine_load_genesis(e, "genesis.json", NULL);
 *   rx_execute_block_rlp(e, hdr, hdr_len, body, body_len, &hash, &result);
 *   rx_hash_t root = rx_compute_state_root(e);
 *   rx_engine_destroy(e);
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

typedef struct rx_engine rx_engine_t;   /* execution engine (state + EVM) */
typedef struct rx_state  rx_state_t;    /* state handle (for queries) */

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
} rx_chain_id_t;

typedef struct {
    rx_chain_id_t chain_id;    /* only RX_CHAIN_MAINNET supported */
    const char   *data_dir;    /* directory for code_store and other disk state.
                                  NULL = in-memory only (no persistence). */
} rx_config_t;

/* ========================================================================
 * Receipts and logs
 * ======================================================================== */

typedef struct {
    rx_address_t address;        /* contract that emitted the log */
    rx_hash_t   *topics;         /* indexed topics (0-4) */
    uint8_t      topic_count;
    uint8_t     *data;           /* non-indexed data */
    size_t       data_len;
} rx_log_t;

typedef struct {
    uint8_t      status;         /* 0 = fail, 1 = success */
    uint8_t      tx_type;        /* 0=legacy, 1=2930, 2=1559, 3=4844 */
    uint64_t     gas_used;       /* gas consumed by this tx */
    uint64_t     cumulative_gas; /* cumulative gas after this tx */
    uint8_t      logs_bloom[256];
    rx_log_t    *logs;           /* log entries */
    size_t       log_count;
    bool         contract_created;
    rx_address_t contract_addr;  /* valid if contract_created */
} rx_receipt_t;

/* ========================================================================
 * Block execution result
 * ======================================================================== */

typedef struct {
    bool        ok;              /* true if execution succeeded */
    uint64_t    gas_used;        /* total gas consumed */
    size_t      tx_count;        /* transactions executed */
    rx_hash_t   state_root;      /* post-execution state root */
    rx_hash_t   receipt_root;    /* receipt trie root */
    uint8_t     logs_bloom[256]; /* aggregate bloom filter */
    rx_receipt_t *receipts;      /* per-tx receipts (tx_count entries) */
} rx_block_result_t;

/** Free receipt data inside a block result. */
void rx_block_result_free(rx_block_result_t *result);

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

/**
 * Load state from a binary snapshot. Call instead of load_genesis.
 * Also loads block hash ring from a separate .hashes file if present.
 */
bool rx_engine_load_state(rx_engine_t *engine, const char *path);

/**
 * Save current state to a binary snapshot.
 * Also saves block hash ring to a separate .hashes file (256*32 bytes).
 */
bool rx_engine_save_state(rx_engine_t *engine, const char *path);

/* ========================================================================
 * Block execution
 * ======================================================================== */

/**
 * Execute a block from RLP-encoded header + body.
 *
 * Decodes the RLP, executes all transactions, applies rewards
 * and withdrawals, finalizes state. The state root in the result
 * is computed automatically.
 *
 * block_hash: keccak256 of the RLP-encoded header. Used for the
 *             BLOCKHASH opcode ring buffer.
 *
 * Returns false only on fatal/internal error (OOM, corrupt data).
 * Check result->ok for execution success.
 */
bool rx_execute_block_rlp(rx_engine_t *engine,
                          const uint8_t *header_rlp, size_t header_len,
                          const uint8_t *body_rlp, size_t body_len,
                          const rx_hash_t *block_hash,
                          rx_block_result_t *result);

/**
 * Compute state root without executing a block.
 *
 * Use when you need the root outside of block execution,
 * e.g. for validation at checkpoint boundaries.
 */
rx_hash_t rx_compute_state_root(rx_engine_t *engine);

/* ========================================================================
 * State queries
 * ======================================================================== */

/**
 * Get state handle for queries.
 *
 * State queries are only valid between block executions — after
 * rx_execute_block_rlp returns and before the next call. The state
 * reflects the finalized result of the last executed block.
 * Do not query state while a block is executing.
 */
rx_state_t *rx_engine_get_state(rx_engine_t *engine);

/** Check if account exists in state. */
bool rx_account_exists(rx_state_t *state, const rx_address_t *addr);

/** Get account nonce. Returns 0 for non-existent accounts. */
uint64_t rx_get_nonce(rx_state_t *state, const rx_address_t *addr);

/** Get account balance (32 bytes, big-endian). */
rx_uint256_t rx_get_balance(rx_state_t *state, const rx_address_t *addr);

/** Get code hash. Returns empty hash for EOAs. */
rx_hash_t rx_get_code_hash(rx_state_t *state, const rx_address_t *addr);

/** Get code size in bytes. Returns 0 for EOAs. */
uint32_t rx_get_code_size(rx_state_t *state, const rx_address_t *addr);

/**
 * Copy contract bytecode into caller buffer.
 * Returns actual code length. If buf is NULL or buf_len is too small,
 * returns the required size without copying.
 */
uint32_t rx_get_code(rx_state_t *state, const rx_address_t *addr,
                     uint8_t *buf, uint32_t buf_len);

/** Get storage value at key. Returns zero for unset slots. */
rx_uint256_t rx_get_storage(rx_state_t *state, const rx_address_t *addr,
                            const rx_uint256_t *key);

/* ========================================================================
 * Status
 * ======================================================================== */

/** Get last executed block number. Returns 0 before first block. */
uint64_t rx_get_block_number(const rx_engine_t *engine);

/* ========================================================================
 * Logging
 * ======================================================================== */

typedef enum {
    RX_LOG_ERROR = 0,
    RX_LOG_WARN  = 1,
    RX_LOG_INFO  = 2,
    RX_LOG_DEBUG = 3,
} rx_log_level_t;

/**
 * Set a log callback. By default the library is silent.
 * The callback receives the level, a null-terminated message,
 * and the userdata pointer passed here.
 */
typedef void (*rx_log_fn)(rx_log_level_t level, const char *msg, void *userdata);
void rx_set_logger(rx_engine_t *engine, rx_log_fn fn, void *userdata);

/* ========================================================================
 * Version
 * ======================================================================== */

/** Returns version string, e.g. "0.1.0". */
const char *rx_version(void);

#ifdef __cplusplus
}
#endif

#endif /* ARTEX_H */
