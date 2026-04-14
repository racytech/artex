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

/* Shared library export/import macros */
#ifdef RX_SHARED
  #ifdef _WIN32
    #ifdef RX_BUILD
      #define RX_API __declspec(dllexport)
    #else
      #define RX_API __declspec(dllimport)
    #endif
  #else
    #ifdef RX_BUILD
      #define RX_API __attribute__((visibility("default")))
    #else
      #define RX_API
    #endif
  #endif
#else
  #define RX_API
#endif

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
 * Error codes
 * ======================================================================== */

typedef enum {
    RX_OK = 0,                /* no error */
    RX_ERR_NULL_ARG,          /* required argument was NULL */
    RX_ERR_INVALID_CONFIG,    /* bad chain_id or config */
    RX_ERR_OUT_OF_MEMORY,     /* allocation failed */
    RX_ERR_ALREADY_INIT,      /* genesis/state already loaded */
    RX_ERR_NOT_INIT,          /* engine not initialized (no genesis/state loaded) */
    RX_ERR_FILE_IO,           /* file open/read/write failed */
    RX_ERR_PARSE,             /* JSON or RLP parse error */
    RX_ERR_DECODE,            /* block header/body decode failed */
    RX_ERR_EXECUTION,         /* block execution internal error */
    RX_ERR_BLOCK_NOT_FOUND,   /* block number outside hash window */
} rx_error_t;

/** Return human-readable string for an error code. */
RX_API const char *rx_error_string(rx_error_t err);

/** Get last error from engine. Thread-safe (per-engine). */
RX_API rx_error_t rx_engine_last_error(const rx_engine_t *engine);

/** Get last error message (may contain details). Returns "" if no error. */
RX_API const char *rx_engine_last_error_msg(const rx_engine_t *engine);

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
RX_API void rx_block_result_free(rx_block_result_t *result);

/* ========================================================================
 * Engine lifecycle
 * ======================================================================== */

/** Create execution engine. Returns NULL on failure (check rx_engine_last_error). */
RX_API rx_engine_t *rx_engine_create(const rx_config_t *config);

/** Destroy engine and free all resources. */
RX_API void rx_engine_destroy(rx_engine_t *engine);

/** Load genesis state from JSON file. Call before first block. */
RX_API bool rx_engine_load_genesis(rx_engine_t *engine, const char *path,
                                   const rx_hash_t *genesis_hash);

/**
 * Load state from a binary snapshot. Call instead of load_genesis.
 * Also loads block hash ring from a separate .hashes file if present.
 */
RX_API bool rx_engine_load_state(rx_engine_t *engine, const char *path);

/**
 * Save current state to a binary snapshot.
 * Also saves block hash ring to a separate .hashes file (256*32 bytes).
 */
RX_API bool rx_engine_save_state(rx_engine_t *engine, const char *path);

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
RX_API bool rx_execute_block_rlp(rx_engine_t *engine,
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
RX_API rx_hash_t rx_compute_state_root(rx_engine_t *engine);

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
RX_API rx_state_t *rx_engine_get_state(rx_engine_t *engine);

/** Check if account exists in state. */
RX_API bool rx_account_exists(rx_state_t *state, const rx_address_t *addr);

/** Get account nonce. Returns 0 for non-existent accounts. */
RX_API uint64_t rx_get_nonce(rx_state_t *state, const rx_address_t *addr);

/** Get account balance (32 bytes, big-endian). */
RX_API rx_uint256_t rx_get_balance(rx_state_t *state, const rx_address_t *addr);

/** Get code hash. Returns empty hash for EOAs. */
RX_API rx_hash_t rx_get_code_hash(rx_state_t *state, const rx_address_t *addr);

/** Get code size in bytes. Returns 0 for EOAs. */
RX_API uint32_t rx_get_code_size(rx_state_t *state, const rx_address_t *addr);

/**
 * Copy contract bytecode into caller buffer.
 * Returns actual code length. If buf is NULL or buf_len is too small,
 * returns the required size without copying.
 */
RX_API uint32_t rx_get_code(rx_state_t *state, const rx_address_t *addr,
                            uint8_t *buf, uint32_t buf_len);

/** Get storage value at key. Returns zero for unset slots. */
RX_API rx_uint256_t rx_get_storage(rx_state_t *state, const rx_address_t *addr,
                                   const rx_uint256_t *key);

/* ========================================================================
 * Block hash query
 * ======================================================================== */

/**
 * Get a block hash from the 256-entry ring buffer.
 * Returns true if the block number is within the window.
 */
RX_API bool rx_get_block_hash(const rx_engine_t *engine, uint64_t block_number,
                              rx_hash_t *out);

/* ========================================================================
 * Status
 * ======================================================================== */

/** Get last executed block number. Returns 0 before first block. */
RX_API uint64_t rx_get_block_number(const rx_engine_t *engine);

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
RX_API void rx_set_logger(rx_engine_t *engine, rx_log_fn fn, void *userdata);

/* ========================================================================
 * Version
 * ======================================================================== */

/** Returns version string, e.g. "0.1.0". */
RX_API const char *rx_version(void);

/* ========================================================================
 * Block commit / revert (chain tip reorg)
 * ======================================================================== */

/**
 * Commit the last executed block — discard undo data.
 *
 * Must be called after rx_execute_block_rlp when the block is accepted.
 * Without this, the undo log accumulates and the next block execution
 * may produce incorrect undo entries.
 */
RX_API bool rx_commit_block(rx_engine_t *engine);

/**
 * Revert the last executed block — restore pre-block state.
 *
 * Walks the undo log (captured on first-touch during execution) and
 * restores account nonce/balance/flags and storage slots to their
 * pre-block values. Also rolls back the block number and hash ring.
 *
 * Depth=1 only (last block). For deeper reorgs, reload a snapshot
 * with rx_engine_load_state and re-execute the new fork's blocks.
 */
RX_API bool rx_revert_block(rx_engine_t *engine);

/* ========================================================================
 * TODO: Future API additions
 *
 * rx_execute_block() — execute from decoded header + body structs.
 *   Avoids RLP encode→decode overhead when caller already has parsed
 *   blocks (e.g. from their own p2p layer or database).
 *
 * rx_engine_load_genesis_alloc() — load genesis from in-memory
 *   address/balance array instead of requiring a JSON file on disk.
 *   Useful for programmatic genesis construction in tests/tools.
 *
 * rx_call() — read-only message execution against current state
 *   without committing (eth_call equivalent). For transaction
 *   simulation, gas estimation, and view function calls.
 * ======================================================================== */

#ifdef __cplusplus
}
#endif

#endif /* ARTEX_H */
