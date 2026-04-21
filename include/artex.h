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
 *   rx_commit_block(e);
 *   rx_hash_t root = rx_compute_state_root(e);
 *   rx_engine_destroy(e);
 *
 * Thread safety:
 *   - rx_engine_t is NOT thread-safe. All calls on the same engine
 *     must be serialized by the caller.
 *   - Multiple engines can be used concurrently from different threads
 *     (each engine owns its own state).
 *   - rx_state_t from rx_engine_get_state is valid only between block
 *     executions — do not query state while a block is executing.
 *   - rx_call is safe to call between blocks (it snapshots and reverts).
 *   - rx_error_string and rx_version are safe to call from any thread.
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
 * Block header / body (for rx_execute_block)
 * ======================================================================== */

typedef struct {
    rx_hash_t    parent_hash;
    rx_hash_t    uncle_hash;
    rx_address_t coinbase;
    rx_hash_t    state_root;
    rx_hash_t    tx_root;
    rx_hash_t    receipt_root;
    uint8_t      logs_bloom[256];
    rx_uint256_t difficulty;
    uint64_t     number;
    uint64_t     gas_limit;
    uint64_t     gas_used;
    uint64_t     timestamp;
    uint8_t      extra_data[32];
    size_t       extra_data_len;
    rx_hash_t    mix_hash;          /* prev_randao post-merge */
    uint64_t     nonce;

    /* EIP-1559 (London+) */
    bool         has_base_fee;
    rx_uint256_t base_fee;

    /* Shanghai+ */
    bool         has_withdrawals_root;
    rx_hash_t    withdrawals_root;

    /* Cancun+ */
    bool         has_blob_gas;
    uint64_t     blob_gas_used;
    uint64_t     excess_blob_gas;
    bool         has_parent_beacon_root;
    rx_hash_t    parent_beacon_root;

    /* Prague+ (EIP-7685) */
    bool         has_requests_hash;
    rx_hash_t    requests_hash;
} rx_block_header_t;

typedef struct {
    uint64_t     index;
    uint64_t     validator_index;
    rx_address_t address;
    uint64_t     amount_gwei;       /* in Gwei (multiply by 1e9 for Wei) */
} rx_withdrawal_t;

typedef struct {
    /** Raw transaction bytes. Each entry is a single transaction:
     *  - Legacy: RLP-encoded transaction list
     *  - Typed (EIP-2718): type_byte || RLP_payload */
    const uint8_t **transactions;   /* array of tx byte pointers */
    size_t         *tx_lengths;     /* length of each tx */
    size_t          tx_count;

    /** EIP-4895 withdrawals (Shanghai+). NULL if none. */
    rx_withdrawal_t *withdrawals;
    size_t           withdrawal_count;
} rx_block_body_t;

/* ========================================================================
 * Genesis allocation (for rx_engine_load_genesis_alloc)
 * ======================================================================== */

/** Single storage slot: key → value (both 32-byte big-endian). */
typedef struct {
    rx_uint256_t key;
    rx_uint256_t value;
} rx_storage_entry_t;

/** Genesis account: address + optional balance, nonce, code, storage. */
typedef struct {
    rx_address_t         address;
    rx_uint256_t         balance;         /* big-endian, zero = no balance */
    uint64_t             nonce;           /* 0 = default */
    const uint8_t       *code;            /* contract bytecode, NULL = EOA */
    uint32_t             code_len;
    rx_storage_entry_t  *storage;         /* storage slots, NULL = none */
    size_t               storage_count;
} rx_genesis_account_t;

/* ========================================================================
 * Engine lifecycle
 * ======================================================================== */

/**
 * Create execution engine. Returns NULL on failure (check rx_engine_last_error).
 *
 * NOTE: EVM supports 1024-deep call stacks. The calling thread must have at
 * least 32MB of stack space or this function will fail. Set before calling
 * via: `ulimit -s 32768`, `setrlimit(RLIMIT_STACK, …)`, or
 * `pthread_attr_setstacksize()`. In Python:
 * `resource.setrlimit(resource.RLIMIT_STACK, (32 << 20, hard))`.
 */
RX_API rx_engine_t *rx_engine_create(const rx_config_t *config);

/** Destroy engine and free all resources. */
RX_API void rx_engine_destroy(rx_engine_t *engine);

/** Load genesis state from JSON file. Call before first block. */
RX_API bool rx_engine_load_genesis(rx_engine_t *engine, const char *path,
                                   const rx_hash_t *genesis_hash);

/**
 * Load genesis state from in-memory account array.
 *
 * Same as rx_engine_load_genesis but takes structured data instead of
 * a JSON file. Useful for programmatic genesis construction.
 */
RX_API bool rx_engine_load_genesis_alloc(rx_engine_t *engine,
                                         const rx_genesis_account_t *accounts,
                                         size_t count,
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
                                 bool compute_root,
                                 rx_block_result_t *result);

/**
 * Execute a block from decoded header + body structs.
 *
 * Same as rx_execute_block_rlp but skips RLP decode — use when the
 * caller already has parsed block data (e.g. from their own p2p layer).
 *
 * block_hash: keccak256 of the RLP-encoded header. Used for the
 *             BLOCKHASH opcode ring buffer.
 *
 * compute_root: if true, compute state root after execution
 *               (result->state_root is valid). If false, skip root
 *               computation for speed (result->state_root is zero).
 *               Use rx_compute_state_root at checkpoint boundaries.
 */
RX_API bool rx_execute_block(rx_engine_t *engine,
                             const rx_block_header_t *header,
                             const rx_block_body_t *body,
                             const rx_hash_t *block_hash,
                             bool compute_root,
                             rx_block_result_t *result);

/**
 * Compute state root without executing a block.
 *
 * Uses cached MPT hashes for clean subtrees and only rehashes the
 * dirty paths touched since the last root computation. Cheap when
 * called regularly (e.g. every block at chain tip, every 256–1024
 * blocks during bulk replay).
 *
 * Use when you need the root outside of block execution,
 * e.g. for validation at checkpoint boundaries.
 */
RX_API rx_hash_t rx_compute_state_root(rx_engine_t *engine);

/**
 * Mark every cached MPT hash dirty.
 *
 * The next call to rx_compute_state_root will walk the entire
 * account index and every per-account storage trie, recomputing
 * every node's hash from scratch. Cost is one full state walk —
 * tens of seconds to a few minutes on mainnet-scale state.
 *
 * Most callers don't need this. The incremental hash cache is
 * correct by construction (every state mutation marks its path
 * dirty before the next root computation runs). Reach for it only
 * in defensive scenarios: after a long no-validation replay
 * window (≫1024 blocks without a checkpoint), or before a
 * `rx_engine_save_state` if you want to guarantee the snapshot's
 * header root is recomputed from scratch rather than from the
 * cached values.
 *
 * Recommended cadence is to validate often enough that you never
 * need this — every block at chain tip, no coarser than every
 * 1024 blocks during bulk replay (matches the `--adaptive` flag's
 * loosest interval).
 */
RX_API void rx_invalidate_state_cache(rx_engine_t *engine);

/* TODO: collapse rx_invalidate_state_cache + rx_compute_state_root
 * into a single rx_compute_state_root_full(engine) entry point.
 *
 * Today the "force a from-scratch recompute" pattern is two separate
 * calls, which means two full traversals of the trie:
 *   1. invalidate_all walks every node and flips its dirty bit
 *   2. compute_state_root walks every node again, sees them all
 *      dirty, computes hashes, clears dirty bits
 *
 * Both walks are bound by the same memory-access pattern over the
 * same set of nodes; the per-node work for invalidate (one byte
 * write) is much cheaper than for compute (encode + keccak), but the
 * walk itself dominates either way. Measured: each walk costs
 * ~15 min on mainnet-scale state.
 *
 * A unified entry point would skip the dirty-bit gate inside the
 * recursive helper, recompute every node in a single traversal, and
 * cut the "force full recompute" cost roughly in half. The internal
 * hashing helper already does the right work per node — only the
 * gating predicate needs to change.
 *
 * Once that lands, rx_invalidate_state_cache can be deleted: its
 * only purpose was to set up for the next compute_state_root call,
 * which the unified function would absorb.
 */

/* ========================================================================
 * Block production
 *
 * The library executes blocks — it does NOT select or order transactions.
 * Callers bring a pool, pick txs, choose ordering; the library turns that
 * plus a header template into a fully assembled block: computes tx_root,
 * withdrawals_root, runs execution to fill state_root / receipts_root /
 * logs_bloom / gas_used, finalizes the header RLP, and hashes it.
 *
 * Protocol-formula fields (base_fee, excess_blob_gas, etc.) are the
 * caller's responsibility — they depend on parent-block context the
 * caller already has. Pass them explicitly in rx_build_header_t.
 * ======================================================================== */

/** Header fields the caller supplies when building a block. Only fields
 * the caller is allowed to choose or derive from parent context are here;
 * the library fills state_root, tx_root, receipts_root, withdrawals_root,
 * logs_bloom, and gas_used. */
typedef struct {
    rx_hash_t    parent_hash;       /* hash of block number-1 */
    rx_address_t coinbase;          /* fee recipient */
    uint64_t     number;            /* must equal engine->last_block + 1 */
    uint64_t     gas_limit;         /* per EIP-1559/EIP-7825 constraints */
    uint64_t     timestamp;         /* must be > parent.timestamp */
    uint8_t      extra_data[32];
    size_t       extra_data_len;
    rx_hash_t    prev_randao;       /* mix_hash post-merge */

    /* London+ — required post-London */
    bool         has_base_fee;
    rx_uint256_t base_fee;

    /* Cancun+ — required post-Cancun */
    bool         has_blob_gas;
    uint64_t     blob_gas_used;
    uint64_t     excess_blob_gas;
    bool         has_parent_beacon_root;
    rx_hash_t    parent_beacon_root;

    /* Prague+ (EIP-7685) */
    bool         has_requests_hash;
    rx_hash_t    requests_hash;
} rx_build_header_t;

/** Result of a successful rx_build_block. Caller frees with
 * rx_build_block_result_free. */
typedef struct {
    rx_hash_t    block_hash;         /* keccak256 of final encoded header */
    rx_hash_t    state_root;
    rx_hash_t    transactions_root;
    rx_hash_t    receipts_root;
    rx_hash_t    withdrawals_root;   /* zero if no withdrawals */
    uint8_t      logs_bloom[256];
    uint64_t     gas_used;
    rx_receipt_t *receipts;          /* one per tx */
    size_t       receipt_count;

    /** Full block RLP = RLP([header, txs, uncles=[], withdrawals?]).
     *  Ready to persist, broadcast, or feed back into rx_execute_block_rlp. */
    uint8_t     *block_rlp;
    size_t       block_rlp_len;
} rx_build_block_result_t;

/**
 * Assemble and execute a block.
 *
 * Steps performed internally:
 *   1. Compute transactions_root from the caller's tx list.
 *   2. Compute withdrawals_root if any withdrawals are present.
 *   3. Execute the block (all txs + withdrawals) with compute_root=true.
 *   4. Fill state_root, receipts_root, logs_bloom, gas_used into the header.
 *   5. RLP-encode the final header and hash it to produce block_hash.
 *   6. Register block_hash in the BLOCKHASH ring buffer.
 *   7. Serialize the full block RLP for the caller.
 *
 * After a successful call the state has been committed (as if
 * rx_commit_block was called). To roll back, use rx_revert_block
 * BEFORE calling rx_build_block again.
 *
 * Returns false on any error (execution failure, invalid input, OOM).
 * On failure the engine's last_error is set and the state is reverted.
 *
 * Each transaction is raw bytes: legacy = RLP list, typed (EIP-2718) =
 * type_byte || RLP(payload). The library handles both transparently.
 */
RX_API bool rx_build_block(rx_engine_t *engine,
                           const rx_build_header_t *header_fields,
                           const uint8_t *const *txs,
                           const size_t *tx_lengths,
                           size_t tx_count,
                           const rx_withdrawal_t *withdrawals,
                           size_t withdrawal_count,
                           rx_build_block_result_t *result);

/** Free receipts and block_rlp in a build-block result. */
RX_API void rx_build_block_result_free(rx_build_block_result_t *result);

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

/**
 * Set a block hash in the 256-entry ring buffer.
 *
 * Normally not needed — rx_engine_load_state loads hashes from the
 * .hashes file automatically. Use this only when the .hashes file
 * is missing (e.g. snapshot from external tools) or to override entries.
 */
RX_API void rx_set_block_hash(rx_engine_t *engine, uint64_t block_number,
                              const rx_hash_t *hash);

/* ========================================================================
 * Status
 * ======================================================================== */

/** Get last executed block number. Returns 0 before first block. */
RX_API uint64_t rx_get_block_number(const rx_engine_t *engine);

/* ========================================================================
 * Engine Statistics
 * ======================================================================== */

typedef struct {
    /* State counts */
    uint32_t account_count;         /* total account slots (incl. dead) */
    uint32_t account_live;          /* live accounts */
    uint32_t resource_count;        /* accounts with code/storage */

    /* Memory breakdown (bytes) */
    uint64_t acct_vec_bytes;        /* account vector */
    uint64_t res_vec_bytes;         /* resource vector */
    uint64_t acct_index_bytes;      /* account index (hart arena) */
    uint64_t total_tracked;         /* sum of above */

    /* Storage pool */
    uint64_t pool_data_size;        /* pool virtual high-water (bytes) */
    uint64_t pool_free_bytes;       /* bytes on pool freelists */
    uint64_t pool_file_size;        /* backing file size (bytes) */

    /* Code store */
    uint64_t code_count;            /* unique contracts stored */
    uint64_t code_cache_hits;
    uint64_t code_cache_misses;

    /* Block execution timing (ms, current window) */
    double   exec_ms;
    double   root_ms;
} rx_stats_t;

/** Get engine statistics. */
RX_API rx_stats_t rx_get_stats(const rx_engine_t *engine);

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
 * Message call (eth_call equivalent)
 * ======================================================================== */

/** Message call input. */
typedef struct {
    rx_address_t from;          /* caller (tx.origin + msg.sender) */
    rx_address_t to;            /* target contract (zero = create) */
    rx_uint256_t value;         /* wei to transfer (big-endian) */
    const uint8_t *data;        /* calldata, NULL = empty */
    size_t        data_len;
    uint64_t      gas;          /* gas limit (0 = use block gas limit) */
} rx_call_msg_t;

/** Message call result. */
typedef struct {
    bool      success;          /* true = EVM_SUCCESS, false = revert/error */
    uint64_t  gas_used;         /* gas consumed */
    uint8_t  *output;           /* return data (caller must free) */
    size_t    output_len;
} rx_call_result_t;

/** Free output data inside a call result. */
RX_API void rx_call_result_free(rx_call_result_t *result);

/**
 * Execute a read-only message call against current state (eth_call).
 *
 * Snapshots state before execution and reverts afterward — no state
 * changes are committed. Use for transaction simulation, gas estimation,
 * and view function calls.
 *
 * The block context uses the last executed block's parameters.
 * Returns false only on fatal/internal error.
 */
RX_API bool rx_call(rx_engine_t *engine,
                    const rx_call_msg_t *msg,
                    rx_call_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* ARTEX_H */
