#include "verkle_state.h"
#include "verkle_key.h"
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Backend Dispatch Helpers
 * ========================================================================= */

static bool vs_set(verkle_state_t *vs,
                   const uint8_t key[32],
                   const uint8_t value[32])
{
    if (vs->type == VS_BACKEND_TREE)
        return verkle_set(vs->tree, key, value);
    return verkle_flat_set(vs->flat, key, value);
}

static bool vs_get(const verkle_state_t *vs,
                   const uint8_t key[32],
                   uint8_t value[32])
{
    if (vs->type == VS_BACKEND_TREE)
        return verkle_get(vs->tree, key, value);
    return verkle_flat_get(vs->flat, key, value);
}

static void vs_root_hash(const verkle_state_t *vs, uint8_t out[32])
{
    if (vs->type == VS_BACKEND_TREE)
        verkle_root_hash(vs->tree, out);
    else
        verkle_flat_root_hash(vs->flat, out);
}

/* =========================================================================
 * Basic Data Helpers (EIP-6800 packed format)
 *
 * Layout within the 32-byte value at suffix 0:
 *   [0]      version     (1 byte)
 *   [1..4]   reserved    (4 bytes, zero)
 *   [5..7]   code_size   (3 bytes, BE uint24, max 16777215)
 *   [8..15]  nonce       (8 bytes, BE uint64)
 *   [16..31] balance     (16 bytes, BE uint128)
 * ========================================================================= */

/** Read the 32-byte packed basic data for an address. Returns zeros if absent. */
static void get_basic_data(const verkle_state_t *vs,
                            const uint8_t addr[20],
                            uint8_t data[32])
{
    uint8_t key[32];
    verkle_account_basic_data_key(key, addr);
    if (!vs_get(vs, key, data))
        memset(data, 0, 32);
}

/** Write the 32-byte packed basic data for an address. */
static void set_basic_data(verkle_state_t *vs,
                            const uint8_t addr[20],
                            const uint8_t data[32])
{
    uint8_t key[32];
    verkle_account_basic_data_key(key, addr);
    vs_set(vs, key, data);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

verkle_state_t *verkle_state_create(void) {
    verkle_state_t *vs = calloc(1, sizeof(verkle_state_t));
    if (!vs) return NULL;
    vs->type = VS_BACKEND_TREE;
    vs->tree = verkle_create();
    if (!vs->tree) { free(vs); return NULL; }
    return vs;
}

verkle_state_t *verkle_state_create_flat(const char *value_dir,
                                          const char *commit_dir)
{
    verkle_state_t *vs = calloc(1, sizeof(verkle_state_t));
    if (!vs) return NULL;
    vs->type = VS_BACKEND_FLAT;
    vs->flat = verkle_flat_create(value_dir, commit_dir);
    if (!vs->flat) { free(vs); return NULL; }
    return vs;
}

verkle_state_t *verkle_state_open_flat(const char *value_dir,
                                        const char *commit_dir)
{
    verkle_state_t *vs = calloc(1, sizeof(verkle_state_t));
    if (!vs) return NULL;
    vs->type = VS_BACKEND_FLAT;
    vs->flat = verkle_flat_open(value_dir, commit_dir);
    if (!vs->flat) { free(vs); return NULL; }
    return vs;
}

void verkle_state_destroy(verkle_state_t *vs) {
    if (!vs) return;
    if (vs->type == VS_BACKEND_TREE)
        verkle_destroy(vs->tree);
    else
        verkle_flat_destroy(vs->flat);
    free(vs);
}

/* =========================================================================
 * Backend Accessors
 * ========================================================================= */

verkle_tree_t *verkle_state_get_tree(verkle_state_t *vs) {
    return (vs->type == VS_BACKEND_TREE) ? vs->tree : NULL;
}

verkle_flat_t *verkle_state_get_flat(verkle_state_t *vs) {
    return (vs->type == VS_BACKEND_FLAT) ? vs->flat : NULL;
}

/* =========================================================================
 * Block Operations
 * ========================================================================= */

bool verkle_state_begin_block(verkle_state_t *vs, uint64_t block_number) {
    if (vs->type == VS_BACKEND_FLAT)
        return verkle_flat_begin_block(vs->flat, block_number);
    return true;
}

bool verkle_state_commit_block(verkle_state_t *vs) {
    if (vs->type == VS_BACKEND_FLAT)
        return verkle_flat_commit_block(vs->flat);
    return true;
}

bool verkle_state_revert_block(verkle_state_t *vs) {
    if (vs->type == VS_BACKEND_FLAT)
        return verkle_flat_revert_block(vs->flat);
    return true;
}

void verkle_state_sync(verkle_state_t *vs) {
    if (vs->type == VS_BACKEND_FLAT)
        verkle_flat_sync(vs->flat);
}

/* =========================================================================
 * Version (from packed basic data byte[0])
 * ========================================================================= */

uint8_t verkle_state_get_version(verkle_state_t *vs,
                                 const uint8_t addr[20])
{
    uint8_t data[32];
    get_basic_data(vs, addr, data);
    return data[VERKLE_BASIC_DATA_VERSION_OFFSET];
}

void verkle_state_set_version(verkle_state_t *vs,
                              const uint8_t addr[20],
                              uint8_t version)
{
    uint8_t data[32];
    get_basic_data(vs, addr, data);
    data[VERKLE_BASIC_DATA_VERSION_OFFSET] = version;
    set_basic_data(vs, addr, data);
}

/* =========================================================================
 * Nonce (from packed basic data bytes[8..15], BE uint64)
 * ========================================================================= */

uint64_t verkle_state_get_nonce(verkle_state_t *vs,
                                const uint8_t addr[20])
{
    uint8_t data[32];
    get_basic_data(vs, addr, data);

    /* Read 8-byte BE nonce from offset 8 */
    uint64_t nonce = 0;
    for (int i = 0; i < 8; i++)
        nonce = (nonce << 8) | data[VERKLE_BASIC_DATA_NONCE_OFFSET + i];
    return nonce;
}

void verkle_state_set_nonce(verkle_state_t *vs,
                            const uint8_t addr[20],
                            uint64_t nonce)
{
    uint8_t data[32];
    get_basic_data(vs, addr, data);

    /* Write 8-byte BE nonce at offset 8 */
    for (int i = 7; i >= 0; i--) {
        data[VERKLE_BASIC_DATA_NONCE_OFFSET + i] = (uint8_t)(nonce & 0xFF);
        nonce >>= 8;
    }
    set_basic_data(vs, addr, data);
}

/* =========================================================================
 * Balance (from packed basic data bytes[16..31], BE uint128)
 *
 * API: caller passes 32-byte LE buffer. Only low 16 bytes (uint128) stored.
 * ========================================================================= */

void verkle_state_get_balance(verkle_state_t *vs,
                              const uint8_t addr[20],
                              uint8_t balance[32])
{
    uint8_t data[32];
    get_basic_data(vs, addr, data);

    /* Convert 16-byte BE (data[16..31]) to 32-byte LE */
    memset(balance, 0, 32);
    for (int i = 0; i < VERKLE_BASIC_DATA_BALANCE_SIZE; i++)
        balance[i] = data[31 - i];
}

void verkle_state_set_balance(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t balance[32])
{
    uint8_t data[32];
    get_basic_data(vs, addr, data);

    /* Convert 32-byte LE to 16-byte BE at data[16..31] */
    for (int i = 0; i < VERKLE_BASIC_DATA_BALANCE_SIZE; i++)
        data[31 - i] = balance[i];

    set_basic_data(vs, addr, data);
}

/* =========================================================================
 * Code Hash (separate value at suffix 1, raw 32 bytes)
 * ========================================================================= */

void verkle_state_get_code_hash(verkle_state_t *vs,
                                const uint8_t addr[20],
                                uint8_t hash[32])
{
    uint8_t key[32];
    verkle_account_code_hash_key(key, addr);
    if (!vs_get(vs, key, hash))
        memset(hash, 0, 32);
}

void verkle_state_set_code_hash(verkle_state_t *vs,
                                const uint8_t addr[20],
                                const uint8_t hash[32])
{
    uint8_t key[32];
    verkle_account_code_hash_key(key, addr);
    vs_set(vs, key, hash);
}

/* =========================================================================
 * Code Size (from packed basic data bytes[5..7], BE uint24)
 * ========================================================================= */

uint64_t verkle_state_get_code_size(verkle_state_t *vs,
                                    const uint8_t addr[20])
{
    uint8_t data[32];
    get_basic_data(vs, addr, data);

    /* 3-byte BE uint24 at offset 5 */
    return ((uint64_t)data[5] << 16) |
           ((uint64_t)data[6] << 8) |
            (uint64_t)data[7];
}

void verkle_state_set_code_size(verkle_state_t *vs,
                                const uint8_t addr[20],
                                uint64_t size)
{
    uint8_t data[32];
    get_basic_data(vs, addr, data);

    /* Write 3-byte BE uint24 at offset 5 (truncates to 24 bits) */
    data[5] = (uint8_t)((size >> 16) & 0xFF);
    data[6] = (uint8_t)((size >> 8) & 0xFF);
    data[7] = (uint8_t)(size & 0xFF);

    set_basic_data(vs, addr, data);
}

/* =========================================================================
 * Code (31-byte chunks with PUSHDATA prefix, EIP-6800)
 * ========================================================================= */

/**
 * Chunkify bytecode into 31-byte chunks with PUSHDATA prefix.
 * Each chunk is 32 bytes: [pushdata_prefix][31 bytes of code].
 * The prefix indicates how many leading bytes are PUSH operand data
 * spilling from a previous chunk.
 */
static uint32_t chunkify_code(const uint8_t *code, uint64_t len,
                               uint8_t **chunks_out)
{
    /* Pad to multiple of 31 */
    uint64_t padded = len;
    if (padded % 31 != 0)
        padded += (31 - (padded % 31));

    uint32_t num_chunks = (uint32_t)(padded / 31);

    /* Allocate padded code buffer */
    uint8_t *padded_code = calloc(padded, 1);
    if (!padded_code) { *chunks_out = NULL; return 0; }
    memcpy(padded_code, code, len);

    /* Compute bytes_to_exec_data: for each byte position, how many bytes
     * of PUSH operand data remain at and after that position. */
    uint8_t *btd = calloc(padded + 32, 1);
    if (!btd) { free(padded_code); *chunks_out = NULL; return 0; }

    uint64_t pos = 0;
    while (pos < len) {
        uint8_t op = padded_code[pos];
        int pushdata = 0;
        if (op >= 0x60 && op <= 0x7F)   /* PUSH1..PUSH32 */
            pushdata = op - 0x5F;
        pos++;
        for (int x = 0; x < pushdata && (pos + (uint64_t)x) < padded + 32; x++)
            btd[pos + x] = (uint8_t)(pushdata - x);
        pos += (uint64_t)pushdata;
    }

    /* Build chunks: byte 0 = min(btd[chunk_start], 31), bytes 1-31 = code slice */
    uint8_t *chunks = malloc((uint64_t)num_chunks * 32);
    if (!chunks) { free(padded_code); free(btd); *chunks_out = NULL; return 0; }

    for (uint32_t i = 0; i < num_chunks; i++) {
        uint64_t start = (uint64_t)i * 31;
        uint8_t prefix = btd[start];
        if (prefix > 31) prefix = 31;
        chunks[i * 32] = prefix;
        memcpy(chunks + i * 32 + 1, padded_code + start, 31);
    }

    free(padded_code);
    free(btd);
    *chunks_out = chunks;
    return num_chunks;
}

bool verkle_state_set_code(verkle_state_t *vs,
                           const uint8_t addr[20],
                           const uint8_t *bytecode,
                           uint64_t len)
{
    verkle_state_set_code_size(vs, addr, len);

    if (len == 0) return true;

    uint8_t *chunks = NULL;
    uint32_t num_chunks = chunkify_code(bytecode, len, &chunks);
    if (!chunks) return false;

    for (uint32_t i = 0; i < num_chunks; i++) {
        uint8_t key[32];
        verkle_code_chunk_key(key, addr, i);
        if (!vs_set(vs, key, chunks + i * 32)) {
            free(chunks);
            return false;
        }
    }

    free(chunks);
    return true;
}

uint64_t verkle_state_get_code(verkle_state_t *vs,
                               const uint8_t addr[20],
                               uint8_t *out,
                               uint64_t max_len)
{
    uint64_t code_size = verkle_state_get_code_size(vs, addr);
    if (code_size == 0) return 0;

    uint64_t read_len = code_size < max_len ? code_size : max_len;
    /* Each chunk stores 31 bytes of code (byte 0 is prefix) */
    uint32_t num_chunks = (uint32_t)((read_len + 30) / 31);

    uint64_t written = 0;
    for (uint32_t i = 0; i < num_chunks; i++) {
        uint8_t key[32], chunk[32];
        verkle_code_chunk_key(key, addr, i);
        if (!vs_get(vs, key, chunk))
            break;

        /* Skip byte 0 (pushdata prefix), copy bytes 1-31 */
        uint64_t remaining = read_len - written;
        uint64_t copy_len = remaining < 31 ? remaining : 31;
        memcpy(out + written, chunk + 1, copy_len);
        written += copy_len;
    }
    return written;
}

/* =========================================================================
 * Storage
 * ========================================================================= */

void verkle_state_get_storage(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t slot[32],
                              uint8_t value[32])
{
    uint8_t key[32];
    verkle_storage_key(key, addr, slot);
    if (!vs_get(vs, key, value))
        memset(value, 0, 32);
}

void verkle_state_set_storage(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t slot[32],
                              const uint8_t value[32])
{
    uint8_t key[32];
    verkle_storage_key(key, addr, slot);
    vs_set(vs, key, value);
}

/* =========================================================================
 * Account Existence
 * ========================================================================= */

bool verkle_state_exists(verkle_state_t *vs, const uint8_t addr[20])
{
    uint8_t key[32], value[32];

    /* Check basic data (suffix 0) */
    verkle_account_basic_data_key(key, addr);
    if (vs_get(vs, key, value)) return true;

    /* Check code hash (suffix 1) */
    verkle_account_code_hash_key(key, addr);
    if (vs_get(vs, key, value)) return true;

    return false;
}

/* =========================================================================
 * Root
 * ========================================================================= */

void verkle_state_root_hash(const verkle_state_t *vs, uint8_t out[32])
{
    vs_root_hash(vs, out);
}
