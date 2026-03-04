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
    return true;  /* tree: no-op */
}

bool verkle_state_commit_block(verkle_state_t *vs) {
    if (vs->type == VS_BACKEND_FLAT)
        return verkle_flat_commit_block(vs->flat);
    return true;  /* tree: no-op */
}

bool verkle_state_revert_block(verkle_state_t *vs) {
    if (vs->type == VS_BACKEND_FLAT)
        return verkle_flat_revert_block(vs->flat);
    return true;  /* tree: no-op */
}

void verkle_state_sync(verkle_state_t *vs) {
    if (vs->type == VS_BACKEND_FLAT)
        verkle_flat_sync(vs->flat);
    /* tree: no-op */
}

/* =========================================================================
 * Version
 * ========================================================================= */

uint8_t verkle_state_get_version(verkle_state_t *vs,
                                 const uint8_t addr[20])
{
    uint8_t key[32], value[32];
    verkle_account_version_key(key, addr);
    if (!vs_get(vs, key, value))
        return 0;
    return value[0];
}

void verkle_state_set_version(verkle_state_t *vs,
                              const uint8_t addr[20],
                              uint8_t version)
{
    uint8_t key[32], value[32];
    memset(value, 0, 32);
    value[0] = version;
    verkle_account_version_key(key, addr);
    vs_set(vs, key, value);
}

/* =========================================================================
 * Nonce
 * ========================================================================= */

uint64_t verkle_state_get_nonce(verkle_state_t *vs,
                                const uint8_t addr[20])
{
    uint8_t key[32], value[32];
    verkle_account_nonce_key(key, addr);
    if (!vs_get(vs, key, value))
        return 0;
    uint64_t nonce;
    memcpy(&nonce, value, sizeof(nonce));
    return nonce;
}

void verkle_state_set_nonce(verkle_state_t *vs,
                            const uint8_t addr[20],
                            uint64_t nonce)
{
    uint8_t key[32], value[32];
    memset(value, 0, 32);
    memcpy(value, &nonce, sizeof(nonce));
    verkle_account_nonce_key(key, addr);
    vs_set(vs, key, value);
}

/* =========================================================================
 * Balance
 * ========================================================================= */

void verkle_state_get_balance(verkle_state_t *vs,
                              const uint8_t addr[20],
                              uint8_t balance[32])
{
    uint8_t key[32];
    verkle_account_balance_key(key, addr);
    if (!vs_get(vs, key, balance))
        memset(balance, 0, 32);
}

void verkle_state_set_balance(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t balance[32])
{
    uint8_t key[32];
    verkle_account_balance_key(key, addr);
    vs_set(vs, key, balance);
}

/* =========================================================================
 * Code Hash
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
 * Code Size
 * ========================================================================= */

uint64_t verkle_state_get_code_size(verkle_state_t *vs,
                                    const uint8_t addr[20])
{
    uint8_t key[32], value[32];
    verkle_account_code_size_key(key, addr);
    if (!vs_get(vs, key, value))
        return 0;
    uint64_t size;
    memcpy(&size, value, sizeof(size));
    return size;
}

void verkle_state_set_code_size(verkle_state_t *vs,
                                const uint8_t addr[20],
                                uint64_t size)
{
    uint8_t key[32], value[32];
    memset(value, 0, 32);
    memcpy(value, &size, sizeof(size));
    verkle_account_code_size_key(key, addr);
    vs_set(vs, key, value);
}

/* =========================================================================
 * Code
 * ========================================================================= */

bool verkle_state_set_code(verkle_state_t *vs,
                           const uint8_t addr[20],
                           const uint8_t *bytecode,
                           uint64_t len)
{
    verkle_state_set_code_size(vs, addr, len);

    uint32_t num_chunks = (uint32_t)((len + 31) / 32);
    for (uint32_t i = 0; i < num_chunks; i++) {
        uint8_t key[32], value[32];
        memset(value, 0, 32);
        uint64_t offset = (uint64_t)i * 32;
        uint64_t remaining = len - offset;
        uint64_t copy_len = remaining < 32 ? remaining : 32;
        memcpy(value, bytecode + offset, copy_len);
        verkle_code_chunk_key(key, addr, i);
        if (!vs_set(vs, key, value))
            return false;
    }
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
    uint32_t num_chunks = (uint32_t)((read_len + 31) / 32);

    for (uint32_t i = 0; i < num_chunks; i++) {
        uint8_t key[32], value[32];
        verkle_code_chunk_key(key, addr, i);
        if (!vs_get(vs, key, value))
            break;
        uint64_t offset = (uint64_t)i * 32;
        uint64_t remaining = read_len - offset;
        uint64_t copy_len = remaining < 32 ? remaining : 32;
        memcpy(out + offset, value, copy_len);
    }
    return read_len;
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

    verkle_account_version_key(key, addr);
    if (vs_get(vs, key, value)) return true;

    verkle_account_nonce_key(key, addr);
    if (vs_get(vs, key, value)) return true;

    verkle_account_balance_key(key, addr);
    if (vs_get(vs, key, value)) return true;

    verkle_account_code_hash_key(key, addr);
    if (vs_get(vs, key, value)) return true;

    verkle_account_code_size_key(key, addr);
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
