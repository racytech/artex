#include "verkle_state.h"
#include "verkle_key.h"
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

verkle_state_t *verkle_state_create(void) {
    verkle_state_t *vs = calloc(1, sizeof(verkle_state_t));
    if (!vs) return NULL;
    vs->tree = verkle_create();
    if (!vs->tree) { free(vs); return NULL; }
    return vs;
}

void verkle_state_destroy(verkle_state_t *vs) {
    if (!vs) return;
    verkle_destroy(vs->tree);
    free(vs);
}

/* =========================================================================
 * Version
 * ========================================================================= */

uint8_t verkle_state_get_version(verkle_state_t *vs,
                                 const uint8_t addr[20])
{
    uint8_t key[32], value[32];
    verkle_account_version_key(key, addr);
    if (!verkle_get(vs->tree, key, value))
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
    verkle_set(vs->tree, key, value);
}

/* =========================================================================
 * Nonce
 * ========================================================================= */

uint64_t verkle_state_get_nonce(verkle_state_t *vs,
                                const uint8_t addr[20])
{
    uint8_t key[32], value[32];
    verkle_account_nonce_key(key, addr);
    if (!verkle_get(vs->tree, key, value))
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
    verkle_set(vs->tree, key, value);
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
    if (!verkle_get(vs->tree, key, balance))
        memset(balance, 0, 32);
}

void verkle_state_set_balance(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t balance[32])
{
    uint8_t key[32];
    verkle_account_balance_key(key, addr);
    verkle_set(vs->tree, key, balance);
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
    if (!verkle_get(vs->tree, key, hash))
        memset(hash, 0, 32);
}

void verkle_state_set_code_hash(verkle_state_t *vs,
                                const uint8_t addr[20],
                                const uint8_t hash[32])
{
    uint8_t key[32];
    verkle_account_code_hash_key(key, addr);
    verkle_set(vs->tree, key, hash);
}

/* =========================================================================
 * Code Size
 * ========================================================================= */

uint64_t verkle_state_get_code_size(verkle_state_t *vs,
                                    const uint8_t addr[20])
{
    uint8_t key[32], value[32];
    verkle_account_code_size_key(key, addr);
    if (!verkle_get(vs->tree, key, value))
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
    verkle_set(vs->tree, key, value);
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
        if (!verkle_set(vs->tree, key, value))
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
        if (!verkle_get(vs->tree, key, value))
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
    if (!verkle_get(vs->tree, key, value))
        memset(value, 0, 32);
}

void verkle_state_set_storage(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t slot[32],
                              const uint8_t value[32])
{
    uint8_t key[32];
    verkle_storage_key(key, addr, slot);
    verkle_set(vs->tree, key, value);
}

/* =========================================================================
 * Account Existence
 * ========================================================================= */

bool verkle_state_exists(verkle_state_t *vs, const uint8_t addr[20])
{
    uint8_t key[32], value[32];

    verkle_account_version_key(key, addr);
    if (verkle_get(vs->tree, key, value)) return true;

    verkle_account_nonce_key(key, addr);
    if (verkle_get(vs->tree, key, value)) return true;

    verkle_account_balance_key(key, addr);
    if (verkle_get(vs->tree, key, value)) return true;

    verkle_account_code_hash_key(key, addr);
    if (verkle_get(vs->tree, key, value)) return true;

    verkle_account_code_size_key(key, addr);
    if (verkle_get(vs->tree, key, value)) return true;

    return false;
}

/* =========================================================================
 * Root
 * ========================================================================= */

void verkle_state_root_hash(const verkle_state_t *vs, uint8_t out[32])
{
    verkle_root_hash(vs->tree, out);
}
