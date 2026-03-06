/*
 * Discv5 Session Management — handshake state + LRU session cache.
 */

#include "../include/discv5_session.h"
#include "../include/secp256k1_wrap.h"
#include <blst.h>
#include <stdlib.h>
#include <string.h>

static const char ID_SIGN_PREFIX[] = "discovery v5 identity proof";
#define ID_SIGN_PREFIX_LEN 27

/* =========================================================================
 * Session cache
 * ========================================================================= */

bool discv5_session_cache_init(discv5_session_cache_t *cache, size_t capacity) {
    cache->entries = calloc(capacity, sizeof(discv5_session_t));
    if (!cache->entries) return false;
    cache->capacity = capacity;
    cache->count = 0;
    cache->clock = 0;
    return true;
}

void discv5_session_cache_destroy(discv5_session_cache_t *cache) {
    free(cache->entries);
    cache->entries = NULL;
    cache->capacity = 0;
    cache->count = 0;
}

discv5_session_t *discv5_session_find(discv5_session_cache_t *cache,
                                       const uint8_t node_id[32]) {
    for (size_t i = 0; i < cache->count; i++) {
        if (memcmp(cache->entries[i].node_id, node_id, 32) == 0) {
            cache->entries[i].last_used = ++cache->clock;
            return &cache->entries[i];
        }
    }
    return NULL;
}

discv5_session_t *discv5_session_get_or_create(discv5_session_cache_t *cache,
                                                const uint8_t node_id[32]) {
    /* Check if already exists */
    discv5_session_t *s = discv5_session_find(cache, node_id);
    if (s) return s;

    /* Use empty slot if available */
    if (cache->count < cache->capacity) {
        s = &cache->entries[cache->count++];
        memset(s, 0, sizeof(*s));
        memcpy(s->node_id, node_id, 32);
        s->last_used = ++cache->clock;
        return s;
    }

    /* Evict LRU entry */
    size_t lru_idx = 0;
    uint64_t lru_time = cache->entries[0].last_used;
    for (size_t i = 1; i < cache->count; i++) {
        if (cache->entries[i].last_used < lru_time) {
            lru_time = cache->entries[i].last_used;
            lru_idx = i;
        }
    }

    s = &cache->entries[lru_idx];
    memset(s, 0, sizeof(*s));
    memcpy(s->node_id, node_id, 32);
    s->last_used = ++cache->clock;
    return s;
}

bool discv5_session_remove(discv5_session_cache_t *cache,
                            const uint8_t node_id[32]) {
    for (size_t i = 0; i < cache->count; i++) {
        if (memcmp(cache->entries[i].node_id, node_id, 32) == 0) {
            /* Swap with last entry */
            if (i < cache->count - 1)
                cache->entries[i] = cache->entries[cache->count - 1];
            cache->count--;
            return true;
        }
    }
    return false;
}

/* =========================================================================
 * Session operations
 * ========================================================================= */

void discv5_session_set_whoareyou(discv5_session_t *session,
                                   const uint8_t *challenge_data, size_t cd_len) {
    session->state = DISCV5_SESS_WHOAREYOU_SENT;
    if (cd_len > sizeof(session->challenge_data))
        cd_len = sizeof(session->challenge_data);
    memcpy(session->challenge_data, challenge_data, cd_len);
    session->challenge_data_len = cd_len;
}

void discv5_session_establish(discv5_session_t *session,
                               const discv5_keys_t *keys,
                               bool is_initiator) {
    session->state = DISCV5_SESS_ESTABLISHED;
    session->keys = *keys;
    session->is_initiator = is_initiator;
}

const uint8_t *discv5_session_encrypt_key(const discv5_session_t *session) {
    /* Initiator encrypts with initiator_key, recipient with recipient_key */
    return session->is_initiator
        ? session->keys.initiator_key
        : session->keys.recipient_key;
}

const uint8_t *discv5_session_decrypt_key(const discv5_session_t *session) {
    /* Initiator decrypts with recipient_key, recipient with initiator_key */
    return session->is_initiator
        ? session->keys.recipient_key
        : session->keys.initiator_key;
}

/* =========================================================================
 * ID nonce verification
 * ========================================================================= */

bool discv5_verify_id_nonce(const uint8_t sig[64],
                             const uint8_t *challenge_data, size_t cd_len,
                             const uint8_t eph_pubkey[33],
                             const uint8_t node_id_b[32],
                             const uint8_t remote_pub[64]) {
    /* Build id-signature-input:
     * "discovery v5 identity proof" || challenge_data || eph_pubkey || node_id_b */
    size_t input_len = ID_SIGN_PREFIX_LEN + cd_len + 33 + 32;
    uint8_t input_stack[512];
    uint8_t *input = (input_len <= sizeof(input_stack))
                      ? input_stack : NULL;
    if (!input) return false;

    memcpy(input, ID_SIGN_PREFIX, ID_SIGN_PREFIX_LEN);
    memcpy(input + ID_SIGN_PREFIX_LEN, challenge_data, cd_len);
    memcpy(input + ID_SIGN_PREFIX_LEN + cd_len, eph_pubkey, 33);
    memcpy(input + ID_SIGN_PREFIX_LEN + cd_len + 33, node_id_b, 32);

    /* SHA-256 hash */
    uint8_t hash[32];
    blst_sha256(hash, input, input_len);

    /* Verify ECDSA signature against known public key */
    return secp256k1_wrap_verify(sig, hash, remote_pub);
}
