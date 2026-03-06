#ifndef ART_NET_DISCV5_SESSION_H
#define ART_NET_DISCV5_SESSION_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "discv5_codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Discv5 Session Management — handshake state + LRU session cache.
 *
 * Lifecycle:
 *   1. Outbound message to unknown node → RANDOM packet + WHOAREYOU_SENT
 *   2. Receive WHOAREYOU → generate handshake, derive keys → ESTABLISHED
 *   3. Receive handshake (we sent WHOAREYOU) → verify, derive keys → ESTABLISHED
 *   4. Established sessions encrypt/decrypt ordinary messages
 *
 * Key direction:
 *   - Initiator (sent first message) encrypts with initiator_key
 *   - Recipient (sent WHOAREYOU) encrypts with recipient_key
 */

/* =========================================================================
 * Session state
 * ========================================================================= */

typedef enum {
    DISCV5_SESS_NONE = 0,
    DISCV5_SESS_WHOAREYOU_SENT,  /* We sent WHOAREYOU, awaiting handshake */
    DISCV5_SESS_ESTABLISHED,     /* Handshake complete, keys ready */
} discv5_session_state_t;

typedef struct {
    uint8_t  node_id[32];
    discv5_session_state_t state;
    discv5_keys_t keys;
    bool     is_initiator;  /* true = we sent the first ordinary message */

    /* Challenge data stored when we send WHOAREYOU (used to verify handshake) */
    uint8_t  challenge_data[128];
    size_t   challenge_data_len;

    uint64_t last_used;     /* LRU clock tick */
} discv5_session_t;

/* =========================================================================
 * Session cache (LRU, flat array)
 * ========================================================================= */

typedef struct {
    discv5_session_t *entries;
    size_t capacity;
    size_t count;
    uint64_t clock;
} discv5_session_cache_t;

/**
 * Initialize session cache.
 * @param capacity  Maximum number of sessions (e.g. 1000)
 */
bool discv5_session_cache_init(discv5_session_cache_t *cache, size_t capacity);

/** Free session cache. */
void discv5_session_cache_destroy(discv5_session_cache_t *cache);

/**
 * Find an existing session by node ID.
 * Updates LRU timestamp. Returns NULL if not found.
 */
discv5_session_t *discv5_session_find(discv5_session_cache_t *cache,
                                       const uint8_t node_id[32]);

/**
 * Get or create a session for a node ID.
 * If cache is full, evicts the least recently used entry.
 * Returns the session (never NULL unless allocation failed).
 */
discv5_session_t *discv5_session_get_or_create(discv5_session_cache_t *cache,
                                                const uint8_t node_id[32]);

/** Remove a session by node ID. Returns true if found and removed. */
bool discv5_session_remove(discv5_session_cache_t *cache,
                            const uint8_t node_id[32]);

/* =========================================================================
 * Session operations
 * ========================================================================= */

/**
 * Mark session as WHOAREYOU_SENT and store challenge data.
 * Call this after sending a WHOAREYOU packet.
 *
 * @param challenge_data  masking-iv || unmasked-header of the WHOAREYOU we sent
 */
void discv5_session_set_whoareyou(discv5_session_t *session,
                                   const uint8_t *challenge_data, size_t cd_len);

/**
 * Establish session with derived keys.
 * Call after successful handshake (either direction).
 *
 * @param is_initiator  true if we sent the first ordinary message
 */
void discv5_session_establish(discv5_session_t *session,
                               const discv5_keys_t *keys,
                               bool is_initiator);

/** Get the key for encrypting outbound messages. */
const uint8_t *discv5_session_encrypt_key(const discv5_session_t *session);

/** Get the key for decrypting inbound messages. */
const uint8_t *discv5_session_decrypt_key(const discv5_session_t *session);

/* =========================================================================
 * ID nonce verification
 * ========================================================================= */

/**
 * Verify an ID nonce signature from a received handshake.
 *
 * Verifies: secp256k1_verify(SHA256("discovery v5 identity proof"
 *           || challenge_data || eph_pubkey || node_id_b), sig, remote_pub)
 *
 * @param sig             64-byte signature from handshake authdata
 * @param challenge_data  Challenge data (masking-iv || header of WHOAREYOU)
 * @param cd_len          Challenge data length
 * @param eph_pubkey      33-byte compressed ephemeral pubkey from handshake
 * @param node_id_b       32-byte node ID of the verifier (us)
 * @param remote_pub      64-byte uncompressed public key of the signer
 */
bool discv5_verify_id_nonce(const uint8_t sig[64],
                             const uint8_t *challenge_data, size_t cd_len,
                             const uint8_t eph_pubkey[33],
                             const uint8_t node_id_b[32],
                             const uint8_t remote_pub[64]);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_DISCV5_SESSION_H */
