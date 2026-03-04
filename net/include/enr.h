#ifndef ART_NET_ENR_H
#define ART_NET_ENR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * ENR — Ethereum Node Record (EIP-778).
 *
 * A signed, versioned record of a node's network identity and endpoints.
 * Used by Discovery v5 to advertise and discover peers.
 *
 * Format: RLP([signature, seq, k1, v1, k2, v2, ...])
 *   - signature: 64 bytes (secp256k1 compact ECDSA, over keccak256 of content)
 *   - seq: uint64 sequence number (incremented on update)
 *   - key-value pairs: sorted by key, ASCII string keys
 *
 * Identity scheme "v4":
 *   - Key "id" = "v4"
 *   - Key "secp256k1" = 33-byte compressed public key
 *   - Node ID = keccak256(uncompressed_pubkey_64_bytes)
 *
 * Max encoded size: 300 bytes.
 */

#define ENR_MAX_SIZE     300
#define ENR_MAX_PAIRS    16
#define ENR_KEY_MAX_LEN  16

/* Key-value pair */
typedef struct {
    char     key[ENR_KEY_MAX_LEN];
    uint8_t  value[256];
    size_t   value_len;
} enr_pair_t;

/* ENR record */
typedef struct {
    uint64_t    seq;
    enr_pair_t  pairs[ENR_MAX_PAIRS];
    size_t      pair_count;

    /* Cached identity info */
    uint8_t     pubkey[64];        /* uncompressed secp256k1 public key (x || y) */
    uint8_t     node_id[32];       /* keccak256(pubkey) */
    bool        has_identity;

    /* Signature */
    uint8_t     signature[64];     /* compact ECDSA signature */
    bool        has_signature;
} enr_t;

/**
 * Initialize an empty ENR.
 */
void enr_init(enr_t *enr);

/**
 * Set a key-value pair. Replaces existing key if present.
 * Keys must be sorted; caller is responsible for order or call enr_sort().
 *
 * @return true on success, false if too many pairs
 */
bool enr_set(enr_t *enr, const char *key,
             const uint8_t *value, size_t value_len);

/**
 * Get a key-value pair by key.
 *
 * @param value_out   Output: pointer to value data (not a copy)
 * @param value_len   Output: value length
 * @return            true if found
 */
bool enr_get(const enr_t *enr, const char *key,
             const uint8_t **value_out, size_t *value_len);

/**
 * Sort key-value pairs by key (required before signing/encoding).
 */
void enr_sort(enr_t *enr);

/**
 * Set the "v4" identity with a secp256k1 private key.
 * Sets "id" = "v4", "secp256k1" = compressed_pubkey.
 * Derives and caches the public key and node ID.
 *
 * @param priv  32-byte secp256k1 private key
 * @return      true on success
 */
bool enr_set_v4_identity(enr_t *enr, const uint8_t priv[32]);

/**
 * Sign the ENR with the given private key.
 * Must have identity set and pairs sorted.
 *
 * @param priv  32-byte secp256k1 private key
 * @return      true on success
 */
bool enr_sign(enr_t *enr, const uint8_t priv[32]);

/**
 * Encode the ENR to RLP.
 *
 * @param out      Output buffer (must be >= ENR_MAX_SIZE)
 * @param out_len  Output: encoded length
 * @return         true on success (including size check)
 */
bool enr_encode(const enr_t *enr, uint8_t *out, size_t *out_len);

/**
 * Decode an ENR from RLP.
 *
 * @param enr    Output: decoded ENR
 * @param data   RLP-encoded ENR
 * @param len    Length of data
 * @return       true on success
 */
bool enr_decode(enr_t *enr, const uint8_t *data, size_t len);

/**
 * Verify the ENR signature.
 * Recovers the public key from signature and verifies it matches
 * the "secp256k1" key in the record.
 *
 * @return true if signature is valid
 */
bool enr_verify(const enr_t *enr);

/**
 * Get the 32-byte Node ID (keccak256 of uncompressed pubkey).
 * Requires identity to be set (via decode or set_v4_identity).
 *
 * @param node_id  Output: 32-byte node ID
 * @return         true if identity is available
 */
bool enr_node_id(const enr_t *enr, uint8_t node_id[32]);

/**
 * Set IP address and port fields.
 */
void enr_set_ip4(enr_t *enr, uint32_t ip, uint16_t udp_port, uint16_t tcp_port);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_ENR_H */
