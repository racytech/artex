/*
 * ENR — Ethereum Node Record (EIP-778).
 *
 * Signed, versioned key-value record for node identity and endpoints.
 * Format: RLP([signature, seq, k1, v1, k2, v2, ...])
 *
 * Identity scheme "v4":
 *   signature = secp256k1 ECDSA over keccak256(content)
 *   content   = RLP([seq, k1, v1, k2, v2, ...])
 *   node_id   = keccak256(uncompressed_pubkey_64)
 */

#include "../include/enr.h"
#include "../include/secp256k1_wrap.h"
#include "../../common/include/rlp.h"
#include "../../common/include/hash.h"
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * Basic operations
 * ========================================================================= */

void enr_init(enr_t *enr) {
    memset(enr, 0, sizeof(*enr));
}

bool enr_set(enr_t *enr, const char *key,
             const uint8_t *value, size_t value_len) {
    if (value_len > sizeof(enr->pairs[0].value))
        return false;

    /* Replace existing key */
    for (size_t i = 0; i < enr->pair_count; i++) {
        if (strcmp(enr->pairs[i].key, key) == 0) {
            memcpy(enr->pairs[i].value, value, value_len);
            enr->pairs[i].value_len = value_len;
            return true;
        }
    }

    /* Add new pair */
    if (enr->pair_count >= ENR_MAX_PAIRS)
        return false;

    enr_pair_t *p = &enr->pairs[enr->pair_count++];
    size_t klen = strlen(key);
    if (klen >= ENR_KEY_MAX_LEN) klen = ENR_KEY_MAX_LEN - 1;
    memcpy(p->key, key, klen);
    p->key[klen] = '\0';
    memcpy(p->value, value, value_len);
    p->value_len = value_len;
    return true;
}

bool enr_get(const enr_t *enr, const char *key,
             const uint8_t **value_out, size_t *value_len) {
    for (size_t i = 0; i < enr->pair_count; i++) {
        if (strcmp(enr->pairs[i].key, key) == 0) {
            if (value_out) *value_out = enr->pairs[i].value;
            if (value_len) *value_len = enr->pairs[i].value_len;
            return true;
        }
    }
    return false;
}

static int pair_cmp(const void *a, const void *b) {
    return strcmp(((const enr_pair_t *)a)->key,
                 ((const enr_pair_t *)b)->key);
}

void enr_sort(enr_t *enr) {
    if (enr->pair_count > 1)
        qsort(enr->pairs, enr->pair_count, sizeof(enr_pair_t), pair_cmp);
}

/* =========================================================================
 * Identity
 * ========================================================================= */

bool enr_set_v4_identity(enr_t *enr, const uint8_t priv[32]) {
    /* Derive uncompressed public key */
    if (!secp256k1_wrap_pubkey_create(enr->pubkey, priv))
        return false;

    /* Compress for the record */
    uint8_t compressed[33];
    if (!secp256k1_wrap_compress(compressed, enr->pubkey))
        return false;

    /* Node ID = keccak256(uncompressed_pubkey_64) */
    hash_t h = hash_keccak256(enr->pubkey, 64);
    memcpy(enr->node_id, h.bytes, 32);
    enr->has_identity = true;

    /* Set key-value pairs */
    const uint8_t id_v4[] = { 'v', '4' };
    enr_set(enr, "id", id_v4, 2);
    enr_set(enr, "secp256k1", compressed, 33);

    return true;
}

bool enr_node_id(const enr_t *enr, uint8_t node_id[32]) {
    if (!enr->has_identity) return false;
    memcpy(node_id, enr->node_id, 32);
    return true;
}

/* =========================================================================
 * RLP helpers
 * ========================================================================= */

/*
 * Build the content list: RLP([seq, k1, v1, k2, v2, ...])
 * This is what gets signed (after keccak256 hashing).
 */
static rlp_item_t *enr_build_content(const enr_t *enr) {
    rlp_item_t *list = rlp_list_new();
    if (!list) return NULL;

    rlp_list_append(list, rlp_uint64(enr->seq));

    for (size_t i = 0; i < enr->pair_count; i++) {
        const enr_pair_t *p = &enr->pairs[i];
        rlp_list_append(list, rlp_string((const uint8_t *)p->key, strlen(p->key)));
        rlp_list_append(list, rlp_string(p->value, p->value_len));
    }

    return list;
}

/* =========================================================================
 * Sign
 * ========================================================================= */

bool enr_sign(enr_t *enr, const uint8_t priv[32]) {
    /* Build content and hash */
    rlp_item_t *content = enr_build_content(enr);
    if (!content) return false;

    bytes_t encoded = rlp_encode(content);
    rlp_item_free(content);

    hash_t h = hash_keccak256(encoded.data, encoded.len);
    bytes_free(&encoded);

    /* Sign */
    int recid;
    if (!secp256k1_wrap_sign(enr->signature, &recid, h.bytes, priv)) {
        return false;
    }

    enr->has_signature = true;
    return true;
}

/* =========================================================================
 * Encode
 * ========================================================================= */

bool enr_encode(const enr_t *enr, uint8_t *out, size_t *out_len) {
    if (!enr->has_signature) return false;

    /* Build full record: RLP([signature, seq, k1, v1, ...]) */
    rlp_item_t *list = rlp_list_new();
    if (!list) return false;

    rlp_list_append(list, rlp_string(enr->signature, 64));
    rlp_list_append(list, rlp_uint64(enr->seq));

    for (size_t i = 0; i < enr->pair_count; i++) {
        const enr_pair_t *p = &enr->pairs[i];
        rlp_list_append(list, rlp_string((const uint8_t *)p->key, strlen(p->key)));
        rlp_list_append(list, rlp_string(p->value, p->value_len));
    }

    bytes_t encoded = rlp_encode(list);
    rlp_item_free(list);

    if (encoded.len > ENR_MAX_SIZE) {
        bytes_free(&encoded);
        return false;
    }

    memcpy(out, encoded.data, encoded.len);
    *out_len = encoded.len;
    bytes_free(&encoded);
    return true;
}

/* =========================================================================
 * Decode
 * ========================================================================= */

bool enr_decode(enr_t *enr, const uint8_t *data, size_t len) {
    if (len > ENR_MAX_SIZE) return false;

    rlp_item_t *root = rlp_decode(data, len);
    if (!root || rlp_get_type(root) != RLP_TYPE_LIST) {
        rlp_item_free(root);
        return false;
    }

    size_t count = rlp_get_list_count(root);
    /* Minimum: signature + seq = 2 items. Must be even count (sig + seq + pairs). */
    if (count < 2 || (count % 2) != 0) {
        rlp_item_free(root);
        return false;
    }

    enr_init(enr);

    /* Item 0: signature (64 bytes) */
    const rlp_item_t *sig_item = rlp_get_list_item(root, 0);
    const bytes_t *sig_bytes = rlp_get_string(sig_item);
    if (!sig_bytes || sig_bytes->len != 64) {
        rlp_item_free(root);
        return false;
    }
    memcpy(enr->signature, sig_bytes->data, 64);
    enr->has_signature = true;

    /* Item 1: seq (uint64) */
    const rlp_item_t *seq_item = rlp_get_list_item(root, 1);
    const bytes_t *seq_bytes = rlp_get_string(seq_item);
    if (!seq_bytes) {
        rlp_item_free(root);
        return false;
    }
    enr->seq = 0;
    for (size_t i = 0; i < seq_bytes->len; i++)
        enr->seq = (enr->seq << 8) | seq_bytes->data[i];

    /* Items 2..N: key-value pairs */
    for (size_t i = 2; i < count; i += 2) {
        const rlp_item_t *k_item = rlp_get_list_item(root, i);
        const rlp_item_t *v_item = rlp_get_list_item(root, i + 1);
        const bytes_t *k_bytes = rlp_get_string(k_item);
        const bytes_t *v_bytes = rlp_get_string(v_item);
        if (!k_bytes || !v_bytes) {
            rlp_item_free(root);
            return false;
        }

        /* Convert key to C string */
        char key[ENR_KEY_MAX_LEN];
        size_t klen = k_bytes->len < ENR_KEY_MAX_LEN - 1 ? k_bytes->len : ENR_KEY_MAX_LEN - 1;
        memcpy(key, k_bytes->data, klen);
        key[klen] = '\0';

        enr_set(enr, key, v_bytes->data, v_bytes->len);
    }

    /* Derive identity if "secp256k1" key is present */
    const uint8_t *comp_key;
    size_t comp_len;
    if (enr_get(enr, "secp256k1", &comp_key, &comp_len) && comp_len == 33) {
        if (secp256k1_wrap_decompress(enr->pubkey, comp_key)) {
            hash_t h = hash_keccak256(enr->pubkey, 64);
            memcpy(enr->node_id, h.bytes, 32);
            enr->has_identity = true;
        }
    }

    rlp_item_free(root);
    return true;
}

/* =========================================================================
 * Verify
 * ========================================================================= */

bool enr_verify(const enr_t *enr) {
    if (!enr->has_signature || !enr->has_identity)
        return false;

    /* Rebuild content and hash */
    rlp_item_t *content = enr_build_content(enr);
    if (!content) return false;

    bytes_t encoded = rlp_encode(content);
    rlp_item_free(content);

    hash_t h = hash_keccak256(encoded.data, encoded.len);
    bytes_free(&encoded);

    /* Try recovery with recid 0 and 1 */
    uint8_t recovered[64];
    for (int recid = 0; recid <= 1; recid++) {
        if (secp256k1_wrap_recover(recovered, enr->signature, recid, h.bytes)) {
            if (memcmp(recovered, enr->pubkey, 64) == 0)
                return true;
        }
    }

    return false;
}

/* =========================================================================
 * IP/Port helpers
 * ========================================================================= */

void enr_set_ip4(enr_t *enr, uint32_t ip, uint16_t udp_port, uint16_t tcp_port) {
    /* IP: 4 bytes big-endian */
    uint8_t ip_bytes[4] = {
        (uint8_t)(ip >> 24), (uint8_t)(ip >> 16),
        (uint8_t)(ip >> 8),  (uint8_t)(ip)
    };
    enr_set(enr, "ip", ip_bytes, 4);

    /* UDP port: big-endian, minimal encoding */
    if (udp_port > 0) {
        if (udp_port <= 0xFF) {
            uint8_t b = (uint8_t)udp_port;
            enr_set(enr, "udp", &b, 1);
        } else {
            uint8_t b[2] = { (uint8_t)(udp_port >> 8), (uint8_t)udp_port };
            enr_set(enr, "udp", b, 2);
        }
    }

    /* TCP port: big-endian, minimal encoding */
    if (tcp_port > 0) {
        if (tcp_port <= 0xFF) {
            uint8_t b = (uint8_t)tcp_port;
            enr_set(enr, "tcp", &b, 1);
        } else {
            uint8_t b[2] = { (uint8_t)(tcp_port >> 8), (uint8_t)tcp_port };
            enr_set(enr, "tcp", b, 2);
        }
    }
}
