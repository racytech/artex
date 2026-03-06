#include "tx_decoder.h"
#include "hash.h"
#include <string.h>
#include <stdlib.h>
#include <secp256k1.h>
#include <secp256k1_recovery.h>

/* =========================================================================
 * RLP field extraction helpers (same pattern as block.c)
 * ========================================================================= */

static bool rlp_to_uint64(const rlp_item_t *item, uint64_t *out) {
    if (!item || rlp_get_type(item) != RLP_TYPE_STRING) return false;
    const bytes_t *b = rlp_get_string(item);
    if (!b || b->len > 8) return false;
    *out = 0;
    for (size_t i = 0; i < b->len; i++)
        *out = (*out << 8) | b->data[i];
    return true;
}

static bool rlp_to_uint256(const rlp_item_t *item, uint256_t *out) {
    if (!item || rlp_get_type(item) != RLP_TYPE_STRING) return false;
    const bytes_t *b = rlp_get_string(item);
    if (!b || b->len > 32) return false;
    uint8_t padded[32];
    memset(padded, 0, 32);
    if (b->len > 0)
        memcpy(padded + (32 - b->len), b->data, b->len);
    *out = uint256_from_bytes(padded, 32);
    return true;
}

static bool rlp_to_address(const rlp_item_t *item, address_t *out, bool *is_create) {
    if (!item || rlp_get_type(item) != RLP_TYPE_STRING) return false;
    const bytes_t *b = rlp_get_string(item);
    if (!b) return false;
    if (b->len == 0) {
        /* Empty "to" = contract creation */
        memset(out->bytes, 0, 20);
        if (is_create) *is_create = true;
        return true;
    }
    if (b->len != 20) return false;
    memcpy(out->bytes, b->data, 20);
    if (is_create) *is_create = false;
    return true;
}

static bool rlp_to_hash(const rlp_item_t *item, hash_t *out) {
    if (!item || rlp_get_type(item) != RLP_TYPE_STRING) return false;
    const bytes_t *b = rlp_get_string(item);
    if (!b || b->len != 32) return false;
    memcpy(out->bytes, b->data, 32);
    return true;
}

/* =========================================================================
 * Access list decoding (EIP-2930)
 * ========================================================================= */

static bool decode_access_list(const rlp_item_t *item,
                               access_list_entry_t **out,
                               size_t *out_count) {
    *out = NULL;
    *out_count = 0;
    if (!item || rlp_get_type(item) != RLP_TYPE_LIST) return true; /* empty is ok */

    size_t count = rlp_get_list_count(item);
    if (count == 0) return true;

    access_list_entry_t *entries = calloc(count, sizeof(access_list_entry_t));
    if (!entries) return false;

    for (size_t i = 0; i < count; i++) {
        const rlp_item_t *entry = rlp_get_list_item(item, i);
        if (!entry || rlp_get_type(entry) != RLP_TYPE_LIST ||
            rlp_get_list_count(entry) < 2) {
            goto fail;
        }

        /* address */
        const rlp_item_t *addr_item = rlp_get_list_item(entry, 0);
        if (!addr_item || rlp_get_type(addr_item) != RLP_TYPE_STRING) goto fail;
        const bytes_t *addr_bytes = rlp_get_string(addr_item);
        if (!addr_bytes || addr_bytes->len != 20) goto fail;
        memcpy(entries[i].address.bytes, addr_bytes->data, 20);

        /* storage keys */
        const rlp_item_t *keys = rlp_get_list_item(entry, 1);
        if (!keys || rlp_get_type(keys) != RLP_TYPE_LIST) goto fail;
        size_t nkeys = rlp_get_list_count(keys);
        if (nkeys > 0) {
            entries[i].storage_keys = calloc(nkeys, sizeof(uint256_t));
            if (!entries[i].storage_keys) goto fail;
            entries[i].storage_keys_count = nkeys;
            for (size_t j = 0; j < nkeys; j++) {
                const rlp_item_t *k = rlp_get_list_item(keys, j);
                if (!k || rlp_get_type(k) != RLP_TYPE_STRING) goto fail;
                const bytes_t *kb = rlp_get_string(k);
                if (!kb || kb->len != 32) goto fail;
                entries[i].storage_keys[j] = uint256_from_bytes(kb->data, 32);
            }
        }
    }

    *out = entries;
    *out_count = count;
    return true;

fail:
    for (size_t i = 0; i < count; i++)
        free(entries[i].storage_keys);
    free(entries);
    return false;
}

/* Forward declaration for auth signer recovery */
static bool recover_sender(const uint8_t signing_hash[32],
                           const uint256_t *r, const uint256_t *s,
                           int recid, address_t *sender);

/* =========================================================================
 * Authorization list decoding (EIP-7702)
 * ========================================================================= */

static bool decode_authorization_list(const rlp_item_t *item,
                                       authorization_t **out,
                                       size_t *out_count) {
    *out = NULL;
    *out_count = 0;
    if (!item || rlp_get_type(item) != RLP_TYPE_LIST) return true; /* empty is ok */

    size_t count = rlp_get_list_count(item);
    if (count == 0) return true;

    authorization_t *auths = calloc(count, sizeof(authorization_t));
    if (!auths) return false;

    for (size_t i = 0; i < count; i++) {
        const rlp_item_t *tuple = rlp_get_list_item(item, i);
        if (!tuple || rlp_get_type(tuple) != RLP_TYPE_LIST ||
            rlp_get_list_count(tuple) != 6) {
            goto fail;
        }

        /* [chain_id, address, nonce, y_parity, r, s] */
        if (!rlp_to_uint256(rlp_get_list_item(tuple, 0), &auths[i].chain_id))
            goto fail;

        const rlp_item_t *addr_item = rlp_get_list_item(tuple, 1);
        if (!addr_item || rlp_get_type(addr_item) != RLP_TYPE_STRING) goto fail;
        const bytes_t *addr_bytes = rlp_get_string(addr_item);
        if (!addr_bytes || addr_bytes->len != 20) goto fail;
        memcpy(auths[i].address.bytes, addr_bytes->data, 20);

        if (!rlp_to_uint64(rlp_get_list_item(tuple, 2), &auths[i].nonce))
            goto fail;

        uint64_t y_parity;
        if (!rlp_to_uint64(rlp_get_list_item(tuple, 3), &y_parity))
            goto fail;
        auths[i].y_parity = (uint8_t)y_parity;

        if (!rlp_to_uint256(rlp_get_list_item(tuple, 4), &auths[i].r))
            goto fail;
        if (!rlp_to_uint256(rlp_get_list_item(tuple, 5), &auths[i].s))
            goto fail;

        /* Recover signer: keccak256(0x05 || rlp([chain_id, address, nonce])) */
        rlp_item_t *auth_list = rlp_list_new();
        /* Clone chain_id field */
        bytes_t chain_enc = rlp_encode(rlp_get_list_item(tuple, 0));
        rlp_item_t *chain_clone = rlp_decode(chain_enc.data, chain_enc.len);
        rlp_list_append(auth_list, chain_clone);
        free(chain_enc.data);
        /* Clone address field */
        bytes_t addr_enc = rlp_encode(rlp_get_list_item(tuple, 1));
        rlp_item_t *addr_clone = rlp_decode(addr_enc.data, addr_enc.len);
        rlp_list_append(auth_list, addr_clone);
        free(addr_enc.data);
        /* Clone nonce field */
        bytes_t nonce_enc = rlp_encode(rlp_get_list_item(tuple, 2));
        rlp_item_t *nonce_clone = rlp_decode(nonce_enc.data, nonce_enc.len);
        rlp_list_append(auth_list, nonce_clone);
        free(nonce_enc.data);

        bytes_t rlp_encoded = rlp_encode(auth_list);
        size_t total = 1 + rlp_encoded.len;
        uint8_t *buf = malloc(total);
        buf[0] = 0x05; /* EIP-7702 auth magic byte */
        memcpy(buf + 1, rlp_encoded.data, rlp_encoded.len);

        hash_t signing_hash = hash_keccak256(buf, total);
        free(buf);
        free(rlp_encoded.data);
        rlp_item_free(auth_list);

        /* Recover — on failure, signer stays zero (transaction.c skips zero signers) */
        int recid = (int)auths[i].y_parity;
        if (recid == 0 || recid == 1) {
            recover_sender(signing_hash.bytes, &auths[i].r, &auths[i].s,
                           recid, &auths[i].signer);
        }
    }

    *out = auths;
    *out_count = count;
    return true;

fail:
    free(auths);
    return false;
}

/* =========================================================================
 * Sender recovery via secp256k1 ecrecover
 * ========================================================================= */

static secp256k1_context *get_secp_ctx(void) {
    static secp256k1_context *ctx = NULL;
    if (!ctx)
        ctx = secp256k1_context_create(SECP256K1_CONTEXT_VERIFY);
    return ctx;
}

static bool recover_sender(const uint8_t signing_hash[32],
                           const uint256_t *r, const uint256_t *s,
                           int recid, address_t *sender) {
    secp256k1_context *ctx = get_secp_ctx();
    if (!ctx) return false;

    /* Build compact signature: r(32) || s(32) */
    uint8_t sig64[64];
    uint256_to_bytes(r, sig64);
    uint256_to_bytes(s, sig64 + 32);

    secp256k1_ecdsa_recoverable_signature sig;
    if (!secp256k1_ecdsa_recoverable_signature_parse_compact(ctx, &sig, sig64, recid))
        return false;

    secp256k1_pubkey pubkey;
    if (!secp256k1_ecdsa_recover(ctx, &pubkey, &sig, signing_hash))
        return false;

    /* Serialize uncompressed: 0x04 || x(32) || y(32) */
    uint8_t pub_ser[65];
    size_t pub_len = 65;
    secp256k1_ec_pubkey_serialize(ctx, pub_ser, &pub_len,
                                  &pubkey, SECP256K1_EC_UNCOMPRESSED);

    /* keccak256(pub[1:65]) → last 20 bytes = address */
    hash_t h = hash_keccak256(pub_ser + 1, 64);
    memcpy(sender->bytes, h.bytes + 12, 20);
    return true;
}

/* =========================================================================
 * Signing hash computation
 * ========================================================================= */

/* Encode an RLP item representing the signing message and hash it. */
static hash_t compute_signing_hash_legacy(const rlp_item_t *fields,
                                          uint64_t chain_id,
                                          bool eip155) {
    /* Legacy signing:
     *   Pre-EIP-155: keccak256(RLP([nonce, gasPrice, gasLimit, to, value, data]))
     *   EIP-155:     keccak256(RLP([nonce, gasPrice, gasLimit, to, value, data, chainId, 0, 0]))
     */
    rlp_item_t *list = rlp_list_new();
    for (size_t i = 0; i < 6; i++) {
        const rlp_item_t *f = rlp_get_list_item(fields, i);
        if (!f) { rlp_item_free(list); return hash_zero(); }
        const bytes_t *b = (rlp_get_type(f) == RLP_TYPE_STRING) ?
            rlp_get_string(f) : NULL;
        if (rlp_get_type(f) == RLP_TYPE_STRING) {
            rlp_list_append(list, rlp_string(b ? b->data : NULL, b ? b->len : 0));
        } else {
            /* Re-encode the list item (for 'to' which could be empty string) */
            bytes_t enc = rlp_encode(f);
            rlp_item_t *decoded = rlp_decode(enc.data, enc.len);
            rlp_list_append(list, decoded);
            free(enc.data);
        }
    }
    if (eip155) {
        rlp_list_append(list, rlp_uint64(chain_id));
        rlp_list_append(list, rlp_string(NULL, 0));
        rlp_list_append(list, rlp_string(NULL, 0));
    }
    bytes_t encoded = rlp_encode(list);
    hash_t h = hash_keccak256(encoded.data, encoded.len);
    free(encoded.data);
    rlp_item_free(list);
    return h;
}

static hash_t compute_signing_hash_typed(uint8_t type_byte,
                                         const rlp_item_t *fields,
                                         size_t field_count) {
    /* Typed tx signing: keccak256(type_byte || RLP([fields without signature])) */
    rlp_item_t *list = rlp_list_new();
    for (size_t i = 0; i < field_count; i++) {
        const rlp_item_t *f = rlp_get_list_item(fields, i);
        if (!f) { rlp_item_free(list); return hash_zero(); }
        /* Clone the field by re-encoding and decoding */
        bytes_t enc = rlp_encode(f);
        rlp_item_t *clone = rlp_decode(enc.data, enc.len);
        rlp_list_append(list, clone);
        free(enc.data);
    }
    bytes_t rlp_encoded = rlp_encode(list);

    /* Prepend type byte */
    size_t total = 1 + rlp_encoded.len;
    uint8_t *buf = malloc(total);
    buf[0] = type_byte;
    memcpy(buf + 1, rlp_encoded.data, rlp_encoded.len);

    hash_t h = hash_keccak256(buf, total);
    free(buf);
    free(rlp_encoded.data);
    rlp_item_free(list);
    return h;
}

/* =========================================================================
 * Legacy transaction decode (type 0)
 * ========================================================================= */

static bool decode_legacy(transaction_t *tx, const rlp_item_t *list,
                          uint64_t chain_id) {
    size_t count = rlp_get_list_count(list);
    if (count != 9) return false;

    /* [nonce, gasPrice, gasLimit, to, value, data, v, r, s] */
    tx->type = TX_TYPE_LEGACY;
    if (!rlp_to_uint64(rlp_get_list_item(list, 0), &tx->nonce)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 1), &tx->gas_price)) return false;
    if (!rlp_to_uint64(rlp_get_list_item(list, 2), &tx->gas_limit)) return false;
    if (!rlp_to_address(rlp_get_list_item(list, 3), &tx->to, &tx->is_create)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 4), &tx->value)) return false;

    /* data (calldata) */
    const rlp_item_t *data_item = rlp_get_list_item(list, 5);
    if (!data_item || rlp_get_type(data_item) != RLP_TYPE_STRING) return false;
    const bytes_t *data_bytes = rlp_get_string(data_item);
    if (data_bytes && data_bytes->len > 0) {
        uint8_t *data_copy = malloc(data_bytes->len);
        if (!data_copy) return false;
        memcpy(data_copy, data_bytes->data, data_bytes->len);
        tx->data = data_copy;
        tx->data_size = data_bytes->len;
    }

    /* Signature: v, r, s */
    uint64_t v;
    uint256_t r, s;
    if (!rlp_to_uint64(rlp_get_list_item(list, 6), &v)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 7), &r)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 8), &s)) return false;

    /* Determine recid and whether EIP-155 */
    int recid;
    bool eip155;
    if (v == 27 || v == 28) {
        recid = (int)(v - 27);
        eip155 = false;
    } else {
        /* EIP-155: v = chain_id * 2 + 35 + y_parity */
        recid = (int)(v - chain_id * 2 - 35);
        if (recid != 0 && recid != 1) return false;
        eip155 = true;
    }

    /* Compute signing hash and recover sender */
    hash_t signing_hash = compute_signing_hash_legacy(list, chain_id, eip155);
    if (!recover_sender(signing_hash.bytes, &r, &s, recid, &tx->sender))
        return false;

    return true;
}

/* =========================================================================
 * Typed transaction decode helpers
 * ========================================================================= */

/* Decode fields common to EIP-2930/1559/4844 after the type-specific gas fields. */
static bool decode_typed_common(transaction_t *tx, const rlp_item_t *list,
                                size_t to_idx, size_t value_idx,
                                size_t data_idx, size_t al_idx) {
    if (!rlp_to_address(rlp_get_list_item(list, to_idx), &tx->to, &tx->is_create))
        return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, value_idx), &tx->value))
        return false;

    /* data */
    const rlp_item_t *data_item = rlp_get_list_item(list, data_idx);
    if (!data_item || rlp_get_type(data_item) != RLP_TYPE_STRING) return false;
    const bytes_t *data_bytes = rlp_get_string(data_item);
    if (data_bytes && data_bytes->len > 0) {
        uint8_t *data_copy = malloc(data_bytes->len);
        if (!data_copy) return false;
        memcpy(data_copy, data_bytes->data, data_bytes->len);
        tx->data = data_copy;
        tx->data_size = data_bytes->len;
    }

    /* access list */
    if (!decode_access_list(rlp_get_list_item(list, al_idx),
                            &tx->access_list, &tx->access_list_count))
        return false;

    return true;
}

/* Recover sender for a typed tx given signature fields at known positions. */
static bool typed_recover_sender(transaction_t *tx, uint8_t type_byte,
                                 const rlp_item_t *list,
                                 size_t sig_fields_count,
                                 size_t yparity_idx, size_t r_idx, size_t s_idx) {
    uint64_t y_parity;
    uint256_t r, s;
    if (!rlp_to_uint64(rlp_get_list_item(list, yparity_idx), &y_parity)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, r_idx), &r)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, s_idx), &s)) return false;

    int recid = (int)y_parity;
    if (recid != 0 && recid != 1) return false;

    hash_t signing_hash = compute_signing_hash_typed(type_byte, list, sig_fields_count);
    return recover_sender(signing_hash.bytes, &r, &s, recid, &tx->sender);
}

/* =========================================================================
 * EIP-2930 (type 1) decode
 * ========================================================================= */

static bool decode_eip2930(transaction_t *tx, const rlp_item_t *list,
                           uint64_t chain_id) {
    /* [chainId, nonce, gasPrice, gasLimit, to, value, data, accessList,
     *  signatureYParity, signatureR, signatureS]  → 11 fields */
    if (rlp_get_list_count(list) != 11) return false;
    (void)chain_id;

    tx->type = TX_TYPE_EIP2930;
    /* chainId at 0 — skip (already known) */
    if (!rlp_to_uint64(rlp_get_list_item(list, 1), &tx->nonce)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 2), &tx->gas_price)) return false;
    if (!rlp_to_uint64(rlp_get_list_item(list, 3), &tx->gas_limit)) return false;

    if (!decode_typed_common(tx, list, 4, 5, 6, 7)) return false;

    /* Fields 0..7 are signing payload (8 fields) */
    return typed_recover_sender(tx, 0x01, list, 8, 8, 9, 10);
}

/* =========================================================================
 * EIP-1559 (type 2) decode
 * ========================================================================= */

static bool decode_eip1559(transaction_t *tx, const rlp_item_t *list,
                           uint64_t chain_id) {
    /* [chainId, nonce, maxPriorityFeePerGas, maxFeePerGas, gasLimit,
     *  to, value, data, accessList,
     *  signatureYParity, signatureR, signatureS]  → 12 fields */
    if (rlp_get_list_count(list) != 12) return false;
    (void)chain_id;

    tx->type = TX_TYPE_EIP1559;
    if (!rlp_to_uint64(rlp_get_list_item(list, 1), &tx->nonce)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 2), &tx->max_priority_fee_per_gas)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 3), &tx->max_fee_per_gas)) return false;
    if (!rlp_to_uint64(rlp_get_list_item(list, 4), &tx->gas_limit)) return false;

    if (!decode_typed_common(tx, list, 5, 6, 7, 8)) return false;

    /* Fields 0..8 are signing payload (9 fields) */
    return typed_recover_sender(tx, 0x02, list, 9, 9, 10, 11);
}

/* =========================================================================
 * EIP-4844 (type 3) decode
 * ========================================================================= */

static bool decode_eip4844(transaction_t *tx, const rlp_item_t *list,
                           uint64_t chain_id) {
    /* [chainId, nonce, maxPriorityFeePerGas, maxFeePerGas, gasLimit,
     *  to, value, data, accessList, maxFeePerBlobGas, blobVersionedHashes,
     *  signatureYParity, signatureR, signatureS]  → 14 fields */
    if (rlp_get_list_count(list) != 14) return false;
    (void)chain_id;

    tx->type = TX_TYPE_EIP4844;
    if (!rlp_to_uint64(rlp_get_list_item(list, 1), &tx->nonce)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 2), &tx->max_priority_fee_per_gas)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 3), &tx->max_fee_per_gas)) return false;
    if (!rlp_to_uint64(rlp_get_list_item(list, 4), &tx->gas_limit)) return false;

    if (!decode_typed_common(tx, list, 5, 6, 7, 8)) return false;

    /* maxFeePerBlobGas */
    if (!rlp_to_uint256(rlp_get_list_item(list, 9), &tx->max_fee_per_blob_gas))
        return false;

    /* blobVersionedHashes — list of 32-byte hashes */
    const rlp_item_t *hashes = rlp_get_list_item(list, 10);
    if (!hashes || rlp_get_type(hashes) != RLP_TYPE_LIST) return false;
    size_t nhashes = rlp_get_list_count(hashes);
    if (nhashes > 0) {
        hash_t *blob_hashes = calloc(nhashes, sizeof(hash_t));
        if (!blob_hashes) return false;
        for (size_t i = 0; i < nhashes; i++) {
            if (!rlp_to_hash(rlp_get_list_item(hashes, i), &blob_hashes[i])) {
                free(blob_hashes);
                return false;
            }
        }
        tx->blob_versioned_hashes = blob_hashes;
        tx->blob_versioned_hashes_count = nhashes;
    }

    /* Fields 0..10 are signing payload (11 fields) */
    return typed_recover_sender(tx, 0x03, list, 11, 11, 12, 13);
}

/* =========================================================================
 * EIP-7702 (type 4) decode
 * ========================================================================= */

static bool decode_eip7702(transaction_t *tx, const rlp_item_t *list,
                           uint64_t chain_id) {
    /* [chainId, nonce, maxPriorityFeePerGas, maxFeePerGas, gasLimit,
     *  destination, value, data, accessList, authorizationList,
     *  signatureYParity, signatureR, signatureS]  → 13 fields */
    if (rlp_get_list_count(list) != 13) return false;
    (void)chain_id;

    tx->type = TX_TYPE_EIP7702;
    if (!rlp_to_uint64(rlp_get_list_item(list, 1), &tx->nonce)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 2), &tx->max_priority_fee_per_gas)) return false;
    if (!rlp_to_uint256(rlp_get_list_item(list, 3), &tx->max_fee_per_gas)) return false;
    if (!rlp_to_uint64(rlp_get_list_item(list, 4), &tx->gas_limit)) return false;

    if (!decode_typed_common(tx, list, 5, 6, 7, 8)) return false;

    /* Authorization list */
    if (!decode_authorization_list(rlp_get_list_item(list, 9),
                                    &tx->authorization_list,
                                    &tx->authorization_list_count))
        return false;

    /* Fields 0..9 are signing payload (10 fields) */
    return typed_recover_sender(tx, 0x04, list, 10, 10, 11, 12);
}

/* =========================================================================
 * Public API
 * ========================================================================= */

bool tx_decode_rlp(transaction_t *tx, const rlp_item_t *item,
                   uint64_t chain_id) {
    if (!tx || !item) return false;
    memset(tx, 0, sizeof(*tx));

    if (rlp_get_type(item) == RLP_TYPE_LIST) {
        /* Legacy transaction — embedded as RLP list */
        return decode_legacy(tx, item, chain_id);
    }

    if (rlp_get_type(item) == RLP_TYPE_STRING) {
        /* Typed transaction — RLP string: type_byte || RLP(fields) */
        const bytes_t *b = rlp_get_string(item);
        if (!b || b->len < 2) return false;

        uint8_t type_byte = b->data[0];

        /* Decode inner RLP (after type byte) */
        rlp_item_t *inner = rlp_decode(b->data + 1, b->len - 1);
        if (!inner || rlp_get_type(inner) != RLP_TYPE_LIST) {
            if (inner) rlp_item_free(inner);
            return false;
        }

        bool ok = false;
        switch (type_byte) {
        case 0x01: ok = decode_eip2930(tx, inner, chain_id); break;
        case 0x02: ok = decode_eip1559(tx, inner, chain_id); break;
        case 0x03: ok = decode_eip4844(tx, inner, chain_id); break;
        case 0x04: ok = decode_eip7702(tx, inner, chain_id); break;
        default: break;
        }

        rlp_item_free(inner);
        if (!ok) tx_decoded_free(tx);
        return ok;
    }

    return false;
}

bool tx_decode_raw(transaction_t *tx, const uint8_t *data, size_t len,
                   uint64_t chain_id) {
    if (!tx || !data || len == 0) return false;

    /* If first byte < 0x80, it's a typed tx (type prefix) */
    if (data[0] < 0x80) {
        /* Typed: build an RLP string wrapping the entire data */
        rlp_item_t *wrapper = rlp_string(data, len);
        if (!wrapper) return false;
        bool ok = tx_decode_rlp(tx, wrapper, chain_id);
        rlp_item_free(wrapper);
        return ok;
    }

    /* Otherwise it's a legacy tx (direct RLP list) */
    rlp_item_t *decoded = rlp_decode(data, len);
    if (!decoded) return false;
    bool ok = tx_decode_rlp(tx, decoded, chain_id);
    rlp_item_free(decoded);
    return ok;
}

void tx_decoded_free(transaction_t *tx) {
    if (!tx) return;
    free((void *)tx->data);
    if (tx->access_list) {
        for (size_t i = 0; i < tx->access_list_count; i++)
            free(tx->access_list[i].storage_keys);
        free(tx->access_list);
    }
    free((void *)tx->blob_versioned_hashes);
    free(tx->authorization_list);
    memset(tx, 0, sizeof(*tx));
}
