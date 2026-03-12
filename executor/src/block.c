#include "block.h"
#include "mem_mpt.h"
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * Helpers: extract typed values from RLP string items
 * ========================================================================= */

/* Copy up to 32 bytes from an RLP string into a hash_t (left-padded). */
static bool rlp_get_hash(const rlp_item_t *item, hash_t *out) {
    if (!item || rlp_get_type(item) != RLP_TYPE_STRING) return false;
    const bytes_t *b = rlp_get_string(item);
    if (!b || b->len > 32) return false;
    memset(out->bytes, 0, 32);
    if (b->len > 0)
        memcpy(out->bytes + (32 - b->len), b->data, b->len);
    return true;
}

/* Copy up to 20 bytes from an RLP string into an address_t (left-padded). */
static bool rlp_get_address(const rlp_item_t *item, address_t *out) {
    if (!item || rlp_get_type(item) != RLP_TYPE_STRING) return false;
    const bytes_t *b = rlp_get_string(item);
    if (!b || b->len > 20) return false;
    memset(out->bytes, 0, 20);
    if (b->len > 0)
        memcpy(out->bytes + (20 - b->len), b->data, b->len);
    return true;
}

/* Decode an RLP string into a uint64_t (big-endian). */
static bool rlp_get_uint64(const rlp_item_t *item, uint64_t *out) {
    if (!item || rlp_get_type(item) != RLP_TYPE_STRING) return false;
    const bytes_t *b = rlp_get_string(item);
    if (!b || b->len > 8) return false;
    *out = 0;
    for (size_t i = 0; i < b->len; i++)
        *out = (*out << 8) | b->data[i];
    return true;
}

/* Decode an RLP string into a uint256_t (big-endian). */
static bool rlp_get_uint256(const rlp_item_t *item, uint256_t *out) {
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

/* =========================================================================
 * Block header RLP decode
 * ========================================================================= */

bool block_header_decode_rlp(block_header_t *hdr,
                             const uint8_t *data, size_t len) {
    if (!hdr || !data || len == 0) return false;
    memset(hdr, 0, sizeof(*hdr));

    rlp_item_t *root = rlp_decode(data, len);
    if (!root || rlp_get_type(root) != RLP_TYPE_LIST) {
        if (root) rlp_item_free(root);
        return false;
    }

    size_t count = rlp_get_list_count(root);
    if (count < 15) {
        rlp_item_free(root);
        return false;
    }

    /* Fields 0-14: always present */
    bool ok = true;
    ok = ok && rlp_get_hash(rlp_get_list_item(root, 0), &hdr->parent_hash);
    ok = ok && rlp_get_hash(rlp_get_list_item(root, 1), &hdr->uncle_hash);
    ok = ok && rlp_get_address(rlp_get_list_item(root, 2), &hdr->coinbase);
    ok = ok && rlp_get_hash(rlp_get_list_item(root, 3), &hdr->state_root);
    ok = ok && rlp_get_hash(rlp_get_list_item(root, 4), &hdr->tx_root);
    ok = ok && rlp_get_hash(rlp_get_list_item(root, 5), &hdr->receipt_root);

    /* Field 6: logsBloom (256 bytes) */
    const rlp_item_t *bloom_item = rlp_get_list_item(root, 6);
    if (bloom_item && rlp_get_type(bloom_item) == RLP_TYPE_STRING) {
        const bytes_t *b = rlp_get_string(bloom_item);
        if (b && b->len == 256)
            memcpy(hdr->logs_bloom, b->data, 256);
        else
            ok = false;
    } else {
        ok = false;
    }

    ok = ok && rlp_get_uint256(rlp_get_list_item(root, 7), &hdr->difficulty);
    ok = ok && rlp_get_uint64(rlp_get_list_item(root, 8), &hdr->number);
    ok = ok && rlp_get_uint64(rlp_get_list_item(root, 9), &hdr->gas_limit);
    ok = ok && rlp_get_uint64(rlp_get_list_item(root, 10), &hdr->gas_used);
    ok = ok && rlp_get_uint64(rlp_get_list_item(root, 11), &hdr->timestamp);

    /* Field 12: extraData (variable length, up to 32 bytes) */
    const rlp_item_t *extra_item = rlp_get_list_item(root, 12);
    if (extra_item && rlp_get_type(extra_item) == RLP_TYPE_STRING) {
        const bytes_t *b = rlp_get_string(extra_item);
        if (b) {
            hdr->extra_data_len = b->len < 32 ? b->len : 32;
            if (hdr->extra_data_len > 0)
                memcpy(hdr->extra_data, b->data, hdr->extra_data_len);
        }
    }

    ok = ok && rlp_get_hash(rlp_get_list_item(root, 13), &hdr->mix_hash);
    ok = ok && rlp_get_uint64(rlp_get_list_item(root, 14), &hdr->nonce);

    if (!ok) {
        rlp_item_free(root);
        return false;
    }

    /* Field 15: baseFeePerGas (London+, EIP-1559) */
    if (count > 15) {
        hdr->has_base_fee = true;
        if (!rlp_get_uint256(rlp_get_list_item(root, 15), &hdr->base_fee)) {
            rlp_item_free(root);
            return false;
        }
    }

    /* Field 16: withdrawalsRoot (Shanghai+) */
    if (count > 16) {
        hdr->has_withdrawals_root = true;
        rlp_get_hash(rlp_get_list_item(root, 16), &hdr->withdrawals_root);
    }

    /* Fields 17-18: blobGasUsed, excessBlobGas (Cancun+) */
    if (count > 18) {
        hdr->has_blob_gas = true;
        rlp_get_uint64(rlp_get_list_item(root, 17), &hdr->blob_gas_used);
        rlp_get_uint64(rlp_get_list_item(root, 18), &hdr->excess_blob_gas);
    }

    /* Field 19: parentBeaconBlockRoot (Cancun+) */
    if (count > 19) {
        hdr->has_parent_beacon_root = true;
        rlp_get_hash(rlp_get_list_item(root, 19), &hdr->parent_beacon_root);
    }

    /* Field 20: requestsHash (Prague+, EIP-7685) */
    if (count > 20) {
        hdr->has_requests_hash = true;
        rlp_get_hash(rlp_get_list_item(root, 20), &hdr->requests_hash);
    }

    rlp_item_free(root);
    return true;
}

/* =========================================================================
 * Block body RLP decode
 * ========================================================================= */

bool block_body_decode_rlp(block_body_t *body,
                           const uint8_t *data, size_t len) {
    if (!body || !data || len == 0) return false;
    memset(body, 0, sizeof(*body));

    rlp_item_t *root = rlp_decode(data, len);
    if (!root || rlp_get_type(root) != RLP_TYPE_LIST) {
        if (root) rlp_item_free(root);
        return false;
    }

    /* Body must have at least 2 elements: [transactions, uncles] */
    size_t count = rlp_get_list_count(root);
    if (count < 2) {
        rlp_item_free(root);
        return false;
    }

    /* First element is the transactions list */
    const rlp_item_t *txs = rlp_get_list_item(root, 0);
    if (!txs || rlp_get_type(txs) != RLP_TYPE_LIST) {
        rlp_item_free(root);
        return false;
    }

    body->_rlp = root;
    body->tx_count = rlp_get_list_count(txs);
    return true;
}

const rlp_item_t *block_body_tx(const block_body_t *body, size_t index) {
    if (!body || !body->_rlp || index >= body->tx_count) return NULL;
    const rlp_item_t *txs = rlp_get_list_item(body->_rlp, body->_tx_list_idx);
    if (!txs) return NULL;
    return rlp_get_list_item(txs, index);
}

/* =========================================================================
 * Full block RLP decode: [header_list, transactions_list, uncles_list, ...]
 * ========================================================================= */

bool block_decode_full_rlp(const uint8_t *data, size_t len,
                           block_header_t *hdr, block_body_t *body) {
    if (!data || len == 0 || !hdr || !body) return false;
    memset(body, 0, sizeof(*body));

    rlp_item_t *root = rlp_decode(data, len);
    if (!root || rlp_get_type(root) != RLP_TYPE_LIST) {
        if (root) rlp_item_free(root);
        return false;
    }

    /* Full block: [header, transactions, uncles, ...] — at least 3 elements */
    size_t count = rlp_get_list_count(root);
    if (count < 3) {
        rlp_item_free(root);
        return false;
    }

    /* Element 0: header list — re-encode and decode */
    const rlp_item_t *hdr_item = rlp_get_list_item(root, 0);
    if (!hdr_item || rlp_get_type(hdr_item) != RLP_TYPE_LIST) {
        rlp_item_free(root);
        return false;
    }

    bytes_t hdr_encoded = rlp_encode(hdr_item);
    if (!hdr_encoded.data) {
        rlp_item_free(root);
        return false;
    }

    bool ok = block_header_decode_rlp(hdr, hdr_encoded.data, hdr_encoded.len);
    free(hdr_encoded.data);
    if (!ok) {
        rlp_item_free(root);
        return false;
    }

    /* Element 1: transactions list */
    const rlp_item_t *txs = rlp_get_list_item(root, 1);
    if (!txs || rlp_get_type(txs) != RLP_TYPE_LIST) {
        rlp_item_free(root);
        return false;
    }

    body->_rlp = root;
    body->_tx_list_idx = 1; /* transactions at index 1 in full block */
    body->tx_count = rlp_get_list_count(txs);

    /* Element 3: withdrawals list (Shanghai+, EIP-4895) */
    if (count > 3) {
        const rlp_item_t *wds = rlp_get_list_item(root, 3);
        if (wds && rlp_get_type(wds) == RLP_TYPE_LIST) {
            size_t wd_count = rlp_get_list_count(wds);
            if (wd_count > 0) {
                body->withdrawals = calloc(wd_count, sizeof(withdrawal_t));
                if (body->withdrawals) {
                    body->withdrawal_count = wd_count;
                    for (size_t w = 0; w < wd_count; w++) {
                        const rlp_item_t *wd = rlp_get_list_item(wds, w);
                        if (!wd || rlp_get_type(wd) != RLP_TYPE_LIST ||
                            rlp_get_list_count(wd) < 4)
                            continue;
                        rlp_get_uint64(rlp_get_list_item(wd, 0),
                                       &body->withdrawals[w].index);
                        rlp_get_uint64(rlp_get_list_item(wd, 1),
                                       &body->withdrawals[w].validator_index);
                        rlp_get_address(rlp_get_list_item(wd, 2),
                                        &body->withdrawals[w].address);
                        rlp_get_uint64(rlp_get_list_item(wd, 3),
                                       &body->withdrawals[w].amount_gwei);
                    }
                }
            }
        }
    }

    return true;
}

size_t block_body_uncle_count(const block_body_t *body) {
    if (!body || !body->_rlp) return 0;
    /* Uncle list is one past the tx list */
    size_t uncle_list_idx = body->_tx_list_idx + 1;
    const rlp_item_t *uncles = rlp_get_list_item(body->_rlp, uncle_list_idx);
    if (!uncles || rlp_get_type(uncles) != RLP_TYPE_LIST) return 0;
    return rlp_get_list_count(uncles);
}

bool block_body_get_uncle(const block_body_t *body, size_t index,
                          block_header_t *hdr) {
    if (!body || !body->_rlp || !hdr) return false;
    size_t uncle_list_idx = body->_tx_list_idx + 1;
    const rlp_item_t *uncles = rlp_get_list_item(body->_rlp, uncle_list_idx);
    if (!uncles || rlp_get_type(uncles) != RLP_TYPE_LIST) return false;
    if (index >= rlp_get_list_count(uncles)) return false;

    const rlp_item_t *uncle_item = rlp_get_list_item(uncles, index);
    if (!uncle_item || rlp_get_type(uncle_item) != RLP_TYPE_LIST) return false;

    /* Re-encode the uncle header RLP and decode it */
    bytes_t encoded = rlp_encode(uncle_item);
    if (!encoded.data) return false;

    bool ok = block_header_decode_rlp(hdr, encoded.data, encoded.len);
    free(encoded.data);
    return ok;
}

void block_body_free(block_body_t *body) {
    if (body) {
        if (body->_rlp) {
            rlp_item_free(body->_rlp);
            body->_rlp = NULL;
        }
        free(body->withdrawals);
        body->withdrawals = NULL;
        body->tx_count = 0;
        body->withdrawal_count = 0;
    }
}

hash_t block_hash_from_rlp(const uint8_t *data, size_t len) {
    hash_t zero = {0};
    if (!data || len == 0) return zero;

    /* Decode outer list */
    rlp_item_t *root = rlp_decode(data, len);
    if (!root || rlp_get_type(root) != RLP_TYPE_LIST) {
        if (root) rlp_item_free(root);
        return zero;
    }

    /* Element 0 is the header list */
    const rlp_item_t *hdr_item = rlp_get_list_item(root, 0);
    if (!hdr_item) {
        rlp_item_free(root);
        return zero;
    }

    /* Re-encode header → keccak256 gives the block hash */
    bytes_t hdr_encoded = rlp_encode(hdr_item);
    rlp_item_free(root);

    if (!hdr_encoded.data) return zero;

    hash_t result = hash_keccak256(hdr_encoded.data, hdr_encoded.len);
    free(hdr_encoded.data);
    return result;
}

/* =========================================================================
 * Block header RLP encode (reverse of decode)
 * ========================================================================= */

/** Encode uint256 as RLP string with leading zeros stripped. */
static rlp_item_t *rlp_uint256(const uint256_t *v) {
    uint8_t buf[32];
    uint256_to_bytes(v, buf);
    size_t off = 0;
    while (off < 32 && buf[off] == 0) off++;
    return rlp_string(buf + off, 32 - off);
}

bytes_t block_header_encode_rlp(const block_header_t *hdr) {
    bytes_t empty = {NULL, 0, 0};
    if (!hdr) return empty;

    rlp_item_t *list = rlp_list_new();
    if (!list) return empty;

    /* [0]  parentHash */
    rlp_list_append(list, rlp_string(hdr->parent_hash.bytes, 32));
    /* [1]  uncleHash */
    rlp_list_append(list, rlp_string(hdr->uncle_hash.bytes, 32));
    /* [2]  coinbase */
    rlp_list_append(list, rlp_string(hdr->coinbase.bytes, 20));
    /* [3]  stateRoot */
    rlp_list_append(list, rlp_string(hdr->state_root.bytes, 32));
    /* [4]  txRoot */
    rlp_list_append(list, rlp_string(hdr->tx_root.bytes, 32));
    /* [5]  receiptRoot */
    rlp_list_append(list, rlp_string(hdr->receipt_root.bytes, 32));
    /* [6]  logsBloom */
    rlp_list_append(list, rlp_string(hdr->logs_bloom, 256));
    /* [7]  difficulty */
    rlp_list_append(list, rlp_uint256(&hdr->difficulty));
    /* [8]  number */
    rlp_list_append(list, rlp_uint64(hdr->number));
    /* [9]  gasLimit */
    rlp_list_append(list, rlp_uint64(hdr->gas_limit));
    /* [10] gasUsed */
    rlp_list_append(list, rlp_uint64(hdr->gas_used));
    /* [11] timestamp */
    rlp_list_append(list, rlp_uint64(hdr->timestamp));
    /* [12] extraData */
    rlp_list_append(list, rlp_string(hdr->extra_data, hdr->extra_data_len));
    /* [13] mixHash */
    rlp_list_append(list, rlp_string(hdr->mix_hash.bytes, 32));
    /* [14] nonce (8 bytes big-endian, preserved in full) */
    {
        uint8_t nonce_be[8];
        for (int i = 7; i >= 0; i--) {
            nonce_be[i] = (uint8_t)(hdr->nonce & 0xFF);
            /* shift only for i > 0 to avoid shifting a 0 value on last iter */
        }
        nonce_be[0] = (uint8_t)((hdr->nonce >> 56) & 0xFF);
        nonce_be[1] = (uint8_t)((hdr->nonce >> 48) & 0xFF);
        nonce_be[2] = (uint8_t)((hdr->nonce >> 40) & 0xFF);
        nonce_be[3] = (uint8_t)((hdr->nonce >> 32) & 0xFF);
        nonce_be[4] = (uint8_t)((hdr->nonce >> 24) & 0xFF);
        nonce_be[5] = (uint8_t)((hdr->nonce >> 16) & 0xFF);
        nonce_be[6] = (uint8_t)((hdr->nonce >>  8) & 0xFF);
        nonce_be[7] = (uint8_t)((hdr->nonce      ) & 0xFF);
        rlp_list_append(list, rlp_string(nonce_be, 8));
    }

    /* [15] baseFeePerGas (London+) */
    if (hdr->has_base_fee)
        rlp_list_append(list, rlp_uint256(&hdr->base_fee));

    /* [16] withdrawalsRoot (Shanghai+) */
    if (hdr->has_withdrawals_root)
        rlp_list_append(list, rlp_string(hdr->withdrawals_root.bytes, 32));

    /* [17-18] blobGasUsed, excessBlobGas (Cancun+) */
    if (hdr->has_blob_gas) {
        rlp_list_append(list, rlp_uint64(hdr->blob_gas_used));
        rlp_list_append(list, rlp_uint64(hdr->excess_blob_gas));
    }

    /* [19] parentBeaconBlockRoot (Cancun+) */
    if (hdr->has_parent_beacon_root)
        rlp_list_append(list, rlp_string(hdr->parent_beacon_root.bytes, 32));

    /* [20] requestsHash (Prague+, EIP-7685) */
    if (hdr->has_requests_hash)
        rlp_list_append(list, rlp_string(hdr->requests_hash.bytes, 32));

    bytes_t encoded = rlp_encode(list);
    rlp_item_free(list);
    return encoded;
}

hash_t block_header_hash(const block_header_t *hdr) {
    hash_t zero = {0};
    bytes_t encoded = block_header_encode_rlp(hdr);
    if (!encoded.data) return zero;
    hash_t h = hash_keccak256(encoded.data, encoded.len);
    free(encoded.data);
    return h;
}

/* =========================================================================
 * Transaction root computation
 * ========================================================================= */

hash_t block_compute_tx_root(const block_body_t *body) {
    hash_t root = {0};

    if (!body || body->tx_count == 0) {
        /* Empty trie root = keccak256(0x80) */
        const uint8_t empty_rlp[] = {0x80};
        hash_t empty = hash_keccak256(empty_rlp, 1);
        return empty;
    }

    size_t n = body->tx_count;
    mpt_unsecured_entry_t *entries = calloc(n, sizeof(*entries));
    if (!entries) return root;

    /* Temporary arrays to hold allocated key/value buffers */
    bytes_t *keys   = calloc(n, sizeof(bytes_t));
    bytes_t *values = calloc(n, sizeof(bytes_t));
    if (!keys || !values) goto cleanup;

    for (size_t i = 0; i < n; i++) {
        /* Key: RLP-encoded transaction index */
        keys[i] = rlp_encode_uint64_direct(i);
        if (!keys[i].data) goto cleanup;

        /* Value: RLP-encoded transaction */
        const rlp_item_t *tx = block_body_tx(body, i);
        if (!tx) goto cleanup;
        values[i] = rlp_encode(tx);
        if (!values[i].data) goto cleanup;

        entries[i].key       = keys[i].data;
        entries[i].key_len   = keys[i].len;
        entries[i].value     = values[i].data;
        entries[i].value_len = values[i].len;
    }

    mpt_compute_root_unsecured(entries, n, &root);

cleanup:
    if (keys) {
        for (size_t i = 0; i < n; i++) free(keys[i].data);
        free(keys);
    }
    if (values) {
        for (size_t i = 0; i < n; i++) free(values[i].data);
        free(values);
    }
    free(entries);
    return root;
}

/* =========================================================================
 * Withdrawals root computation (EIP-4895)
 * ========================================================================= */

hash_t block_compute_withdrawals_root(const withdrawal_t *withdrawals,
                                       size_t count) {
    hash_t root = {0};

    if (count == 0) {
        const uint8_t empty_rlp[] = {0x80};
        return hash_keccak256(empty_rlp, 1);
    }
    if (!withdrawals) return root;

    mpt_unsecured_entry_t *entries = calloc(count, sizeof(*entries));
    bytes_t *keys   = calloc(count, sizeof(bytes_t));
    bytes_t *values = calloc(count, sizeof(bytes_t));
    if (!entries || !keys || !values) goto cleanup;

    for (size_t i = 0; i < count; i++) {
        /* Key: RLP-encoded withdrawal index in list */
        keys[i] = rlp_encode_uint64_direct(i);
        if (!keys[i].data) goto cleanup;

        /* Value: RLP([index, validator_index, address, amount]) */
        rlp_item_t *list = rlp_list_new();
        if (!list) goto cleanup;
        rlp_list_append(list, rlp_uint64(withdrawals[i].index));
        rlp_list_append(list, rlp_uint64(withdrawals[i].validator_index));
        rlp_list_append(list, rlp_string(withdrawals[i].address.bytes, 20));
        rlp_list_append(list, rlp_uint64(withdrawals[i].amount_gwei));
        values[i] = rlp_encode(list);
        rlp_item_free(list);
        if (!values[i].data) goto cleanup;

        entries[i].key       = keys[i].data;
        entries[i].key_len   = keys[i].len;
        entries[i].value     = values[i].data;
        entries[i].value_len = values[i].len;
    }

    mpt_compute_root_unsecured(entries, count, &root);

cleanup:
    if (keys) {
        for (size_t i = 0; i < count; i++) free(keys[i].data);
        free(keys);
    }
    if (values) {
        for (size_t i = 0; i < count; i++) free(values[i].data);
        free(values);
    }
    free(entries);
    return root;
}
