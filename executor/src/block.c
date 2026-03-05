#include "block.h"
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
    const rlp_item_t *txs = rlp_get_list_item(body->_rlp, 0);
    if (!txs) return NULL;
    return rlp_get_list_item(txs, index);
}

void block_body_free(block_body_t *body) {
    if (body && body->_rlp) {
        rlp_item_free(body->_rlp);
        body->_rlp = NULL;
        body->tx_count = 0;
    }
}
